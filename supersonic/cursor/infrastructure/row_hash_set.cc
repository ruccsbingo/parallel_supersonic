// Copyright 2010 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#include "supersonic/cursor/infrastructure/row_hash_set.h"

#include <ext/new_allocator.h>
#include <string.h>
#include <algorithm>
using std::copy;
using std::max;
using std::min;
using std::reverse;
using std::sort;
using std::swap;
#include <cstddef>
#include <string>
using std::string;
#include <vector>
using std::vector;

#include "supersonic/utils/integral_types.h"
#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/macros.h"
#include "supersonic/utils/port.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/infrastructure/bit_pointers.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/base/infrastructure/types_infrastructure.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/cursor/infrastructure/iterators.h"
#include "supersonic/cursor/infrastructure/table.h"
#include "supersonic/proto/supersonic.pb.h"

namespace supersonic {

namespace row_hash_set {

// This is row-oriented version of row_hash_set. It has depending on test case
// 20% - 200% better performance compared to row_hash_set with hashtable. It is
// optimized to work fast with huge amount of output rows. It replaced other
// row-oriented version
// The previous column-oriented code is available in older revisions of source
// files in Perforce:

#define CPP_TYPE(type) typename TypeTraits<type>::cpp_type


// Instantiates a particular specialization of ValueComparator<type>.
ValueComparatorInterface* CreateValueComparator(DataType type) {
  return TypeSpecialization<ValueComparatorInterface*, ValueComparatorFactory>(
      type);
}

// A helper function that creates a key selector that selects all attributes
// from schema.
const BoundSingleSourceProjector* CreateAllAttributeSelector(
    const TupleSchema& schema) {
  scoped_ptr<const SingleSourceProjector> projector(ProjectAllAttributes());
  return SucceedOrDie(projector->Bind(schema));
}

RowHashSetImpl::RowHashSetImpl(
    const TupleSchema& block_schema,
    BufferAllocator* const allocator,
    const BoundSingleSourceProjector* key_selector,
    bool is_multiset)
    : key_selector_(
          key_selector
              ? key_selector
              : CreateAllAttributeSelector(block_schema)),
      index_(block_schema, allocator),
      index_appender_(&index_, true),
      index_key_(key_selector_->result_schema()),
      query_key_(key_selector_->result_schema()),
      last_row_id_size_(0),
      comparator_(query_key_.schema()),
      hash_mask_(0),
      is_multiset_(is_multiset) {
  // FindUnique and FindMany compute hash index % hash_mask_, which results
  // in a non-negative integer, used to index the indirection tables.
  // To make sure that the newly created instance adheres to the implicit
  // invariants, we need to pre-allocate and fill certain dummy values.
  last_row_id_.reset(new int[1]);
  last_row_id_[0] = -1;
  prev_row_id_.reset(new int[1]);
}

bool RowHashSetImpl::ReserveRowCapacity(rowcount_t row_count) {
  if (index_.row_capacity() >= row_count) return true;
  if (!index_.ReserveRowCapacity(row_count)) return false;
  key_selector_->Project(index_.view(), &index_key_);
  comparator_.set_right_view(&index_key_);
  hash_.reserve(index_.row_capacity());
  if (is_multiset_) equal_row_ids_.resize(index_.row_capacity());

  // Arbitrary load factor of 75%. (Same is used in Java HashMap).
  while (last_row_id_size_ * 3 < index_.row_capacity() * 4) {
    if (last_row_id_size_ == 0) {
      last_row_id_size_ = 16;
    } else {
      last_row_id_size_ *= 2;
    }
  }
  last_row_id_.reset(new int[last_row_id_size_]);
  std::fill(last_row_id_.get(), last_row_id_.get() + last_row_id_size_, -1);
  hash_mask_ = last_row_id_size_ - 1;

  prev_row_id_.reset(new int[last_row_id_size_]);

  // Rebuild the linked lists that depend on hash_index (which changed, as
  // it depends on hash_mask_, that changed).
  if (is_multiset_) {
    for (rowid_t i = 0; i < equal_row_groups_.size(); ++i) {
      rowid_t first = equal_row_groups_[i].first;
      int hash_index = (hash_mask_ & hash_[first]);
      prev_row_id_[first] = last_row_id_[hash_index];
      last_row_id_[hash_index] = first;
    }
  } else {
    for (rowid_t i = 0; i < index_.view().row_count(); ++i) {
      int hash_index = (hash_mask_ & hash_[i]);
      prev_row_id_[i] = last_row_id_[hash_index];
      last_row_id_[hash_index] = i;
    }
  }
  return true;
}

void RowHashSetImpl::FindUnique(
    const View& query, const bool_const_ptr selection_vector,
    FindResult* result) const {
  FindInternal(query, selection_vector, result->mutable_row_ids());
}

void RowHashSetImpl::FindMany(
    const View& query, const bool_const_ptr selection_vector,
    FindMultiResult* result) const {
  result->set_equal_row_ids((equal_row_ids_.empty()) ? NULL :
                            &equal_row_ids_.front());
  // For each query row, the matching row (if any) is also the head of a group
  // of possibly many Rows.
  FindInternal(query, selection_vector, result->mutable_row_ids());
}

void RowHashSetImpl::FindInternal(
    const View& query, const bool_const_ptr selection_vector,
    rowid_t* result_row_ids) const {
  DCHECK(query.schema().EqualByType(key_selector_->result_schema()));
  CHECK_GE(arraysize(query_hash_), query.row_count());
  comparator_.set_left_view(&query);

  HashQuery(query, query.row_count(), query_hash_);
  ViewRowIterator iterator(query);
  while (iterator.next()) {
    const rowid_t query_row_id = iterator.current_row_index();
    rowid_t* const result_row_id = result_row_ids + query_row_id;
    if (selection_vector != NULL && !selection_vector[query_row_id]) {
      *result_row_id = kInvalidRowId;
    } else {
      int hash_index = (hash_mask_ & query_hash_[query_row_id]);
      int index_row_id = last_row_id_[hash_index];
      if (comparator_.hash_comparison_only()) {
        while (index_row_id != -1 &&
               (query_hash_[query_row_id] != hash_[index_row_id])) {
          index_row_id = prev_row_id_[index_row_id];
        }
      } else {
        while (index_row_id != -1 &&
               (query_hash_[query_row_id] != hash_[index_row_id]
                || !comparator_.Equal(query_row_id, index_row_id))) {
          index_row_id = prev_row_id_[index_row_id];
        }
      }
      *result_row_id = (index_row_id != -1) ? index_row_id : kInvalidRowId;
    }
  }
}

size_t RowHashSetImpl::InsertUnique(
    const View& query, const bool_const_ptr selection_vector,
    FindResult* result) {

    DCHECK(query.schema().EqualByType(index_.schema()))
        << "Expected: " << index_.schema().GetHumanReadableSpecification()
        << ", is: " << query.schema().GetHumanReadableSpecification();

    // Best-effort; if fails, we may end up adding less rows later.
    ReserveRowCapacity(index_.row_count() + query.row_count());
    CHECK_GE(arraysize(query_hash_), query.row_count());

    key_selector_->Project(query, &query_key_);
    HashQuery(query_key_, query.row_count(), query_hash_);
    comparator_.set_left_view(&query_key_);

    ViewRowIterator iterator(query);
    while (iterator.next()) {
        const int64 query_row_id = iterator.current_row_index();
        rowid_t* const result_row_id =
            result ? (result->mutable_row_ids() + query_row_id) : NULL;
        if (selection_vector != NULL && !selection_vector[query_row_id]) {
            if (result_row_id)
                *result_row_id = kInvalidRowId;
        } else {
            // TODO(user): The following 12 lines are identical to the piece
            // of code in FindInternal. Needs refactoring (but be careful about a
            // performance regression).
            int hash_index = (hash_mask_ & query_hash_[query_row_id]);
            int index_row_id = last_row_id_[hash_index];

            if (comparator_.hash_comparison_only()) {
                while (index_row_id != -1 &&
                        (query_hash_[query_row_id] != hash_[index_row_id])) {
                    index_row_id = prev_row_id_[index_row_id];
                }
            } else {
                while (index_row_id != -1 &&
                        (query_hash_[query_row_id] != hash_[index_row_id]
                         || !comparator_.Equal(query_row_id, index_row_id))) {
                    index_row_id = prev_row_id_[index_row_id];
                }
            }

            if (index_row_id == -1) {
                index_row_id = index_.row_count();
                if (index_row_id  == index_.row_capacity() ||
                        !index_appender_.AppendRow(iterator)) break;
                hash_.push_back(query_hash_[query_row_id]);
                prev_row_id_[index_row_id] = last_row_id_[hash_index];
                last_row_id_[hash_index] = index_row_id;
            }
            if (result_row_id)
                *result_row_id = index_row_id;
        }
    }
    return iterator.current_row_index();
}


size_t RowHashSetImpl::InsertMany(
    const View& query, const bool_const_ptr selection_vector,
    FindMultiResult* result) {
  DCHECK(query.schema().EqualByType(index_.schema()))
      << "Expected: " << index_.schema().GetHumanReadableSpecification()
      << ", is: " << query.schema().GetHumanReadableSpecification();
  // Best-effort; if fails, we may end up adding less rows later.
  ReserveRowCapacity(index_.row_count() + query.row_count());
  CHECK_GE(arraysize(query_hash_), query.row_count());

  key_selector_->Project(query, &query_key_);
  // We create the hashes for all the rows in the query, we store them in
  // query_hash_.
  HashQuery(query_key_, query.row_count(), query_hash_);
  comparator_.set_left_view(&index_key_);

  if (result)
    result->set_equal_row_ids(&equal_row_ids_.front());
  rowid_t insert_row_id = index_.row_count();
  ViewRowIterator iterator(query);
  while (iterator.next()) {
    const rowid_t query_row_id = iterator.current_row_index();
    rowid_t* const result_row_id =
        (result != NULL) ? (result->mutable_row_ids() + query_row_id) : NULL;
    if (selection_vector != NULL && !selection_vector[query_row_id]) {
      if (result_row_id)
        *result_row_id = kInvalidRowId;
    } else {
      // Copy query row into the index.
      if (query_row_id  == index_.row_capacity() ||
          !index_appender_.AppendRow(iterator)) break;
      hash_.push_back(query_hash_[query_row_id]);
      int hash_index = (hash_mask_ & query_hash_[query_row_id]);
      int index_row_id = last_row_id_[hash_index];

      // We iterate over all rows with the same hash_index (which is a
      // truncation of the hash), looking for a row with the same hash as
      // our row.
      if (comparator_.hash_comparison_only()) {
        // This is the case where hash-comparison is equivalent to a full
        // comparison.
        while (index_row_id != -1 &&
               (query_hash_[query_row_id] != hash_[index_row_id])) {
          index_row_id = prev_row_id_[index_row_id];
        }
      } else {
        while (index_row_id != -1 &&
               (query_hash_[query_row_id] != hash_[index_row_id]
                || !comparator_.Equal(insert_row_id, index_row_id))) {
          index_row_id = prev_row_id_[index_row_id];
        }
      }

      // If we found no row with an equal hash, we start a new group at the end
      // of the list of rows with the same hash_index.
      if (index_row_id == -1) {
        index_row_id = insert_row_id;
        prev_row_id_[index_row_id] = last_row_id_[hash_index];
        last_row_id_[hash_index] = index_row_id;
      }

      // Head of the linked list grouping Rows with the same key as query row.
      EqualRowIdsLink& equal_row_ids = equal_row_ids_[index_row_id];
      if (insert_row_id == index_row_id) {
        // First Row with such key in the index; create a single-element list.
        equal_row_ids.group_id = equal_row_groups_.size();
        equal_row_ids.next = kInvalidRowId;
        equal_row_groups_.push_back(EqualRowGroup());
        EqualRowGroup& equal_row_group = equal_row_groups_.back();
        equal_row_group.first = insert_row_id;
        equal_row_group.last = insert_row_id;
      } else {
        // Another Row with the same key; append to the linked list.
        EqualRowGroup& equal_row_group =
            equal_row_groups_[equal_row_ids.group_id];
        equal_row_ids_[equal_row_group.last].next = insert_row_id;
        equal_row_group.last = insert_row_id;
        equal_row_ids_[insert_row_id].group_id = equal_row_ids.group_id;
        equal_row_ids_[insert_row_id].next = kInvalidRowId;
      }

      if (result_row_id)
        *result_row_id = index_row_id;

      insert_row_id++;
    }
  }
  return iterator.current_row_index();
}

void RowHashSetImpl::Clear() {
  // Be more aggresive in freeing memory. Otherwise clients like
  // BestEffortGroupAggregate may end up with memory_limit->Available() == 0
  // after clearing RowHashSet.
  delete index_.extract_block();
  key_selector_->Project(index_.view(), &index_key_);
  hash_.clear();
  std::fill(last_row_id_.get(), last_row_id_.get() + last_row_id_size_, -1);
  equal_row_groups_.clear();
}

// TODO(user): More internal datastructures could be compacted (last_row_id_ and
// prev_row_id_), but it would require recomputing their content.
void RowHashSetImpl::Compact() {
  index_.Compact();
  // Using the swap trick to trim excess vector capacity.
  vector<size_t>(hash_).swap(hash_);
  vector<EqualRowGroup>(equal_row_groups_).swap(equal_row_groups_);
}

void RowHashSetImpl::HashQuery(
    const View& key_columns, rowcount_t row_count, size_t* hash) {
  const TupleSchema& key_schema = key_columns.schema();
  // TODO(user): this should go into the constructor.
  // If somebody uses hash-join to do a cross-join (that is no columns are
  // given as key) we have to fill out the hash table with any single hash,
  // say zero.
  if (key_schema.attribute_count() == 0) {
    memset(hash, '\0', row_count * sizeof(*hash));
  }
  // In the other case the first ColumnHasher will initialize the data for
  // us.
  for (int c = 0; c < key_schema.attribute_count(); ++c) {
    ColumnHasher column_hasher =
        GetColumnHasher(key_schema.attribute(c).type(), c != 0, false);
    const Column& key_column = key_columns.column(c);
    column_hasher(key_column.data(), key_column.is_null(), row_count, hash);
  }
}

//index_ has fixed capacity
//hash_ has fixed capacity
//last_row_id_size_ has fixed size
//last_row_id has fixed size
//hash_mask has fixed size
//prev_row_id_ needs grown
bool FineSafeRowHashSetImpl::ReserveRowCapacity(rowcount_t block_capacity){

  return true;
}

// RowSet variant.
size_t FineSafeRowHashSetImpl::InsertUnique(
        const View& query, const bool_const_ptr selection_vector,
        FindResult* result){

    static int hash_counter = 0;

    key_selector_->Project(index_->view(), &index_key_);
    key_selector_->Project(query, &query_key_);
    HashQuery(query_key_, query.row_count(), query_hash_);

    ViewRowIterator iterator(query);
    while (iterator.next()) {
        const int64 query_row_id = iterator.current_row_index();
        rowid_t* const result_row_id =
            result ? (result->mutable_row_ids() + query_row_id) : NULL;

        // TODO(user): The following 12 lines are identical to the piece
        // of code in FindInternal. Needs refactoring (but be careful about a
        // performance regression).
        int hash_index = (hash_mask_ & query_hash_[query_row_id]);

        //read last_row_id_
        pthread_rwlock_wrlock(last_row_id_locks_[hash_index]);
        int index_row_id = last_row_id_[hash_index];

        while (index_row_id != -1 &&
                //read hash_
                (query_hash_[query_row_id] != (*hash_)[index_row_id]
                 || query_key_.column(0).typed_data<INT32>()[query_row_id] 
                    != index_->view().column(0).typed_data<INT32>()[index_row_id])) {
            //read prev_row_id_
            index_row_id = prev_row_id_[index_row_id];
        }

        if (index_row_id == -1) {
            //we need a chain lock
            pthread_rwlock_wrlock(chain_lock_);

            //read index_
            index_row_id = index_->row_count();
            index_appender_->AppendRow(iterator);

            //write hash_
            hash_->push_back(query_hash_[query_row_id]);

            //read last_row_id_[hash_index] and write prev_row_id_
            prev_row_id_[index_row_id] = last_row_id_[hash_index];

            //write last_row_id_ 
            last_row_id_[hash_index] = index_row_id;

            pthread_rwlock_unlock(chain_lock_);
        }
        if (result_row_id)
            *result_row_id = index_row_id;

        pthread_rwlock_unlock(last_row_id_locks_[hash_index]);
    }

    return iterator.current_row_index();
}

size_t RoughSafeRowHashSetImpl::InsertUnique(
    const View& query, const bool_const_ptr selection_vector,
    FindResult* result) {

    int status;
    status = pthread_rwlock_wrlock(&rough_lock_);
    if(status != 0){
        std::cout<<status<<"write lock failure"<<std::endl;
    }


    DCHECK(query.schema().EqualByType(index_.schema()))
        << "Expected: " << index_.schema().GetHumanReadableSpecification()
        << ", is: " << query.schema().GetHumanReadableSpecification();

    // Best-effort; if fails, we may end up adding less rows later.
    ReserveRowCapacity(index_.row_count() + query.row_count());
    CHECK_GE(arraysize(query_hash_), query.row_count());

    key_selector_->Project(query, &query_key_);
    HashQuery(query_key_, query.row_count(), query_hash_);
    comparator_.set_left_view(&query_key_);

    ViewRowIterator iterator(query);
    while (iterator.next()) {
        const int64 query_row_id = iterator.current_row_index();
        rowid_t* const result_row_id =
            result ? (result->mutable_row_ids() + query_row_id) : NULL;
        if (selection_vector != NULL && !selection_vector[query_row_id]) {
            if (result_row_id)
                *result_row_id = kInvalidRowId;
        } else {
            // TODO(user): The following 12 lines are identical to the piece
            // of code in FindInternal. Needs refactoring (but be careful about a
            // performance regression).
            int hash_index = (hash_mask_ & query_hash_[query_row_id]);
            int index_row_id = last_row_id_[hash_index];

            if (comparator_.hash_comparison_only()) {
                while (index_row_id != -1 &&
                        (query_hash_[query_row_id] != hash_[index_row_id])) {
                    index_row_id = prev_row_id_[index_row_id];
                }
            } else {
                while (index_row_id != -1 &&
                        (query_hash_[query_row_id] != hash_[index_row_id]
                         || !comparator_.Equal(query_row_id, index_row_id))) {
                    index_row_id = prev_row_id_[index_row_id];
                }
            }

            if (index_row_id == -1) {
                index_row_id = index_.row_count();
                if (index_row_id  == index_.row_capacity() ||
                        !index_appender_.AppendRow(iterator)) break;
                hash_.push_back(query_hash_[query_row_id]);
                prev_row_id_[index_row_id] = last_row_id_[hash_index];
                last_row_id_[hash_index] = index_row_id;
            }
            if (result_row_id)
                *result_row_id = index_row_id;
        }
    }

    status = pthread_rwlock_unlock(&rough_lock_);
    if(status != 0){
        std::cout<<status<<"write unlock failure"<<std::endl;
    }

    return iterator.current_row_index();
}

void RowIdSetIterator::Next() {
  current_ = equal_row_ids_[current_].next;
}

RowHashSet::RowHashSet(const TupleSchema& block_schema,
                       BufferAllocator* const allocator)
    : impl_(new RowHashSetImpl(block_schema, allocator, NULL, false)) {}

RowHashSet::RowHashSet(
    const TupleSchema& block_schema,
    BufferAllocator* const allocator,
    const BoundSingleSourceProjector* key_selector)
    : impl_(new RowHashSetImpl(block_schema, allocator, key_selector, false)) {}

    RowHashSet::RowHashSet(
            const TupleSchema& table_schema,
            const TupleSchema& key_schema,
            BufferAllocator* const allocator,
            Table * g_index,
            TableRowAppender<DirectRowSourceReader<ViewRowIterator> >* g_index_appender,
            vector<size_t>* g_hash,
            int* g_last_row_id,
            int g_lock_size,
            int* g_prev_row_id,
            int g_prev_row_id_size, 
            pthread_rwlock_t** g_last_row_id_locks,
            pthread_rwlock_t* g_chain_lock){

        impl_ = new FineSafeRowHashSetImpl(
                    table_schema, 
                    key_schema, 
                    allocator, 
                    g_index,
                    g_index_appender,
                    g_hash,
                    g_last_row_id,
                    g_lock_size,
                    g_prev_row_id,
                    g_prev_row_id_size,
                    g_last_row_id_locks,
                    g_chain_lock);
    }

RowHashSet::~RowHashSet() {
    //To do: This pointer will be delete several times in multi-threads environment,
    //so we must lock it
    //if(NULL != impl_) delete impl_;
}

bool RowHashSet::ReserveRowCapacity(rowcount_t capacity) {
  return impl_->ReserveRowCapacity(capacity);
}

void RowHashSet::Find(const View& query, FindResult* result) const {
  impl_->FindUnique(query, bool_ptr(NULL), result);
}

void RowHashSet::Find(const View& query, const bool_const_ptr selection_vector,
                  FindResult* result) const {
  impl_->FindUnique(query, selection_vector, result);
}

size_t RowHashSet::Insert(const View& query, FindResult* result) {
  return impl_->InsertUnique(query, bool_ptr(NULL), result);
}

size_t RowHashSet::Insert(const View& query,
                          const bool_const_ptr selection_vector,
                          FindResult* result) {
  return impl_->InsertUnique(query, selection_vector, result);
}

size_t RowHashSet::Insert(const View& query) {
  return impl_->InsertUnique(query, bool_ptr(NULL), NULL);
}

size_t RowHashSet::Insert(const View& query,
                          const bool_const_ptr selection_vector) {
  return impl_->InsertUnique(query, selection_vector, NULL);
}

void RowHashSet::Clear() {
  return impl_->Clear();
}

void RowHashSet::Compact() {
  return impl_->Compact();
}

const View& RowHashSet::indexed_view() const { return impl_->indexed_view(); }

rowcount_t RowHashSet::size() const { return indexed_view().row_count(); }

RowHashMultiSet::RowHashMultiSet(const TupleSchema& block_schema,
                                 BufferAllocator* const allocator)
    : impl_(new RowHashSetImpl(block_schema, allocator, NULL, true)) {}

RowHashMultiSet::RowHashMultiSet(
    const TupleSchema& block_schema,
    BufferAllocator* const allocator,
    const BoundSingleSourceProjector* key_selector) :
    impl_(new RowHashSetImpl(block_schema, allocator, key_selector, true)) {}

RowHashMultiSet::~RowHashMultiSet() {
  delete impl_;
}

bool RowHashMultiSet::ReserveRowCapacity(rowcount_t capacity) {
  return impl_->ReserveRowCapacity(capacity);
}

void RowHashMultiSet::Find(const View& query, FindMultiResult* result) const {
  impl_->FindMany(query, bool_ptr(NULL), result);
}

void RowHashMultiSet::Find(
    const View& query, const bool_const_ptr selection_vector,
    FindMultiResult* result) const {
  impl_->FindMany(query, selection_vector, result);
}

size_t RowHashMultiSet::Insert(const View& query, FindMultiResult* result) {
  return impl_->InsertMany(query, bool_ptr(NULL), result);
}

size_t RowHashMultiSet::Insert(
    const View& query, const bool_const_ptr selection_vector,
    FindMultiResult* result) {
  return impl_->InsertMany(query, selection_vector, result);
}

size_t RowHashMultiSet::Insert(const View& query) {
  return impl_->InsertMany(query, bool_ptr(NULL), NULL);
}

size_t RowHashMultiSet::Insert(
    const View& query, const bool_const_ptr selection_vector) {
  return impl_->InsertMany(query, selection_vector, NULL);
}

void RowHashMultiSet::Clear() {
  return impl_->Clear();
}

void RowHashMultiSet::Compact() {
  return impl_->Compact();
}

const View& RowHashMultiSet::indexed_view() const {
  return impl_->indexed_view();
}

rowcount_t RowHashMultiSet::size() const { return indexed_view().row_count(); }

#undef CPP_TYPE


}  // namespace row_hash_set

}  // namespace supersonic
