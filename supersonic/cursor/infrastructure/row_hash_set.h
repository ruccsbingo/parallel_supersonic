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

#ifndef SUPERSONIC_CURSOR_INFRASTRUCTURE_ROW_HASH_SET_H_
#define SUPERSONIC_CURSOR_INFRASTRUCTURE_ROW_HASH_SET_H_

#include <stddef.h>
#include <iostream>

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/base/infrastructure/bit_pointers.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/cursor/infrastructure/table.h"

#include "huawei/pthreads/rwlock.h"

namespace supersonic {

class BoundSingleSourceProjector;
class BufferAllocator;
class TupleSchema;
class View;

namespace row_hash_set {

static const rowid_t kInvalidRowId = -1;

class FindMultiResult;
class FindResult;
class RowHashSetImpl;

// A row container that stores rows in an internal storage block (defined by
// a schema passed in constructor) and groups rows with identical keys together.
// Allows efficient key-based row lookups and insertions with respectively Find
// and Insert operations that work on blocks (technically Views) of rows at
// a time.
//
// A logical atomic piece of data in the set is a row. Rows must have at least
// one key column and can have any number of value columns. Row equality is
// established using only key columns. The storage block and insert query views
// should have the same schema. Find queries should only include key columns.
//
// RowHashSet can hold at most one row with a given key. Attempts to insert
// subsequent rows with the same key will result in row id of the existing row
// being returned.
//
// RowHashMultiSet can hold any number of rows with a given key. New rows are
// always inserted. For each row passed to Insert the returned row id is
// different.
//
// At most Cursor::kDefaultRowCount rows can be passed in a single call to
// Find / Insert.
class RowHashSet {
 public:
  typedef FindResult ResultType;

  ~RowHashSet();

  RowHashSet(){}

  // All columns are key.
  explicit RowHashSet(const TupleSchema& block_schema,
                      BufferAllocator* const allocator);

  // Takes ownership of a SingleSourceProjector that indicates the subset of
  // internal block's columns that are key columns. It must be bound to block's
  // schema.
  RowHashSet(const TupleSchema& block_schema,
             BufferAllocator* const allocator,
             const BoundSingleSourceProjector* key_selector);

  // Ensures that the hash set has capacity for at least the specified
  // number of rows. Returns false on OOM. Using this method may reduce
  // the number of reallocations (normally, the row capacity starts at 16
  // and grows by a factor of two every time the hash table needs to grow).
  bool ReserveRowCapacity(rowcount_t row_capacity);

  // Writes into result a vector such that the n-th element is the result of
  // a lookup of the n-th query row. Each result is an id of a row in the block
  // or kInvalid if key not found.
  void Find(const View& query, FindResult* result) const;

  // A variant of Find that takes an additional selection vector; it must have
  // query.row_count elements. If selection_vector[i] == false then query row
  // at index i is not processed at all. Its corresponding result entry is set
  // to kInvalidRowId.
  void Find(const View& query,
            const bool_const_ptr selection_vector,
            FindResult* result) const;

  // Writes into result a vector such that the n-th element is the result of
  // an insertion of the n-th query row. For each query row the insertion
  // happens only if the row's key is not NULL and is not already present in
  // the block. Returns the number of rows successfully inserted. Can be
  // lower than query.row_count() if data being inserted is of variable-
  // length type and the storage block's arena can't accomodate a copy of
  // a variable-length data buffer.
  size_t Insert(const View& query, FindResult* result);

  // A variant of Insert that takes an additional selection vector; it must have
  // query.row_count elements. If selection_vector[i] == false then query row
  // at index i is not processed at all. Its corresponding result entry is set
  // to kInvalidRowId.
  size_t Insert(const View& query, const bool_const_ptr selection_vector,
                FindResult* result);

  // A variant of Insert that doesn't return the result vector (insertions still
  // take place).
  size_t Insert(const View& query);

  // Same as above, with selection vector.
  size_t Insert(const View& query, const bool_const_ptr selection_vector);

  // Clears set to contain 0 rows.
  void Clear();

  // Compacts internal datastructures to minimize memory usage.
  void Compact();

  // The read-only content.
  const View& indexed_view() const;

  // Number of rows successfully inserted so far.
  rowcount_t size() const;

  RowHashSetImpl * Get(){
      return impl_;
  }

  void Set(RowHashSetImpl* impl){
      impl_ = impl;
  }


 private:
  RowHashSetImpl* impl_;
};

// For multiset. See equal_row_groups_.
struct EqualRowGroup {
  rowid_t first;
  rowid_t last;
};

// See equal_row_ids_.
struct EqualRowIdsLink {
  rowid_t group_id;
  rowid_t next;
};

// Abstract interface of classes that compare particular values (table cells)
// in corresponding columns for equality.
struct ValueComparatorInterface {
  virtual ~ValueComparatorInterface() {}
  virtual bool Equal(rowid_t row_id_a, rowid_t row_id_b) const = 0;
  virtual void set_left_column(const Column* left_column) = 0;
  virtual void set_right_column(const Column* right_column) = 0;
  virtual bool non_colliding_hash_type() = 0;
  virtual bool any_column_nullable() = 0;
};

// A concrete implementation for an arbitrary data type. Columns have to be set
// before Equal call.
template <DataType type>
class ValueComparator : public ValueComparatorInterface {
 public:
  bool Equal(rowid_t row_id_a, rowid_t row_id_b) const {
    if (any_column_nullable_) {
      const bool is_null_a = (left_column_->is_null() != NULL) &&
          left_column_->is_null()[row_id_a];
      const bool is_null_b = (right_column_->is_null() != NULL) &&
          right_column_->is_null()[row_id_b];
      if (is_null_a || is_null_b) {
        return is_null_a == is_null_b;
      }
    }
    return comparator_((left_column_->typed_data<type>() + row_id_a),
                       (right_column_->typed_data<type>() + row_id_b));
  }

  void set_left_column(const Column* left_column) {
    left_column_ = left_column;
    update_any_column_nullable();
  }

  void set_right_column(const Column* right_column) {
    right_column_ = right_column;
    update_any_column_nullable();
  }

  bool non_colliding_hash_type() {
    return type == INT64 || type == INT32 || type == UINT64 || type == UINT32
        || type == BOOL;
  }

  bool any_column_nullable() {
    return any_column_nullable_;
  }

 private:
  void update_any_column_nullable() {
    if (left_column_ != NULL && right_column_ != NULL) {
      any_column_nullable_ = (left_column_->is_null() != NULL ||
                              right_column_->is_null() != NULL);
    }
  }
  EqualityWithNullsComparator<type, type, false, false> comparator_;
  bool any_column_nullable_;
  const Column* left_column_;
  const Column* right_column_;
};

// Helper struct used by CreateValueComparator.
struct ValueComparatorFactory {
  template <DataType type>
  ValueComparatorInterface* operator()() const {
    return new ValueComparator<type>();
  }
};

// Instantiates a particular specialization of ValueComparator<type>.
ValueComparatorInterface* CreateValueComparator(DataType type);

// A compound row equality comparator, implemented with a vector of individual
// value comparators. Views have to be set befor equal call.
class RowComparator {
 public:
  explicit RowComparator(const TupleSchema& key_schema) :
    left_view_(NULL),
    right_view_(NULL) {
    for (int i = 0; i < key_schema.attribute_count(); i++) {
      comparators_.push_back(
          CreateValueComparator(key_schema.attribute(i).type()));
    }
    one_column_with_non_colliding_hash_ =
        (comparators_.size() == 1 &&
         comparators_[0]->non_colliding_hash_type());
  }

  ~RowComparator() {
    //To do: This pointer will be delete several times in multi-threads environment,
    //so we must lock it
    
    //for (int i = 0; i < comparators_.size(); i++) 
    //    if(NULL != comparators_[i])
    //        delete comparators_[i];
  }

  // Function equal assumes that hashes of two rows are equal. It performs
  // comparison on every key-column of two given rows.
  bool Equal(int left_pos, int right_pos) const {
    for (int i = 0; i < comparators_.size(); i++) {
      if (!PREDICT_TRUE(comparators_[i]->Equal(left_pos, right_pos)))
        return false;
    }
    return true;
  }

  void set_left_view(const View* left_view) {
    left_view_ = left_view;
    for (int i = 0; i < comparators_.size(); ++i) {
      comparators_[i]->set_left_column(&left_view_->column(i));
    }
    hash_comparison_only_ = one_column_with_non_colliding_hash_ &&
        !comparators_[0]->any_column_nullable();
  }

  void set_right_view(const View* right_view) {
    right_view_ = right_view;
    for (int i = 0; i < comparators_.size(); ++i) {
      comparators_[i]->set_right_column(&right_view_->column(i));
    }
    hash_comparison_only_ = one_column_with_non_colliding_hash_ &&
        !comparators_[0]->any_column_nullable();
  }

  // It is true iff there is only one key-column, which has non-colliding hash
  // function. In such case it is enough to compare hashes and not to compare
  // key-values.
  bool hash_comparison_only() {
    return hash_comparison_only_;
  }

 private:
  vector<ValueComparatorInterface*> comparators_;
  // Pointers to compared views. RowComparator doesn't take its ownership.
  const View* left_view_;
  const View* right_view_;
  bool one_column_with_non_colliding_hash_;
  bool hash_comparison_only_;
};

// The actual row hash set implementation.
// TODO(user): replace vectors and scoped_arrays with Tables, to close the
// loop on memory management.
class RowHashSetImpl {
 public:
  RowHashSetImpl(
      const TupleSchema& block_schema,
      BufferAllocator* const allocator,
      const BoundSingleSourceProjector* key_selector,
      bool is_multiset);

  bool ReserveRowCapacity(rowcount_t block_capacity);

  // RowSet variant.
  void FindUnique(
      const View& query, const bool_const_ptr selection_vector,
      FindResult* result) const;

  // RowMultiSet variant.
  void FindMany(
      const View& query, const bool_const_ptr selection_vector,
      FindMultiResult* result) const;

  // RowSet variant.
  virtual size_t InsertUnique(
      const View& query, const bool_const_ptr selection_vector,
      FindResult* result);

  // RowMultiSet variant.
  size_t InsertMany(
      const View& query, const bool_const_ptr selection_vector,
      FindMultiResult* result);

  void Clear();

  void Compact();

  const View& indexed_view() const { return index_.view(); }

 //private:
 protected:
  void FindInternal(
      const View& query, const bool_const_ptr selection_vector,
      rowid_t* result_row_ids) const;

  // Computes the column of hashes for the 'key' view, given the row count.
  // TODO(user): perhaps make the row_count part of the view?
  static void HashQuery(const View& key, rowcount_t row_count, size_t* hash);

  // Selects key columns from index_ and from queries to Insert.
  scoped_ptr<const BoundSingleSourceProjector> key_selector_;

  // Contains all inserted rows; Find and Insert match against these rows
  // using last_row_id_ and prev_row_id_.
  Table index_;

  TableRowAppender<DirectRowSourceReader<ViewRowIterator> > index_appender_;

  // View over the index's key columns. Contains keys of all the rows inserted
  // into the index.
  View index_key_;

  // A fixed-size vector of links used to group all Rows with the same key into
  // a linked list. Used only by FindMany / InsertMany which implement
  // RowHashMultiSet functionality. In a group of Rows with identical key, only
  // the first one is inserted in the hashtable; all other Rows are linked from
  // the first one using the links below.
  vector<EqualRowIdsLink> equal_row_ids_;

  // Identifies groups of rows with the same key. Within the group, the items
  // form a linked list described by equal_row_ids_.
  vector<EqualRowGroup> equal_row_groups_;

  // View over key columns of a query to insert.
  View query_key_;

  //  Array for keeping block rows' hashes.
  vector<size_t> hash_;

  // Size of last_row_id_. Should be power of 2. Adjusted by ReserveRowCapacity.
  int last_row_id_size_;

  // Value of last_row_id_[hash_index] denotes position of last row in
  // indexed_block_ having same value of hash_index =
  // (query_hash_[query_row_id] & hash_mask_).
  scoped_array<int> last_row_id_;

  // Index of a previous row having the same hash_index = (row.hash &
  // hash_mask_) but a different key. (For multisets, there may be many rows
  // with the same key (and thus hash); only the first instance is actually
  // written to this array).
  scoped_array<int> prev_row_id_;

  // Structure used for comparing rows.
  mutable RowComparator comparator_;

  // Placeholder for hash values calculated in one go over entire query to
  // find/insert.
  mutable size_t query_hash_[Cursor::kDefaultRowCount];

  // Bit mask used for calculating last_row_id_ index.
  int hash_mask_;

  const bool is_multiset_;

  friend class RowIdSetIterator;
};

class RoughSafeRowHashSetImpl:public RowHashSetImpl{
 public:
     RoughSafeRowHashSetImpl(TupleSchema &schema, BufferAllocator* allocator)
         : RowHashSetImpl(schema, allocator, NULL, false) {
             //To do: init its base class RowHashSetImpl
             int status = rwlock_init(&rough_lock_);
             if(status != 0){
                 std::cout<<status<<"init lock failure"<<std::endl;
             }
         }

  // RowSet variant.
  size_t InsertUnique(
      const View& query, const bool_const_ptr selection_vector,
      FindResult* result);

 private:
  rwlock_t rough_lock_;
};

class RowHashMultiSet {
 public:
  typedef FindMultiResult ResultType;

  ~RowHashMultiSet();

  // All columns are key.
  explicit RowHashMultiSet(const TupleSchema& block_schema,
                           BufferAllocator* const allocator);

  // Takes ownership of a SingleSourceProjector that indicates the subset of
  // internal block's columns that are key columns. It must be bound to block's
  // schema.
  RowHashMultiSet(const TupleSchema& block_schema,
                  BufferAllocator* const allocator,
                  const BoundSingleSourceProjector* key_selector);

  // Ensures that the hash set has capacity for at least the specified
  // number of rows. Returns false on OOM. Using this method may reduce
  // the number of reallocations (normally, the row capacity starts at 16
  // and grows by a factor of two every time the hash table needs to grow).
  bool ReserveRowCapacity(rowcount_t capacity);

  // Writes into result a vector such that the n-th element is the result of
  // a lookup of the n-th query row. Each result is a set of ids of rows in the
  // block (empty if key not found).
  void Find(const View& query, FindMultiResult* result) const;

  // A variant of Find that takes an additional selection vector; it must have
  // query.row_count elements. If selection_vector[i] == false then query row
  // at index i is not processed at all. Its corresponding result entry is set
  // to kInvalidRowId.
  void Find(const View& query, const bool_const_ptr selection_vector,
            FindMultiResult* result) const;

  // Writes into result a vector such that the n-th element is the result of
  // an insertion of the n-th query row. The insertion happens for each query
  // row with non-NULL key. Returns the number of rows successfully inserted.
  // Can be lower than query.row_count() if data being inserted is of
  // variable-length type and the storage block's arena can't accomodate a copy
  // of a variable-length data buffer.
  size_t Insert(const View& query, FindMultiResult* result);

  // A variant of Insert that takes an additional selection vector; it must have
  // query.row_count elements. If selection_vector[i] == false then query row
  // at index i is not processed at all. Its corresponding result entry is set
  // to kInvalidRowId.
  size_t Insert(const View& query, const bool_const_ptr selection_vector,
                FindMultiResult* result);

  // A variant of Insert that doesn't return the result vector (insertions still
  // take place).
  size_t Insert(const View& query);

  // Same as above, with selection vector.
  size_t Insert(const View& query, const bool_const_ptr selection_vector);

  // Clears multiset to contain 0 rows.
  void Clear();

  // Compacts internal datastructures to minimize memory usage.
  void Compact();

  // The read-only content.
  const View& indexed_view() const;

  // Number of rows in the storage block (successfully inserted so far).
  rowcount_t size() const;

  // Total number of rows this hash set can hold.
  rowcount_t capacity() const;

 private:
  RowHashSetImpl* impl_;
};


// Result type of RowHashSet::Find and RowHashSet::Insert. A vector of row ids,
// one for each row in the original query, of the resulting inserted row or of a
// row with identical key already present in the storage block. In case of Find,
// the special RowId value kInvalid denotes absence of a matching row.
class FindResult {
 public:
  // FindResult should have as big capacity as is the size of the corresponding
  // query.
  explicit FindResult(rowcount_t query_row_count) :
      row_ids_(new rowid_t[query_row_count]),
      row_ids_size_(query_row_count) { }

  rowid_t Result(rowid_t query_id) const {
    DCHECK_GT(row_ids_size_, 0);
    return row_ids_[query_id];
  }

  // Exposes the entire array of Results.
  const rowid_t* row_ids() { return row_ids_.get(); }

 private:
  rowid_t* mutable_row_ids() { return row_ids_.get(); }

  scoped_array<rowid_t> row_ids_;
  rowcount_t row_ids_size_;

  friend class RowHashSetImpl;
  friend class RoughSafeRowHashSetImpl;
};


struct EqualRowIdsLink;

// An iterator over a set of ids of equivalent rows (with identical keys). For
// use with RowHashMultiSet where the return value for a single query row is
// a set of row ids.
class RowIdSetIterator {
 public:
  // Public constructor; the iterator starts AtEnd().
  RowIdSetIterator() : equal_row_ids_(NULL), current_(kInvalidRowId) { }

  bool AtEnd() const { return current_ == kInvalidRowId; }
  rowid_t Get() const {
    DCHECK(!AtEnd());
    return current_;
  }
  void Next();

 private:
  RowIdSetIterator(const EqualRowIdsLink* equal_row_ids, rowid_t first)
      : equal_row_ids_(equal_row_ids),
        current_(first) {}

  const EqualRowIdsLink* equal_row_ids_;
  rowid_t current_;

  friend class FindMultiResult;
};


// Result type of RowHashMultiSet::Find and RowHashMultiSet::Insert. A vector
// of sets of row ids, one for each row in the original query.
class FindMultiResult {
 public:
  // FindMultiResult should have as big capacity as is the size of
  // the corresponding query.
  explicit FindMultiResult(rowcount_t query_row_count)
      : query_row_count_(query_row_count),
        row_ids_(new rowid_t[query_row_count]),
        equal_row_ids_(NULL) {}

  RowIdSetIterator Result(rowid_t query_id) const {
    return RowIdSetIterator(equal_row_ids_, row_ids_[query_id]);
  }

 private:
  rowid_t* mutable_row_ids() { return row_ids_.get(); }

  void set_equal_row_ids(const EqualRowIdsLink* equal_row_ids) {
    equal_row_ids_ = equal_row_ids;
  }

  rowcount_t query_row_count_;
  scoped_array<rowid_t> row_ids_;
  const EqualRowIdsLink* equal_row_ids_;

  friend class RowHashSetImpl;
};

}  // namespace row_hash_set

}  // namespace supersonic

#endif  // SUPERSONIC_CURSOR_INFRASTRUCTURE_ROW_HASH_SET_H_
