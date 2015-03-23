#ifndef PARALLEL_SUPERSONIC
#define PARALLEL_SUPERSONIC

#include "supersonic/supersonic.h"
#include "supersonic/utils/strings/stringpiece.h"
#include "supersonic/cursor/infrastructure/view_cursor.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/memory/memory.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/cursor/base/cursor_transformer.h"
#include "supersonic/cursor/base/operation.h"
#include "supersonic/cursor/core/aggregate.h"
#include "supersonic/cursor/core/aggregator.h"
#include "supersonic/cursor/core/spy.h"
#include "supersonic/cursor/core/sort.h"
#include "supersonic/cursor/core/benchmarks.h"
#include "supersonic/cursor/infrastructure/ordering.h"
#include "supersonic/cursor/infrastructure/row_hash_set.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/testing/block_builder.h"
#include "supersonic/testing/comparators.h"
#include "gtest/gtest.h"
#include "supersonic/utils/container_literal.h"

typedef struct ThreadTag{
    pthread_t thread_id;            //id of thread
    int number;                     //number of thread
    int count;                      //sum of threads
    supersonic::View * input_view;              //input view of all threads
    supersonic::Table * result_table;           //output result of per thread
    supersonic::row_hash_set::RowHashSet* key;              
}ThreadTag;

typedef struct FineTag{
    supersonic::TupleSchema* table_schema;
    supersonic::TupleSchema* key_schema;
    supersonic::Table * g_index;
    supersonic::TableRowAppender<supersonic::DirectRowSourceReader<supersonic::ViewRowIterator> >
        * g_index_appender;
    std::vector<size_t>* g_hash;
    int* g_last_row_id;
    int g_lock_size;
    int* g_prev_row_id;
    int g_prev_row_id_size;
    //last_row_id_ has read-write-conflict
    pthread_rwlock_t** g_last_row_id_locks;
    pthread_rwlock_t* g_chain_lock;
}Fine;
#endif
