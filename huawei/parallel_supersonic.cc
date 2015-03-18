#include <stddef.h>
#include <stdio.h>
#include <iostream>
#include <pthread.h>

#include <limits>
using std::numeric_limits;

#include "huawei/pthreads/barrier.h"
#include "huawei/parallel_supersonic.h"

using namespace supersonic;


//function declare start
int execute_selected_algorithm(uint64_t data, int threads_num);

int single_thread(uint64_t data);

int parallel_local_hashtable_aggregate(
        View ** views, 
        Table ** result_tables, 
        int threads_num);

int parallel_global_hashtable_aggregate(
        View ** views, 
        Table ** result_tables, 
        int threads_num);

void * thread_aggregate(void * args);

FailureOrOwned<Cursor> create_execute_plan(const View * input_view);

Table ** create_output_tables(int threads_num);

Table * create_table();

void print_view(int number, const View * view);

void load_data(uint64_t data, Table * table);

int split_views(Table * input_table, int threads_num, View ** views);
//function declare end


//global variant start 
barrier_t barrier;

int g_thread_id = -1;

//0:single thread
//1:parallel local hashtable 
//2:parallel global hashtable
int g_algorithm_id = 0;

row_hash_set::RowHashSet g_key_row_set;

ThreadTag ** threads;
//global variant end

int main(int argc, char *argv[]){

    int num = 2;
    uint64_t data = 1024;

    int i=1;

    if(argc>1){

        while(i<argc-1){
            switch(argv[i][0]){
                case 'n':                   // the number of threads
                    num = atoi(argv[i+1]);
                    break;
                case 't':                   // the selected number of thread
                    g_thread_id = atoi(argv[i+1]);
                    break;
                case 'a':                   // the selected algorithm
                    g_algorithm_id = atoi(argv[i+1]);
                    break;
                case 'd':
                    data = atoi(argv[i+1]);
                    break;
            }
            i+=2;
        }
    }

    //execute selected algorithm
    switch(g_algorithm_id){
        case 0:
            //single thread start
            std::cout<<"single thread start"<<std::endl;
            single_thread(data);
            break;
        case 1:
        case 2:
            execute_selected_algorithm(data, num);
            break;
    }

    return 0;
}

int single_thread(uint64_t data){

    Table * input_table = create_table();
    load_data(data, input_table);

    scoped_ptr<Cursor> result_cursor;
    result_cursor.reset(SucceedOrDie(create_execute_plan(&input_table->view())));

    ResultView result(result_cursor->Next(-1));

    //print final aggregate result
    print_view(-1, &result.view());

    return 0;
}

void * thread_aggregate(void * args){

    ThreadTag * thread = (ThreadTag *) args;

    View * view = thread->input_view;

    //print original data in the table
    //print_view(thread->number, view);

    scoped_ptr<Cursor> result_cursor;
    result_cursor.reset(SucceedOrDie(create_execute_plan(view)));

    //To do: need a barrier
    barrier_wait(&barrier);

    ResultView result(result_cursor->Next(-1));

    //to do (user): maybe we do not need to append the local result to 
    //a new table
    Table * table = thread->result_table;

    table->AppendView(result.view());

    //print local aggregate result
    //print_view(thread->number, &result.view());
    
    barrier_wait(&barrier);

    return NULL;
}

int execute_selected_algorithm(uint64_t data, int threads_num){

    int i, status;

    Table * input_table = create_table();
    load_data(data, input_table);

    //print total table
    //print_view(-1, &input_table->view());

    //allocate memory for local result table
    Table ** result_tables = create_output_tables(threads_num);

    //split views for each thread
    View ** views = (View **)malloc(sizeof(View *) * threads_num);
    split_views(input_table, threads_num, views);

    //allocate memory for data structure of each thread
    threads = (ThreadTag **) malloc(sizeof(ThreadTag *) * threads_num);
    memset(threads, 0, sizeof(ThreadTag *) * threads_num);
    for(i=0; i<threads_num; ++i){
        threads[i] = (ThreadTag *) malloc(sizeof(ThreadTag));
        memset(threads[i], 0, sizeof(ThreadTag));
    }

    barrier_init(&barrier, threads_num);

    switch(g_algorithm_id){
        case 1:
            //parallel local hashtable start
            std::cout<<"parallel local hashtable start"<<endl;
            parallel_local_hashtable_aggregate(views, result_tables, threads_num);
            break;
        case 2:
            //parallel global hashtable start
            std::cout<<"parallel global hashtable start"<<endl;
            parallel_global_hashtable_aggregate(views, result_tables, threads_num);
            break;
    }

    Table * final_table = create_table();
    for(i=0; i<threads_num; ++i){

        status = pthread_join(threads[i]->thread_id, NULL);

        if(status != 0){
            printf("%d %s\n", status, "Join thread failed");
        }

        //append local result table into a table
        final_table->AppendView(threads[i]->result_table->view());
    }

    // do finial aggregate
    // we need one thread to complete final aggregate, 
    // so we must set g_algorithm_id = 0
    g_algorithm_id = 0;
    
    scoped_ptr<Cursor> result_cursor;
    result_cursor.reset(SucceedOrDie(create_execute_plan(&final_table->view())));
    ResultView result(result_cursor->Next(-1));

    //print final aggregate result
    print_view(-1, &result.view());

    barrier_destroy(&barrier);

    return 0;

}


int parallel_local_hashtable_aggregate(
        View ** views, 
        Table ** result_tables, 
        int threads_num){

    int i, status;

    for(i=0; i<threads_num; ++i){

        threads[i]->number = i;

        threads[i]->count = threads_num;

        threads[i]->input_view = views[i];

        threads[i]->result_table = result_tables[i];

        //threads[i]->key = NULL;

        status = pthread_create(
                &threads[i]->thread_id, 
                0, 
                thread_aggregate, 
                threads[i]);

        if(status != 0){
            printf("%d %s\n", status, "create thread failure");
            exit(-1);
        }
    }

    return 0;
}

int parallel_global_hashtable_aggregate(
        View ** views, 
        Table ** result_tables, 
        int threads_num){

    int i, status;

    TupleSchema schema;

    schema.add_attribute(Attribute("id", INT32, NOT_NULLABLE));

    //lock the operation InsertUnique
    g_key_row_set.Set(
            new row_hash_set::RoughSafeRowHashSetImpl(schema, HeapBufferAllocator::Get()));  

    for(i=0; i<threads_num; ++i){

        threads[i]->number = i;

        threads[i]->count = threads_num;

        threads[i]->input_view = views[i];

        threads[i]->result_table = result_tables[i];

        threads[i]->key = &g_key_row_set;

        status = pthread_create(
                &threads[i]->thread_id, 
                0, 
                thread_aggregate, 
                threads[i]);

        if(status != 0){
            printf("%d %s\n", status, "create thread failure");
            exit(-1);
        }
    }

    return 0;
}

FailureOrOwned<Cursor> create_execute_plan(const View * input_view){

    Operation* input = ScanView(*input_view);

    CompoundSingleSourceProjector * group_by_column = new CompoundSingleSourceProjector();
    group_by_column->add(ProjectNamedAttribute("id"));

    scoped_ptr<AggregationSpecification> aggregator(new AggregationSpecification());

    aggregator->AddAggregation(SUM, "score", "sum");

    scoped_ptr<Operation> group_agg_operator(
            GroupAggregate(group_by_column, aggregator.release(), NULL, input));

    return group_agg_operator->CreateCursor();
}

void load_data(uint64_t data, Table * table){

    TableRowWriter * table_writer = new TableRowWriter(table);

    for(int i=0; i<data; ++i){
        table_writer->AddRow().Int32(i*17%5).Int32(i*7%3).CheckSuccess();
        
        //sequence record to test split view function
        //table_writer->AddRow().Int32(i).Int32(i).CheckSuccess();
    }

    delete table_writer;

    return ;
}

Table * create_table(){

    TupleSchema schema;

    schema.add_attribute(Attribute("id", INT32, NOT_NULLABLE));
    schema.add_attribute(Attribute("score", INT32, NOT_NULLABLE));

    Table *  table = new Table(schema, HeapBufferAllocator::Get());

    return table;
}

Table ** create_output_tables(int threads_num){

    Table ** tables = (Table **) malloc(sizeof(Table *) * threads_num);

    for(int i=0; i<threads_num; ++i){
        tables[i] = create_table();
    }

    return tables;
}

int split_views(Table * input_table, int threads_num, View ** views){

    int i;
    uint64_t num = input_table->row_count();
    uint64_t step = num/threads_num;

    uint64_t row_count = 0;
    uint64_t start = 0;

    for(i=0; i<threads_num; ++i){

        row_count = (i==threads_num -1) ? num : step;
        start = step * i;
        num -= step;

        views[i] = new View(input_table->view(), start, row_count);
    }

    return 0;
}

void print_view(int number, const View * view){

    //print selected thread to print its view
    if(-1 != g_thread_id && g_thread_id != number)
        return ;

    for(int j=0; j<view->row_count();j++)
    {
        std::cout<<number<<"\t";

        std::cout<<view->column(0).typed_data<INT32>()[j]<<"\t";

        std::cout<<view->column(1).typed_data<INT32>()[j]<<"\t";

        std::cout<<std::endl;
    }
    std::cout<<"-------------------------------------------"<<std::endl;

}
