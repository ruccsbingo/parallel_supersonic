#include <stdlib.h>

#include <iostream>

#include "supersonic/supersonic.h"

#define COUNT   0x8000000

using namespace supersonic; 

int main()
{
    int32 i;

    scoped_ptr<Cursor> result_cursor;

    TupleSchema right_schema;
    TupleSchema left_schema;

    scoped_ptr<Table> right_table;
    scoped_ptr<Table> left_table;

    // create table schema and initialize the table
    right_schema.add_attribute(Attribute("right_id", INT32, NOT_NULLABLE));
    left_schema.add_attribute(Attribute("left_id", INT32, NOT_NULLABLE));

    right_table.reset(new Table(right_schema, HeapBufferAllocator::Get()));
    left_table.reset(new Table(left_schema, HeapBufferAllocator::Get()));

    // insert into left and right tables with random numbers
    right_table->SetRowCapacity(COUNT);
    for (i = 0; i < COUNT; i ++)
    {
        right_table->AddRow();
        right_table->Set<INT32>(0, i, i);
    }
    std::cout << "load right table complete!" << std::endl;

    left_table->SetRowCapacity(COUNT);
    for (i = 0; i < COUNT; i ++)
    {
        left_table->AddRow();
        left_table->Set<INT32>(0, i, rand() % COUNT);
    }
    std::cout << "load left table complete!" << std::endl;

    // initialize key selector
    scoped_ptr<const SingleSourceProjector> right_selector(ProjectNamedAttribute("right_id"));
    scoped_ptr<const SingleSourceProjector> left_selector(ProjectNamedAttribute("left_id"));

    // initialize result projector
    scoped_ptr<CompoundMultiSourceProjector> result_projector(new CompoundMultiSourceProjector());

    scoped_ptr<CompoundSingleSourceProjector> result_right_projector(new CompoundSingleSourceProjector());
    result_right_projector->add(ProjectNamedAttribute("right_id"));
    scoped_ptr<CompoundSingleSourceProjector> result_left_projector(new CompoundSingleSourceProjector());
    result_left_projector->add(ProjectNamedAttribute("left_id"));

    result_projector->add(1, result_right_projector.release());
    result_projector->add(0, result_left_projector.release());

    // create hash join openration and cursor
    scoped_ptr<Operation> hash_join(new HashJoinOperation(INNER, left_selector.release(), right_selector.release(), result_projector.release(), UNIQUE, ScanView(left_table->view()), ScanView(right_table->view())));

    result_cursor.reset(SucceedOrDie(hash_join->CreateCursor()));

    std::cout << "ready for join!" << std::endl;

    // traverse the join result and check the result count
    rowcount_t result_count = 0;
    scoped_ptr<ResultView> rv(new ResultView(result_cursor->Next(-1)));

    while (!rv->is_done())
    {
        const View& view = rv->view();
        result_count += view.row_count();

//        for (int i = 0; i < view.row_count(); i ++)
//            std::cout << view.column(0).typed_data<INT32>()[i] << "\t" << view.column(1).typed_data<INT32>()[i] << std::endl;

        rv.reset(new ResultView(result_cursor->Next(-1)));
    }

    std::cout << "join complete!" << std::endl;

    // output the result
    if (result_count == COUNT)
        std::cout << "passed" << std::endl;
    else
        std::cout << "result dismatch" << std::endl;

    return 0;
}
