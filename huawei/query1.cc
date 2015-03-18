/*
 * TimeTestQuery.cc
 *
 *  Created on: 2014年7月8日
 *      Author: helong
 */


// Copyright 2012 Google Inc. All Rights Reserved.
// Author: tomasz.kaftal@gmail.com (Tomasz Kaftal)
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
// The last tutorial file in this section covers one very important thing we
// haven't considered so far - performing joins. Finally, we will discuss
// a larger and more complex operation tree to see how a real-world example
// works in practice.
#include <string>
#include <fstream>
#include <stdio.h>
#include <stdlib.h>
#include <iomanip>
#include <memory>
#pragma   warning (disable: 4786)
#include <vector>
#include <iostream>
#include <time.h>
using namespace std;

#include <map>
using std::map;
using std::multimap;
#include <set>
using std::multiset;
using std::set;
#include <utility>
using std::make_pair;
using std::pair;

#include "gtest/gtest.h"

#include "supersonic/supersonic.h"
#include "supersonic/cursor/core/sort.h"
#include "supersonic/cursor/infrastructure/ordering.h"
#include "supersonic/utils/strings/stringpiece.h"
#include "supersonic/base/memory/memory.h"

// Include some map utilities to use for result verification.
#include "supersonic/utils/map-util.h"

using supersonic::Attribute;
using supersonic::Block;
using supersonic::Cursor;
using supersonic::Operation;
using supersonic::FailureOr;
using supersonic::FailureOrOwned;
using supersonic::GetConstantExpressionValue;
using supersonic::TupleSchema;
using supersonic::Table;
using supersonic::TableRowWriter;
using supersonic::View;
using supersonic::ViewCopier;
using supersonic::HashJoinOperation;
using supersonic::HeapBufferAllocator;
using supersonic::JoinType;
using supersonic::ProjectNamedAttribute;
using supersonic::ProjectNamedAttributeAs;
using supersonic::rowid_t;
using supersonic::SingleSourceProjector;
using supersonic::MultiSourceProjector;
using supersonic::CompoundSingleSourceProjector;
using supersonic::CompoundMultiSourceProjector;
using supersonic::ResultView;
using supersonic::ScanView;
using supersonic::SucceedOrDie;
using supersonic::NamedAttribute;
using supersonic::AggregationSpecification;
using supersonic::SortOrder;
using supersonic::Sort;
using supersonic::Filter;
using supersonic::ProjectAllAttributes;
using supersonic::BufferAllocator;
using supersonic::MemoryLimit;


using supersonic::If;
using supersonic::IfNull;
using supersonic::Less;
using supersonic::CompoundExpression;
using supersonic::Expression;
using supersonic::Compute;
using supersonic::Generate;
using supersonic::ParseStringNulling;
using supersonic::ConstBool;
using supersonic::ConstString;
using supersonic::ConstInt32;
using supersonic::ConstDate;
using supersonic::Null;
using supersonic::BoundExpressionTree;
using supersonic::EvaluationResult;
using supersonic::ParseStringNulling;
using supersonic::GetConstantExpressionValue;
using supersonic::Divide;

using supersonic::INNER;
using supersonic::UNIQUE;
using supersonic::SUM;
using supersonic::ASCENDING;
using supersonic::COUNT;

using supersonic::INT32;
using supersonic::NOT_NULLABLE;
using supersonic::NULLABLE;
using supersonic::STRING;
using supersonic::DATE;
using supersonic::BOOL;
using supersonic::FLOAT;
using supersonic::UINT64;
using supersonic::DOUBLE;


using supersonic::rowcount_t;

struct LINEITEM{
	LINEITEM(int ORDERKEY,int PARTKEY,int SUPPKEY,int LINENUMBER,float QUANTITY,float EXTENDEDPRICE,float DISCOUNT,float TAX,string RETURNFLAG,string LINESTATUS,string SHIPDATE,string
COMMITDATE,string RECEIPTDATE,string SHIPINSTRUCT,string SHIPMODE,string COMMENT);
        int L_ORDERKEY;
        int L_PARTKEY;
        int L_SUPPKEY;
        int L_LINENUMBER;
        float L_QUANTITY;
        float L_EXTENDEDPRICE;
        float L_DISCOUNT;
        float L_TAX;
        string L_RETURNFLAG;
        string L_LINESTATUS;
        string L_SHIPDATE;
        string L_COMMITDATE;
        string L_RECEIPTDATE;
        string L_SHIPINSTRUCT;
        string L_SHIPMODE;
        string L_COMMENT;
	};


	LINEITEM::LINEITEM(int ORDERKEY,int PARTKEY,int SUPPKEY,int LINENUMBER,float QUANTITY,float EXTENDEDPRICE,float DISCOUNT,float TAX,string RETURNFLAG,string LINESTATUS,string SHIPDATE,string COMMITDATE,string RECEIPTDATE,string SHIPINSTRUCT,string SHIPMODE,string COMMENT){
    	L_ORDERKEY=ORDERKEY;
        L_PARTKEY=PARTKEY;
        L_SUPPKEY=SUPPKEY;
        L_LINENUMBER=LINENUMBER;
        L_QUANTITY=QUANTITY;
        L_EXTENDEDPRICE=EXTENDEDPRICE;
        L_DISCOUNT=DISCOUNT;
        L_TAX=TAX;
        L_RETURNFLAG=RETURNFLAG;
        L_LINESTATUS=LINESTATUS;
        L_SHIPDATE=SHIPDATE;
        L_COMMITDATE=COMMITDATE;
        L_RECEIPTDATE=RECEIPTDATE;
        L_SHIPINSTRUCT=SHIPINSTRUCT;
        L_SHIPMODE=SHIPMODE;
        L_COMMENT=COMMENT;
	}


	void StringSplit_LINEITEM(string s,vector<LINEITEM>& vec_l){
		vector<string> vec_s;
		char splitchar='|';
		if(vec_s.size()>0)
			vec_s.clear();
		int length = s.length();
		int j=0;
		int start=0;
		for(int i=0;i<length;i++)
		{
			if(s[i] == splitchar)
			{
				vec_s.push_back(s.substr(start,i - start));
				start = i+1;
			}
			else if(i == length-1)
			{
				vec_s.push_back(s.substr(start,i+1 - start));
			}
		}

		int ORDERKEY=atoi(vec_s[0].c_str());
		int PARTKEY=atoi(vec_s[1].c_str());
		int SUPPKEY=atoi(vec_s[2].c_str());
		int LINENUMBER=atoi(vec_s[3].c_str());
		float QUANTITY=atof(vec_s[4].c_str());
		float EXTENDEDPRICE=atof(vec_s[5].c_str());
		float DISCOUNT=atof(vec_s[6].c_str());
		float TAX=atof(vec_s[7].c_str());
		string RETURNFLAG=vec_s[8];
		string LINESTATUS=vec_s[9];
		vec_s[10][4] = '/';
		vec_s[10][7] = '/';
		string SHIPDATE=vec_s[10];
		string COMMITDATE=vec_s[11];
		string RECEIPTDATE=vec_s[12];
		string SHIPINSTRUCT=vec_s[13];
		string SHIPMODE=vec_s[14];
		string COMMENT=vec_s[15];
		vec_s.clear();
		LINEITEM one(ORDERKEY,PARTKEY,SUPPKEY,LINENUMBER,QUANTITY,EXTENDEDPRICE,DISCOUNT,TAX,RETURNFLAG,LINESTATUS,SHIPDATE,COMMITDATE,RECEIPTDATE,SHIPINSTRUCT,SHIPMODE,COMMENT);//
		vec_l.push_back(one);
	}


vector<LINEITEM> LINEITEM_read(){
	ifstream infile("/home/bingo/supersonic/data/1g/lineitem.tbl");
	string temp;
	vector<LINEITEM> vec_l;

	if (infile.is_open()){
		while(getline(infile,temp))
		{
			StringSplit_LINEITEM(temp,vec_l);

		}
	}
	return vec_l;
}

long get_elasped_time(struct timespec start, struct timespec end) {

	return 1000000000L * (end.tv_sec - start.tv_sec)
		+ (end.tv_nsec - start.tv_nsec);

}

class QueryOneTest {

public:
    void SetUp() {

	lineitem_schema.add_attribute(Attribute("L_ORDERKEY", INT32, NOT_NULLABLE));
	lineitem_schema.add_attribute(Attribute("L_PARTKEY", INT32, NOT_NULLABLE));
	lineitem_schema.add_attribute(Attribute("L_SUPPKEY", INT32, NOT_NULLABLE));
	lineitem_schema.add_attribute(Attribute("L_LINENUMBER", INT32, NOT_NULLABLE));
	lineitem_schema.add_attribute(Attribute("L_QUANTITY", FLOAT, NOT_NULLABLE));
	lineitem_schema.add_attribute(Attribute("L_EXTENDEDPRICE", FLOAT, NOT_NULLABLE));
	lineitem_schema.add_attribute(Attribute("L_DISCOUNT", FLOAT, NOT_NULLABLE));
	lineitem_schema.add_attribute(Attribute("L_TAX", FLOAT, NOT_NULLABLE));

	lineitem_schema.add_attribute(Attribute("L_RETURNFLAG", STRING, NOT_NULLABLE));
	lineitem_schema.add_attribute(Attribute("L_LINESTATUS", STRING, NOT_NULLABLE));
	lineitem_schema.add_attribute(Attribute("L_SHIPDATE", DATE, NOT_NULLABLE));
	lineitem_schema.add_attribute(Attribute("L_COMMITDATE", DATE, NOT_NULLABLE));
	lineitem_schema.add_attribute(Attribute("L_RECEIPTDATE", DATE, NOT_NULLABLE));
	lineitem_schema.add_attribute(Attribute("L_SHIPINSTRUCT", STRING, NOT_NULLABLE));
	lineitem_schema.add_attribute(Attribute("L_SHIPMODE", STRING, NOT_NULLABLE));
	lineitem_schema.add_attribute(Attribute("L_COMMENT", STRING, NOT_NULLABLE));


	lineitem_table.reset(new Table(lineitem_schema,
				HeapBufferAllocator::Get()));

        lineitem_table_writer.reset(new TableRowWriter(lineitem_table.get()));
    }



    int32 AddData(int32 L_ORDERKEY,int32 L_PARTKEY,int32 L_SUPPKEY,int32 L_LINENUMBER,
            float L_QUANTITY,float L_EXTENDEDPRICE,float L_DISCOUNT,float L_TAX,
            const StringPiece& L_RETURNFLAG,const StringPiece& L_LINESTATUS,
            const StringPiece& L_SHIPDATE,const StringPiece& L_COMMITDATE,
            const StringPiece& L_RECEIPTDATE, const StringPiece& L_SHIPINSTRUCT,
            const StringPiece& L_SHIPMODE, const StringPiece& L_COMMENT) {

        scoped_ptr<const Expression> date_or_null1(
                ParseStringNulling(DATE, ConstString(L_SHIPDATE)));

        bool L_SHIPDATE_is_null = false;

        FailureOr<int32> L_SHIPDATE_as_int32 = GetConstantExpressionValue<DATE>(
                *date_or_null1,
                &L_SHIPDATE_is_null);

        scoped_ptr<const Expression> date_or_null2(
                ParseStringNulling(DATE, ConstString(L_COMMITDATE))); 

        bool L_COMMITDATE_is_null = false;

        FailureOr<int32> L_COMMITDATE_as_int32 = GetConstantExpressionValue<DATE>(*date_or_null2,
                &L_COMMITDATE_is_null);

        scoped_ptr<const Expression> date_or_null3(
                ParseStringNulling(DATE, ConstString(L_RECEIPTDATE)));

        bool L_RECEIPTDATE_is_null = false;

        FailureOr<int32> L_RECEIPTDATE_as_int32 =
            GetConstantExpressionValue<DATE>(*date_or_null3, &L_RECEIPTDATE_is_null);

        lineitem_table_writer->AddRow()
            .Int32(L_ORDERKEY)
            .Int32(L_PARTKEY)
            .Int32(L_SUPPKEY)
            .Int32(L_LINENUMBER)
            .Float(L_QUANTITY)
            .Float(L_EXTENDEDPRICE)
            .Float(L_DISCOUNT)
            .Float(L_TAX)
            .String(L_RETURNFLAG)
            .String(L_LINESTATUS)
            .Date(L_SHIPDATE_as_int32.get())
            .Date(L_COMMITDATE_as_int32.get())
            .Date(L_RECEIPTDATE_as_int32.get())
            .String(L_SHIPINSTRUCT)
            .String(L_SHIPMODE)
            .String(L_COMMENT)
            .CheckSuccess();

        return L_LINENUMBER;
    }


    void TestResults() {

        struct timespec start, end;

        clock_gettime(CLOCK_REALTIME, &start);

        Operation * scan = ScanView(lineitem_table->view());

        /*Filter Start*/

        const Expression * LOE = LessOrEqual( NamedAttribute("L_SHIPDATE"),ConstDate(10471));

        scoped_ptr<Operation> filter(
                Filter(LOE,ProjectAllAttributes(), scan));

        /*Filter End*/

        scoped_ptr<Operation> computefilter(Compute(
                    (new CompoundExpression)
                    ->Add(NamedAttribute("L_RETURNFLAG"))
                    ->Add(NamedAttribute("L_LINESTATUS"))
                    ->Add(NamedAttribute("L_QUANTITY"))
                    ->Add(NamedAttribute("L_EXTENDEDPRICE"))
                    ->Add(NamedAttribute("L_DISCOUNT"))
                    ->AddAs("disc_price",
                        Multiply(NamedAttribute("L_EXTENDEDPRICE"),
                            Minus(ConstInt32(1),NamedAttribute("L_DISCOUNT"))))
                    ->AddAs("charge",
                        Multiply(Multiply(NamedAttribute("L_EXTENDEDPRICE"),
                                Minus(ConstInt32(1),NamedAttribute("L_DISCOUNT"))), 
                            Plus(ConstInt32(1),NamedAttribute("L_TAX")))),
                    filter.release()));

      /*Group函数*/

      scoped_ptr<AggregationSpecification> specification(new AggregationSpecification());
      specification->AddAggregation(SUM, "L_QUANTITY", "sum_qty");
      specification->AddAggregation(SUM, "L_EXTENDEDPRICE", "sum_base_price");
      specification->AddAggregation(COUNT, "L_QUANTITY", "count_qty");
      specification->AddAggregation(COUNT, "L_EXTENDEDPRICE", "count_price");
      specification->AddAggregation(SUM, "L_DISCOUNT", "sum_discount");
      specification->AddAggregation(COUNT, "L_DISCOUNT", "count_discount");
      specification->AddAggregation(SUM, "disc_price", "sum_disc_price");
      specification->AddAggregation(SUM, "charge", "sum_charge");
      specification->AddAggregation(COUNT, "", "count_order");

      CompoundSingleSourceProjector* key_projector1= new CompoundSingleSourceProjector();
      key_projector1->add(ProjectNamedAttribute( "L_RETURNFLAG"));
      key_projector1->add(ProjectNamedAttribute( "L_LINESTATUS"));

      scoped_ptr< Operation> aggregation(
              GroupAggregate(key_projector1,
                  specification.release(),
                  NULL,/*ScanforGroup*/
                  computefilter.release()));


      clock_gettime(CLOCK_REALTIME, &start);

      scoped_ptr<Operation> compute(Compute(
                  (new CompoundExpression)
                  ->Add(NamedAttribute("L_RETURNFLAG"))
                  ->Add(NamedAttribute("L_LINESTATUS"))
                  ->Add(NamedAttribute("sum_qty"))
                  ->Add(NamedAttribute("sum_base_price"))
                  ->Add(NamedAttribute("sum_disc_price"))
                  ->Add(NamedAttribute("sum_charge"))
                  ->AddAs("avg_qty",
                      Divide(NamedAttribute("sum_qty"), NamedAttribute("count_qty")))

                  ->AddAs("avg_price",
                      Divide(NamedAttribute("sum_base_price"), NamedAttribute("count_price")))

                  ->AddAs("avg_disc",
                      Divide(NamedAttribute("sum_discount"), NamedAttribute("count_discount")))

                  ->Add(NamedAttribute("count_order")),  aggregation.release()));


      /*Sort*/
      scoped_ptr<const SingleSourceProjector>projector(ProjectNamedAttribute("L_RETURNFLAG"));
      scoped_ptr<SortOrder> sort_order(new SortOrder());
      sort_order->add(projector.release(), ASCENDING);
      const size_t mem_limit = 128;
      scoped_ptr<Operation> sort(
              Sort(sort_order.release(),
                  NULL,
                  mem_limit,
                  compute.release()));


      scoped_ptr<Cursor> Tresult_cursor;
      Tresult_cursor.reset(SucceedOrDie(sort->CreateCursor()));//scoped_ptr<Cursor> result_cursor;

      ResultView Tresult(Tresult_cursor->Next(-1));
      clock_gettime(CLOCK_REALTIME, &end);
      long elasped = get_elasped_time(start, end);
      std::cout<<"Sort cursor time is :"<<elasped/1000000<<"ms"<<std::endl;
  }

// Supersonic objects.
scoped_ptr<Cursor> result_cursor;

TupleSchema lineitem_schema;

BufferAllocator* buffer_allocator_;

scoped_ptr<Table> lineitem_table;
scoped_ptr<TableRowWriter> lineitem_table_writer;


};


int main(void) {

    double loadtime_start = (double)clock();

    QueryOneTest test;

    test.SetUp();

    vector<LINEITEM> vec_LINEITEM=LINEITEM_read();

    for(int j=0;j<vec_LINEITEM.size();j++){
	test.AddData(
			vec_LINEITEM[j].L_ORDERKEY,
			vec_LINEITEM[j].L_PARTKEY,
			vec_LINEITEM[j].L_SUPPKEY,
			vec_LINEITEM[j].L_LINENUMBER,
			vec_LINEITEM[j].L_QUANTITY,
			vec_LINEITEM[j].L_EXTENDEDPRICE,
			vec_LINEITEM[j].L_DISCOUNT,
			vec_LINEITEM[j].L_TAX,
			vec_LINEITEM[j].L_RETURNFLAG,
			vec_LINEITEM[j].L_LINESTATUS,
			vec_LINEITEM[j].L_SHIPDATE,
			vec_LINEITEM[j].L_COMMITDATE,
			vec_LINEITEM[j].L_RECEIPTDATE,
			vec_LINEITEM[j].L_SHIPINSTRUCT,
			vec_LINEITEM[j].L_SHIPMODE,
			vec_LINEITEM[j].L_COMMENT);

    }

    double loadtime_finish = (double)clock();

    std::cout<<"QUERY 1:"<<std::endl;

    std::cout<<"Loaddata time is :"<<(loadtime_finish-loadtime_start)/CLOCKS_PER_SEC<<"s"<<std::endl;

    //for(int i =0;i<4;i++)
    //{
    
    struct timespec start, end;
    clock_gettime(CLOCK_REALTIME, &start);

    //double time_start = (double)clock();
    
    test.TestResults();

    //  for(int k = 0;k<10000;k++)
    //  std::cout<<"test time"<<std::endl;
    //  double time_finish = (double)clock();
    //  std::cout<<time_start<<"and"<<time_finish<<std::endl;
    
    clock_gettime(CLOCK_REALTIME, &end);

    long elasped = get_elasped_time(start, end);

    std::cout<<"Executing query time is :"<<elasped/1000000<<"ms"<<std::endl;
    //  }
    
    return 0;
}







