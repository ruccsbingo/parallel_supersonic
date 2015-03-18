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
//#include "supersonic/testing/expression_test_helper.h"
// Include some map utilities to use for result verification.
#include "supersonic/utils/map-util.h"
#include "supersonic/utils/container_literal.h"
#include "supersonic/proto/supersonic.pb.h"

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
using supersonic::NamedAttribute;
using supersonic::ExpressionList;


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
using supersonic::ConstFloat;
using supersonic::Null;
using supersonic::BoundExpressionTree;
using supersonic::EvaluationResult;
using supersonic::ParseStringNulling;
using supersonic::GetConstantExpressionValue;
using supersonic::Divide;
using supersonic::ExpressionList;
using supersonic::Case;
using supersonic::NotEqual;
using supersonic::Substring;
using supersonic::Multiply;


using supersonic::INNER;
using supersonic::UNIQUE;
using supersonic::SUM;
using supersonic::ASCENDING;
using supersonic::COUNT;
using supersonic::DESCENDING;
using supersonic::Or;
using supersonic::And;

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
	LINEITEM(int ORDERKEY,int PARTKEY,int SUPPKEY,int LINENUMBER,float QUANTITY,float EXTENDEDPRICE,
			float DISCOUNT,float TAX,string RETURNFLAG,string LINESTATUS,string SHIPDATE,
			string COMMITDATE, string RECEIPTDATE,string SHIPINSTRUCT,string SHIPMODE,string COMMENT);
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


LINEITEM::LINEITEM(int ORDERKEY,int PARTKEY,int SUPPKEY,int LINENUMBER,float QUANTITY,
		float EXTENDEDPRICE,float DISCOUNT,float TAX,string RETURNFLAG,
		string LINESTATUS,string SHIPDATE,string COMMITDATE,string RECEIPTDATE,
		string SHIPINSTRUCT,string SHIPMODE,string COMMENT){
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
	vec_s[11][4] = '/';
	vec_s[11][7] = '/';
	string COMMITDATE=vec_s[11];
	vec_s[12][4] = '/';
	vec_s[12][7] = '/';
	string RECEIPTDATE=vec_s[12];
	string SHIPINSTRUCT=vec_s[13];
	string SHIPMODE=vec_s[14];
	string COMMENT=vec_s[15];
	vec_s.clear();
	LINEITEM one(ORDERKEY,PARTKEY,SUPPKEY,LINENUMBER,QUANTITY,EXTENDEDPRICE,
			DISCOUNT,TAX,RETURNFLAG,LINESTATUS,SHIPDATE,COMMITDATE,
			RECEIPTDATE,SHIPINSTRUCT,SHIPMODE,COMMENT);//
	vec_l.push_back(one);
}

int LINEITEM_read(vector<LINEITEM>& vec_l){
	ifstream infile("/home/bingo/supersonic/data/1g/lineitem.tbl");
	string temp;
	if (infile.is_open()){
		while(getline(infile,temp))
		{
			StringSplit_LINEITEM(temp,vec_l);
		}
	}
	infile.close();
	return 0;
}


struct PART{
		PART(int PARTKEY,
		string NAME,
		string MFGR,
		string BRAND,
		string TYPE,
		int SIZE,
		string CONTAINER,
		float RETAILPRICE,
		string COMMENT);

		int P_PARTKEY;
		string P_NAME;
		string P_MFGR;
		string P_BRAND;
		string P_TYPE;
		int P_SIZE;
		string P_CONTAINER;
		float P_RETAILPRICE;
		string P_COMMENT;
};

PART::PART(int PARTKEY,
		string NAME,
		string MFGR,
		string BRAND,
		string TYPE,
		int SIZE,
		string CONTAINER,
		float RETAILPRICE,
		string COMMENT){
	P_PARTKEY = PARTKEY;
	P_NAME = NAME;
	P_MFGR = MFGR;
	P_BRAND = BRAND;
	P_TYPE = TYPE;
	P_SIZE = SIZE;
	P_CONTAINER = CONTAINER;
	P_RETAILPRICE = RETAILPRICE;
	P_COMMENT = COMMENT;
}

void StringSplit_PART(string s,vector<PART>& vec_p){
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

	int PARTKEY = atoi(vec_s[0].c_str());
	string NAME = vec_s[1];
	string MFGR = vec_s[2];
	string BRAND = vec_s[3];
	string TYPE = vec_s[4];
	int SIZE = atoi(vec_s[5].c_str());
	string CONTAINER = vec_s[6];
	float RETAILPRICE = atof(vec_s[7].c_str());
	string COMMENT = vec_s[8];

	vec_s.clear();
	PART one( PARTKEY,
			NAME,
			MFGR,
			BRAND,
			TYPE,
			SIZE,
			CONTAINER,
			RETAILPRICE,
			COMMENT);
	vec_p.push_back(one);
}

int PART_read(vect<PART>& vec_p){

	ifstream infile("/home/xc/tpch_2_16_1/data/1g/part.tbl");
	string temp;
	if (infile.is_open()){
		while(getline(infile,temp))
		{
			StringSplit_PART(temp,vec_p);
		}
	}
	infile.close();
	return 0;
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

		part_schema.add_attribute(Attribute("P_PARTKEY", INT32, NOT_NULLABLE));
		part_schema.add_attribute(Attribute("P_NAME", STRING, NOT_NULLABLE));
		part_schema.add_attribute(Attribute("P_MFGR", STRING, NOT_NULLABLE));
		part_schema.add_attribute(Attribute("P_BRAND", STRING, NOT_NULLABLE));
		part_schema.add_attribute(Attribute("P_TYPE", STRING, NOT_NULLABLE));
		part_schema.add_attribute(Attribute("P_SIZE", INT32, NOT_NULLABLE));
		part_schema.add_attribute(Attribute("P_CONTAINER", STRING, NOT_NULLABLE));
		part_schema.add_attribute(Attribute("P_RETAILPRICE", FLOAT, NOT_NULLABLE));
		part_schema.add_attribute(Attribute("P_COMMENT", STRING, NOT_NULLABLE));

		lineitem_table.reset(new Table(lineitem_schema,
					HeapBufferAllocator::Get()));

		part_table.reset(new Table(part_schema,
					HeapBufferAllocator::Get()));

		lineitem_table_writer.reset(new TableRowWriter(lineitem_table.get()));
		part_table_writer.reset(new TableRowWriter(part_table.get()));

	}

	int32 AddLineitemData(int32 L_ORDERKEY, int32 L_PARTKEY, int32 L_SUPPKEY, int32 L_LINENUMBER,
			float L_QUANTITY, float L_EXTENDEDPRICE, float L_DISCOUNT, float L_TAX,
			const StringPiece& L_RETURNFLAG, const StringPiece& L_LINESTATUS,
			const StringPiece& L_SHIPDATE, const StringPiece& L_COMMITDATE,
			const StringPiece& L_RECEIPTDATE, const StringPiece& L_SHIPINSTRUCT,
			const StringPiece& L_SHIPMODE, const StringPiece& L_COMMENT){

		scoped_ptr<const Expression> date_or_null1(
				ParseStringNulling(DATE, ConstString(L_SHIPDATE)));

		bool L_SHIPDATE_is_null = false;

		FailureOr<int32> L_SHIPDATE_as_int32 =
			GetConstantExpressionValue<DATE>(*date_or_null1,
					&L_SHIPDATE_is_null);

		scoped_ptr<const Expression> date_or_null2(
				ParseStringNulling(DATE, ConstString(L_COMMITDATE)));

		bool L_COMMITDATE_is_null = false;

		FailureOr<int32> L_COMMITDATE_as_int32 =
			GetConstantExpressionValue<DATE>(*date_or_null2,
					&L_COMMITDATE_is_null);

		scoped_ptr<const Expression> date_or_null3(
				ParseStringNulling(DATE, ConstString(L_RECEIPTDATE)));

		bool L_RECEIPTDATE_is_null = false;

		FailureOr<int32> L_RECEIPTDATE_as_int32 =
			GetConstantExpressionValue<DATE>(*date_or_null3,
					&L_RECEIPTDATE_is_null);

		/*
		if (!date_published_is_null) {
			book_table->Set<DATE>(3, row_id, data_published_as_int32.get());
		} else {
			book_table->SetNull(3, row_id);
		}
		*/

		lineitem_table_writer
			->AddRow().Int32(L_ORDERKEY).Int32(L_PARTKEY).Int32(L_SUPPKEY).Int32(L_LINENUMBER)
			.Float(L_QUANTITY).Float(L_EXTENDEDPRICE).Float(L_DISCOUNT).Float(L_TAX)
			.String(L_RETURNFLAG).String(L_LINESTATUS)
			.Date(L_SHIPDATE_as_int32.get()).Date(L_COMMITDATE_as_int32.get()).Date(L_RECEIPTDATE_as_int32.get())
			.String(L_SHIPINSTRUCT).String(L_SHIPMODE).String(L_COMMENT)
			.CheckSuccess();

		return L_LINENUMBER;
	}

	int32 AddPartData(int32 P_PARTKEY,
			const StringPiece& P_NAME,
			const StringPiece& P_MFGR,
			const StringPiece& P_BRAND,
			const StringPiece& P_TYPE,
			int32 P_SIZE,
			const StringPiece& P_CONTAINER,
			float P_RETAILPRICE,
			const StringPiece& P_COMMENT
			) {
		part_table_writer
			->AddRow().Int32(P_PARTKEY).String(P_NAME).String(P_MFGR).String(P_BRAND)
			.String(P_TYPE).Int32(P_SIZE)
			.String(P_CONTAINER).Float(P_RETAILPRICE).String(P_COMMENT)
			.CheckSuccess();
		return P_PARTKEY;
	}

	void TestResults() {

		Operation * partscan = ScanView(part_table->view());
		Operation * lineitemscan = ScanView(lineitem_table->view());

		const Expression * Lwhere1 = GreaterOrEqual(NamedAttribute("L_SHIPDATE"),ConstDate(9374));
		const Expression * Lwhere2 = Less(NamedAttribute("L_SHIPDATE"),ConstDate(9404));

		scoped_ptr< Operation> Lfilter(Filter(And(Lwhere1, Lwhere2), ProjectAllAttributes(), lineitemscan));

		/*Hash join*/
		scoped_ptr<const SingleSourceProjector> P_selector(ProjectNamedAttribute("P_PARTKEY"));
		scoped_ptr<const SingleSourceProjector> L_selector(ProjectNamedAttribute("L_PARTKEY"));
		scoped_ptr<CompoundMultiSourceProjector> LPresult_projector(
				new CompoundMultiSourceProjector());

		scoped_ptr<CompoundSingleSourceProjector> result_P_projector(
				new CompoundSingleSourceProjector());

		result_P_projector->add(ProjectNamedAttribute("P_TYPE"));
		result_P_projector->add(ProjectNamedAttribute("P_PARTKEY"));

		scoped_ptr<CompoundSingleSourceProjector> result_L_projector(
				new CompoundSingleSourceProjector());
		result_L_projector->add(ProjectNamedAttribute("L_PARTKEY"));
		result_L_projector->add(ProjectNamedAttribute("L_EXTENDEDPRICE"));
		result_L_projector->add(ProjectNamedAttribute("L_DISCOUNT"));


		LPresult_projector->add(0, result_L_projector.release());
		LPresult_projector->add(1, result_P_projector.release());

		scoped_ptr<Operation> LPhash_join(
				new HashJoinOperation(/* join type */ INNER,
					/* select left */ L_selector.release(),
					/* select right */ P_selector.release(),
					/* project result */ LPresult_projector.release(),
					/* unique keys on the right ? */ UNIQUE,
					/* left data */ Lfilter.release(),
					/* right data */ partscan));

		const Expression* stringcontain = StringContains(NamedAttribute("P_TYPE"),ConstString("PROMO"));

		const Expression * case1 = Case(MakeExpressionList(util::gtl::Container(
						stringcontain,
						ConstInt32(0),
						ConstBool(true),
						Multiply(NamedAttribute("L_EXTENDEDPRICE"),
							Minus(ConstInt32(1), NamedAttribute("L_DISCOUNT"))),
						ConstBool(false), ConstInt32(0))));

		scoped_ptr<Operation> LPcompute(Compute(
					(new CompoundExpression)
					->AddAs("sum1",case1)
					->AddAs("sum2",
						Multiply(NamedAttribute("L_EXTENDEDPRICE"),
							Minus(ConstInt32(1),NamedAttribute("L_DISCOUNT")))
					       )
					,LPhash_join.release()));

		scoped_ptr<AggregationSpecification> specification(new AggregationSpecification());

		specification->AddAggregation(SUM, "sum1", "sum1");
		specification->AddAggregation(SUM, "sum2", "sum2");

		scoped_ptr< Operation> aggregation(ScalarAggregate(specification.release(), LPcompute.release()));

		/*
		scoped_ptr<Cursor> Gresult_cursor;
		Gresult_cursor.reset(SucceedOrDie(aggregation->CreateCursor()));//scoped_ptr<Cursor> result_cursor;
		ResultView Gresult(Gresult_cursor->Next(-1));
		std::cout<<"The Result of Sort is :"<<std::endl;
		std::cout<<"column count is:"<<Gresult.view().column_count()<<std::endl;
		std::cout<<"row count is:"<<Gresult.view().row_count()<<std::endl;
		for (int32 k=0;k<Gresult.view().column_count();k++)
		{
			std::cout<<Gresult.view().schema().attribute(k).name()<<"\t";
		}
		std::cout<<std::endl;
		for(int j=0; j<Gresult.view().row_count();j++)
		{

			std::cout<<Gresult.view().column(0).typed_data<INT32>()[j]<<"\t";
			std::cout<<Gresult.view().column(1).typed_data<FLOAT>()[j]<<"\t";

			std::cout<<std::endl;
		}
		*/

		scoped_ptr<Operation> LPcompute1(Compute(
					(new CompoundExpression)
					->AddAs("promo_revenue", 
						Multiply(ConstFloat(100.00), 
							Divide(NamedAttribute("sum1"), 
								NamedAttribute("sum2"))))
					,aggregation.release()));

		scoped_ptr<Cursor> Tresult_cursor;

		Tresult_cursor.reset(SucceedOrDie(LPcompute1->CreateCursor()));//scoped_ptr<Cursor> result_cursor;

		ResultView Tresult(Tresult_cursor->Next(-1));

		std::cout<<"row count is:"<<Tresult.view().row_count()<<std::endl;

		/*
		std::cout<<"The Result of Sort is :"<<std::endl;
		std::cout<<"column count is:"<<Tresult.view().column_count()<<std::endl;
		std::cout<<"row count is:"<<Tresult.view().row_count()<<std::endl;
		for (int32 k=0;k<Tresult.view().column_count();k++)
		{
			std::cout<<Tresult.view().schema().attribute(k).name()<<"\t";
		}
		std::cout<<std::endl;
		for(int j=0; j<Tresult.view().row_count();j++)
		{

			std::cout<<Tresult.view().column(0).typed_data<DOUBLE>()[j]<<"\t";

			std::cout<<std::endl;
		}
		*/
	}

	ExpressionList* MakeExpressionList(const vector<const Expression*>& expressions){
		scoped_ptr<ExpressionList> list(new ExpressionList());
		for (int i = 0; i < expressions.size(); ++i) {
			list->add(expressions[i]);
		}
		return list.release();
	}

	// Supersonic objects.
	scoped_ptr<Cursor> result_cursor;

	TupleSchema lineitem_schema;
	TupleSchema part_schema;

	BufferAllocator* buffer_allocator_;

	scoped_ptr<Table> lineitem_table;
	scoped_ptr<Table> part_table;

	scoped_ptr<TableRowWriter> lineitem_table_writer;
	scoped_ptr<TableRowWriter> part_table_writer;
};

long get_elasped_time(struct timespec start, struct timespec end){
	return 1000000000L * (end.tv_sec - start.tv_sec)
		+ (end.tv_nsec - start.tv_nsec);
}

int main(void) {

	double loadtime_start = (double)clock();

	QueryOneTest test;

	test.SetUp();

	vector<LINEITEM> vec_LINEITEM;
	LINEITEM_read(vec_LINEITEM);

	for(int j=0;j<vec_LINEITEM.size();j++){
		test.AddLineitemData(
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
		if((j%10000) == 0)
			std::cout<<j<<std::endl;
	}

	vec_LINEITEM.clear();

	vector<LINEITEM>(vec_LINEITEM).swap(vec_LINEITEM);

	vector<PART> vec_PART;
	PART_read(vec_PART);

	for(int j=0;j<vec_PART.size();j++){
		test.AddPartData(
				vec_PART[j].P_PARTKEY,
				vec_PART[j].P_NAME,
				vec_PART[j].P_MFGR,
				vec_PART[j].P_BRAND,
				vec_PART[j].P_TYPE,
				vec_PART[j].P_SIZE,
				vec_PART[j].P_CONTAINER,
				vec_PART[j].P_RETAILPRICE,
				vec_PART[j].P_COMMENT);
	}

	vec_PART.clear();
	vector<PART>(vec_PART).swap(vec_PART);

	double loadtime_finish = (double)clock();

	std::cout<<"QUERY 14:"<<std::endl;
	std::cout<<"Loaddata time is :"<<(loadtime_finish-loadtime_start)/CLOCKS_PER_SEC<<"s"<<std::endl;

	// for(int i =0;i<4;i++)
	// {
	
	struct timespec start, end;
	clock_gettime(CLOCK_REALTIME, &start);

	// double time_start = (double)clock();
	
	test.TestResults();

	//  for(int k = 0;k<10000;k++)
	//  std::cout<<"test time"<<std::endl;
	//  double time_finish = (double)clock();
	//  std::cout<<time_start<<"and"<<time_finish<<std::endl;
	
	clock_gettime(CLOCK_REALTIME, &end);

	long elasped = get_elasped_time(start, end);

	std::cout<<"Executing query time is :"<<elasped/1000000<<"ms"<<std::endl;

	// }
	
	return 0;
}
