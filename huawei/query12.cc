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
using supersonic::Null;
using supersonic::BoundExpressionTree;
using supersonic::EvaluationResult;
using supersonic::ParseStringNulling;
using supersonic::GetConstantExpressionValue;
using supersonic::Divide;
using supersonic::ExpressionList;
using supersonic::Case;
using supersonic::NotEqual;


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
			float DISCOUNT,float TAX,string RETURNFLAG,string LINESTATUS,string SHIPDATE,string
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


LINEITEM::LINEITEM(int ORDERKEY,int PARTKEY,int SUPPKEY,int LINENUMBER,float QUANTITY,float EXTENDEDPRICE,
		float DISCOUNT,float TAX,string RETURNFLAG,string LINESTATUS,string SHIPDATE,string COMMITDATE,
		string RECEIPTDATE,string SHIPINSTRUCT,string SHIPMODE,string COMMENT){
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
	LINEITEM one(ORDERKEY,PARTKEY,SUPPKEY,LINENUMBER,QUANTITY,EXTENDEDPRICE,DISCOUNT,
			TAX,RETURNFLAG,LINESTATUS,SHIPDATE,COMMITDATE,RECEIPTDATE,SHIPINSTRUCT,SHIPMODE,COMMENT);//
	vec_l.push_back(one);
}

vector<LINEITEM> LINEITEM_read(){
	ifstream infile("/home/xc/tpch_2_16_1/data/1g/lineitem.tbl");
	string temp;
	vector<LINEITEM> vec_l;
	int i =0;
	if (infile.is_open()){
		while(getline(infile,temp))
		{
			StringSplit_LINEITEM(temp,vec_l);
			i++;
			//	if((i%100000) == 0)
			//	std::cout<<i<<std::endl;
		}
	}
	infile.close();
	return vec_l;
}


struct ORDERS{
	ORDERS(int ORDERKEY,
			int CUSTKEY,
			string ORDERSTATUS,
			float TOTALPRICE,
			string ORDERDATE,
			string ORDERPRIORITY,
			string CLERK,
			int SHIPPRIORITY,
			string COMMENT);

	int O_ORDERKEY;
	int O_CUSTKEY;
	string O_ORDERSTATUS;
	float O_TOTALPRICE;
	string O_ORDERDATE;
	string O_ORDERPRIORITY;
	string O_CLERK;
	int O_SHIPPRIORITY;
	string O_COMMENT;
};


ORDERS::ORDERS(int ORDERKEY,
		int CUSTKEY,
		string ORDERSTATUS,
		float TOTALPRICE,
		string ORDERDATE,
		string ORDERPRIORITY,
		string CLERK,
		int SHIPPRIORITY,
		string COMMENT)
{
	O_ORDERKEY = ORDERKEY;
	O_CUSTKEY = CUSTKEY;
	O_ORDERSTATUS = ORDERSTATUS;
	O_TOTALPRICE = TOTALPRICE;
	O_ORDERDATE = ORDERDATE;
	O_ORDERPRIORITY = ORDERPRIORITY;
	O_CLERK = CLERK;
	O_SHIPPRIORITY = SHIPPRIORITY;
	O_COMMENT = COMMENT;
}

void StringSplit_ORDERS(string s,vector<ORDERS>& vec_o){
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

	int ORDERKEY = atoi(vec_s[0].c_str());
	int CUSTKEY = atoi(vec_s[1].c_str());
	string ORDERSTATUS = vec_s[2];
	float TOTALPRICE = atoi(vec_s[3].c_str());
	vec_s[4][4] = '/';
	vec_s[4][7] = '/';
	string ORDERDATE = vec_s[4];
	string ORDERPRIORITY = vec_s[5];
	string CLERK = vec_s[6];
	int SHIPPRIORITY = atoi(vec_s[7].c_str());
	string COMMENT = vec_s[8];


	vec_s.clear();
	ORDERS one( ORDERKEY,
			CUSTKEY,
			ORDERSTATUS,
			TOTALPRICE,
			ORDERDATE,
			ORDERPRIORITY,
			CLERK,
			SHIPPRIORITY,
			COMMENT);
	vec_o.push_back(one);
}

vector<ORDERS> ORDERS_read(){
	ifstream infile("/home/xc/tpch_2_16_1/data/1g/orders.tbl");
	string temp;
	vector<ORDERS> vec_o;
	if (infile.is_open()){
		while(getline(infile,temp))
		{
			StringSplit_ORDERS(temp,vec_o);

		}
	}
	infile.close();
	return vec_o;
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

		orders_schema.add_attribute(Attribute("O_ORDERKEY", INT32, NOT_NULLABLE));
		orders_schema.add_attribute(Attribute("O_CUSTKEY", INT32, NOT_NULLABLE));
		orders_schema.add_attribute(Attribute("O_ORDERSTATUS", STRING, NOT_NULLABLE));
		orders_schema.add_attribute(Attribute("O_TOTALPRICE", FLOAT, NOT_NULLABLE));
		orders_schema.add_attribute(Attribute("O_ORDERDATE", DATE, NOT_NULLABLE));
		orders_schema.add_attribute(Attribute("O_ORDERPRIORITY", STRING, NOT_NULLABLE));
		orders_schema.add_attribute(Attribute("O_CLERK", STRING, NOT_NULLABLE));
		orders_schema.add_attribute(Attribute("O_SHIPPRIORITY", INT32, NOT_NULLABLE));
		orders_schema.add_attribute(Attribute("O_COMMENT", STRING, NOT_NULLABLE));


		lineitem_table.reset(new Table(lineitem_schema,
					HeapBufferAllocator::Get()));

		orders_table.reset(new Table(orders_schema,
					HeapBufferAllocator::Get()));
		//两种方法写入数据到tables中：
		//1、TableRowWriter  比较适合于简单的测试环境。
		//2、直接写入table。

		lineitem_table_writer.reset(new TableRowWriter(lineitem_table.get()));
		orders_table_writer.reset(new TableRowWriter(orders_table.get()));

	}

	int32 AddLineitemData(int32 L_ORDERKEY,int32 L_PARTKEY,int32 L_SUPPKEY,int32 L_LINENUMBER,
			float L_QUANTITY,float L_EXTENDEDPRICE,float L_DISCOUNT,float L_TAX,
			const StringPiece& L_RETURNFLAG,const StringPiece& L_LINESTATUS,
			const StringPiece& L_SHIPDATE,const StringPiece& L_COMMITDATE,const StringPiece& L_RECEIPTDATE,
			const StringPiece& L_SHIPINSTRUCT,const StringPiece& L_SHIPMODE,
			const StringPiece& L_COMMENT) {

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
		/*   if (!date_published_is_null) {
		     book_table->Set<DATE>(3, row_id, data_published_as_int32.get());
		     } else {
		     book_table->SetNull(3, row_id);
		     }*/
		lineitem_table_writer
			->AddRow().Int32(L_ORDERKEY).Int32(L_PARTKEY).Int32(L_SUPPKEY).Int32(L_LINENUMBER)
			.Float(L_QUANTITY).Float(L_EXTENDEDPRICE).Float(L_DISCOUNT).Float(L_TAX)
			.String(L_RETURNFLAG).String(L_LINESTATUS)
			.Date(L_SHIPDATE_as_int32.get()).Date(L_COMMITDATE_as_int32.get()).Date(L_RECEIPTDATE_as_int32.get())
			.String(L_SHIPINSTRUCT).String(L_SHIPMODE).String(L_COMMENT)
			.CheckSuccess();
		return L_LINENUMBER;
	}

	int32 AddOrdersData(int32 O_ORDERKEY,
			int32 O_CUSTKEY,
			const StringPiece&  O_ORDERSTATUS,
			float O_TOTALPRICE,
			const StringPiece&  O_ORDERDATE,
			const StringPiece&  O_ORDERPRIORITY,
			const StringPiece&  O_CLERK,
			int32 O_SHIPPRIORITY,
			const StringPiece&  O_COMMENT) {
		scoped_ptr<const Expression> date_or_null1(
				ParseStringNulling(DATE, ConstString(O_ORDERDATE)));
		bool O_ORDERDATE_is_null = false;
		FailureOr<int32> O_ORDERDATE_as_int32 =
			GetConstantExpressionValue<DATE>(*date_or_null1,
					&O_ORDERDATE_is_null);

		orders_table_writer
			->AddRow().Int32(O_ORDERKEY).Int32(O_CUSTKEY).String(O_ORDERSTATUS).Float(O_TOTALPRICE)
			.Date(O_ORDERDATE_as_int32.get()).String(O_ORDERPRIORITY)
			.String(O_CLERK).Int32(O_SHIPPRIORITY).String(O_COMMENT)
			.CheckSuccess();
		return O_ORDERKEY;
	}

	long get_elasped_time(struct timespec start, struct timespec end) {
		return 1000000000L * (end.tv_sec - start.tv_sec)
			+ (end.tv_nsec - start.tv_nsec);
	}

	void TestResults() {
		struct timespec start, end;

		Operation * ordersscan = ScanView(orders_table->view());
		Operation * lineitemscan = ScanView(lineitem_table->view());

		/*filter the lineitem table, where L_SHIPMODE in (MAIL,SHIP)*/
		//  const Expression * Lwhere1 = Equal(NamedAttribute("L_SHIPMODE"),ConstString("MAIL"));
		//  const Expression * Lwhere2 = Equal(NamedAttribute("L_SHIPMODE"),ConstString("SHIP"));

		scoped_ptr<ExpressionList> in(new ExpressionList());
		in->add(ConstString("MAIL"));
		in->add(ConstString("SHIP"));

		const Expression * Lwhere1 = In(NamedAttribute("L_SHIPMODE"),in.release());

		/*filter the lineitem table, where l_commitdate < l_receiptdate*/
		const Expression * Lwhere2 = Less(NamedAttribute("L_COMMITDATE"),NamedAttribute("L_RECEIPTDATE"));

		/*filter the lineitem table, where l_shipdate < l_commitdate*/
		const Expression * Lwhere3 = Less(NamedAttribute("L_SHIPDATE"),NamedAttribute("L_COMMITDATE"));

		/*filter the lineitem table, wherel_receiptdate >= 8766*/
		const Expression * Lwhere4 = GreaterOrEqual(NamedAttribute("L_RECEIPTDATE"),ConstDate(8766));

		/*filter the lineitem table, wherel_receiptdate >= 8766*/
		const Expression * Lwhere5 = GreaterOrEqual(NamedAttribute("L_RECEIPTDATE"),ConstDate(8766));

		/*filter the lineitem table, wherel_receiptdate < 9131*/
		const Expression * Lwhere6 = Less(NamedAttribute("L_RECEIPTDATE"),ConstDate(9131));

		scoped_ptr< Operation> Lfilter(Filter(And(And(And(And(And(Lwhere1,Lwhere2),Lwhere3),Lwhere4),Lwhere5),Lwhere6),ProjectAllAttributes(), lineitemscan));

		/*case*/

		const Expression * case1 = Case(MakeExpressionList(util::gtl::Container(
						Or(Equal(NamedAttribute("O_ORDERPRIORITY"),ConstString("1-URGENT")),
							Equal(NamedAttribute("O_ORDERPRIORITY"),ConstString("2-HIGH"))),
						ConstInt32(0),
						ConstBool(true), ConstInt32(1),
						ConstBool(false), ConstInt32(0))));

		const Expression * case2 = Case(MakeExpressionList(util::gtl::Container(
						And(NotEqual(NamedAttribute("O_ORDERPRIORITY"),ConstString("1-URGENT")),
							NotEqual(NamedAttribute("O_ORDERPRIORITY"),ConstString("2-HIGH"))),
						ConstInt32(0),
						ConstBool(true), ConstInt32(1),
						ConstBool(false), ConstInt32(0))));

		scoped_ptr<Operation> Ocompute(Compute(
					(new CompoundExpression)
					->Add(NamedAttribute("O_ORDERKEY"))
					->AddAs("high_line_count",case1)
					->AddAs("low_line_count",case2)
					,ordersscan));
		/*hash join*/
		scoped_ptr<const SingleSourceProjector> O_selector(
				ProjectNamedAttribute("O_ORDERKEY"));
		scoped_ptr<const SingleSourceProjector> L_selector(
				ProjectNamedAttribute("L_ORDERKEY"));
		scoped_ptr<CompoundMultiSourceProjector> LOresult_projector(
				new CompoundMultiSourceProjector());

		scoped_ptr<CompoundSingleSourceProjector> result_O_projector(
				new CompoundSingleSourceProjector());
		result_O_projector->add(ProjectNamedAttribute("high_line_count"));
		result_O_projector->add(ProjectNamedAttribute("low_line_count"));

		scoped_ptr<CompoundSingleSourceProjector> result_L_projector(
				new CompoundSingleSourceProjector());
		result_L_projector->add(ProjectNamedAttribute("L_SHIPMODE"));

		LOresult_projector->add(0, result_O_projector.release());
		LOresult_projector->add(1, result_L_projector.release());

		scoped_ptr<Operation> LOhash_join(
				new HashJoinOperation(/* join type */ INNER,
					/* select left */ O_selector.release(),
					/* select right */ L_selector.release(),
					/* project result */ LOresult_projector.release(),
					/* unique keys on the right ? */ UNIQUE,
					/* left data */ Ocompute.release(),
					/* right data */ Lfilter.release()));

		/*group by*/
		scoped_ptr<AggregationSpecification> specification(new AggregationSpecification());
		specification-> AddAggregation(SUM, "high_line_count", "high_line_count");
		specification-> AddAggregation(SUM, "low_line_count", "low_line_count");

		scoped_ptr< const SingleSourceProjector >
			key_projector(ProjectNamedAttribute( "L_SHIPMODE"));

		scoped_ptr< Operation> aggregation(GroupAggregate(key_projector.release(),
					specification.release(),
					NULL,
					LOhash_join.release()));
		/*order by*/
		scoped_ptr< const SingleSourceProjector >projector(ProjectNamedAttribute( "L_SHIPMODE"));

		scoped_ptr< SortOrder> sort_order(new SortOrder());
		sort_order-> add(projector.release(), ASCENDING);//指定排序顺序
		const size_t mem_limit = 128;//限定内存大小
		scoped_ptr< Operation> sort(Sort(sort_order.release(),
					NULL,
					mem_limit,
					aggregation.release()));//调用排序算法，其中的input_view是scoped_ptr<View>input_view。
		// ResultView Tresult(sort.get()->CreateCursor()->Next(-1));
		scoped_ptr<Cursor> Tresult_cursor;
		Tresult_cursor.reset(SucceedOrDie(sort->CreateCursor()));//scoped_ptr<Cursor> result_cursor;
		ResultView Tresult(Tresult_cursor->Next(-1));
		std::cout<<"row count is:"<<Tresult.view().row_count()<<std::endl;
		/*        std::cout<<"The Result of Sort is :"<<std::endl;
			  std::cout<<"column count is:"<<Tresult.view().column_count()<<std::endl;

			  for (int32 k=0;k<Tresult.view().column_count();k++)
			  {
			  std::cout<<Tresult.view().schema().attribute(k).name()<<"\t";
			  }
			  std::cout<<std::endl;
			  for(int j=0; j<Tresult.view().row_count();j++)
			  {

			  std::cout<<Tresult.view().column(0).typed_data<STRING>()[j]<<"\t";
			  std::cout<<Tresult.view().column(1).typed_data<INT32>()[j]<<"\t";
			  std::cout<<Tresult.view().column(2).typed_data<INT32>()[j]<<"\t";


			  std::cout<<std::endl;
			  }*/

	}

	ExpressionList* MakeExpressionList(
			const vector<const Expression*>& expressions) {
		scoped_ptr<ExpressionList> list(new ExpressionList());
		for (int i = 0; i < expressions.size(); ++i) {
			list->add(expressions[i]);
		}
		return list.release();
	}

	// Supersonic objects.
	scoped_ptr<Cursor> result_cursor;

	TupleSchema lineitem_schema;
	TupleSchema orders_schema;

	BufferAllocator* buffer_allocator_;

	scoped_ptr<Table> lineitem_table;
	scoped_ptr<Table> orders_table;

	scoped_ptr<TableRowWriter> lineitem_table_writer;
	scoped_ptr<TableRowWriter> orders_table_writer;
};

long get_elasped_time(struct timespec start, struct timespec end) {
	return 1000000000L * (end.tv_sec - start.tv_sec)
		+ (end.tv_nsec - start.tv_nsec);
}

int main(void) {

	double loadtime_start = (double)clock();

	QueryOneTest test;
	test.SetUp();
	vector<LINEITEM> vec_LINEITEM=LINEITEM_read();

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
		//  if((j%10000) == 0)
		//  std::cout<<j<<std::endl;
	}
	vec_LINEITEM.clear();
	vector<LINEITEM>(vec_LINEITEM).swap(vec_LINEITEM);
	vector<ORDERS> vec_ORDERS=ORDERS_read();
	for(int j=0;j<vec_ORDERS.size();j++){
		test.AddOrdersData(
				vec_ORDERS[j].O_ORDERKEY,
				vec_ORDERS[j].O_CUSTKEY,
				vec_ORDERS[j].O_ORDERSTATUS,
				vec_ORDERS[j].O_TOTALPRICE,
				vec_ORDERS[j].O_ORDERDATE,
				vec_ORDERS[j].O_ORDERPRIORITY,
				vec_ORDERS[j].O_CLERK,
				vec_ORDERS[j].O_SHIPPRIORITY,
				vec_ORDERS[j].O_COMMENT);
	}
	vec_ORDERS.clear();
	vector<ORDERS>(vec_ORDERS).swap(vec_ORDERS);
	double loadtime_finish = (double)clock();
	std::cout<<"Loaddata time is :"<<(loadtime_finish-loadtime_start)/CLOCKS_PER_SEC<<"s"<<std::endl;
	//  for(int i =0;i<4;i++)
	//  {
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
	//	  }
	return 0;
}
