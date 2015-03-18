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
	        LINEITEM one(ORDERKEY,PARTKEY,SUPPKEY,LINENUMBER,QUANTITY,EXTENDEDPRICE,DISCOUNT,TAX,RETURNFLAG,LINESTATUS,SHIPDATE,COMMITDATE,RECEIPTDATE,SHIPINSTRUCT,SHIPMODE,COMMENT);//
	        vec_l.push_back(one);
						}

		vector<LINEITEM> LINEITEM_read(){
			ifstream infile("/home/xc/tpch_2_16_1/data/100m/lineitem.tbl");
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
		ifstream infile("/home/xc/tpch_2_16_1/data/100m/orders.tbl");
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


PART::PART(	int PARTKEY,
			string NAME,
			string MFGR,
			string BRAND,
			string TYPE,
			int SIZE,
			string CONTAINER,
			float RETAILPRICE,
			string COMMENT)
		{
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
	vector<PART> PART_read(){
		ifstream infile("/home/xc/tpch_2_16_1/data/100m/part.tbl");
		string temp;
		vector<PART> vec_p;
		if (infile.is_open()){
			while(getline(infile,temp))
			{
				StringSplit_PART(temp,vec_p);
			}
							}
		infile.close();
		return vec_p;
		}
		
		struct CUSTOMER{
			CUSTOMER(int SUSTKEY,string NAME,string ADDRESS,int NATIONKEY,
					string PHONE,float ACCTBAL,string MKTSEGMENT,string COMMENT);
			int C_CUSTKEY;
			string C_NAME;
			string C_ADDRESS;
			int C_NATIONKEY;
			string C_PHONE;
			float C_ACCTBAL;
			string C_MKTSEGMENT;
			string C_COMMENT;
			};


	CUSTOMER::CUSTOMER(int SUSTKEY,string NAME,string ADDRESS,int NATIONKEY,
			string PHONE,float ACCTBAL,string MKTSEGMENT,string COMMENT){
				 C_CUSTKEY = SUSTKEY;
				 C_NAME = NAME;
				 C_ADDRESS = ADDRESS;
				 C_NATIONKEY = NATIONKEY;
				 C_PHONE = PHONE;
				 C_ACCTBAL = ACCTBAL;
				 C_MKTSEGMENT = MKTSEGMENT;
				 C_COMMENT = COMMENT;
			}

		void StringSplit_CUSTOMER(string s,vector<CUSTOMER>& vec_c){
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

			int SUSTKEY=atoi(vec_s[0].c_str());
			string NAME =vec_s[1];
			string ADDRESS = vec_s[2];
			int NATIONKEY = atoi(vec_s[3].c_str());
			string PHONE = vec_s[4];
			float ACCTBAL = atof(vec_s[5].c_str());
			string MKTSEGMENT = vec_s[6];
			string COMMENT = vec_s[7];


	        vec_s.clear();
	        CUSTOMER one(SUSTKEY,NAME,ADDRESS,NATIONKEY,PHONE,ACCTBAL,MKTSEGMENT,COMMENT);//
	        vec_c.push_back(one);
		}
		vector<CUSTOMER> CUSTOMER_read(){
			ifstream infile("/home/xc/tpch_2_16_1/data/100m/customer.tbl");
			string temp;
			vector<CUSTOMER> vec_c;
			if (infile.is_open()){
				while(getline(infile,temp))
				{
					StringSplit_CUSTOMER(temp,vec_c);

				}
								}
			infile.close();
			return vec_c;
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

		part_schema.add_attribute(Attribute("P_PARTKEY", INT32, NOT_NULLABLE));
	    part_schema.add_attribute(Attribute("P_NAME", STRING, NOT_NULLABLE));
	    part_schema.add_attribute(Attribute("P_MFGR", STRING, NOT_NULLABLE));
	    part_schema.add_attribute(Attribute("P_BRAND", STRING, NOT_NULLABLE));
	    part_schema.add_attribute(Attribute("P_TYPE", STRING, NOT_NULLABLE));
	    part_schema.add_attribute(Attribute("P_SIZE", INT32, NOT_NULLABLE));
	    part_schema.add_attribute(Attribute("P_CONTAINER", STRING, NOT_NULLABLE));
	    part_schema.add_attribute(Attribute("P_RETAILPRICE", FLOAT, NOT_NULLABLE));
	    part_schema.add_attribute(Attribute("P_COMMENT", STRING, NOT_NULLABLE));
		
    customer_schema.add_attribute(Attribute("C_CUSTKEY", INT32, NOT_NULLABLE));
    customer_schema.add_attribute(Attribute("C_NAME", STRING, NOT_NULLABLE));
    customer_schema.add_attribute(Attribute("C_ADDRESS", STRING, NOT_NULLABLE));
    customer_schema.add_attribute(Attribute("C_NATIONKEY", INT32, NOT_NULLABLE));
    customer_schema.add_attribute(Attribute("C_PHONE", STRING, NOT_NULLABLE));
    customer_schema.add_attribute(Attribute("C_ACCTBAL", FLOAT, NOT_NULLABLE));
    customer_schema.add_attribute(Attribute("C_MKTSEGMENT", STRING, NOT_NULLABLE));
    customer_schema.add_attribute(Attribute("C_COMMENT", STRING, NOT_NULLABLE));

	    lineitem_table.reset(new Table(lineitem_schema,
	                                 HeapBufferAllocator::Get()));

	    orders_table.reset(new Table(orders_schema,
	                                 HeapBufferAllocator::Get()));
		part_table.reset(new Table(part_schema,
	                                 HeapBufferAllocator::Get()));		
		customer_table.reset(new Table(customer_schema,
                                 HeapBufferAllocator::Get()));									 
	    //两种方法写入数据到tables中：
	    //1、TableRowWriter  比较适合于简单的测试环境。
	    //2、直接写入table。

	    lineitem_table_writer.reset(new TableRowWriter(lineitem_table.get()));
	    orders_table_writer.reset(new TableRowWriter(orders_table.get()));
		part_table_writer.reset(new TableRowWriter(part_table.get()));
		    customer_table_writer.reset(new TableRowWriter(customer_table.get()));
		
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
	   int32 AddCustomerData(int32 C_CUSTKEY,const StringPiece& C_NAME,
		  	  	  	  	const StringPiece& C_ADDRESS,int32 C_NATIONKEY,
		  	  	  	  	const StringPiece& C_PHONE,float C_ACCTBAL,
		  	  	  	  	const StringPiece& C_MKTSEGMENT,const StringPiece& C_COMMENT) {

    customer_table_writer
        ->AddRow().Int32(C_CUSTKEY).String(C_NAME).String(C_ADDRESS)
         	 	  .Int32(C_NATIONKEY).String(C_PHONE).Float(C_ACCTBAL)
        		  .String(C_MKTSEGMENT).String(C_COMMENT)
        		  .CheckSuccess();
    return C_CUSTKEY;
  }
long get_elasped_time(struct timespec start, struct timespec end) {
 	    return 1000000000L * (end.tv_sec - start.tv_sec)
  	            + (end.tv_nsec - start.tv_nsec);
 }
 
 
 void TestResults() {
   	  //检查结果是否满足需求，首先，我们必须把轮询rows，将它们放到一个内存块里。

 	  struct timespec start, end;
     clock_gettime(CLOCK_REALTIME, &start);
	 Operation * scan = ScanView(lineitem_table->view());
	// result_cursor.reset(SucceedOrDie(scan->CreateCursor()));



    /*Filter Start*/
    const Expression * LOE = LessOrEqual( NamedAttribute("L_SHIPDATE"),ConstDate(10471));
    scoped_ptr<Operation> filter(
         Filter(LOE,ProjectAllAttributes(), scan));


   // result_cursor.reset(SucceedOrDie(filter->CreateCursor()));



    //ResultView viewtest = filter->CreateCursor()->Next(-1);


    /*Filter End*/
    scoped_ptr<Operation> computefilter(Compute(
            (new CompoundExpression)
                ->Add(NamedAttribute("L_RETURNFLAG"))
                ->Add(NamedAttribute("L_LINESTATUS"))
                ->Add(NamedAttribute("L_QUANTITY"))
                ->Add(NamedAttribute("L_EXTENDEDPRICE"))
                ->Add(NamedAttribute("L_DISCOUNT"))
                ->AddAs("disc_price",
            Multiply(NamedAttribute("L_EXTENDEDPRICE"),Minus(ConstInt32(1),NamedAttribute("L_DISCOUNT"))))

   		 	    ->AddAs("charge",
   		 			Multiply(Multiply(NamedAttribute("L_EXTENDEDPRICE"),Minus(ConstInt32(1),NamedAttribute("L_DISCOUNT"))), Plus(ConstInt32(1),NamedAttribute("L_TAX"))))
                      ,  filter.release()));

                   //   Multiply(NamedAttribute("L_EXTENDEDPRICE"),Minus(ConstInt32(1),NamedAttribute("L_DISCOUNT"))





    /*Group函数*/

    scoped_ptr<AggregationSpecification> specification(new AggregationSpecification());
    specification-> AddAggregation(SUM, "L_QUANTITY", "sum_qty");//data是要做相加运算的列的名称，data_sums是输出名称
    specification-> AddAggregation(SUM, "L_EXTENDEDPRICE", "sum_base_price");//data是要做相加运算的列的名称，data_sums是输出名称
    specification-> AddAggregation(COUNT, "L_QUANTITY", "count_qty");
    specification-> AddAggregation(COUNT, "L_EXTENDEDPRICE", "count_price");
    specification-> AddAggregation(SUM, "L_DISCOUNT", "sum_discount");//data是要做相加运算的列的名称，data_sums是输出名称
    specification-> AddAggregation(COUNT, "L_DISCOUNT", "count_discount");
    specification-> AddAggregation(SUM, "disc_price", "sum_disc_price");//data是要做相加运算的列的名称，data_sums是输出名称
    specification-> AddAggregation(SUM, "charge", "sum_charge");//data是要做相加运算的列的名称，data_sums是输出名称

    specification-> AddAggregation(COUNT, "", "count_order");


    //scoped_ptr< const SingleSourceProjector >key_projector(ProjectNamedAttribute( "L_RETURNFLAG"));
    CompoundSingleSourceProjector* key_projector1= new CompoundSingleSourceProjector();
    key_projector1->add(ProjectNamedAttribute( "L_RETURNFLAG"));
    key_projector1->add(ProjectNamedAttribute( "L_LINESTATUS"));

  //对表input_view以key为关键字分组，做specification运算。NULL是为分组聚集的选项，可具体看参数。
    scoped_ptr< Operation> aggregation(GroupAggregate(key_projector1/*key_projector.release()*/,
                                                     specification.release(),
                                                     NULL,
                                                     /*ScanforGroup*/computefilter.release()));





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



    /*Sort函数*/
  //  Operation * ScanforSort = ScanView(result.view());
    scoped_ptr< const SingleSourceProjector >projector(ProjectNamedAttribute( "L_RETURNFLAG"));//选出排序属性
    scoped_ptr< SortOrder> sort_order(new SortOrder());
    sort_order-> add(projector.release(), ASCENDING);//指定排序顺序
    const size_t mem_limit = 128;//限定内存大小，
       scoped_ptr< Operation> sort(Sort(sort_order.release(),
                                     NULL,
                                     mem_limit,
                                     /*ScanforSort*/compute.release()));//调用排序算法，其中的input_view是scoped_ptr<View>input_view。


    scoped_ptr<Cursor> Tresult_cursor;
    Tresult_cursor.reset(SucceedOrDie(sort->CreateCursor()));//scoped_ptr<Cursor> result_cursor;

    ResultView Tresult(Tresult_cursor->Next(-1));
   	clock_gettime(CLOCK_REALTIME, &end);
   	long elasped = get_elasped_time(start, end);
   	std::cout<<"Sort cursor time is :"<<elasped/1000000<<"ms"<<std::endl;
        std::cout<<"compile test!"<<std::endl;





  }
 
 
 void TestResults3() {
	  struct timespec start, end;

	  Operation * customerscan = ScanView(customer_table->view());
	  Operation * ordersscan = ScanView(orders_table->view());
	  Operation * lineitemscan = ScanView(lineitem_table->view());

	  /*filter the customer table, where C_MKTSEGMENT = BUILDING*/
	  const Expression * Cwhere = Equal(NamedAttribute("C_MKTSEGMENT"),ConstString("BUILDING"));
	  scoped_ptr< Operation> Cfilter(Filter(Cwhere,ProjectAllAttributes(), customerscan));

	  /*filter the orders table, where O_ORDERDATE <= 1995-03-15*/

	  const Expression * Owhere = LessOrEqual(NamedAttribute("O_ORDERDATE"),ConstDate(9204));
	  scoped_ptr< Operation> Ofilter(Filter(Owhere,ProjectAllAttributes(), ordersscan));

	  /*filter the lineitem table, where L_SHIPDATE >= 1995-03-15*/
	  const Expression * Lwhere = GreaterOrEqual(NamedAttribute("L_SHIPDATE"),ConstDate(9204));
	  scoped_ptr< Operation> Lfilter(Filter(Lwhere,ProjectAllAttributes(), lineitemscan));


	  /*Customer and Orders hash join*/
	  scoped_ptr<const SingleSourceProjector> C_selector(
        ProjectNamedAttribute("C_CUSTKEY"));
      scoped_ptr<const SingleSourceProjector> O_selector(
        ProjectNamedAttribute("O_CUSTKEY"));

      scoped_ptr<CompoundMultiSourceProjector> COresult_projector(
             new CompoundMultiSourceProjector());

      scoped_ptr<CompoundSingleSourceProjector> result_O_projector(
          new CompoundSingleSourceProjector());
      result_O_projector->add(ProjectNamedAttribute("O_ORDERDATE"));
      result_O_projector->add(ProjectNamedAttribute("O_SHIPPRIORITY"));
      result_O_projector->add(ProjectNamedAttribute("O_ORDERKEY"));

      COresult_projector->add(0, result_O_projector.release());

      scoped_ptr<Operation> COhash_join(
          new HashJoinOperation(/* join type */ INNER,
                                /* select left */ O_selector.release(),
                                /* select right */ C_selector.release(),
                                /* project result */ COresult_projector.release(),
                                /* unique keys on the right ? */ UNIQUE,
                                /* left data */ Ofilter.release(),
                                /* right data */ Cfilter.release()));


      /*lineitem and CO hash join*/
      scoped_ptr<const SingleSourceProjector> CO_selector(
              ProjectNamedAttribute("O_ORDERKEY"));
      scoped_ptr<const SingleSourceProjector> L_selector(
              ProjectNamedAttribute("L_ORDERKEY"));

      scoped_ptr<CompoundMultiSourceProjector> LCOresult_projector(
                   new CompoundMultiSourceProjector());

      scoped_ptr<CompoundSingleSourceProjector> result_CO_projector(
                new CompoundSingleSourceProjector());
      result_CO_projector->add(ProjectNamedAttribute("O_ORDERDATE"));
      result_CO_projector->add(ProjectNamedAttribute("O_SHIPPRIORITY"));

      scoped_ptr<CompoundSingleSourceProjector> result_L_projector(
                new CompoundSingleSourceProjector());
      result_L_projector->add(ProjectNamedAttribute("L_ORDERKEY"));
      result_L_projector->add(ProjectNamedAttribute("L_EXTENDEDPRICE"));
      result_L_projector->add(ProjectNamedAttribute("L_DISCOUNT"));


      LCOresult_projector->add(0, result_L_projector.release());
      LCOresult_projector->add(1, result_CO_projector.release());

      scoped_ptr<Operation> LCOhash_join(
              new HashJoinOperation(/* join type */ INNER,
                                    /* select left */ L_selector.release(),
                                    /* select right */ CO_selector.release(),
                                    /* project result */ LCOresult_projector.release(),
                                    /* unique keys on the right ? */ UNIQUE,
                                    /* left data */ Lfilter.release(),
                                    /* right data */ COhash_join.release()));



      scoped_ptr<Operation> compute(Compute(
              (new CompoundExpression)
                  ->Add(NamedAttribute("L_ORDERKEY"))
                  ->AddAs("revenue",
                               Multiply(NamedAttribute("L_EXTENDEDPRICE"),Minus(ConstInt32(1),NamedAttribute("L_DISCOUNT"))))
                  ->Add(NamedAttribute("O_ORDERDATE"))
                  ->Add(NamedAttribute("O_SHIPPRIORITY"))
                  ,LCOhash_join.release()));

      /*Group by*/
      scoped_ptr<AggregationSpecification> specification(new AggregationSpecification());
          specification-> AddAggregation(SUM, "revenue", "revenue");

      CompoundSingleSourceProjector* key_projector= new CompoundSingleSourceProjector();
      key_projector->add(ProjectNamedAttribute( "L_ORDERKEY"));
      key_projector->add(ProjectNamedAttribute( "O_ORDERDATE"));
      key_projector->add(ProjectNamedAttribute( "O_SHIPPRIORITY"));

      scoped_ptr< Operation> aggregation(GroupAggregate(key_projector,
                                                       specification.release(),
                                                       NULL,
                                                       compute.release()));



      /*Order by*/
      clock_gettime(CLOCK_REALTIME, &start);
      CompoundSingleSourceProjector* projector= new CompoundSingleSourceProjector();
      projector->add(ProjectNamedAttribute( "revenue"));
      projector->add(ProjectNamedAttribute( "O_ORDERDATE"));

      scoped_ptr< SortOrder> sort_order(new SortOrder());
      sort_order-> add(projector, DESCENDING);//指定排序顺序
      const size_t mem_limit = 128;//限定内存大小
      scoped_ptr< Operation> sort(Sort(sort_order.release(),
                                          NULL,
                                          mem_limit,
                                          aggregation.release()));//调用排序算法，其中的input_view是scoped_ptr<View>input_view。
     // ResultView Tresult(sort.get()->CreateCursor()->Next(-1));
      result_cursor.reset(SucceedOrDie(sort->CreateCursor()));

       scoped_ptr<Block> result_space(new Block(result_cursor->schema(),
                                                    HeapBufferAllocator::Get()));

           ViewCopier copier(result_cursor->schema(),  true);
           rowcount_t offset = 0;
           scoped_ptr<ResultView> rv(new ResultView(result_cursor->Next(-1)));

           //!rv->is_done()的意思是游标既没有读完而且也没有发生错误的情况下，执行循环体。
           while (!rv->is_done()) {
             const View& view = rv->view();
             rowcount_t view_row_count = view.row_count();

             //为新值分配block，我们事先不知道需要多少个。
             result_space->Reallocate(offset + view_row_count);

             rowcount_t rows_copied = copier.Copy(view_row_count,
                                                  view,
                                                  offset,
                                                  result_space.get());

             offset += rows_copied;
             rv.reset(new ResultView(result_cursor->Next(-1)));
           }

           const View& result_view(result_space->view());



    //  scoped_ptr<Cursor> Tresult_cursor;
    //    Tresult_cursor.reset(SucceedOrDie(sort->CreateCursor()));//scoped_ptr<Cursor> result_cursor;
    //     ResultView Tresult(Tresult_cursor->Next(-1));
         clock_gettime(CLOCK_REALTIME, &end);
  	   long elasped = get_elasped_time(start, end);
  	  std::cout<<"the whole sort time is :"<<elasped/1000000<<"ms"<<std::endl;
            std::cout<<"The Result of Sort is :"<<std::endl;
      //      std::cout<<"column count is:"<<Tresult.view().column_count()<<std::endl;
            std::cout<<"row count is:"<<result_view.row_count()<<std::endl;

  }
 
 
 
 


	  
	  
	  	  void TestResults12() {

		  Operation * partscan = ScanView(part_table->view());
		  Operation * lineitemscan = ScanView(lineitem_table->view());
		  const Expression * Lwhere1 = GreaterOrEqual(NamedAttribute("L_SHIPDATE"),ConstDate(9374));
		  const Expression * Lwhere2 = Less(NamedAttribute("L_SHIPDATE"),ConstDate(9404));
		  scoped_ptr< Operation> Lfilter(Filter(And(Lwhere1,Lwhere2),ProjectAllAttributes(), lineitemscan));



		  /*Hash join*/
		  scoped_ptr<const SingleSourceProjector> P_selector(
		  	              ProjectNamedAttribute("P_PARTKEY"));
		  	      scoped_ptr<const SingleSourceProjector> L_selector(
		  	              ProjectNamedAttribute("L_PARTKEY"));
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
						  	  	  	  	  	  	  Multiply(NamedAttribute("L_EXTENDEDPRICE"),Minus(ConstInt32(1),NamedAttribute("L_DISCOUNT"))),
						  	  	  	  	  	  	  ConstBool(false), ConstInt32(0))));
				  scoped_ptr<Operation> LPcompute(Compute(
				                (new CompoundExpression)
				                    ->AddAs("sum1",case1)
				                    ->AddAs("sum2",Multiply(NamedAttribute("L_EXTENDEDPRICE"),Minus(ConstInt32(1),NamedAttribute("L_DISCOUNT"))))
				                    ,LPhash_join.release()));

			      scoped_ptr<AggregationSpecification> specification(new AggregationSpecification());
			          specification-> AddAggregation(SUM, "sum1", "sum1");
			          specification-> AddAggregation(SUM, "sum2", "sum2");
			      scoped_ptr< Operation> aggregation(ScalarAggregate(specification.release(),
			                                                       LPcompute.release()));
			      /*scoped_ptr<Cursor> Gresult_cursor;
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
			     				                 }*/
				  scoped_ptr<Operation> LPcompute1(Compute(
				                (new CompoundExpression)
				                    ->AddAs("promo_revenue",Multiply(ConstFloat(100.00),Divide(NamedAttribute("sum1"), NamedAttribute("sum2"))))
				                    ,aggregation.release()));

				  scoped_ptr<Cursor> Tresult_cursor;
				     Tresult_cursor.reset(SucceedOrDie(LPcompute1->CreateCursor()));//scoped_ptr<Cursor> result_cursor;
				     ResultView Tresult(Tresult_cursor->Next(-1));
				        std::cout<<"row count is:"<<Tresult.view().row_count()<<std::endl;
				       /* std::cout<<"The Result of Sort is :"<<std::endl;
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
				                 }*/


	  }
	  
	  
	  void TestResults14() {
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

	  TupleSchema part_schema;

      scoped_ptr<Table> part_table;
	  scoped_ptr<TableRowWriter> part_table_writer;


		TupleSchema customer_schema;

		scoped_ptr<Table> customer_table;

		scoped_ptr<TableRowWriter> customer_table_writer;


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
	   vector<PART> vec_PART=PART_read();
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
	    vector<CUSTOMER> vec_CUSTOMER=CUSTOMER_read();
  for(int j=0;j<vec_CUSTOMER.size();j++){
  test.AddCustomerData(
   vec_CUSTOMER[j].C_CUSTKEY,
   vec_CUSTOMER[j].C_NAME,
   vec_CUSTOMER[j].C_ADDRESS,
   vec_CUSTOMER[j].C_NATIONKEY,
   vec_CUSTOMER[j].C_PHONE,
   vec_CUSTOMER[j].C_ACCTBAL,
   vec_CUSTOMER[j].C_MKTSEGMENT,
   vec_CUSTOMER[j].C_COMMENT);
 }
  vec_CUSTOMER.clear();
  vector<CUSTOMER>(vec_CUSTOMER).swap(vec_CUSTOMER);
	  double loadtime_finish = (double)clock();
	 	  std::cout<<"Loaddata time is :"<<(loadtime_finish-loadtime_start)/CLOCKS_PER_SEC<<"s"<<std::endl;
	 	//  for(int i =0;i<4;i++)
		//  {
	 	 struct timespec start, end;
		  clock_gettime(CLOCK_REALTIME, &start);
		 // double time_start = (double)clock();
		  test.TestResults();
		  test.TestResults3();
		 test.TestResults12();
		  test.TestResults14();
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











