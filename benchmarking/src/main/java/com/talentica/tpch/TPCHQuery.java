/*
 * *****************************************************************************
 *   Copyright 2017 Talentica Software Pvt. Ltd.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *  *****************************************************************************
 */

package com.talentica.tpch;

/**
 * Created by rajkishoreh on 27/6/17.
 */

public interface TPCHQuery {

    String sql1 = "select L_RETURNFLAG,L_LINESTATUS,sum(L_QUANTITY) as sum_qty,"
            + "sum(L_EXTENDEDPRICE) as sum_base_price,sum(L_EXTENDEDPRICE*(1-L_DISCOUNT)) as sum_disc_price,"
            + "sum(L_EXTENDEDPRICE*(1-L_DISCOUNT)*(1+L_TAX)) as sum_charge,avg(L_QUANTITY) as avg_qty,"
            + "avg(L_EXTENDEDPRICE) as avg_price,avg(L_DISCOUNT) as avg_disc,count(*) as count_order "
            + "from LINEITEM "
            + "where "
            + "L_SHIPDATE <= date '1998-12-01' - interval '90' day "
            + "group by L_RETURNFLAG,L_LINESTATUS "
            + "order by L_RETURNFLAG,L_LINESTATUS";

    String  sql2 = "select S_ACCTBAL,S_NAME,N_NAME,P_PARTKEY,P_MFGR,S_ADDRESS,S_PHONE,S_COMMENT "
            + "from PART,SUPPLIER,PARTSUPP,NATION,REGION "
            + "where P_PARTKEY = PS_PARTKEY "
            + "and S_SUPPKEY = PS_SUPPKEY "
            + "and P_SIZE = 15 "
            + "and P_TYPE like '%BRASS' "
            + "and S_NATIONKEY = N_NATIONKEY "
            + "and N_REGIONKEY = R_REGIONKEY "
            + "and R_NAME = 'EUROPE' "
            + "and PS_SUPPLYCOST = ( "
            + "select min(PS_SUPPLYCOST) "
            + "from PARTSUPP, SUPPLIER,NATION, REGION "
            + "where P_PARTKEY = PS_PARTKEY "
            + "and S_SUPPKEY = PS_SUPPKEY "
            + "and S_NATIONKEY = N_NATIONKEY "
            + "and N_REGIONKEY = R_REGIONKEY "
            + "and R_NAME = 'EUROPE' ) "
            + "order by S_ACCTBAL desc,N_NAME,S_NAME,P_PARTKEY";


    String sql3= "select L_ORDERKEY,sum(L_EXTENDEDPRICE*(1-L_DISCOUNT)) as revenue,O_ORDERDATE,O_SHIPPRIORITY "
            + "from CUSTOMER,ORDERS,LINEITEM "
            + "where C_MKTSEGMENT = 'BUILDING' "
            + "and C_CUSTKEY = O_CUSTKEY "
            + "and L_ORDERKEY = O_ORDERKEY "
            + "and O_ORDERDATE < date '1995-03-15' "
            + "and L_SHIPDATE > date '1995-03-15' "
            + "group by L_ORDERKEY,O_ORDERDATE,O_SHIPPRIORITY "
            + "order by revenue desc,O_ORDERDATE";

    String sql4 = "select O_ORDERPRIORITY,count(*) as order_count "
            + "from ORDERS "
            + "where O_ORDERDATE >= date '1993-07-01' "
            + "and O_ORDERDATE < date '1993-07-01' + interval '3' month "
            + "and exists ( "
            + "select * "
            + "from LINEITEM "
            + "where L_ORDERKEY = O_ORDERKEY "
            + "and L_COMMITDATE < L_RECEIPTDATE ) "
            + "group by O_ORDERPRIORITY "
            + "order by O_ORDERPRIORITY";

    String sql5 = "select N_NAME,sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as revenue "
            + "from CUSTOMER,ORDERS,LINEITEM,SUPPLIER,NATION,REGION "
            + "where C_CUSTKEY = O_CUSTKEY "
            + "and L_ORDERKEY = O_ORDERKEY "
            + "and L_SUPPKEY = S_SUPPKEY "
            + "and C_NATIONKEY = S_NATIONKEY "
            + "and S_NATIONKEY = N_NATIONKEY "
            + "and N_REGIONKEY = R_REGIONKEY "
            + "and R_NAME = 'ASIA' "
            + "and O_ORDERDATE >= date '1994-01-01' "
            + "and O_ORDERDATE < date '1994-01-01' + interval '1' year "
            + "group by N_NAME "
            + "order by revenue desc";

    String sql6 = "select sum(L_EXTENDEDPRICE*L_DISCOUNT) as revenue "
            + "from LINEITEM "
            + "where L_SHIPDATE >= date '1994-01-01' "
            + "and L_SHIPDATE < date '1994-01-01' + interval '1' year "
            + "and L_DISCOUNT between 0.06 - 0.01 and 0.06 + 0.01 "
            + "and L_QUANTITY < 24";

    String subquerySql7 = "select n1.N_NAME as supp_nation,n2.N_NAME as cust_nation,year(L_SHIPDATE) as l_year,"
            + "L_EXTENDEDPRICE * (1 - L_DISCOUNT) as volume "
            + "from SUPPLIER,LINEITEM,ORDERS,CUSTOMER,NATION n1,NATION n2 "
            + "where S_SUPPKEY = L_SUPPKEY "
            + "and O_ORDERKEY = L_ORDERKEY "
            + "and C_CUSTKEY = O_CUSTKEY "
            + "and S_NATIONKEY = n1.N_NATIONKEY "
            + "and C_NATIONKEY = n2.N_NATIONKEY "
            + "and ( (n1.N_NAME = 'FRANCE' and n2.N_NAME = 'GERMANY') "
            + "or (n1.N_NAME = 'GERMANY' and n2.N_NAME = 'FRANCE')) "
            + "and L_SHIPDATE between date '1995-01-01' and date '1996-12-31' ";

    String sqlWithSubquerySql7= "select supp_nation,cust_nation,l_year, sum(volume) as revenue "
            + "from shipping "
            + "group by supp_nation,cust_nation,l_year "
            + "order by supp_nation,cust_nation,l_year";

    String sql8 = "select o_year,sum(case when nation = 'BRAZIL' then volume else 0 end) / sum(volume) as mkt_share "
            + "from ( "
            + "select year(O_ORDERDATE) as o_year,L_EXTENDEDPRICE * (1-L_DISCOUNT) as volume,n2.N_NAME as nation "
            + "from PART,SUPPLIER,LINEITEM,ORDERS,CUSTOMER,NATION n1,NATION n2,REGION "
            + "where P_PARTKEY = L_PARTKEY "
            + "and S_SUPPKEY = L_SUPPKEY "
            + "and L_ORDERKEY = O_ORDERKEY "
            + "and O_CUSTKEY = C_CUSTKEY "
            + "and C_NATIONKEY = n1.N_NATIONKEY "
            + "and n1.N_REGIONKEY = R_REGIONKEY "
            + "and R_NAME = 'AMERICA' "
            + "and S_NATIONKEY = n2.N_NATIONKEY "
            + "and O_ORDERDATE between date '1995-01-01' and date '1996-12-31' "
            + "and P_TYPE = 'ECONOMY ANODIZED STEEL' ) as all_nations "
            + "group by o_year "
            + "order by o_year";

    String sql9 = "select nation,o_year,sum(amount) as sum_profit "
            + "from ( "
            + "select N_NAME as nation,year(O_ORDERDATE) as o_year,"
            + "L_EXTENDEDPRICE * (1 - L_DISCOUNT) - PS_SUPPLYCOST * L_QUANTITY as amount "
            + "from PART,SUPPLIER,LINEITEM,PARTSUPP,ORDERS,NATION "
            + "where S_SUPPKEY = L_SUPPKEY "
            + "and PS_SUPPKEY = L_SUPPKEY "
            + "and PS_PARTKEY = L_PARTKEY "
            + "and P_PARTKEY = L_PARTKEY "
            + "and O_ORDERKEY = L_ORDERKEY "
            + "and S_NATIONKEY = N_NATIONKEY "
            + "and P_NAME like '%green%' ) as profit "
            + "group by nation,o_year "
            + "order by nation,o_year desc";

    String sql10 = "select C_CUSTKEY,C_NAME,sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as revenue,C_ACCTBAL,"
            + "N_NAME,C_ADDRESS,C_PHONE,C_COMMENT "
            + "from CUSTOMER,ORDERS,LINEITEM,NATION "
            + "where C_CUSTKEY = O_CUSTKEY "
            + "and L_ORDERKEY = O_ORDERKEY "
            + "and O_ORDERDATE >= date '1993-10-01' "
            + "and O_ORDERDATE < date '1993-10-01' + interval '3' month "
            + "and L_RETURNFLAG = 'R' "
            + "and C_NATIONKEY = N_NATIONKEY "
            + "group by C_CUSTKEY,C_NAME,C_ACCTBAL,C_PHONE,N_NAME,C_ADDRESS,C_COMMENT "
            + "order by revenue desc";

    String sql11 = "select PS_PARTKEY,sum(PS_SUPPLYCOST * PS_AVAILQTY) as value "
            + "from PARTSUPP,SUPPLIER,NATION "
            + "where PS_SUPPKEY = S_SUPPKEY and S_NATIONKEY = N_NATIONKEY and N_NAME = 'GERMANY' "
            + "group by PS_PARTKEY having sum(PS_SUPPLYCOST * PS_AVAILQTY) > "
            + "( select sum(PS_SUPPLYCOST * PS_AVAILQTY) * 0.0001 "
            + "from PARTSUPP,SUPPLIER,NATION "
            + "where PS_SUPPKEY = S_SUPPKEY and S_NATIONKEY = N_NATIONKEY and N_NAME = 'GERMANY' ) "
            + " order by value desc";

    String sql12 = "select L_SHIPMODE,sum(case when O_ORDERPRIORITY ='1-URGENT' or O_ORDERPRIORITY ='2-HIGH' then 1 else 0 end)"
            + " as high_line_count,"
            + "sum(case when O_ORDERPRIORITY <> '1-URGENT' and O_ORDERPRIORITY <> '2-HIGH' then 1 else 0 end) "
            + "as low_line_count "
            + "from ORDERS,LINEITEM "
            + "where O_ORDERKEY = L_ORDERKEY "
            + "and L_SHIPMODE in ('MAIL', 'SHIP') "
            + "and L_COMMITDATE < L_RECEIPTDATE "
            + "and L_SHIPDATE < L_COMMITDATE "
            + "and L_RECEIPTDATE >= date '1994-01-01' "
            + "and L_RECEIPTDATE < date '1994-01-01' + interval '1' year "
            + "group by L_SHIPMODE "
            + "order by L_SHIPMODE";

    String sql13 = "select c_orders.c_count, count(*) as custdist "
            + "from ( "
            + "select C_CUSTKEY,count(O_ORDERKEY) as c_count "
            + "from CUSTOMER left outer join ORDERS on "
            + "C_CUSTKEY = O_CUSTKEY "
            + "and O_COMMENT not like '%special%requests%' "
            + "group by C_CUSTKEY ) as c_orders "
            + "group by c_count "
            + "order by custdist desc,c_count desc";

    String sql14 = "select 100.00 * sum("
            + "case when P_TYPE like 'PROMO%' then L_EXTENDEDPRICE*(1-L_DISCOUNT) else 0 end)"
            + " / sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as promo_revenue " + "from LINEITEM,PART "
            + "where L_PARTKEY = P_PARTKEY " + "and L_SHIPDATE >= date '1995-09-01' "
            + "and L_SHIPDATE < date '1995-09-01' + interval '1' month";

    String sqlView15 ="SELECT L_SUPPKEY as SUPPLIER_NO, SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)) as TOTAL_REVENUE "
            + "FROM LINEITEM WHERE L_SHIPDATE >= date '1996-01-01' AND L_SHIPDATE < date '1996-01-01' + interval '3' month GROUP BY L_SUPPKEY";

    String sqlWithView15 = "SELECT S_SUPPKEY, S_NAME, S_ADDRESS, S_PHONE, TOTAL_REVENUE FROM SUPPLIER, REVENUE0 WHERE S_SUPPKEY = "
            + "SUPPLIER_NO AND TOTAL_REVENUE = (SELECT MAX(TOTAL_REVENUE) FROM REVENUE0) ORDER BY S_SUPPKEY";

    String sql16 = "select P_BRAND,P_TYPE,P_SIZE,count(distinct PS_SUPPKEY) as supplier_cnt "
            + "from PARTSUPP,PART "
            + "where P_PARTKEY = PS_PARTKEY "
            + "and P_BRAND <> 'Brand#45' "
            + "and P_TYPE not like 'MEDIUM POLISHED%' "
            + "and P_SIZE in (49, 14, 23, 45, 19, 3, 36, 9) "
            + "and PS_SUPPKEY not in ( "
            + "select S_SUPPKEY "
            + "from SUPPLIER "
            + "where S_COMMENT like '%Customer%Complaints%' ) "
            + "group by P_BRAND,P_TYPE,P_SIZE "
            + "order by supplier_cnt desc,P_BRAND,P_TYPE,P_SIZE";

    String sql17 = "select sum(L_EXTENDEDPRICE) / 7.0 as avg_yearly "
            + "from LINEITEM,PART "
            + "where P_PARTKEY = L_PARTKEY "
            + "and P_BRAND = 'Brand#23' "
            + "and P_CONTAINER = 'MED BOX' "
            + "and L_QUANTITY < ( "
            + "select 0.2 * avg(L_QUANTITY) "
            + "from LINEITEM "
            + "where L_PARTKEY = P_PARTKEY )";

    String sql18 = "select C_NAME,C_CUSTKEY,O_ORDERKEY,O_ORDERDATE,O_TOTALPRICE,sum(L_QUANTITY) "
            + "from CUSTOMER,ORDERS,LINEITEM "
            + "where O_ORDERKEY in ( "
            + "select L_ORDERKEY "
            + "from LINEITEM "
            + "group by L_ORDERKEY having "
            + "sum(L_QUANTITY) > 300 ) "
            + "and C_CUSTKEY = O_CUSTKEY "
            + "and O_ORDERKEY = L_ORDERKEY "
            + "group by C_NAME,C_CUSTKEY,O_ORDERKEY,O_ORDERDATE,O_TOTALPRICE "
            + "order by O_TOTALPRICE desc,O_ORDERDATE";

    String sql19 = "select sum(l_extendedprice * (1 - l_discount) ) as revenue from LINEITEM, PART where "
            + "( p_partkey = l_partkey and p_brand = 'Brand#12' and p_container in ( 'SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') "
            + "and l_quantity >= 1 and l_quantity <= 11 and p_size between 1 and 5 and l_shipmode in ('AIR', 'AIR REG') "
            + "and l_shipinstruct = 'DELIVER IN PERSON') or (p_partkey = l_partkey and p_brand = 'Brand#23' "
            + "and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') and l_quantity >= 10 "
            + "and l_quantity <= 20 and p_size between 1 and 10 and l_shipmode in ('AIR', 'AIR REG') "
            + "and l_shipinstruct = 'DELIVER IN PERSON') or (p_partkey = l_partkey and p_brand = 'Brand#34' "
            + "and p_container in ( 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') and l_quantity >= 20 "
            + "and l_quantity <= 30 and p_size between 1 and 15 and l_shipmode in ('AIR', 'AIR REG') "
            + "and l_shipinstruct = 'DELIVER IN PERSON')";

    String sql20= "select s_name,s_address from SUPPLIER,NATION where s_suppkey in "
            + "( select ps_suppkey from PARTSUPP where ps_partkey in ( select p_partkey from PART where p_name like 'forest%' ) "
            + "and ps_availqty > ( select 0.5 * sum(l_quantity) from LINEITEM where l_partkey = ps_partkey "
            + "and l_suppkey = ps_suppkey and l_shipdate >= date '1994-01-01'  "
            + "and l_shipdate < date '1994-01-01'  + interval '1' year ) ) and s_nationkey = n_nationkey "
            + "and n_name = 'CANADA' order by s_name";

    String sql21 = "select s_name, count(*) as numwait from SUPPLIER, LINEITEM l1, ORDERS, NATION where s_suppkey = l1.l_suppkey "
            + "and o_orderkey = l1.l_orderkey and o_orderstatus = 'F' and l1.l_receiptdate > l1.l_commitdate and "
            + "exists ( select * from LINEITEM l2 where l2.l_orderkey = l1.l_orderkey and l2.l_suppkey <> l1.l_suppkey ) "
            + "and not exists ( select * from LINEITEM l3 where l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey "
            + "and l3.l_receiptdate > l3.l_commitdate ) and s_nationkey = n_nationkey and n_name = 'SAUDI ARABIA' "
            + "group by s_name order by numwait desc,s_name";

    String sql22 = "select cntrycode, count(*) as numcust, sum(c_acctbal) as totacctbal from ( select substring( c_phone , 1 , 2 ) as "
            + "cntrycode,c_acctbal from CUSTOMER where substring( c_phone , 1 , 2 ) in ( '13', '31', '23', '29', '30', '18', '17' ) and c_acctbal > "
            + "( select avg( c_acctbal ) from CUSTOMER where c_acctbal > 0.00 and substring( c_phone , 1 , 2 ) in ( '13', '31', '23', '29', '30', '18', '17' ) ) "
            + "and not exists ( select * from ORDERS where o_custkey = c_custkey ) ) as custsale group by cntrycode order by cntrycode";


}