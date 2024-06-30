#!/bin/bash
hive_conf="set hive.exec.dynamic.partition.mode=nonstrict;SET hive.exec.max.dynamic.partitions=100000;SET hive.exec.max.dynamic.partitions.pernode=100000;"

drop_table=" DROP TABLE IF EXISTS CustomerSummaryOutput;"

create_table_sql="create table CustomerSummaryOutput (
 customerNumber int
,customerName string 
, OrderAmount	decimal(10,2)
, paymentAmount decimal(10,2)
)
  row format delimited 
  fields terminated by ',' 
  stored as textfile;"

insert_query=" insert into CustomerSummaryOutput
select 
	 o.customerNumber
	, cx.CustomerName
	, sum(od.quantityordered * od.priceeach) as OrderAmount
	, py.amount as PaymentAmount
from orders o 
	join orderdetails od on (o.ordernumber = od.ordernumber)
	join customers cx on 	(o.customerNumber = cx.customerNumber)
	join payments py on 	(py.customerNumber = cx.customerNumber)
	
group by 
	 o.customerNumber
	, cx.CustomerName
	, py.amount 
;"

query="select count(*) from salesSummary;"

hive -e "$hive_conf $drop_table $create_table_sql $insert_query $query"