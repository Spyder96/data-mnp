#!/bin/bash

hive_conf="set hive.exec.dynamic.partition.mode=nonstrict;SET hive.exec.max.dynamic.partitions=100000;SET hive.exec.max.dynamic.partitions.pernode=100000;"

drop_table=" DROP TABLE IF EXISTS ProductSummary;"

create_table_sql="create table ProductSummary (
 ProductCode string
,ProductName string 
, ProductLine 		string
, OrderAmount	decimal(10,2)
) partitioned by (orderYear int, orderMonth int)
 row format delimited 
  fields terminated by ',' 
  stored as textfile;"

insert_data_4b=" insert into ProductSummary
 partition (OrderYear, OrderMonth) 
select 
	  p.ProductCode
	, p.ProductName
	, p.ProductLine
	, year(orderDate) as OrderYear 
	, month(orderDate) as OrderMonth
	, sum(od.quantityordered * od.priceeach) as sales
	
from orders o 
	join orderdetails od on (o.ordernumber = od.ordernumber)
	join products p on 	(od.ProductCode = p.ProductCode)
	
group by 
	 p.ProductCode
	, p.ProductName
	, p.ProductLine
	, year(orderDate)  
	, month(orderDate) 
;"

query="select count(*) from ProductSummary;"

hive -e "$hive_conf $drop_table $create_table_sql $insert_data_4b $query"

