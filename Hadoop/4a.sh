#!/bin/bash

hive_conf="set hive.exec.dynamic.partition.mode=nonstrict;SET hive.exec.max.dynamic.partitions=100000;SET hive.exec.max.dynamic.partitions.pernode=100000;"

drop_table=" DROP TABLE IF EXISTS SalesSummary;"
create_table_4asql="create table SalesSummary (
 salesRepCode int, 
 salesRepName string
, Sales	decimal(10,2)
) partitioned by (orderYear int, orderMonth int)
  stored as parquet;"
  
 insert_data_4a=" insert into salesSummary
 partition (OrderYear, OrderMonth)
select 
	emp.employeeNumber
	, CONCAT(emp.lastName , ' ' , emp.firstName) as employeeName
	, sum(od.quantityordered * od.priceeach) as Sales
	, year(orderDate) as OrderYear 
	, month(orderDate) as OrderMonth
	
	
from orders o 
	join orderdetails od on (o.ordernumber = od.ordernumber)
	join customers cx on 	(o.customerNumber = cx.customerNumber)
	join employees emp on (cx.salesRepEmployeeNumber = emp.employeeNumber)
group by
	emp.employeeNumber
	, CONCAT(emp.lastName , ' ' , emp.firstName)
	, year(orderDate)
	, month(orderDate);"

query="select count(*) from salesSummary;"

hive -e "$hive_conf $drop_table $create_table_4asql $insert_data_4a $query"