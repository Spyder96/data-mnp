#!/bin/bash

# Constants
jdbcconnection="jdbc:mysql://localhost:3306/classicmodels"
password="hadoop"
username="root"
targetfolder="/home/labuser/database/classicmodels/"
mappingpartitions=2

echo "Starting Hadoop and YARN services"

#start-all.sh
#mapred --daemon start historyserver


# List tables
echo "Tables in Mysql"
tablelist=`sqoop list-tables --connect $jdbcconnection --password $password --username $username 2>&1 | grep -v -e "2024" -e "Warning:" -e "jdbc.Driver" -e "root of your"`
echo "Tables found : $tablelist"

echo "Loading Tables to hdfs using sqoop"

for table in $tablelist

do
  echo -e "****\n\nTable name = $table , target dir = $targetfolder$table\n\n****"
  if [ "$table" = "productlines" ]; 
    then 
    sqoop import -Dorg.apache.sqoop.splitter.allow_text_splitter=true --connect $jdbcconnection --password $password --username $username --table $table -m $mappingpartitions  --target-dir $targetfolder$table --delete-target-dir --split-by textDescription > /dev/null  #condition for productlines failure resolve
  else
    sqoop import -Dorg.apache.sqoop.splitter.allow_text_splitter=true --connect $jdbcconnection --password $password --username $username --table $table -m $mappingpartitions  --target-dir $targetfolder$table --delete-target-dir > /dev/null  
  fi

echo "Import Complete"
echo "****************************************"
done
### Failures productlines fixed in if condition by changing column Set using
### ALTER TABLE productlines MODIFY textDescription VARCHAR(4000) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
### and then using split-by on the column


for table in $tablelist;  
do   
mysql -h hdpserver -u hive -phadoop "classicmodels" -s -e "show create table $table;" | sed -n '1p' | cut -f2- | sed 's/\\n/\'$'\n''/g' |sed 's/ENGINE=InnoDB DEFAULT CHARSET=latin1//' | sed 's/TableCreate Table//'  > mysql_ddl_$table.sql
grep -v "mysql: [Warning] Using a password on the command line interface can be insecure." mysql_ddl_$table.sql


# Columns for Hive table creation
columns=`mysql -h hdpserver -u hive -phadoop "classicmodels" --silent -e "SELECT column_name FROM information_schema.columns WHERE table_name='$table';" | grep -v "COLUMN_NAME" | sed ':a;N;$!ba;s/\n/, /g'`

#Creating statements for Hive table
echo  "CREATE TABLE $table ( $columns ) row format delimited fields terminated by "," stored as textfile location $targetfolder$table;" 


done 


for table in $tablelist;  
do 
# Columns for Hive table creation
columns=`mysql -h hdpserver -u hive -phadoop "classicmodels" --silent -e "SELECT column_name, data_type FROM information_schema.columns WHERE table_name='$table';" | grep -v "COLUMN_NAME" | sed ':a;N;$!ba;s/\n/, /g' | sed 's\decimal\decimal(10,2)\g' | sed 's\varchar\string\g' | sed 's/\t/ /g'`

#Creating External Hive tables
hive -e "DROP TABLE IF EXISTS $table"
echo "Table name = $table"
hive -e "CREATE TABLE  $table ( $columns ) row format delimited fields terminated by \",\" stored as textfile location \"$targetfolder$table\";"

done
 #### Failures: orders, products, productlines

## Fixing Failed list, fails are due to column type changes, this was done manually

fail_list="
orders
products
productlines
"
for table in $fail_list;  
do 
# Columns for Hive table creation
  columns=`mysql -h hdpserver -u hive -phadoop "classicmodels" --silent -e "SELECT column_name, data_type FROM information_schema.columns WHERE table_name='$table';" | grep -v "COLUMN_NAME" | sed ':a;N;$!ba;s/\n/, /g' | sed 's\decimal\decimal(10,2)\g' | sed 's\varchar\string\g' | sed 's/\t/ /g'`

#Creating External Hive tables
#hive -e "DROP TABLE IF EXISTS $table"
  echo "Table name = $table"
  echo "CREATE EXTERNAL TABLE  $table ( $columns ) row format delimited fields terminated by \",\" stored as textfile location \"$targetfolder$table\";"

done

####
hive -e "DROP  TABLE IF EXISTS orders;"
hive -e "CREATE external TABLE  orders ( orderNumber int, orderDate date, requiredDate date, shippedDate date, status string, comments string, customerNumber int ) row format delimited fields terminated by ',' stored as textfile location '/home/labuser/database/classicmodels/orders';"

hive -e "DROP TABLE IF EXISTS products;"
hive -e "CREATE TABLE  products ( productCode string, productName string, productLine string, productScale string, productVendor string, productDescription string, quantityInStock int, buyPrice decimal(10,2), MSRP decimal(10,2) ) row format delimited fields terminated by ',' stored as textfile location '/home/labuser/database/classicmodels/products';"


hive -e "DROP TABLE IF EXISTS productlines;"
hive -e "CREATE TABLE  productlines ( productLine string, textDescription string, htmlDescription string, image string ) row format delimited fields terminated by ',' stored as textfile location '/home/labuser/database/classicmodels/productlines';"

#verifying table record counts
for table in $tablelist;  
do 
  echo -e "\t\tTable name = $table\n\n"
  hive_output=$(hive -e "SELECT COUNT(*) AS count FROM $table;" 2>&1| tail -5)
  
  echo -e "Output for table $table:\n$hive_output"
  echo "****************************************"
done


######## Partioning
hive_conf="set hive.exec.dynamic.partition.mode=nonstrict;"
drop_table="drop table if exists ordersummary;"
create_table_sql="create table ordersummary (
 orderNumber int, 
 orderdate date,
 productCode string, 
 quantityOrdered int, 
 priceEach decimal(10,2), 
 amount decimal(10,2),    
 customerNumber int
) partitioned by (orderYear int, orderMonth int)
  stored as parquet;"


insert_data="insert into ordersummary 
partition(orderYear, orderMonth)
select o.ordernumber, o.orderdate, od.productcode, od.quantityordered, od.priceEach, (od.quantityordered * od.priceeach) as amount, o.customerNumber, year(orderDate), month(orderDate) from orders o join orderdetails od on (o.ordernumber = od.ordernumber);"

count_query="select count(*) from ordersummary;"
hive -e "$hive_conf $drop_table $create_table_sql $insert_data $count_query"
