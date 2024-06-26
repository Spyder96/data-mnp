#!/bin/bash

# MySQL:Constants
jdbcconnection="jdbc:mysql://localhost:3306/classicmodels"
password="hadoop"
username="root"
targetfolder="/home/labuser/classicmodels/"
mappingpartitions=2

echo "Starting Hadoop and YARN services"

start-all.sh
mapred --daemon start historyserver


# List tables
echo "Tables in Mysql"
tablelist=`sqoop list-tables --connect $jdbcconnection --password $password --username $username 2>&1 | grep -v -e "2024" -e "Warning:" -e "jdbc.Driver" -e "root of your"`
echo "Tables found : $tablelist"

echo "Loading Tables to hdfs using sqoop"

for table in $tablelist

do
  echo "Table name = $table , target dir = $targetfolder$table"
  sqoop import -Dorg.apache.sqoop.splitter.allow_text_splitter=true --connect $jdbcconnection --password $password --username $username --table $table -m $mappingpartitions  --target-dir $targetfolder$table --delete-target-dir > /dev/null  

echo "Import Complete"
echo "****************************************"
done
### Failures productlines


for table in $tablelist;  
do   
mysql -h hdpserver -u hive -phadoop "classicmodels" -s -e "show create table $table;" | sed -n '1p' | cut -f2- | sed 's/\\n/\'$'\n''/g' |sed 's/ENGINE=InnoDB DEFAULT CHARSET=latin1//' | sed 's/TableCreate Table//'  > mysql_ddl_$table.sql
grep -v "mysql: [Warning] Using a password on the command line interface can be insecure." mysql_ddl_$table.sql


done 


for table in $tablelist;  
do 
# Columns for Hive table creation
columns=`mysql -h hdpserver -u hive -phadoop "classicmodels" --silent -e "SELECT column_name, data_type FROM information_schema.columns WHERE table_name='$table';" | grep -v "COLUMN_NAME" | sed ':a;N;$!ba;s/\n/, /g' | sed 's\decimal\decimal(10,2)\g' | sed 's\varchar\string\g' | sed 's/\t/ /g'`

#Creating External Hive tables
hive -e "DROP TABLE IF EXISTS $table"
echo "Table name = $table"
hive -e "CREATE EXTERNAL TABLE  $table ( $columns ) row format delimited fields terminated by ',' stored as textfile location '$targetfolder$table';"

done
 #### Failures: orders, products, productlines

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
hive -e "CREATE external TABLE  orders ( orderNumber int, orderDate date, requiredDate date, shippedDate date, status string, comments string, customerNumber int ) row format delimited fields terminated by ',' stored as textfile location '/home/labuser/classicmodels/ext/orders';"

hive -e "DROP TABLE IF EXISTS products;"
hive -e "CREATE TABLE  products ( productCode string, productName string, productLine string, productScale string, productVendor string, productDescription string, quantityInStock int, buyPrice decimal(10,2), MSRP decimal(10,2) ) row format delimited fields terminated by ',' stored as textfile location '/home/labuser/classicmodels/products';"


hive -e "DROP TABLE IF EXISTS productlines;"
hive -e "CREATE TABLE  productlines ( productLine string, textDescription string, htmlDescription string, image string ) row format delimited fields terminated by ',' stored as textfile location '/home/labuser/classicmodels/productlines';"



