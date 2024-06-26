#!/bin/bash

# Constants
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



for table in $tablelist;  
do   
mysql -h hdpserver -u hive -phadoop "classicmodels" -s -e "show create table orders;" | sed -n '1p' | cut -f2- | sed 's/\\n/\'$'\n''/g' |sed 's/ENGINE=InnoDB DEFAULT CHARSET=latin1//' | sed 's/TableCreate Table//'  > mysql_ddl_$table.sql
grep -v "mysql: [Warning] Using a password on the command line interface can be insecure." mysql_ddl_$table.sql
 done 




