#!/bin/sh

JDBC_URL="jdbc:hive2://hs2-difin-dwx-dev.dw-difin-dwx-dev.svbr-nqvp.int.cldr.work/default;transportMode=http;httpPath=cliservice;socketTimeout=60;ssl=true"
USER=csso_dfingerman
PASS=ASDqwe12#

DB=tpcds_partitioned_parquet_1000_iceberg

# shellcheck disable=SC2116
for TABLE in $(echo call_center catalog_page catalog_returns catalog_sales customer customer_address customer_demographics date_dim household_demographics income_band inventory item promotion reason ship_mode store store_returns store_sales time_dim warehouse web_page web_returns web_sales web_site)
do
  $HIVE_HOME/bin/beeline -u ${JDBC_URL} -n ${USER} -p ${PASS} --hivevar DB=${DB} --hivevar TABLE=${TABLE} -f count_star.sql
done
