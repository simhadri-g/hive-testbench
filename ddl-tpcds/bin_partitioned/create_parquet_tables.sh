#!/bin/sh

JDBC_URL="jdbc:hive2://localhost:10000/"
USER=
PASS=

FORMAT=orc
TARGET_DB=tpcds_partitioned_${FORMAT}_1000_external
SOURCE_DB=tpcds_1000_text
REDUCERS=2500
PROPS="TBLPROPERTIES ('engine.hive.enabled'='true', 'format-version'='2')"
ICEBERG=false

# shellcheck disable=SC2116
for SQL_FILE in $(echo call_center.sql catalog_page.sql catalog_returns.sql catalog_sales.sql customer.sql customer_address.sql customer_demographics.sql date_dim.sql household_demographics.sql income_band.sql inventory.sql item.sql promotion.sql reason.sql ship_mode.sql store.sql store_returns.sql store_sales.sql time_dim.sql warehouse.sql web_page.sql web_returns.sql web_sales.sql web_site.sql)
do
  if [ "${ICEBERG}" == "false" ] 
  then
    $HIVE_HOME/bin/beeline -u ${JDBC_URL} -n ${USER} -p ${PASS} --hivevar FILE=${FORMAT} --hivevar SOURCE=${SOURCE_DB} --hivevar DB=${TARGET_DB} --hivevar REDUCERS=${REDUCERS} --hivevar STORED_BY="" --hivevar TABLE_PROPS="" -f $SQL_FILE
  else
    $HIVE_HOME/bin/beeline -u ${JDBC_URL} -n ${USER} -p ${PASS} --hivevar FILE=${FORMAT} --hivevar SOURCE=${SOURCE_DB} --hivevar DB=${TARGET_DB} --hivevar REDUCERS=${REDUCERS} --hivevar STORED_BY="STORED BY ICEBERG" --hivevar TABLE_PROPS="${PROPS}" -f $SQL_FILE
  fi
done

$HIVE_HOME/bin/beeline -u ${JDBC_URL} -n ${USER} -p ${PASS} --hivevar DB=${TARGET_DB} --hivevar REDUCERS=${REDUCERS} -f analyze.sql

rm -fr ${TARGET_DB}
mkdir ${TARGET_DB}
COUNT_OUT=counts.txt

for TABLE in $(echo call_center catalog_page catalog_returns catalog_sales customer customer_address customer_demographics date_dim household_demographics income_band inventory item promotion reason ship_mode store store_returns store_sales time_dim warehouse web_page web_returns web_sales web_site)
do
  $HIVE_HOME/bin/beeline -u ${JDBC_URL} -n ${USER} -p ${PASS} --hivevar DB=${TARGET_DB} --hivevar TABLE=${TABLE} -f count_star.sql >> ${TARGET_DB}/${COUNT_OUT}
done