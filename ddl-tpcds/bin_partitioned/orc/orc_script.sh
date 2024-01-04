JDBC_URL="jdbc:hive2://localhost:10000/"
USER=hive
PASS=hive

FORMAT=orc
TARGET_DB=tpcds_partitioned_${FORMAT}_1000_external
SOURCE_DB=tpcds_1000_text
REDUCERS=2500



# shellcheck disable=SC2116
for SQL_FILE in $(echo call_center.sql catalog_page.sql catalog_returns.sql catalog_sales.sql customer.sql customer_address.sql customer_demographics.sql date_dim.sql household_demographics.sql income_band.sql inventory.sql item.sql promotion.sql reason.sql ship_mode.sql store.sql store_returns.sql store_sales.sql time_dim.sql warehouse.sql web_page.sql web_returns.sql web_sales.sql web_site.sql)
do
    $HIVE_HOME/bin/beeline -u ${JDBC_URL} -n ${USER} -p ${PASS} --hivevar FILE=${FORMAT} --hivevar SOURCE=${SOURCE_DB} --hivevar DB=${TARGET_DB} --hivevar REDUCERS=${REDUCERS} --hivevar STORED_BY="" --hivevar TABLE_PROPS="" -f $SQL_FILE
done
