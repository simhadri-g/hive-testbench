JDBC_URL="jdbc:hive2://localhost:10000/"
USER=hive
PASS=hive

FORMAT=orc
TARGET_DB=tpcds_partitioned_${FORMAT}_1000_external_db
REDUCERS=2500

sudo rm -fr ${TARGET_DB}
sudo mkdir ${TARGET_DB}
COUNT_OUT=counts.txt

for TABLE in $(echo call_center catalog_page catalog_returns catalog_sales customer customer_address customer_demographics date_dim household_demographics income_band inventory item promotion reason ship_mode store store_returns store_sales time_dim warehouse web_page web_returns web_sales web_site)
do
  $HIVE_HOME/bin/beeline -u ${JDBC_URL} -n ${USER} -p ${PASS} --hivevar DB=${TARGET_DB} --hivevar TABLE=${TABLE} -f count_star.sql >> ${TARGET_DB}/${COUNT_OUT}
done