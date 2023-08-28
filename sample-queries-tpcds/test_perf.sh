#!/bin/sh

JDBC_URL="jdbc:hive2://hs2-difin-dwx-dev.dw-difin-dwx-dev.svbr-nqvp.int.cldr.work/default;transportMode=http;httpPath=cliservice;socketTimeout=60;ssl=true"
USER=csso_dfingerman
PASS=ASDqwe12#

DB=tpcds_partitioned_parquet_1000_iceberg

rm -fr $DB
mkdir $DB

REPORT=$DB/report.txt
rm -fr $REPORT

# shellcheck disable=SC2116
for QUERY in `ls -1 query*.sql`
do
  echo "use $DB;" > tmp.sql
  echo "set hive.query.results.cache.enabled=false; set hive.tez.bloom.filter.merge.threads=0; set mapreduce.input.fileinputformat.list-status.num-threads=50; set hive.disable.unsafe.external.table.operations=true;" >> tmp.sql
  
  cat ${QUERY} >> tmp.sql
  QUERY_NUM=`echo ${QUERY} | cut -d "y" -f 2 | cut -d "." -f 1`
  
  for ITERATION in `echo 1 2 3 4`
  do
      OUTPUT=${DB}/${QUERY}.${ITERATION}
    
      ${HIVE_HOME}/bin/beeline -u ${JDBC_URL} -n ${USER} -p ${PASS} -f tmp.sql > ${OUTPUT} 2>&1
      status=$?
      
      if [ $status -ne 0 ]
      then
        echo Query ${QUERY} failed!
        echo "Query: ${QUERY_NUM}; iteration: ${ITERATION}; FAILED!" >> $REPORT
      else
        RUN_DAG=`cat ${OUTPUT} | grep "Run DAG" | tr -s ' ' | cut -d " " -f 5`
        TOTAL_TIME=`cat ${OUTPUT} | grep "rows selected" | tr -s ' ' | cut -d "(" -f 2 | cut -d " " -f 1`
        
        if [ -z "${TOTAL_TIME}" ]
        then
          TOTAL_TIME=`cat ${OUTPUT} | grep "row selected" | tr -s ' ' | cut -d "(" -f 2 | cut -d " " -f 1`
        fi
        
        echo "Query: ${QUERY_NUM}; iteration: ${ITERATION}; Run Dag: ${RUN_DAG}; Total time: ${TOTAL_TIME}" >> $REPORT
      fi
  done
    
  rm -f tmp.sql
  
done