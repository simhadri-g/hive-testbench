set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.created.files=1000000;
set hive.exec.parallel=true;
set hive.exec.reducers.max=${REDUCERS};
set hive.stats.autogather=true;
-- set hive.optimize.sort.dynamic.partition=true;
-- set hive.optimize.sort.dynamic.partition=true;
set tez.runtime.empty.partitions.info-via-events.enabled=true;
set tez.runtime.report.partition.stats=true;
set hive.tez.auto.reducer.parallelism=true;
set hive.tez.min.partition.factor=0.01; 
set hive.optimize.sort.dynamic.partition.threshold=0;
set iceberg.mr.schema.auto.conversion=true;

create database if not exists ${DB};
use ${DB};

create external table web_returns
(
      wr_returned_time_sk bigint
,     wr_item_sk bigint
,     wr_refunded_customer_sk bigint
,     wr_refunded_cdemo_sk bigint
,     wr_refunded_hdemo_sk bigint
,     wr_refunded_addr_sk bigint
,     wr_returning_customer_sk bigint
,     wr_returning_cdemo_sk bigint
,     wr_returning_hdemo_sk bigint
,     wr_returning_addr_sk bigint
,     wr_web_page_sk bigint
,     wr_reason_sk bigint
,     wr_order_number bigint
,     wr_return_quantity int
,     wr_return_amt decimal(7,2)
,     wr_return_tax decimal(7,2)
,     wr_return_amt_inc_tax decimal(7,2)
,     wr_fee decimal(7,2)
,     wr_return_ship_cost decimal(7,2)
,     wr_refunded_cash decimal(7,2)
,     wr_reversed_charge decimal(7,2)
,     wr_account_credit decimal(7,2)
,     wr_net_loss decimal(7,2)
)
partitioned by (wr_returned_date_sk bigint)
${STORED_BY}
stored as ${FILE}
location "s3a://dfingerman-bucket/my-dl/warehouse/tablespace/external/hive/${DB}.db/web_returns"
${TABLE_PROPS}
;

MSCK REPAIR TABLE web_returns;
