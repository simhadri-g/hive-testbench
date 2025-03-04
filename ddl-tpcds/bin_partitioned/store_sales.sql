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

drop table if exists store_sales;

create external table store_sales
(
      ss_sold_time_sk bigint
,     ss_item_sk bigint
,     ss_customer_sk bigint
,     ss_cdemo_sk bigint
,     ss_hdemo_sk bigint
,     ss_addr_sk bigint
,     ss_store_sk bigint
,     ss_promo_sk bigint
,     ss_ticket_number bigint
,     ss_quantity int
,     ss_wholesale_cost decimal(7,2)
,     ss_list_price decimal(7,2)
,     ss_sales_price decimal(7,2)
,     ss_ext_discount_amt decimal(7,2)
,     ss_ext_sales_price decimal(7,2)
,     ss_ext_wholesale_cost decimal(7,2)
,     ss_ext_list_price decimal(7,2)
,     ss_ext_tax decimal(7,2)
,     ss_coupon_amt decimal(7,2)
,     ss_net_paid decimal(7,2)
,     ss_net_paid_inc_tax decimal(7,2)
,     ss_net_profit decimal(7,2)
)
partitioned by (ss_sold_date_sk bigint)
${STORED_BY}
stored as ${FILE}
${TABLE_PROPS};

from ${SOURCE}.store_sales ss
insert overwrite table store_sales 
-- partition (ss_sold_date_sk) 
select
        ss.ss_sold_time_sk,
        ss.ss_item_sk,
        ss.ss_customer_sk,
        ss.ss_cdemo_sk,
        ss.ss_hdemo_sk,
        ss.ss_addr_sk,
        ss.ss_store_sk,
        ss.ss_promo_sk,
        ss.ss_ticket_number,
        ss.ss_quantity,
        ss.ss_wholesale_cost,
        ss.ss_list_price,
        ss.ss_sales_price,
        ss.ss_ext_discount_amt,
        ss.ss_ext_sales_price,
        ss.ss_ext_wholesale_cost,
        ss.ss_ext_list_price,
        ss.ss_ext_tax,
        ss.ss_coupon_amt,
        ss.ss_net_paid,
        ss.ss_net_paid_inc_tax,
        ss.ss_net_profit,
        ss.ss_sold_date_sk
        where ss.ss_sold_date_sk is not null
;

from ${SOURCE}.store_sales ss
insert overwrite table store_sales 
--partition (ss_sold_date_sk) 
select
        ss.ss_sold_time_sk,
        ss.ss_item_sk,
        ss.ss_customer_sk,
        ss.ss_cdemo_sk,
        ss.ss_hdemo_sk,
        ss.ss_addr_sk,
        ss.ss_store_sk,
        ss.ss_promo_sk,
        ss.ss_ticket_number,
        ss.ss_quantity,
        ss.ss_wholesale_cost,
        ss.ss_list_price,
        ss.ss_sales_price,
        ss.ss_ext_discount_amt,
        ss.ss_ext_sales_price,
        ss.ss_ext_wholesale_cost,
        ss.ss_ext_list_price,
        ss.ss_ext_tax,
        ss.ss_coupon_amt,
        ss.ss_net_paid,
        ss.ss_net_paid_inc_tax,
        ss.ss_net_profit,
        ss.ss_sold_date_sk
        where ss.ss_sold_date_sk is null
        sort by ss.ss_sold_date_sk
;
