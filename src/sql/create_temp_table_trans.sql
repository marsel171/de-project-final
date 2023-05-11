drop table if exists table_name;


create table table_name (
    operation_id varchar(60) not null,
    account_number_from bigint null,
    account_number_to bigint null,
    currency_code int null,
    country varchar(30) null,
    status varchar(30) null,
    transaction_type varchar(30) null,
    amount bigint null,
    transaction_dt timestamp(0) null,
    load_id identity
                        )
ORDER BY transaction_dt segmented by hash(operation_id, transaction_dt) all nodes
PARTITION BY transaction_dt::date
GROUP BY calendar_hierarchy_day(transaction_dt::date, 3, 2);
