-- Создаем схему STAGING
CREATE SCHEMA IF NOT EXISTS YAMARSELR2015YANDEXRU__STAGING;

--Создаем таблицу transaction
drop table if exists YAMARSELR2015YANDEXRU__STAGING.transaction;


create table yamarselr2015yandexru__staging.transaction (
    operation_id varchar(60) not null,
    account_number_from bigint null,
    account_number_to bigint null,
    currency_code int null,
    country varchar(30) null,
    status varchar(30) null,
    transaction_type varchar(30) null,
    amount bigint null,
    transaction_dt timestamp(0) null,
    constraint transactions_pk primary key (operation_id, transaction_dt, status) enabled
                                                        )
ORDER BY transaction_dt segmented by hash(operation_id, transaction_dt) all nodes
PARTITION BY transaction_dt::date
GROUP BY calendar_hierarchy_day(transaction_dt::date, 3, 2);

--Создаем таблицу currencies
drop table if exists YAMARSELR2015YANDEXRU__STAGING.currencies;


create table yamarselr2015yandexru__staging.currencies (
    date_update timestamp(0) null,
    currency_code int null,
    currency_code_with int null,
    currency_code_div numeric(5, 3) null,
    constraint currencies_pk primary key (currency_code, currency_code_with, date_update) enabled
                                                       )
ORDER BY date_update segmented by hash(currency_code) all nodes
PARTITION BY date_update::date
GROUP BY calendar_hierarchy_day(date_update::date, 3, 2);
