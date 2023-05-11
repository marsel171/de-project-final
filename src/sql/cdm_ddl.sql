-- Создаем схему CDM
CREATE SCHEMA IF NOT EXISTS YAMARSELR2015YANDEXRU;

--Создаем витрину global_metrics
drop table if exists YAMARSELR2015YANDEXRU.global_metrics;


create table if not exists yamarselr2015yandexru.global_metrics (
    date_update timestamp(0) not null,
    currency_from int not null,
    amount_total numeric(16, 2) not null,
    cnt_transactions int not null,
    avg_transactions_per_account numeric(16, 2) null,
    cnt_accounts_make_transactions int,
    constraint global_metrics_pk primary key (date_update, currency_from) enabled
    )
ORDER BY date_update segmented by hash(date_update, currency_from) all nodes
PARTITION BY coalesce (date_update::date,'1900-01-01');
