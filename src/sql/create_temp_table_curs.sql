drop table if exists table_name;


create table table_name (
    date_update timestamp(0) null,
    currency_code int null,
    currency_code_with int null,
    currency_code_div numeric(5, 3) null,
    load_id identity
                        )
ORDER BY date_update segmented by hash(currency_code) all nodes;
