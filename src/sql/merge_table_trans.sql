MERGE INTO YAMARSELR2015YANDEXRU__STAGING.transaction t
USING table_name s
ON t.operation_id = s.operation_id
    and t.account_number_from = s.account_number_from
    and t.account_number_to = s.account_number_to
    and t.transaction_dt = s.transaction_dt
WHEN MATCHED THEN UPDATE SET
    currency_code = s.currency_code,
    country = s.country,
    status = s.status,
    transaction_type = s.transaction_type,
    amount = s.amount
WHEN NOT MATCHED THEN INSERT (
    account_number_from,
    account_number_to,
    currency_code,
    country,
    status,
    transaction_type,
    amount,
    transaction_dt,
    operation_id)
VALUES (
    s.account_number_from,
    s.account_number_to,
    s.currency_code,
    s.country,
    s.status,
    s.transaction_type,
    s.amount,
    s.transaction_dt,
    s.operation_id);