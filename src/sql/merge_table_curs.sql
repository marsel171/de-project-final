MERGE INTO YAMARSELR2015YANDEXRU__STAGING.currencies t
USING table_name s
ON t.currency_code = s.currency_code
    and t.currency_code_with = s.currency_code_with
    and t.date_update = s.date_update
WHEN MATCHED THEN UPDATE SET
    currency_code_div = s.currency_code_div
WHEN NOT MATCHED THEN INSERT (
    date_update,
    currency_code,
    currency_code_with,
    currency_code_div)
VALUES (
    s.date_update,
    s.currency_code,
    s.currency_code_with,
    s.currency_code_div);
