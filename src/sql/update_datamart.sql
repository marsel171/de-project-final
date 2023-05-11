insert into yamarselr2015yandexru.global_metrics with chargeback as (SELECT operation_id
                                                                     FROM   yamarselr2015yandexru__staging.transaction t
                                                                     WHERE  status = 'chargeback'), r as (SELECT to_char(t.transaction_dt,'yyyy-mm-dd') as transaction_day ,
                                            t.operation_id ,
                                            t.currency_code as currency_from ,
                                            t.amount as amount ,
                                            coalesce(c.currency_code_div, 1) as usd_currency_div ,
                                            account_number_from as account_make_transaction
                                     FROM   yamarselr2015yandexru__staging.transaction t
                                         LEFT JOIN chargeback cb using (operation_id)
                                         LEFT JOIN yamarselr2015yandexru__staging.currencies c
                                             ON to_char(c.date_update,'yyyy-mm-dd') = to_char(t.transaction_dt,'yyyy-mm-dd') and
                                                c.currency_code_with = 420 and
                                                t.currency_code = c.currency_code
                                     WHERE  t.status = 'done'
                                        and transaction_day = '{execution_date}'
                                        and t.account_number_from > 0
                                        and t.account_number_to > 0
                                        and t.transaction_type in ('c2a_incoming', 'c2b_partner_incoming', 'sbp_incoming', 'sbp_outgoing', 'transfer_incoming', 'transfer_outgoing')
                                     ORDER BY t.operation_id)
SELECT to_date(r.transaction_day,'yyyy-mm-dd') as date_update ,
       r.currency_from::int as currency_from ,
       sum(r.amount * r.usd_currency_div)::numeric(18, 2) as amount_total ,
       count(distinct operation_id)::int as cnt_transactions ,
       count(distinct account_make_transaction)::int as cnt_accounts_make_transactions ,
       cnt_transactions / cnt_accounts_make_transactions::numeric(16,
                                                                  2) as avg_transactions_per_account
FROM   r
GROUP BY r.transaction_day, r.currency_from
ORDER BY r.transaction_day, r.currency_from;
