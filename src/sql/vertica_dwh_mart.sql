--- Создание таблицы витрины global_metrics
CREATE TABLE if not exists UNKNOWNPAVELYANDEXRU__DWH.global_metrics (
    date_update date not null ,
    currency_from varchar not null,
    amount_total numeric(16,2)
    ,cnt_transactions numeric(16,2)
    ,avg_transactions_per_account int
    ,cnt_accounts_make_transactions int
)
order by date_update
unsegmented all nodes
PARTITION BY date_trunc('month',date_update)
;



delete from UNKNOWNPAVELYANDEXRU__DWH.global_metrics where date_update::date = '{ds}'::date - 1;
insert into UNKNOWNPAVELYANDEXRU__DWH.global_metrics

select --t.country, 
t.sent_dttm::date, t.currency_code, 
sum(t.amount*c.currency_with_div)::numeric(15,2) as amount_total 
,sum(t.amount) cnt_transactions 
,(sum(t.amount*c.currency_with_div) / count(distinct t.operation_id))::int as avg_transactions_per_account 
,count(distinct case when t.currency_code=420 then t.account_number_from else null end) as cnt_accounts_make_transactions
from UNKNOWNPAVELYANDEXRU__STAGING.transactions t
left join UNKNOWNPAVELYANDEXRU__STAGING.currencies c on c.currency_code = t.currency_code and t.sent_dttm::Date=c.sent_dttm::date
where t.sent_dttm::Date = '{ds}'::date - 1 and
t.account_number_from>0 
--and t.country='russia' 
and t.status='done' 
and t.transaction_type like '%comin%'
--and t.object_id = 'fb564efc-cb2d-4b32-9751-55eff5730f3c'
--and c.currency_code_with=420
group by 1,2 --,3 
order by 1,2 --,3