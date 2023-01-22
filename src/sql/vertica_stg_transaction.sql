--- Создание таблицы
CREATE TABLE if not exists UNKNOWNPAVELYANDEXRU__STAGING.transactions (
    ID IDENTITY(1,1) NOT null,
    object_id varchar ,
    object_type varchar ,
    sent_dttm  timestamp
    ,operation_id varchar
    ,account_number_from int
    ,account_number_to int
    ,currency_code int
    ,country varchar
    ,status varchar
    ,transaction_type varchar
    ,amount int
    ,transaction_dt timestamp

    /*UNIQUE (object_id, object_type,sent_dttm,operation_id,account_number_from,account_number_to,currency_code,country,status,
    transaction_type,amount,transaction_dt) ENABLED*/
)
    order by sent_dttm, object_id
    segmented by hash(sent_dttm, object_id) all nodes
    PARTITION BY sent_dttm::date
    GROUP BY calendar_hierarchy_day(sent_dttm::date, 1, 1)
    ;

--- Создание проекции
CREATE PROJECTION IF NOT EXISTS UNKNOWNPAVELYANDEXRU__STAGING.transactions_dt
(object_id,sent_dttm) AS

SELECT object_id,sent_dttm
FROM UNKNOWNPAVELYANDEXRU__STAGING.transactions
ORDER BY sent_dttm
----SEGMENTED BY hash(sent_dttm,object_id) ALL NODES KSAFE 1
;

--- Создание таблицы таблицы RAW для избежания заливки дубликатов
CREATE TABLE if not exists UNKNOWNPAVELYANDEXRU__STAGING.transactions_raw (
    ID IDENTITY(1,1) NOT null,
    object_id varchar ,
    object_type varchar ,
    sent_dttm  timestamp
    ,operation_id varchar
    ,account_number_from int
    ,account_number_to int
    ,currency_code int
    ,country varchar
    ,status varchar
    ,transaction_type varchar
    ,amount int
    ,transaction_dt timestamp
)
    order by sent_dttm, object_id
    segmented by hash(sent_dttm, object_id) all nodes
    PARTITION BY sent_dttm::date
    GROUP BY calendar_hierarchy_day(sent_dttm::date, 1, 1)
    ;