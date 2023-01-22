--- Создание таблицы
CREATE TABLE if not exists UNKNOWNPAVELYANDEXRU__STAGING.currencies (
    ID IDENTITY(1,1) NOT null,
    object_id varchar ,
    object_type varchar ,
    sent_dttm  timestamp
    ,date_update timestamp
    ,currency_code integer
    ,currency_code_with integer
    ,currency_with_div float

    ----UNIQUE (object_id, object_type,sent_dttm,date_update,currency_code,currency_code_with,currency_with_div) ENABLED
)
order by sent_dttm, object_id
segmented by hash(sent_dttm, object_id) all nodes
PARTITION BY sent_dttm::date
GROUP BY calendar_hierarchy_day(sent_dttm::date, 1, 1)
;

--- Создание проекции
CREATE PROJECTION IF NOT EXISTS UNKNOWNPAVELYANDEXRU__STAGING.currencies_dt
(object_id,sent_dttm) AS

SELECT object_id,sent_dttm
FROM UNKNOWNPAVELYANDEXRU__STAGING.currencies
ORDER BY sent_dttm
----SEGMENTED BY hash(sent_dttm,object_id) ALL NODES KSAFE 1
;

--- Создание промежуточной таблицы RAW для избежания заливки дубликатов
CREATE TABLE if not exists UNKNOWNPAVELYANDEXRU__STAGING.currencies_raw (
    ID IDENTITY(1,1) NOT null,
    object_id varchar ,
    object_type varchar ,
    sent_dttm  timestamp
    ,date_update timestamp
    ,currency_code integer
    ,currency_code_with integer
    ,currency_with_div float
)
order by sent_dttm, object_id
segmented by hash(sent_dttm, object_id) all nodes
PARTITION BY sent_dttm::date
GROUP BY calendar_hierarchy_day(sent_dttm::date, 1, 1)
;