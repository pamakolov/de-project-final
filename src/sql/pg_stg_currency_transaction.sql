CREATE SCHEMA IF NOT EXISTS stg ;


CREATE TABLE if not exists stg.input_kafka_currency (
id serial NOT null, --GENERATED ALWAYS AS IDENTITY,
object_id varchar ,
object_type varchar ,
sent_dttm  timestamp
,date_update timestamp
,currency_code int
,currency_code_with int
,currency_with_div float
);



CREATE TABLE if not exists stg.input_kafka_TRANSACTION (
id serial NOT null, --GENERATED ALWAYS AS IDENTITY,
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
);

