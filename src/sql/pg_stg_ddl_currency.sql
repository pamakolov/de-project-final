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