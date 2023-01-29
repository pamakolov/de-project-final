from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType, IntegerType
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
from pyspark.sql.types import MapType,StringType
from pyspark.sql.functions import from_json
from time import sleep
import os

#PYSPARK_PIN_THREAD=True
os.environ["PYSPARK_PIN_THREAD"] = 'true'

# топики для входящих сообщений-транзакций
TOPIC_NAME = 'transaction-service-input'


# библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ",".join(
    [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
        "org.postgresql:postgresql:42.4.0"
    ]
)


kafka_user = 'producer_consumer'
kafka_pass = 'producer_consumer'

# настройки security для kafka
kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512'
}


# схема 1-го входного сообщения-транзакции: object_type = TRANSACTION
schema_trans = StructType([
    StructField('object_id',StringType()),
    StructField('object_type',StringType()),
    StructField('sent_dttm', TimestampType()),
    
        StructField('payload', StructType([
            StructField('operation_id', StringType()),
            StructField('account_number_from', IntegerType()),
            StructField('account_number_to', IntegerType()),
            StructField('currency_code', IntegerType())
            ,StructField('country', StringType())
            ,StructField('status', StringType())
            ,StructField('transaction_type', StringType())
            ,StructField('amount', IntegerType())
            ,StructField('transaction_dt', TimestampType())
        ]))
])

# схема 2-го входного сообщения-транзакции: object_type = currency
schema_currency = StructType([
    StructField('object_id',StringType()),
    StructField('object_type',StringType()),
    StructField('sent_dttm', TimestampType()),
    
        StructField('payload', StructType([
            StructField('date_update', TimestampType()),
            StructField('currency_code', IntegerType()),
            StructField('currency_code_with', IntegerType()),
            StructField('currency_with_div', DoubleType())
        ]))
])


# создание сессии
def spark_init(test_name) -> SparkSession:

    return (
        SparkSession
        .builder
        .config("spark.executor.memory", "5g")
        .config("spark.driver.memory", "2g")
        .config("spark.memory.offHeap.enabled",True)
        .config("spark.memory.offHeap.size","1g")
        .appName({test_name})
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.packages", spark_jars_packages)
        .getOrCreate()
    )

# читаем сообщения из топика Kafka
def read_stream(spark: SparkSession) -> DataFrame:

    df = (
        spark
        .readStream
        .format('kafka')
        .option('kafka.bootstrap.servers', 'rc1a-ubhlak8r592fle1t.mdb.yandexcloud.net:9091') \
        .option('kafka.security.protocol', 'SASL_SSL')
        .option('kafka.sasl.jaas.config', f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{kafka_user}" password="{kafka_pass}";')
        .option('kafka.partition.assignment.strategy', 'org.apache.kafka.clients.consumer.RoundRobinAssignor') 
        .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') 
        .option('maxOffsetsPerTrigger', "100")      
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "earliest")
        .load()
        )

    return df

# читаем сообщения из топика Kafka
def parse_json_initial(df) -> DataFrame:

    df_parse = (
        df
        .withColumn('val', F.col('value').cast(StringType()))
        .withColumn("parsevalue", from_json(F.col('val'), schema_currency))
        .select('parsevalue.object_type','value')
    )

    return df_parse

# метод для записи данных в PostgreSQL
def foreach_batch_function(df, epoch_id):

    print('Число строк DF: ', df.count())
    # сохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka
    df_persist = df.persist()
    
    # Postgres creds 
    mode="append"
    host = 'localhost' 
    port = 5432 
    schema = 'stg' 
    dbtable_curr = 'stg.input_kafka_currency' 
    dbtable_trans = 'stg.input_kafka_TRANSACTION' 
    dbtable = 'stg.input_kafka'
    databasename = 'de'
    url = 'jdbc:postgresql://' + str(host) + ':' + str(port) + '/' + str(databasename)
    user = 'jovyan'
    password = 'jovyan'

    print('start loading into PG ------------------>')
    df_parse_currency = df_persist.filter(F.col("object_type") == "CURRENCY")
    df_parse_transaction = df_persist.filter(F.col("object_type") == "TRANSACTION")
    print('Число строк df_parse_currency: ', df_parse_currency.count())
    print('Число строк df_parse_transaction: ', df_parse_transaction.count())

    print('Парсим df_parse_currency ---------------->')
    df_parse_currency_ = (
        df_parse_currency
        .withColumn('val', F.col('value').cast(StringType()))
        .drop('value')
        .withColumn("parsevalue", from_json(F.col('val'), schema_currency))
        .selectExpr('parsevalue.*')
        .select('object_id','object_type','sent_dttm','payload.*')
        #.select('parsevalue.object_id','parsevalue.object_type', 'value','parsevalue')
        )
    # грузим в PG
    print('loading CURRENCY TABLE ------------------>')
    df_parse_currency_ \
    .write \
    .mode('append') \
    .format("jdbc") \
    .option("url", url) \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", dbtable_curr) \
    .option("user", user) \
    .option("password", password) \
    .save()

    print('Парсим df_parse_transaction ---------------->')
    df_parse_transaction_ = (
        df_parse_transaction
        .withColumn('val', F.col('value').cast(StringType()))
        .drop('value')
        .withColumn("parsevalue", from_json(F.col('val'), schema_trans))
        .selectExpr('parsevalue.*')
        .select('object_id','object_type','sent_dttm','payload.*')
        #.select('parsevalue.object_id','parsevalue.object_type', 'value','parsevalue')
        )
    # грузим в PG
    print('loading TRANSACTION TABLE ------------------>')
    df_parse_transaction_ \
    .write \
    .mode('append') \
    .format("jdbc") \
    .option("url", url) \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", dbtable_trans) \
    .option("user", user) \
    .option("password", password) \
    .save()

    print('data was loaded in PG ------------------>')

    # очищаем память от df
    df_persist.unpersist()
    print('df was deleted from cache ---------------------->')



if __name__ == "__main__":

    spark = spark_init('start reading stream')
    print('Session is started', '-------------------------------->')
    # читаем сообщения из топика Kafka
    df = read_stream(spark)
    print('DF is read from function read_stream() -----------------------')
    df_parse = parse_json_initial(df)
    print('df_parse is read from function parse_json_initial() -----------------------')

    # запускаем стриминг
    print('запускаем стриминг -------------------->')
    (
        df_parse
        .writeStream
        #.trigger(processingTime="10 seconds")
        .foreachBatch(foreach_batch_function)
        .start()
        .awaitTermination(0.75*60*60) # listen for 45 mins
    )
