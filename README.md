# Итоговый проект

Привет!

## Update - 3-я версия.

1. в дагах почти везде поменял DDL-скрипты на чтение из файлов, оставил парочку голых скриптов для разнообразия)

2. Хотел еще для общего развития попробовать прочитать файлы из S3 и что-то не очень понял как)

Если есть какое-то авторское решение, то можешь кинуть пример чтения какого-нибудь файла из проекта? на будущее сохраню себе как пример на всякий случай.

ну и вроде бы больше вопросов у меня нет)

----------------------------------------------------

## Update - 2-я, обновленная версия

### Часть 1-2. Чтение из топика Kafka \ ETL из Postgresql в Vertica

1. Разделил 1 прошлый даг на 2:
    - в первом даге читаем сообщения из Kafka и грузим в Postgresql (удалил если что закоменченные строки)
    - во 2-м через проверку ExternalTaskSensor 1-го дага запускаем даг загрузки из Postgres в Vertica

2. не понял только насчет твоего комментария, что много sql в теле дага - лучше вынести в отдельные файлы скрипты что ли?
или в даге не нужны DDL-скрипты? ну я их оставил специально, чтоб поднял докер и сразу все работает :)

3. скриншоты графов из тасок обоих дагов добавил в папку `img` - файлы `1st_dag` и `2nd_dag`

### Часть 3. Metabase

1. тут получилось добавить вертику в список БД - в прошлый раз что-то тупило, тут после рестарта контейнера все появилось

2. скриншот дашборда `dashboard.png` положил в папку `img`

3. тут только вопрос правильно ли я собрал витрину - может логику не так понял из текста









## все, что ниже относится к 1-й версии

пока кидаю первую черновую версию :)

есть ряд вопросов и моментов, которые хотел бы уточнить и посоветоваться.

Пока напишу по пунктам как и что реализовано сейчас:

### Часть 1. Чтение из топика Kafka

1. Все происходит внутри докера, который предлагалось запустить
``docker run -d -p 8998:8998 -p 8280:8280 -p 15432:5432 --name=de-final-prj-local sindb/de-final-prj:latest``

2. в файле *etl_from_kafka_to_pg.py* читаем через Pyspark Streaming топик *transaction-service-input* из кафки
3. затем его разбиваем streaming DF на 2 датафрейма (разбиваем по типу object type) и грузим сразу их в 2 соответствующие таблицы в Postgresql

Вопросы:

a) тут нюанс, что при повторном запуске файла будут заливаться дубли в Postgres (и как я почитал вроде нет upsert решения для таких случаев) - 
я этот момент потом учел при ETL в Vertica путем создания временной таблицы, куда сперва заливаю данные, а потом уже в нужную stg таблицу.

В тексте предлагалось использовать hdfs, но не очень понял как осуществить логику.
Я правильно понимаю, что идеальная модель - это создать DAG (потом настроить сенсор на него и в другом ETL даге, где переливка из PG -> Vertica), 
где пишем джобу и читаем сообщения из кафки и потом грузим в хадуп ``partitionBy['object_type','sent_dttm']`` с перезаписью ?
(Затем читаем эти файлы и грузим в PG? Зачем тогда написали про промежуточный этап с PG, если можно напрямую csv\parquet залить в вертику?)

но как правильно указать адрес по которому сохранять? вот к примеру у меня есть такой IP 158.160.40.41 с логином и паролем(!)
и вот такой путь */user/master/data* . как мне внутри докера указать адрес куда сохранять на ВМ в хадупе файлы?

потыкал то, что выдал телеграм-бот из проекта по Data lake и нет доступа у меня на создание своих директорий внутри HDFS на виртуальных машинах.

Или вообще все сохранять внутри докера к примеру в формате CSV?

Или оставлять текущий вариант, когда сразу лью в PG без сохранения на диск?


### Часть 2. ETL из Postgresql в Vertica

1. файл дага лежит тут */lessons/dags/dag/DAG_ETL_from_PG_to_Vertica.py*
2. прописаны DDL 2х таблиц *_raw* (currency + transaction) в STG-слое, куда сперва льем данные из PG, потом селектим оттуда данные и загружаем уже в таргетные таблицы, без дубликатов
3. потом создаем DDL витрины *global_metrics* и грузим туда данные, где удаляем данные из нее на вчерашний день пробега дага ``{ds} - 1 day``
4. также добавляю в этот DAG и bashoperator, который запускает стриминг чтения из кафки и заливки в PG. Я этот таск специально распараллелил,
но видимо в докере работает 1 ядро и все остальные таски у меня висят в очереди когда идет стриминг.
если не запускать стриминг в даге, то весь даг отрабатывает, все льется и считается без дублей и ошибок

вопрос - как быть в таком случае, если нужно запускать стриминг файл и эта таска всегда будет в режиме Running ?

**в папке img есть файл af.jpg**


### Часть 3. Metabase

1. не могу подключиться к Vertica, ее нет в выпадающем списке
положил все драйверы, и старые и новые - не помогает. что нужно сделать чтоб вертика появилась?

**в папке img есть файлы metabase.jpg и driver_vertica.jpg**


Ну и в целом расскажи какие ошибки в логике и как правильно надо было делать эту задачу? :)
