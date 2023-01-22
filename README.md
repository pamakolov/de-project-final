# Итоговый проект

# Финальный проект

Привет!

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





### Как работать с репозиторием
1. В вашем GitHub-аккаунте автоматически создастся репозиторий `de-project-final` после того, как вы привяжете свой GitHub-аккаунт на Платформе.
2. Скопируйте репозиторий на свой компьютер. В качестве пароля укажите ваш `Access Token`, который нужно получить на странице [Personal Access Tokens](https://github.com/settings/tokens)):
	* `git clone https://github.com/Yandex-Practicum/de-project-final`
3. Перейдите в директорию с проектом: 
	* `cd de-project-final`
4. Выполните проект и сохраните получившийся код в локальном репозитории:
	* `git add .`
	* `git commit -m 'my best commit'`
5. Обновите репозиторий в вашем GitHub-аккаунте:
	* `git push origin main`

### Структура репозитория
Файлы в репозитории будут использоваться для проверки и обратной связи по проекту. Поэтому постарайтесь публиковать ваше решение согласно установленной структуре: так будет проще соотнести задания с решениями.

Внутри `src` расположены папки:
- `/src/dags` - вложите в эту папку код DAG, который поставляет данные из источника в хранилище. Назовите DAG `1_data_import.py`. Также разместите здесь DAG, который обновляет витрины данных. Назовите DAG `2_datamart_update.py`.
- `/src/sql` - сюда вложите SQL-запрос формирования таблиц в `STAGING`- и `DWH`-слоях, а также скрипт подготовки данных для итоговой витрины.
- `/src/py` - если источником вы выберете Kafka, то в этой папке разместите код запуска генерации и чтения данных в топик.
- `/src/img` - здесь разместите скриншот реализованного над витриной дашборда.
