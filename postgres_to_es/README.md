# Загрузка данных из Postgres в Elasticsearch

### Установить переменные окружения для подключения к Postgres

`DB_NAME='database_name'`  
`DB_USER='username`  
`DB_PASSWORD=password`  
`DB_HOST=127.0.0.1`  
`DB_PORT=5432`  

### Установить переменные окружения для подключения к ElasticSearch

`ES_HOST='http://127.0.0.1:9200'`  
`ES_USER='username`  
`ES_PASSWORD=password`  

### В файле data.json задать изначальное значение для даты последнего изменения фильмов (date_from), например:

`{"date_from": "2021-05-15", "offset": 0}`

### Запуск переноса данных

`python load_data.py`
