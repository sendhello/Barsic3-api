# Сервис Barsic Web

Сервис для извлечения, трансформации и сохранения отчетов из системы Датакрат Барс2.

* **Язык приложения:** Python 3.12
* **Поддерживаемые протоколы взаимодействия:** REST API
* **Инфраструктурные зависимости:** Postgres, Redis
* **Зависимости от системных пакетов:** отсутствуют
* **Зависимости от расширений PostgreSQL:** отсутствуют
* **Часть окружения:** development
* **Минимальные системные требования:** 1 CPU, 1Gb RAM

## Поддержка сервиса

Группа разработки:

* Иван Баженов (*[@sendhello](https://github.com/sendhello)*)

## Описание обязательных методов для запуска сервиса

### Запуск сервиса
```commandline
# Из корня проекта
docker compose up --build
```

### Документация
* http://127.0.0.1/barsic/openapi (Swagger)
* http://127.0.0.1/barsic/openapi.json (openapi)

## Описание дополнительных методов сервиса

### Описание ENV переменных

| Имя переменной           | Возможное значение                         | Описание                                                                                |
|:-------------------------|--------------------------------------------|:----------------------------------------------------------------------------------------|
| DEBUG                    | False                                      | Режим отладки                                                                           |
| PROJECT_NAME             | Barsic                                     | Название сервиса (отображается в Swagger)                                               |
| REDIS_HOST               | redis                                      | Имя сервера Redis                                                                       |
| REDIS_PORT               | 6379                                       | Порт сервера Redis                                                                      |
| POSTGRES_HOST            | localhost                                  | Host Postgres                                                                           |
| POSTGRES_PORT            | 5432                                       | Порт Postgres                                                                           |
| POSTGRES_DB              | barsic                                     | Имя БД Postgres                                                                         |
| POSTGRES_USER            | app                                        | Имя пользователя Postgres                                                               |
| POSTGRES_PASSWORD        | 123qwe                                     | Пароль пользователя Postgres                                                            |

### Запуск тестовой среды с локальной БД

1. Положить бекапы баз в папку dev/backup. 
Имена файлов должны совпадать с именами оригинальных БД (Beach.bak и т.п.)

2. Запустить тестовую среду:
    ```commandline
    docker compose -f docker-compose-dev.yml up
    ```

3. Скачать и запустить менеджер БД, 
например Azure Data Studio[https://docs.microsoft.com/en-us/sql/azure-data-studio/download-azure-data-studio?view=sql-server-ver15]

4. Восстановить все БД

### Установка Microsoft ODBC 18 на MacOS
```commandline
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew update
HOMEBREW_ACCEPT_EULA=Y brew install msodbcsql18 mssql-tools18
```

### Получение Яндекс токена для сохранения отчетов на Яндекс-диске
Создать приложение яндекс и получить токен можно по инструкции https://yandex.ru/dev/disk/webdav/