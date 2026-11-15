# Практикум 4: изменения PostgreSQL в Kafka

Учебный пример CDC: меняем строки в базе и смотрим сообщения в терминале.
CDC (Change Data Capture) — получение изменений данных.

Путь сообщения: **PostgreSQL → Debezium в Kafka Connect → Kafka → Python consumer**.

- PostgreSQL хранит таблицы `users` и `orders`.
- Debezium читает изменения из журнала базы и отправляет их в Kafka.
- Consumer — небольшая Python-программа, которая читает Kafka и печатает результат.
- Prometheus и Grafana нужны для просмотра метрик.

## 1. Подготовка

Нужны Docker с Compose, Python 3, Git и curl. На Windows можно использовать
Docker Desktop с Linux-контейнерами и терминал WSL Ubuntu. **Все команды ниже
рассчитаны на Bash (Linux, macOS или WSL), не на PowerShell.**

Проверяем инструменты:

```bash
docker --version
docker compose version
python3 --version
curl --version
```

Скачиваем проект:

```bash
git clone https://github.com/SVFr13nD/kafka-practicum-4.git
cd kafka-practicum-4
```

Если скачан ZIP, распакуйте его и откройте терминал в папке, где лежит
`docker-compose.yaml`. Все следующие команды выполняйте из этой папки.

Это локальный учебный стенд с тестовыми паролями. Не размещайте его на
публичном сервере. Образы и Python-библиотека скачиваются из интернета.

## 2. Запуск контейнеров

```bash
docker compose up -d --build
docker compose ps
```

Первый запуск может занять несколько минут. Должны запуститься шесть сервисов:
`zookeeper`, `kafka`, `postgres`, `kafka-connect`, `prometheus`, `grafana`.
У Kafka и PostgreSQL дождитесь состояния `healthy`.

Проверяем Kafka Connect:

```bash
curl -fsS http://localhost:8083/connectors
```

При первом запуске ответ — `[]`. Ошибка подключения означает, что сервис
еще не готов или не запустился. Посмотрите логи, затем повторите проверку:

```bash
docker compose logs --tail=100 kafka-connect
```

## 3. Подключение PostgreSQL к Kafka

```bash
curl -fsS -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  --data @connector/postgres-connector.json
```

Ответ с настройками означает, что запрос принят. Теперь проверьте работу:

```bash
curl -fsS http://localhost:8083/connectors/postgres-connector/status
```

И у `connector`, и у задачи внутри `tasks` должно быть `"state":"RUNNING"`
(пробелы в JSON могут отличаться). Если задача `FAILED`, причина находится
в поле `trace` и в логах Kafka Connect. Если POST вернул 409, коннектор уже
существует: повторно создавать его не нужно, проверьте статус.

## 4. Запуск consumer

В этом же терминале создаем отдельное окружение Python:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python consumer/consumer.py
```

На Ubuntu, если модуль `venv` отсутствует, установите пакет `python3-venv`
через пакетный менеджер и повторите создание окружения.

Оставьте consumer работающим. Он читает два топика:
`postgres.public.users` и `postgres.public.orders`.

Сначала появятся события `SNAPSHOT` — строки, которые уже были в таблицах.
Затем consumer будет ждать новые изменения. При повторном запуске он снова
читает доступную историю с начала: группа и сохранение позиции здесь отключены.

## 5. Проверка изменений

Откройте **второй терминал**, перейдите в ту же папку проекта и выполните:

```bash
docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U postgres -d appdb < postgres/test-data.sql
```

Скрипт добавляет пользователей и заказы, меняет email и количество товара,
удаляет заказ `Mouse`. В первом терминале должны появиться операции:

| Операция | Значение |
| --- | --- |
| SNAPSHOT | Начальное чтение существующих строк |
| INSERT | Добавление строки |
| UPDATE | Изменение строки |
| DELETE | Удаление строки |
| Tombstone | Пустое значение после удаления, это нормально |

`Было` — данные до изменения, `Стало` — после. При INSERT поле `Было` равно
`null`, при DELETE поле `Стало` равно `null`. При стандартных настройках
PostgreSQL старые данные могут содержать только ключ; полный старый ряд
не гарантирован. Даты выводятся как прислал Debezium, иногда числами.
В consumer специально нет отдельного преобразования дат.

Повторный запуск SQL добавляет еще тестовые строки. Это не сброс данных.

## 6. Метрики

- Grafana: http://localhost:3000 — логин `admin`, пароль `admin`.
- Prometheus: http://localhost:9090 — откройте Status → Targets.
- Метрики Kafka Connect: http://localhost:9404/metrics.

В Grafana откройте Dashboards: дашборд Kafka Connect / Debezium и источник
Prometheus подключаются из файлов проекта. Некоторые графики могут быть
пустыми, пока коннектор не начал работу или нет новых событий.

## Если что-то не работает

| Проблема | Что проверить |
| --- | --- |
| Cannot connect to the Docker daemon | Запущен ли Docker / Docker Desktop |
| Порт уже занят | Порты 5432, 9092, 8083, 9404, 9090 и 3000 должны быть свободны |
| Конфликт имени контейнера | В compose заданы фиксированные имена; не запускайте две копии стенда |
| Consumer: NoBrokersAvailable | Состояние Kafka в `docker compose ps` и ее логи |
| Consumer молчит | Статус задачи коннектора; выполнен ли тестовый SQL |
| Нет модуля kafka | Активируйте `.venv`, повторите установку requirements |
| Ошибка загрузки образа / JAR | Проверьте интернет и полный текст ошибки сборки |

Логи нужного сервиса:

```bash
docker compose logs --tail=100 kafka
docker compose logs --tail=100 postgres
docker compose logs --tail=100 kafka-connect
```

## Остановка и повторный запуск

Остановите consumer сочетанием Ctrl+C. Для паузы без удаления контейнеров:

```bash
docker compose stop
```

Продолжить работу с теми же контейнерами:

```bash
docker compose start
```

Для начала заново: команда ниже **удаляет контейнеры и данные стенда**,
включая анонимный том PostgreSQL. Применяйте только к этому учебному проекту:

```bash
docker compose down -v
docker compose up -d --build
```

Затем снова выполните шаги 3–5. Именованные тома для надежного хранения здесь
не настроены. Обычный `down` тоже не подходит для сохранения всего стенда:
Kafka хранит данные в контейнере, а анонимный том PostgreSQL при новом `up`
не подключается автоматически. Для короткой паузы используйте `stop/start`.

## Где что находится

| Файл / папка | Назначение |
| --- | --- |
| docker-compose.yaml | Запуск сервисов |
| postgres/init.sql | Создание таблиц и начальные строки |
| postgres/test-data.sql | Проверочные INSERT, UPDATE, DELETE |
| connector/postgres-connector.json | Подключение Debezium к базе |
| consumer/consumer.py | Чтение и печать событий |
| kafka-connect/Dockerfile | Образ Connect с экспортом метрик |
| monitoring/ | Настройки Prometheus, Grafana и JMX |

## Основа и ограничения

Основа: [dyackonov777/kafka-practice_4_CDC-](https://github.com/dyackonov777/kafka-practice_4_CDC-).
Конфигурации сервисов и мониторинга сохранены из исходного архива.
Consumer и инструкция упрощены для учебной работы.

Формат событий описан в [документации Debezium](https://debezium.io/documentation/reference/stable/connectors/postgresql.html).

Проверены синтаксис Python, JSON/YAML и обработка примерных событий consumer.
Полный запуск Docker-стенда в среде подготовки не выполнялся: Docker недоступен.
