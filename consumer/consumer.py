import json
from kafka import KafkaConsumer

# Без группы: при каждом запуске читаем сохраненные сообщения с начала.
consumer = KafkaConsumer(
    "postgres.public.users",
    "postgres.public.orders",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id=None,
    enable_auto_commit=False,
)

operations = {"r": "SNAPSHOT", "c": "INSERT", "u": "UPDATE", "d": "DELETE"}
print("Жду изменения из PostgreSQL. Для выхода нажмите Ctrl+C.")

try:
    for message in consumer:
        # После DELETE Debezium может прислать сообщение без значения.
        if message.value is None:
            print("\nTombstone (пустое сообщение):", message.topic)
            continue

        try:
            value = json.loads(message.value.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            print("Не получилось прочитать JSON:", message.topic)
            continue

        if not isinstance(value, dict):
            print("Ожидался JSON-объект:", value)
            continue

        # В сообщении со схемой данные находятся внутри payload.
        data = value.get("payload", value)
        if not isinstance(data, dict):
            print("Нет данных события:", data)
            continue

        print("\nТопик:", message.topic)
        print("Операция:", operations.get(data.get("op"), data.get("op")))
        print("Было:", json.dumps(data.get("before"), ensure_ascii=False))
        print("Стало:", json.dumps(data.get("after"), ensure_ascii=False))
except KeyboardInterrupt:
    print("\nConsumer остановлен.")
finally:
    consumer.close()
