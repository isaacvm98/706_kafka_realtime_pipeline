import json
import psycopg2
from kafka import KafkaConsumer

def run_consumer():
    """Consumes messages from Kafka and inserts them into PostgreSQL."""
    try:
        print("[Consumer] Connecting to Kafka at localhost:9092...")
        consumer = KafkaConsumer(
            "orders",
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id="orders-consumer-group",
        )
        print("[Consumer] ✓ Connected to Kafka successfully!")

        print("[Consumer] Connecting to PostgreSQL...")
        conn = psycopg2.connect(
            dbname="kafka_db",
            user="kafka_user",
            password="kafka_password",
            host="localhost",
            port="5432",
        )
        conn.autocommit = True
        cur = conn.cursor()
        print("[Consumer] ✓ Connected to PostgreSQL successfully!")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS orders (
                order_id VARCHAR(50) PRIMARY KEY,
                status VARCHAR(50),
                category VARCHAR(50),
                value NUMERIC(10, 2),
                timestamp TIMESTAMP,
                city VARCHAR(100),
                payment_method VARCHAR(50),
                discount NUMERIC(4, 2)
            );
            """
        )
        print("[Consumer] ✓ Table 'orders' ready.")
        print("[Consumer] 🎧 Listening for messages...\\n")

        message_count = 0
        for message in consumer:
            try:
                order_data = message.value

                insert_query = """
                    INSERT INTO orders (order_id, status, category, value, timestamp, city, payment_method, discount)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (order_id) DO NOTHING;
                """
                cur.execute(
                    insert_query,
                    (
                        order_data["order_id"],
                        order_data["status"],
                        order_data["category"],
                        order_data["value"],
                        order_data["timestamp"],
                        order_data.get("city", "N/A"),
                        order_data["payment_method"],
                        order_data["discount"],
                    ),
                )
                message_count += 1
                print(f"[Consumer] ✓ #{message_count} Inserted order {order_data['order_id']} | {order_data['category']} | ${order_data['value']} | {order_data['city']}")

            except Exception as e:
                print(f"[Consumer ERROR] Failed to process message: {e}")
                continue

    except Exception as e:
        print(f"[Consumer ERROR] {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    run_consumer()
