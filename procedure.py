# procedure_enhanced.py
import time
import json
import random
from confluent_kafka import Producer
from datetime import datetime
from faker import Faker

conf = {
    'bootstrap.servers': 'localhost:19092',
    'client.id': 'enhanced-producer',
}

producer = Producer(conf)
TOPIC_NAME = "user_data_topic"
fake = Faker()

def delivery_report(err, msg):
    if err is not None:
        print(f'Gửi lỗi: {err}')
    else:
        print(f'Sent: {msg.topic()} [{msg.partition()}]')

def generate_fake_data():
    """Tạo dữ liệu với mix hợp lệ và không hợp lệ"""
    
    # 70% clean data, 30% dirty data
    if random.random() < 0.7:
        # Clean data
        data = {
            "id": fake.uuid4(),
            "name": fake.name(),
            "age": random.randint(18, 65),
            "role": random.choice(["Admin", "User", "Dev", "Manager", "Analyst"]),
            "salary": round(random.uniform(1000, 10000), 2),
            "email": fake.email(),
            "transaction_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    else:
        # Dirty data (test cases)
        data = {
            "id": fake.uuid4(),
            "name": random.choice([fake.name(), "123Invalid", "", None]),
            "age": random.choice([200, -5, None, random.randint(18, 65)]),
            "role": random.choice(["Admin", "User", "Dev", "Hacker", "Invalid", None]),
            "salary": random.choice([-1000, 50000, None, round(random.uniform(1000, 10000), 2)]),
            "email": random.choice([fake.email(), "invalid-email", "no-at.com", None]),
            "transaction_date": random.choice([
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "2023-01-01 00:00:00",  # Old date
                "invalid-date",
                None
            ])
        }
    return data

if __name__ == "__main__":
    print(f"Bắt đầu gửi dữ liệu vào topic '{TOPIC_NAME}'...")
    
    try:
        while True:
            data = generate_fake_data()
            value_bytes = json.dumps(data).encode('utf-8')
            
            producer.produce(
                TOPIC_NAME, 
                value=value_bytes, 
                callback=delivery_report
            )
            producer.poll(0)
            
            time.sleep(2)  # 2 giây/record

    except KeyboardInterrupt:
        print("\nĐang dừng...")
        producer.flush()
        print("Đã dừng gửi dữ liệu.")