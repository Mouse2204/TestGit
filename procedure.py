from confluent_kafka import Producer
import pandas as pd
import json
import time
import sys

conf = {
    'bootstrap.servers': 'localhost:9092', 
    'client.id': 'gene-producer-v1'
}

# Tên Topic
TOPIC_NAME = "gene-expression"

# Khởi tạo Producer
try:
    producer = Producer(conf)
except Exception as e:
    print(f"❌ Không thể tạo Producer: {e}")
    sys.exit(1)

# Hàm Callback: Được gọi khi Kafka xác nhận đã nhận tin (hoặc báo lỗi)
def delivery_report(err, msg):
    if err is not None:
        print(f"⚠️ Gửi lỗi: {err}")
    else:
        # Uncomment dòng dưới nếu muốn in log chi tiết từng tin (sẽ spam màn hình)
        # print(f"✅ Đã gửi tới {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")
        pass

def process_csv(file_path):
    print(f"📂 Đang đọc file {file_path}...")
    try:
        df = pd.read_csv(file_path)
    except FileNotFoundError:
        print("❌ Không tìm thấy file CSV!")
        return

    print(f"🚀 Bắt đầu streaming {len(df)} gen vào Kafka...")
    
    count = 0
    for index, row in df.iterrows():
        # Chuẩn bị dữ liệu
        numeric_values = row[1:].values.astype(float)
        
        data = {
            'gene_id': str(row['time']),
            't40': float(row.get('40', 0)),
            't50': float(row.get('50', 0)),
            'max_val': float(numeric_values.max()),
            'min_val': float(numeric_values.min())
        }
        
        # Chuyển sang JSON string
        value_json = json.dumps(data)
        
        # Gửi tin (Asynchronous)
        # key=str(row['time']) giúp các tin của cùng 1 gen luôn vào cùng 1 partition (đúng thứ tự)
        producer.produce(
            TOPIC_NAME, 
            key=str(row['time']), 
            value=value_json, 
            callback=delivery_report
        )
        
        # Trigger hàm callback (quan trọng để giải phóng bộ nhớ đệm của librdkafka)
        producer.poll(0)
        
        count += 1
        if count % 100 == 0:
            print(f"-> Đã push {count} gen...")
            
        # Giả lập độ trễ (nếu cần test streaming chậm)
        # time.sleep(0.01)

    # Quan trọng: Chờ gửi nốt các tin còn trong hàng đợi trước khi thoát
    print("⏳ Đang xả hàng đợi (Flushing)...")
    producer.flush()
    print(f"✅ Hoàn tất! Tổng cộng {count} gen đã được gửi.")

if __name__ == "__main__":
    process_csv("Spellman.csv")