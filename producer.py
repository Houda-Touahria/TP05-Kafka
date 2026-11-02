import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

# اسم التوبيك الذي سنرسل إليه الأحداث
TOPIC = "campus-sensors"

# إنشاء كائن الـ Producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    acks="all"
)

try:
    # محاكاة إرسال 30 قراءة من الحساسات
    for event_id in range(1, 31):
        event = {
            "event_id": event_id,
            "sensor_id": f"sensor-{random.randint(1, 4):02d}",
            "temperature_c": round(random.uniform(18, 42), 2),
            "humidity_pct": round(random.uniform(25, 85), 2),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        # إرسال الحدث إلى كافكا
        producer.send(TOPIC, value=event)
        print("Sent:", event)
        # الانتظار ثانية واحدة بين كل قراءة وأخرى
        time.sleep(1)
        
    # تفريغ أي رسائل متبقية في الذاكرة المؤقتة للتأكد من وصولها للوسيط
    producer.flush()
finally:
    # إغلاق الاتصال بعد الانتهاء
    producer.close()