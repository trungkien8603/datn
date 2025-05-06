from flask import Flask
import threading
import firebase_admin
from firebase_admin import credentials, messaging, db
import json
import paho.mqtt.client as mqtt
from datetime import datetime
import os

# ====== FLASK KHỞI TẠO ======
app = Flask(__name__)

# ====== KHỞI TẠO FIREBASE ADMIN SDK ======

firebase_json = os.getenv("FIREBASE_CONFIG_JSON")
service_account_info = json.loads(firebase_json)

# Truyền dict vào Certificate
cred = credentials.Certificate(service_account_info)
# with open("serviceAccountKey.json", "r") as f:
#     service_account_json = json.load(f)

# cred = credentials.Certificate(service_account_json)
firebase_admin.initialize_app(cred, {
    'databaseURL': 'https://firstproject-f55ec-default-rtdb.asia-southeast1.firebasedatabase.app'
})

# ====== MQTT CẤU HÌNH ======
MQTT_BROKER = "192.168.41.101"
MQTT_PORT = 1883
MQTT_TOPIC = "health_data"

# ====== HÀM XỬ LÝ DỮ LIỆU MQTT ======
def on_message(client, userdata, msg):
    print("[MQTT] Nhận message mới")
    try:
        payload = json.loads(msg.payload.decode())
        spo2 = payload.get("spo2")
        heartRate = payload.get("heartRate")
        fallStatus = payload.get("fallStatus")

        print(f"[MQTT] SpO2: {spo2}, Heart rate: {heartRate}, Fall: {fallStatus}")

        # Ghi đè dữ liệu lên Firebase (thay vì push thêm)
        ref = db.reference("users/kien")  # Định danh cố định cho dữ liệu mới nhất
        ref.set({  # Sử dụng set() để ghi đè
            "spo2": spo2,
            "heartRate": heartRate,
            "fallStatus": fallStatus,
            # "timestamp": datetime.now().isoformat()
        })
        print("[Firebase] Đã ghi đè dữ liệu")

        # Gửi thông báo nếu phát hiện ngã
        if fallStatus == 1:
            message = messaging.Message(
                topic="fall_alert",
                notification=messaging.Notification(
                    title="Cảnh báo ngã!",
                    body="Phát hiện người dùng bị ngã!",
                ),
                android=messaging.AndroidConfig(
                    notification=messaging.AndroidNotification(
                        priority="high",
                        channel_id="fall_channel",
                        sound="warning"
                    )
                ),
            )
            response = messaging.send(message)
            print("[FCM] Gửi thành công:", response)

    except Exception as e:
        print("[ERROR] Lỗi khi xử lý MQTT data:", e)

# ====== CHẠY LUỒNG MQTT RIÊNG BIỆT ======
def mqtt_thread():
    print("[MQTT] Khởi động luồng MQTT...")
    client = mqtt.Client()
    client.on_message = on_message
    try:
        client.connect(MQTT_BROKER, MQTT_PORT)
        print("[MQTT] Đã kết nối broker")
        client.subscribe(MQTT_TOPIC)
        print(f"[MQTT] Đã subscribe topic: {MQTT_TOPIC}")
        client.loop_forever()
    except Exception as e:
        print("[ERROR] MQTT connection error:", e)

# ====== KHỞI ĐỘNG SERVER & MQTT ======
if __name__ == "__main__":
    threading.Thread(target=mqtt_thread, daemon=True).start()
    print("[FLASK] Server đang chạy, chỉ xử lý MQTT...")
    app.run(host="0.0.0.0", port=5000)
