from flask import Flask, request, jsonify
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
cred = credentials.Certificate(json.loads(firebase_json))
firebase_admin.initialize_app(cred, {
    'databaseURL': 'https://firstproject-f55ec-default-rtdb.asia-southeast1.firebasedatabase.app'
})

# ====== MQTT CẤU HÌNH ======
MQTT_BROKER = "test.mosquitto.org"
MQTT_PORT = 1883
MQTT_TOPIC = "health_data"

# ====== HÀM XỬ LÝ DỮ LIỆU MQTT ======
def on_message(client, userdata, msg):
    print("[MQTT] Nhận message mới")
    try:
        payload = json.loads(msg.payload.decode())
        spo2 = payload.get("spo2")
        heart_rate = payload.get("heart_rate")
        print(f"[MQTT] SpO2: {spo2}, Heart rate: {heart_rate}")

        # Gửi dữ liệu lên Firebase (push để tạo key riêng)
        ref = db.reference("health_data")
        result = ref.push({
            "spo2": spo2,
            "heart_rate": heart_rate,
            "timestamp": datetime.now().isoformat()
        })
        print("[Firebase] Đã ghi dữ liệu với key:", result.key)

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
        
    threading.Thread(target=mqtt_thread, daemon=True).start()

# ====== ROUTE HTTP NHẬN fall_detect TỪ ESP32 ======
@app.route("/esp-data", methods=["POST"])
def receive_data():
    data = request.get_json()
    if not data:
        return jsonify({"error": "No data provided"}), 400

    fall_detect = data.get("fall_detect")
    if fall_detect == 1:
        # Gửi thông báo FCM
        try:
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
            print("[ERROR] Gửi FCM thất bại:", e)
            return jsonify({"error": "FCM failed"}), 500

        return jsonify({"message": "Fall detected, FCM sent!"}), 200
    else:
        print("[HTTP] Không có ngã được phát hiện.")
        return jsonify({"message": "No fall detected"}), 200

# ====== CHẠY ỨNG DỤNG ======
if __name__ == "__main__":
    print("[FLASK] Server đang chạy trên cổng 5000...")
    app.run(host="0.0.0.0", port=5000)
