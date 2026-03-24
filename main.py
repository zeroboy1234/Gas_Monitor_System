import threading
from threading import Lock
import minimalmodbus
import time
import json
import sqlite3
import paho.mqtt.client as mqtt
from datetime import datetime
from gpiozero import OutputDevice, Button

import smbus2
import calendar
import ntplib

# ================== GLOBAL ==================
relay = None
i2c1_lock = Lock()

pending_publish = {}   # mid -> record_id
pending_lock = Lock()

DB_PATH = "mqtt_buffer.db"
MAX_BUFFER_SIZE = 30000

PORT = "/dev/rs485"
BAUDRATE = 9600
TIMEOUT = 1

THINGSBOARD_HOST = "iotcenter.duckdns.org"
ACCESS_TOKEN = "74NU0O2q0wTPa1OtoLZD" 
MQTT_PORT = 1883

REQUEST_DATA_REG = 8
DATA_READY_REG = 9
DATA_START_REG = 10

mqtt_connected = False

# RTC
rtcInitialized = False
RTC_ADDR = 0x68
RTC_BUS  = 1

# GPIO Configuration
RELAY_PIN = 6  # GPIO6

# ================== GPIO SETUP ==================

def init_gpio():
    global relay
    try:
        relay = OutputDevice(RELAY_PIN)
        relay.on()  # HIGH
        log("[GPIO] RELAY PIN set to HIGH")
    except Exception as e:
        log(f"[ERROR][GPIO] Failed to initialize: {e}")

def cleanup_gpio():
    try:
        if relay:
            relay.close()
        log("[GPIO] Cleanup completed")
    except Exception as e:
        log(f"[ERROR][GPIO] Cleanup failed: {e}")

# ================== LOG ==================

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

# ================== MODBUS ==================

instruments = [
    minimalmodbus.Instrument(PORT, 1),
    minimalmodbus.Instrument(PORT, 2),
    minimalmodbus.Instrument(PORT, 3),
    minimalmodbus.Instrument(PORT, 4),
    minimalmodbus.Instrument(PORT, 5)
]

for instr in instruments:
    instr.serial.baudrate = BAUDRATE
    instr.serial.timeout = TIMEOUT
    instr.mode = minimalmodbus.MODE_RTU

# ================== DATABASE ==================

def init_database():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS telemetry_buffer (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER NOT NULL,
            payload TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            retry_count INTEGER DEFAULT 0
        )
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_timestamp
        ON telemetry_buffer(timestamp)
    """)
    conn.commit()
    conn.close()
    log("[SYSTEM] Database buffer initialized")

def add_to_buffer(data):
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM telemetry_buffer")
        count = cursor.fetchone()[0]

        if count >= MAX_BUFFER_SIZE:
            cursor.execute("""
                DELETE FROM telemetry_buffer
                WHERE id IN (
                    SELECT id FROM telemetry_buffer
                    ORDER BY timestamp ASC
                    LIMIT 1
                )
            """)
            log("[BUFFER] Buffer full → removed oldest record")

        ts = data[0]["ts"]
        cursor.execute(
            "INSERT INTO telemetry_buffer (timestamp, payload) VALUES (?, ?)",
            (ts, json.dumps(data))
        )
        conn.commit()
        conn.close()
        log(f"[BUFFER] Stored data (total={count + 1})")
        return True
    except Exception as e:
        log(f"[ERROR][BUFFER] Store failed: {e}")
        return False

def get_buffered_data(limit):
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, payload FROM telemetry_buffer
            ORDER BY timestamp ASC
            LIMIT ?
        """, (limit,))
        records = cursor.fetchall()
        conn.close()
        return [(r[0], json.loads(r[1])) for r in records]
    except Exception as e:
        log(f"[ERROR][BUFFER] Read failed: {e}")
        return []

def remove_from_buffer(record_ids):
    if not record_ids:
        return
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        placeholders = ",".join("?" * len(record_ids))
        cursor.execute(
            f"DELETE FROM telemetry_buffer WHERE id IN ({placeholders})",
            record_ids
        )
        conn.commit()
        conn.close()
        log(f"[BUFFER] Cleared {len(record_ids)} record(s)")
    except Exception as e:
        log(f"[ERROR][BUFFER] Delete failed: {e}")

def get_buffer_count():
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM telemetry_buffer")
        count = cursor.fetchone()[0]
        conn.close()
        return count
    except:
        return 0

# ================== MQTT CALLBACKS ==================

def on_publish(client, userdata, mid):
    with pending_lock:
        record_id = pending_publish.pop(mid, None)

    if record_id is not None:
        remove_from_buffer([record_id])
        log(f"[BUFFER→MQTT] ACK received (mid={mid}) → record {record_id} removed")
    else:
        log(f"[MQTT] ACK received for live data (mid={mid})")

def on_connect(client, userdata, flags, rc):
    global mqtt_connected
    if rc == 0:
        mqtt_connected = True
        relay.off()
        log("[MQTT] Connected to ThingsBoard")
    else:
        log(f"[ERROR][MQTT] Connect failed (rc={rc})")

def on_disconnect(client, userdata, rc):
    global mqtt_connected
    mqtt_connected = False
    relay.on()
    log("[WARN][MQTT] Disconnected from broker")

def init_mqtt_client():
    log("[SYSTEM] Initializing MQTT client")
    client = mqtt.Client()
    client.username_pw_set(ACCESS_TOKEN)
    client.on_publish = on_publish
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.reconnect_delay_set(1, 60)
    client.connect_async(THINGSBOARD_HOST, MQTT_PORT, 60)
    client.loop_start()
    return client

# ================== MQTT SEND ==================

def send_buffered_data(client):
    buffer_count = get_buffer_count()
    if buffer_count == 0:
        return

    log(f"[BUFFER→MQTT] Sending buffered data ({buffer_count} record(s))")
    batch_size = 50

    while True:
        records = get_buffered_data(batch_size)
        if not records:
            break

        for record_id, data in records:
            if not mqtt_connected:
                log("[WARN][BUFFER→MQTT] MQTT disconnected, pause resend")
                return

            try:
                payload = json.dumps(data)
                result = client.publish(
                    "v1/devices/me/telemetry",
                    payload,
                    qos=1
                )
                if result.rc == mqtt.MQTT_ERR_SUCCESS:
                    with pending_lock:
                        pending_publish[result.mid] = record_id
                    log(f"[BUFFER→MQTT] Sent record {record_id} (mid={result.mid})")
                else:
                    log(f"[ERROR][BUFFER→MQTT] Send failed rc={result.rc}")
            except Exception as e:
                log(f"[ERROR][BUFFER→MQTT] Exception: {e}")
                return

        time.sleep(0.3)

def publish_to_thingsboard(client, data):
    if not mqtt_connected:
        log("[MQTT] Offline → store data to buffer")
        add_to_buffer(data)
        return False

    try:
        payload = json.dumps(data)
        result = client.publish(
            "v1/devices/me/telemetry",
            payload,
            qos=1
        )
        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            log("[MQTT] Live data sent")
            return True
        else:
            log(f"[ERROR][MQTT] Publish failed rc={result.rc} → buffer")
            add_to_buffer(data)
            return False
    except Exception as e:
        log(f"[ERROR][MQTT] Exception → buffer: {e}")
        add_to_buffer(data)
        return False

def buffer_flush_worker():
    while True:
        if mqtt_connected:
            send_buffered_data(mqtt_client)
        time.sleep(5)

# ================== MODBUS READ ==================

def create_na_values(suffix):
    return {
        f"temp{suffix}": "N/A",
        f"humi{suffix}": "N/A",
        f"co2{suffix}": "N/A",
        f"aqi{suffix}": "N/A",
        f"tvoc{suffix}": "N/A"
    }

MAX_RETRIES = 2  # Thêm vào phần GLOBAL

def read_nodes():
    log("[SYSTEM] Reading sensor nodes")
    nodes_ready = {}

    for instr in instruments:
        node_id = instr.address
        
        # Retry logic cho write_register
        for attempt in range(1 + MAX_RETRIES):  # 1 lần đầu + 2 retry
            try:
                if attempt > 0:
                    log(f"[NODE {node_id}] Retry {attempt}/{MAX_RETRIES}")
                else:
                    log(f"[NODE {node_id}] Sending data request")
                
                instr.serial.reset_input_buffer()
                instr.serial.reset_output_buffer()
                instr.write_register(REQUEST_DATA_REG, 1)
                nodes_ready[node_id] = True
                log(f"[NODE {node_id}] Request OK")
                break  # Thành công thì thoát vòng lặp
                
            except Exception as e:
                if attempt < MAX_RETRIES:
                    log(f"[WARN][NODE {node_id}] Attempt {attempt + 1} failed: {e}")
                    time.sleep(0.2)  # Delay trước khi retry
                else:
                    nodes_ready[node_id] = False
                    log(f"[ERROR][NODE {node_id}] All attempts failed: {e}")

    time.sleep(104)

    results = []
    ts = rtc_get_utc_timestamp()
    for instr in instruments:
        suffix = f"-{instr.address}"
        if not nodes_ready.get(instr.address, False):
            payload = create_na_values(suffix)
        else:
            payload = read_node_data(instr, suffix)
        
        results.append({
            "ts": ts,
            "values": payload
        })

    return results


def read_node_data(instr, suffix):
    # Retry logic cho read operations
    for attempt in range(1 + MAX_RETRIES):
        try:
            if attempt > 0:
                log(f"[NODE{suffix}] Read retry {attempt}/{MAX_RETRIES}")
                time.sleep(0.2)
            
            # Kiểm tra data ready
            instr.serial.reset_input_buffer()
            instr.serial.reset_output_buffer()
            if instr.read_register(DATA_READY_REG) != 1:
                log(f"[NODE{suffix}] Data not ready")
                return create_na_values(suffix)

            # Đọc registers
            instr.serial.reset_input_buffer()
            instr.serial.reset_output_buffer()
            regs = instr.read_registers(DATA_START_REG, 7)
            temp, humi, co2, aqi, tvoc, st_scd, st_ens = regs

            log(f"[DATA][NODE{suffix}] T={temp/100}°C H={humi/100}% CO2={co2}ppm TVOC={tvoc}ppb AQI={aqi}")

            return {
                f"temp{suffix}": temp/100 if st_scd == 0 else "N/A",
                f"humi{suffix}": humi/100 if st_scd == 0 else "N/A",
                f"co2{suffix}": co2 if st_scd == 0 else "N/A",
                f"aqi{suffix}": aqi if st_ens == 0 else "N/A",
                f"tvoc{suffix}": tvoc if st_ens == 0 else "N/A"
            }
            
        except Exception as e:
            if attempt < MAX_RETRIES:
                log(f"[WARN][NODE{suffix}] Read attempt {attempt + 1} failed: {e}")
            else:
                log(f"[ERROR][NODE{suffix}] All read attempts failed: {e}")
    
    # Nếu tất cả retry đều fail
    return create_na_values(suffix)

def _bcd2dec(bcd):
    return (bcd >> 4) * 10 + (bcd & 0x0F)

def _dec2bcd(dec):
    return ((dec // 10) << 4) | (dec % 10)

def rtc_init():
    global rtcInitialized
    for attempt in range(4):
        log(f"[RTC] Kết nối thử lần {attempt + 1}...")
        try:
            bus = smbus2.SMBus(RTC_BUS)
            bus.read_byte_data(RTC_ADDR, 0x00)
            bus.close()
            log("[RTC] Kết nối OK trên /dev/i2c-1")
            rtcInitialized = True
            return True
        except Exception as e:
            if attempt < 3:
                log(f"[RTC] Lỗi: {e}. Thử lại sau 0.5 giây...")
                time.sleep(0.5)
            else:
                log(f"[RTC] Không thể kết nối sau 4 lần thử.")
                return False

def rtc_get_utc_timestamp():
    try:
        with i2c1_lock:
            bus = smbus2.SMBus(RTC_BUS)
            data = bus.read_i2c_block_data(RTC_ADDR, 0x00, 7)
            bus.close()
        sec = _bcd2dec(data[0] & 0x7F)
        mn  = _bcd2dec(data[1])
        hr  = _bcd2dec(data[2] & 0x3F)
        day = _bcd2dec(data[4])
        mon = _bcd2dec(data[5] & 0x1F)
        yr  = _bcd2dec(data[6]) + 2000
        t = (yr, mon, day, hr, mn, sec, 0, 0, 0)
        log(f"[RTC] Đọc OK: {yr:04d}-{mon:02d}-{day:02d} {hr:02d}:{mn:02d}:{sec:02d} UTC")
        return calendar.timegm(t) * 1000
    except Exception as e:
        log(f"[RTC] Lỗi đọc: {e}")
        return int(time.time() * 1000)

def rtc_set_datetime(utc_struct):
    with i2c1_lock:
        bus = smbus2.SMBus(RTC_BUS)
        bus.write_byte_data(RTC_ADDR, 0x00, _dec2bcd(utc_struct.tm_sec))
        bus.write_byte_data(RTC_ADDR, 0x01, _dec2bcd(utc_struct.tm_min))
        bus.write_byte_data(RTC_ADDR, 0x02, _dec2bcd(utc_struct.tm_hour))
        bus.write_byte_data(RTC_ADDR, 0x03, 0x01)
        bus.write_byte_data(RTC_ADDR, 0x04, _dec2bcd(utc_struct.tm_mday))
        bus.write_byte_data(RTC_ADDR, 0x05, _dec2bcd(utc_struct.tm_mon))
        bus.write_byte_data(RTC_ADDR, 0x06, _dec2bcd(utc_struct.tm_year - 2000))
        bus.close()

def rtc_sync_from_ntp():
    try:
        log("[RTC] Đồng bộ NTP...")
        c = ntplib.NTPClient()
        response = c.request('pool.ntp.org', version=3)
        utc_struct = time.gmtime(response.tx_time)
        rtc_set_datetime(utc_struct)
        log(f"[RTC] Đồng bộ OK: {time.strftime('%Y-%m-%d %H:%M:%S UTC', utc_struct)}")
        return True
    except Exception as e:
        log(f"[RTC] Đồng bộ NTP thất bại: {e}")
        return False

def rtc_sync_worker():
    while True:
        rtc_sync_from_ntp()
        time.sleep(3 * 24 * 3600)

# ================== MAIN ==================

if __name__ == "__main__":
    log("[SYSTEM] Modbus–MQTT Gateway starting")

    init_gpio()

    init_database()
    mqtt_client = init_mqtt_client()

    for _ in range(10):
        if mqtt_connected:
            break
        time.sleep(1)

    threading.Thread(
        target=buffer_flush_worker,
        daemon=True
    ).start()

    if not rtc_init():
        log("[WARNING] RTC DS3231 không khả dụng")

    threading.Thread(target=rtc_sync_worker, daemon=True).start()
    log("[MAIN] RTC sync thread đã khởi động")

    try:
        cycle = 0
        while True:
            cycle += 1
            log(f"[SYSTEM] Cycle {cycle} started")

            data = read_nodes()
            publish_to_thingsboard(mqtt_client, data)

            pending = get_buffer_count()
            if pending:
                log(f"[BUFFER] Pending records: {pending}")

            time.sleep(196)

    except KeyboardInterrupt:
        log("[SYSTEM] Shutdown requested")
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        cleanup_gpio()
        log("[SYSTEM] Gateway stopped")
