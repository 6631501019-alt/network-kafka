# Network Analytics & Telemetry: Kafka + Paramiko + GNS3 Orchestration

ระบบ Network Automation & Telemetry แบบ Asynchronous Event-Driven Architecture โดยใช้ **Apache Kafka** เป็น Message Broker ร่วมกับ **Paramiko (Python SSH)** ในการสั่งการอุปกรณ์เครือข่าย Cisco Routers บน **GNS3** พร้อมหน้าเว็บ Dashboard สำหรับควบคุมและติดตามผล

---

## 1. System Architecture

```text
[ Web Frontend ]  -->  [ FastAPI Backend ]  -->  [ Apache Kafka ]
  (Port 8000)               (Port 8000)             (Port 9092)
                                                         │
                                               Topic: router-tasks
                                                         │
                                                         ▼
[ Cisco Routers ]  <--  [ Paramiko Worker ]  <───────────┘
 (GNS3 Topology)          (SSH Client)
  - Router 1                                             │
  - Router 2                                    Topic: router-results
                                                         │
                                                         ▼
[ Web Frontend ]  <──  [ FastAPI Backend ]  <────────────┘
```

### การไหลของข้อมูล (Data Flow):
1. **Frontend**: ผู้ใช้เลือก Router และคำสั่ง Cisco IOS (เช่น `show ip interface brief`, `show ip route`) แล้วกดส่ง
2. **FastAPI Backend**: รับคำสั่งผ่าน REST API (`POST /api/tasks`) แล้วทำหน้าที่เป็น **Kafka Producer** ส่งข้อความเข้า Topic `router-tasks`
3. **Kafka Broker**: ทำหน้าที่เป็น Message Queue พักข้อมูลและกระจายงานแบบ Asynchronous
4. **Paramiko Worker**: ทำหน้าที่เป็น **Kafka Consumer** ดึงงานจากคิว แล้วเปิด SSH Session ไปยัง Router1 / Router2 ใน GNS3
5. **GNS3 Routers**: รันคำสั่ง CLI และส่งผลลัพธ์กลับมายัง Worker
6. **Worker**: นำผลลัพธ์ส่งกลับเข้า Kafka Topic `router-results`
7. **Backend**: ดึงผลลัพธ์และบรอดแคสต์ผ่าน WebSocket / REST API ให้ Frontend แสดงผลในหน้าจอ Terminal แบบ Real-time

---

## 2. โครงสร้างไฟล์ในโปรเจกต์ (Project Structure)

```text
Kafka/
├── docker-compose.yml           # รัน Apache Kafka (KRaft mode) + Kafka-UI
├── run.sh                       # สคริปต์คำสั่งเดียวเริ่มระบบทั้งหมด (One-Click Start)
├── test_pipeline.py             # สคริปต์ทดสอบ Kafka Integration Test อัตโนมัติ
├── backend/
│   ├── app.py                   # FastAPI Application (Producer & WebSocket)
│   ├── models.py                # Pydantic Schemas
│   ├── config.py                # Kafka Topics & Server Configurations
│   └── requirements.txt
├── worker/
│   ├── worker.py                # Worker Daemon (Kafka Consumer)
│   ├── router_client.py         # Paramiko SSH Client (พร้อม Fallback)
│   ├── inventory.py             # IP, Port, Username, Password ของ Router
│   ├── mock_router.py           # ตัวจำลอง Cisco IOS CLI เสมือน
│   └── requirements.txt
├── frontend/
│   ├── index.html               # หน้า Dashboard สไตล์ Network Operations Center
│   ├── app.js                   # จัดการส่งงานและรับผลลัพธ์ Real-time
│   └── style.css                # การจัดหน้าและอนิเมชันสถานะระบบ
└── gns3/
    ├── router1_config.cfg       # คอนฟิก Cisco IOS สำหรับ Router 1
    ├── router2_config.cfg       # คอนฟิก Cisco IOS สำหรับ Router 2
    └── GNS3_SETUP_GUIDE.md      # คู่มือการต่อสาย Cloud/NAT ใน GNS3 อย่างละเอียด
```

---

## 3. วิธีการเริ่มใช้งาน (Quick Start)

### วิธีที่ 1: รันคำสั่งเดียวผ่าน `run.sh` (แนะนำ)
เปิด Terminal ในโฟลเดอร์โปรเจกต์ แล้วพิมพ์:

```bash
cd /home/gus/Desktop/Kafka
./run.sh
```

สคริปต์จะเริ่มทำงาน:
1. สตาร์ท Kafka Broker (Docker)
2. สตาร์ท Kafka UI (Docker)
3. เตรียม Virtual Environment (`venv`)
4. สตาร์ท Worker Process
5. สตาร์ท FastAPI Backend

---

## 4. พอร์ตและบริการที่เปิดใช้งาน

| Service | URL | คำอธิบาย |
|---|---|---|
| **Web Dashboard** | [http://localhost:8000](http://localhost:8000) | หน้าเว็บหลักสำหรับสั่งการ Router และดูผลลัพธ์ |
| **Kafka UI** | [http://localhost:8085](http://localhost:8085) | หน้าจอตรวจสอบ Kafka Topics, Messages, และ Consumer Groups |
| **API Documentation** | [http://localhost:8000/docs](http://localhost:8000/docs) | Swagger Interactive API Docs สำหรับทดสอบ REST API |
| **Kafka Broker** | `localhost:9092` | พอร์ตหลักของ Apache Kafka Broker |

---

## 5. การทดสอบระบบ (Integration Test)

คุณสามารถทดสอบการทำงานของ Kafka Pipeline ได้ทันทีด้วยสคริปต์:

```bash
./venv/bin/python3 test_pipeline.py
```

---

## 6. การเชื่อมต่อกับ GNS3 จริง

ดูรายละเอียดขั้นตอนอย่างสมบูรณ์ได้ใน [gns3/GNS3_SETUP_GUIDE.md](gns3/GNS3_SETUP_GUIDE.md)

1. ใน GNS3 ลากโหนด **NAT** หรือ **Cloud** มาเชื่อมกับพอร์ต `GigabitEthernet0/0` ของ Router1 และ Router2
2. Copy คอนฟิกจาก `gns3/router1_config.cfg` ไปวางใน Router1
3. Copy คอนฟิกจาก `gns3/router2_config.cfg` ไปวางใน Router2
4. ตรวจสอบว่าสามารถ `ssh admin@192.168.122.101` ได้จากเครื่อง Host
5. เมื่อรันคำสั่งจากหน้าเว็บ Dashboard ตัว Worker จะเชื่อมต่อ SSH เข้าไปยัง Router จริงใน GNS3 ทันที!
*(หมายเหตุ: หากยังไม่ได้เปิด GNS3 ระบบมี Mock Fallback อัตโนมัติ เพื่อให้สามารถเดโมระบบและทดสอบหน้าจอได้ตลอดเวลา)*
