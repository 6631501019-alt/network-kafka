# คู่มือการติดตั้งและเชื่อมต่อ GNS3 (GNS3 Setup & Integration Guide)

คู่มือนี้สำหรับเชื่อมต่อ **Worker (Paramiko)** ที่รันอยู่บนเครื่อง Host เข้ากับ **Router1** และ **Router2** ภายใน **GNS3**

---

## 1. ผังการต่อสายใน GNS3 (Topology)

```text
                  +--------------------------+
                  |   Host Machine (Linux)   |
                  |  Kafka + Worker (Paramiko)|
                  +------------+-------------+
                               |
                   [ NAT Node / Cloud Node ]
                  (เช่น virbr0 / 192.168.122.x)
                               |
             +-----------------+-----------------+
             | (Gig0/0)                          | (Gig0/0)
    +--------+---------+                +--------+---------+
    |     Router1      |  (Gig0/1)      |     Router2      |
    | 192.168.122.101  +----------------+ 192.168.122.102  |
    +------------------+   10.0.0.0/24  +------------------+
```

---

## 2. ขั้นตอนการตั้งค่าใน GNS3

### ขั้นตอนที่ 1: วาง Node ใน GNS3 Workspace
1. ลาก **NAT** (แนะนำ) หรือ **Cloud** node ออกมาวางในหน้าต่าง Workspace
2. ลาก **Cisco IOS Router** (เช่น Cisco 7200 หรือ c3725 หรือ IOU) มา 2 ตัว ตั้งชื่อว่า **Router1** และ **Router2**

### ขั้นตอนที่ 2: ลากสายเชื่อมต่อ (Connections)
- **สายที่ 1**: จาก `NAT` (nat0) ไปยัง `Router1` พอร์ต `GigabitEthernet0/0` (หรือ `FastEthernet0/0`)
- **สายที่ 2**: จาก `NAT` (nat0) ไปยัง `Router2` พอร์ต `GigabitEthernet0/0`
  *(หมายเหตุ: หาก NAT มีพอร์ตเดียว ให้ลากผ่าน Ethernet Switch หรือใช้ Cloud node ที่ผูกกับ `virbr0`)*
- **สายที่ 3**: เชื่อมระหว่าง `Router1` (`GigabitEthernet0/1`) และ `Router2` (`GigabitEthernet0/1`)

### ขั้นตอนที่ 3: เปิด Router และตั้งค่า SSH
คลิกขวาที่ Router1 เลือก **Console** แล้ว Copy โค้ดจากไฟล์ `gns3/router1_config.cfg` ไปวาง:
```cisco
enable
configure terminal
hostname Router1
ip domain-name lab.local
crypto key generate rsa modulus 1024
username admin privilege 15 secret admin123
line vty 0 4
 transport input ssh
 login local
 exit
interface GigabitEthernet0/0
 ip address 192.168.122.101 255.255.255.0
 no shutdown
 exit
end
write memory
```

ทำแบบเดียวกันบน Router2 โดยใช้คอนฟิกจาก `gns3/router2_config.cfg`:
- กำหนด IP: `192.168.122.102 255.255.255.0`

---

## 3. ทดสอบการเชื่อมต่อจากเครื่อง Host

เปิด Terminal บนเครื่องคอมพิวเตอร์ของคุณ แล้วทดสอบ Ping และ SSH:

```bash
# ทดสอบ Ping
ping -c 3 192.168.122.101
ping -c 3 192.168.122.102

# ทดสอบ SSH เข้า Router1
ssh admin@192.168.122.101
# (Password: admin123)
```

หากเข้าได้สำเร็จ แสดงว่าพร้อมใช้งานกับระบบ Worker แล้ว!

---

## 4. หาก Router ของคุณได้ IP อื่น
ถ้า IP ของ Router ใน GNS3 เป็นวงอื่น (เช่น ได้รับ DHCP เป็น `192.168.122.55` หรือวง `192.168.x.x` อื่น):
คุณสามารถแก้ไข IP ได้ที่ไฟล์ `worker/inventory.py` หรือกำหนดผ่าน Environment Variable:

```bash
export ROUTER1_HOST="192.168.122.55"
export ROUTER2_HOST="192.168.122.56"
```

---

## 5. โหมด Simulation (Mock Mode)
หากคุณต้องการเดโมระบบ หรืออาจารย์ขอดูตอนที่ยังไม่ได้เปิด GNS3:
- ตัว Worker มีระบบ **Mock Router Fallback** อัตโนมัติ หากไม่สามารถเชื่อมต่อ Router จริงได้ ระบบจะสลับไปตอบจำลอง Cisco IOS ให้ทันที ทำให้ Dashboard แสดงผลได้ 100% เสมอ
- หากต้องการบังคับใช้โหมดจำลองตลอดเวลา สามารถตั้งค่าได้ด้วย `export MOCK_ROUTER="true"`
