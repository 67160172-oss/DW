# ETL Lab Report

Student ID: <67160172>
Name: <Pojcharapon Promma>

## 1. Data Quality Problems Found
- **customers.csv**: มี `customer_id` ซ้ำ 2 รายการ (C004, C009), จังหวัด (`province`) เขียนหลายรูปแบบปนกัน (ไทย/อังกฤษ, ตัวพิมพ์ใหญ่-เล็ก, ตัวย่อ เช่น "BKK", "chon buri", "ชลบุรี"), และมี email ว่าง 1 แถว (C013 ก็ไม่มี province ด้วย)
- **products.json**: โครงสร้างเป็น nested JSON (`category.name`, `pricing.price`) ต้อง flatten ก่อนใช้งาน, ราคาบางรายการเป็น string ที่มี comma คั่นหลักพัน (เช่น "1,299.00") ไม่ใช่ตัวเลข, และมีสินค้า 1 รายการที่ category เป็น null
- **orders.csv**: มี `order_id` ซ้ำแบบซ้ำทั้งแถว 3 รายการ (O0011, O0041, O0101), รูปแบบวันที่ปนกัน 4 แบบ (YYYY-MM-DD, YYYY/MM/DD, DD/MM/YYYY, DD-Mon-YYYY) รวมถึงค่าที่ไม่ใช่วันที่เลย ("not-a-date"), สถานะ (`status`) ตัวพิมพ์ใหญ่-เล็กปนกัน (PAID/paid), และพบค่าที่ผิดกฎธุรกิจ: qty ติดลบ, unit_price ติดลบ, discount_pct เกิน 100 รวมถึง `customer_id`/`product_id` ที่ไม่มีอยู่จริงในตาราง master (C999, P999)

## 2. Cleaning / Transformation Rules
- **Customers**: ลบแถวซ้ำด้วย `drop_duplicates(subset="customer_id", keep="first")`; ทำ province ให้เป็นมาตรฐานผ่าน `PROVINCE_MAP` (จับคู่แบบ case-insensitive สำหรับอังกฤษ และจับคู่ตรงสำหรับภาษาไทย) ค่าที่ว่างหรือไม่มีให้เป็น "Unknown"; email ว่างให้แทนที่ด้วย "unknown@example.com"
- **Products**: flatten ด้วย `pd.json_normalize` แล้ว rename คอลัมน์ `category.name` → `category`, `pricing.price` → `price`; แปลง price เป็น numeric โดยลบ comma ก่อน cast เป็น float; category ที่เป็น null/ว่าง แทนที่ด้วย "Unknown"
- **Orders**: ลบแถวซ้ำด้วย `order_id`; แปลงสถานะเป็นตัวพิมพ์เล็กทั้งหมด (`.str.lower()`); parse วันที่โดยลองรูปแบบที่รู้จักทั้ง 4 แบบตามลำดับ ถ้าไม่ตรงเลยถือว่าวันที่ไม่ถูกต้อง; ตรวจสอบกฎ qty>0, unit_price>0, 0≤discount_pct≤100 และวันที่ต้อง parse ได้ — แถวที่ไม่ผ่านเงื่อนไขใดเงื่อนไขหนึ่งจะถูกแยกไปที่ rejects พร้อมเหตุผล
- **Merge**: กรองเฉพาะสถานะ paid/completed เข้าสู่ fact; join กับ customers และ products ที่ทำความสะอาดแล้ว; order ที่อ้างอิง customer_id/product_id ที่ไม่มีอยู่จริงถูกแยกไป rejects ในสเตจ referential_integrity; คำนวณ `gross_amount = qty * unit_price`, `discount_amount = gross_amount * discount_pct / 100`, `sales_amount = gross_amount - discount_amount`

## 3. Rejected Records
จำนวน: 80 รายการ (จาก 183 orders ทั้งหมด หลังตัดซ้ำเหลือ 180 รายการที่ไม่ซ้ำ)

เหตุผลหลัก:
- สถานะไม่ใช่ paid/completed (pending 44 + cancelled 32 = 76 รายการ) — ไม่ถือว่าเป็นข้อผิดพลาดของข้อมูล แต่เป็นกฎทางธุรกิจที่ระบุให้เก็บเฉพาะยอดขายที่เกิดขึ้นจริง
- qty ≤ 0 หรือไม่ใช่ตัวเลข: 1 รายการ
- unit_price ≤ 0 หรือไม่ใช่ตัวเลข: 1 รายการ
- discount_pct อยู่นอกช่วง 0–100: 1 รายการ
- order_date ไม่ถูกต้อง (parse ไม่ได้): 1 รายการ

## 4. ETL Validation
- Valid transformed rows: 100
- Warehouse rows: 100
- Duplicate order_id: 0
- Source total sales: 192,074.66
- Warehouse total sales: 192,074.66
- Validation status: PASS

## 5. Idempotency Test
จำนวน fact_sales หลัง run ครั้งที่ 1: 100

จำนวน fact_sales หลัง run ครั้งที่ 2: 100

อธิบายผล: จำนวนแถวใน `fact_sales` ไม่เพิ่มขึ้นเมื่อรัน pipeline ซ้ำ เพราะตาราง `fact_sales` กำหนด `order_id` เป็น `PRIMARY KEY` และการโหลดข้อมูลใช้คำสั่ง `INSERT OR REPLACE` ซึ่งจะอัปเดตแถวเดิมที่มี key ตรงกันแทนที่จะแทรกแถวใหม่ซ้ำ ทำให้ pipeline สามารถรันซ้ำได้อย่างปลอดภัย (idempotent) โดยไม่เกิดข้อมูลซ้ำใน warehouse
