# ทบทวน spec ต้นทาง (งานที่ส่งต่อมาจาก ChatGPT)

spec โดยรวมดี: ขอบเขตชัด, ห้าม `0.0.0.0`, ห้ามรันคำสั่ง host, ห้าม silent fallback, ให้ระบุ mock ตรง ๆ
แต่มีจุดที่ถ้าทำตามตัวอักษรจะได้ระบบที่มีช่องโหว่หรือพังในกรณีจริง รายการนี้คือสิ่งที่พบและวิธีที่ implementation ปิดไว้

## ช่องโหว่ / กับดักที่ spec ไม่ได้พูดถึง

| # | spec เขียนว่า | ถ้าทำตามตรง ๆ จะเกิด | ทำจริงอย่างไร | อยู่ที่ |
|---|---|---|---|---|
| 1 | "ตรวจสอบ Node Identity ตาม Trust Configuration" | ทางง่ายคือเทียบ `node_id` ที่อีกฝั่งบอกมากับรายชื่อ → ใครก็อ้างชื่อ `node-b` ได้ | ต้องพิสูจน์ด้วยลายเซ็น Ed25519 สองทาง บน transcript ที่มี nonce ใหม่ทุกครั้ง + ชื่อทั้งสองฝั่ง + role | `discovery/handshake.rs` |
| 2 | "OSPF-inspired state exchange" + auto-registration | state ที่ส่งต่อหลายทอดถ้าไม่เซ็น: node ไหนก็ปลอม capability/ทรัพยากรของ node อื่นได้ (route hijack) หรือชี้ address ของ node อื่นไปที่อื่น | advert เซ็นโดยเจ้าของ relay แก้ไม่ได้ · address ที่เรียนรู้ต้องผ่าน `AddressPolicy` ก่อน dial (ไม่งั้นเป็นช่อง SSRF ให้ peer สั่งให้เราไปต่อ host ใดก็ได้) | `discovery/advert.rs`, `flooding.rs` |
| 3 | "Version/Sequence Number กัน stale state" | seq ที่เริ่มนับใหม่ทุกครั้ง → node ที่ restart จะถูก peer มองว่า advert ใหม่ "เก่ากว่า" ของก่อน restart และถูกทิ้ง | ลำดับใช้ `(incarnation, seq)` · incarnation เก็บใน `data_dir` และเพิ่มขึ้นเสมอแม้นาฬิกาถอยหลัง | `registry`, `node/mod.rs` |
| 4 | "Automatic Routing เลือก Node อื่นได้" เมื่อ node ล้ม | ถ้าลองเครื่องถัดไปหลัง timeout งานอาจถูกรัน **สองครั้ง** (เครื่องแรกรันไปแล้วแต่คำตอบหาย) — อันตรายกับงานที่ไม่ idempotent | at-most-once: ลองเครื่องถัดไปเฉพาะเมื่อพิสูจน์ได้ว่ายังไม่ได้รัน (link ไม่พร้อม / `busy`) | `routing/dispatch.rs` |
| 5 | "Unauthorized Capability ถูกปฏิเสธ" | ถ้าตรวจที่ router ฝั่งผู้เรียกอย่างเดียว ผู้เรียกที่ข้าม router ก็เรียกได้ | เครื่องปลายทางตรวจเองด้วย id ที่พิสูจน์ใน handshake (มีเทสต์ที่ข้าม router ส่งตรง) | `node/state.rs::authorize` |
| 6 | Job status / cancel | spec ไม่ได้บอกว่าใครยกเลิกได้ → node ที่เชื่อใจตัวไหนก็ยกเลิกงานของคนอื่นได้ | job เห็น/ยกเลิกได้เฉพาะ node ที่ส่งมา (อื่น ๆ ได้ `job_not_found` ไม่บอกว่ามีอยู่) | `execution/jobs.rs` |
| 7 | UTCP Adapter บน Node A | endpoint HTTP บน localhost ที่ไม่มี auth = โปรแกรมไหนในเครื่อง **รวมถึงหน้าเว็บในเบราว์เซอร์ (DNS rebinding)** สั่งงานทั้ง cluster ได้ | bearer token (ไฟล์ในข้อมูลของ node) + ตรวจ `Host` ต้องเป็น loopback ของตัวเอง + จำกัดขนาด/เวลา | `synapta-gateway/src/http.rs` |
| 8 | "จำกัดขนาด Message" | จำกัดหลังอ่านแล้ว = ยังจองหน่วยความจำตามที่อีกฝั่งประกาศ | ตรวจ length prefix ก่อนจอง · จำกัดจำนวน connection, request ค้างต่อ connection, idle timeout, handshake timeout | `transport/framing.rs`, `discovery/server.rs` |
| 9 | "Node ไม่รับงานก่อนผ่าน Handshake" | ระวังตอบ request ที่มาก่อน handshake ด้วยการ "รอ" — ทำให้ connection ค้างกินทรัพยากร | ก่อน `welcome` รับแค่ `hello` อย่างอื่นตอบ `not_ready` แล้วปิดทันที | `handshake.rs::accept` |
| 10 | Peer ที่เรียนรู้จาก gossip | node รู้จักตัวเองผ่าน advert ที่วนกลับมา แล้ว dial ตัวเอง หรือส่ง advert ของตัวเองวนไม่จบ | ไม่ dial address ตัวเอง · handshake ปฏิเสธ "connected to self" · advert ของตัวเองที่วนกลับมาถูกทิ้ง | `link.rs`, `flooding.rs` |
| 11 | Explicit target ต้องไม่เปลี่ยนเครื่อง | ถ้า node ปลายทางที่ได้รับงานส่งต่อเองได้ (multi-hop) ก็เปลี่ยนเครื่องได้อีกชั้นหนึ่งและเสี่ยง routing loop | node ที่ได้รับ `invoke` รันเองหรือปฏิเสธเท่านั้น ไม่ส่งต่อ | `node/service.rs` |

## จุดที่ spec ขอแต่ไม่ได้ทำตามตัวอักษร (พร้อมเหตุผล)

- **แยก crate `synapta-transport`** — ไม่แยก เพราะสิ่งที่ต้องการคือ abstraction (trait `Transport`) ซึ่งเป็น module ใน core ก็พอ
  (spec เองบอกว่าอย่าแยก crate เพื่อให้ดูใหญ่)
- **โฟลเดอร์ `tests/integration`, `tests/e2e` ระดับ workspace** — Cargo วาง integration test ไว้ใน crate:
  `synapta-core/tests/acceptance.rs` (in-process) และ `synapta-node/tests/e2e.rs` (3 process)

## สิ่งที่ spec ถูกต้องและสำคัญ (คงไว้ครบ)

ห้ามรันคำสั่งบน host (มีเทสต์สแกน source กันไว้) · loopback เท่านั้น · explicit target ไม่ fallback · ทุก decision อธิบายได้ ·
ไม่ใช้ LLM ทำ routing · ระบุ mock ชัด · ห้ามอ้าง UTCP ถ้าไม่ได้ตรวจกับ spec จริง
