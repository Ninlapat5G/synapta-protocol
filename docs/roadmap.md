# ข้อจำกัดที่รู้แล้ว และ Roadmap

## Known limitations (v0.0.4)

### ความปลอดภัย
- **node id เห็นในที่แจ้งตอน handshake** — คนดักสายรู้ว่า node ไหนคุยกับ node ไหน (ไม่รู้ว่าคุยอะไร)
  ถ้าต้องซ่อน: ย้าย id เข้าไปในส่วนที่เข้ารหัส (แลกกับการรู้ช้าลงว่าอีกฝั่งไม่อยู่ใน trust list)
- **ต่อกับ node v0.0.1 / v0.0.2 / v0.0.3 ไม่ได้** — ต้องอัปเดตทุก node เป็น v0.0.4 พร้อมกันครั้งนี้ (ได้ `version_mismatch` ชัดเจน ไม่ค้าง)
  ตั้งแต่ v0.0.4 ไป release ที่ไม่เปลี่ยน wire version คุยกับรุ่นก่อนได้ (ดู "Version และความเข้ากันได้" ใน `protocol.md`)
- **ยังไม่มี build ไหนพูดได้หลาย wire version** — การต่อรองและการกันหลอกให้ถอย version มีแล้ว แต่ช่วงยังมีค่าเดียว
  วันที่ต้องเปลี่ยน wire แบบเข้ากันไม่ได้ ต้องเลือกว่าจะเก็บโค้ดของ version เก่าไว้กี่รุ่น
- **heartbeat timeout ต่ำเกินบนเครื่อง CPU แน่น** ทำ link หลุดและงานที่ค้างบน link ได้ `ConnectionLost` — ค่าเริ่ม 3 s พอสำหรับเครื่องทั่วไป
  (เทสต์ใช้ 600 ms เพื่อจับ peer ตายเร็ว และพบว่าหลุดเมื่อรันโหลดหนักซ้อนกัน 3 ชุด)
- **ไม่มีการเพิกถอน key** — เอา peer ออกจาก trust list ต้อง restart node
- **peer ที่เชื่อใจแล้วยังทำ DoS ได้** — เช่น ส่ง advert ถี่ ๆ ให้ตรวจลายเซ็น (มีเพดาน connection/request แต่ไม่มี rate limit)
- **API token** สร้างใหม่ทุกครั้งที่ node เริ่ม ผู้ใช้ UTCP ต้องอ่านไฟล์ใหม่หลัง restart · บน Windows สิทธิ์ไฟล์อาศัย ACL ของโฟลเดอร์
- **ทุกคนที่ถือ token ทำได้เท่ากับตัว node** — ยังไม่มีสิทธิ์แยกต่อผู้ใช้/agent
- ช่อง handshake ขาเข้ามีจำกัด (64) โปรแกรมในเครื่องที่เปิด connection ค้างไว้กินช่องได้ครั้งละ `handshake_ms`

### Discovery / state
- `adverts.sync` ส่ง advert ทั้งหมดทุกครั้งที่ link ใหม่ขึ้น (ไม่มี DB-description summary แบบ OSPF) — ใช้ได้กับ cluster เล็ก
  cluster ใหญ่จะชนเพดาน frame 1 MiB
- full mesh: ทุก node ต่อทุก node (N×(N−1) connection) ไม่มี multi-hop / area
- ข้อมูลทรัพยากรช้ากว่าความจริงได้ถึง `advert_min_interval_ms` + เวลาเดินทาง — ledger ของผู้เรียกแก้ให้เฉพาะงานของตัวเอง
  งานจากผู้เรียกคนอื่นยังเห็นช้า (เครื่องปลายทางตรวจซ้ำและตอบ `busy` แล้ว routing ไปเครื่องถัดไป)
  ภายใต้โหลดหนัก `busy` เด้งยังเกิดราว 1.5 ครั้งต่องาน (ดู `test-report.md`) — ปลอดภัย แต่เสียไป-กลับหนึ่งรอบ
  ถ้าต้องลด: ให้ `busy` แนบจำนวนงานจริงกลับมา แล้ว ledger จดว่าเครื่องนั้นเต็มจนกว่า advert ใหม่มา
- peer ที่รู้จาก advert และ dialer ยอมแพ้แล้ว จะถูก dial ใหม่ต่อเมื่อเห็น advert ใหม่ของมัน (หรือมันต่อเข้ามาเอง)
  seed ใน config ไม่มีปัญหานี้ (ลองใหม่ทุก `seed_retry_ms` ตลอด) แต่ seed ที่ปฏิเสธเราถาวร (เช่นไม่อยู่ใน trust list) จะขึ้น log เตือนทุกรอบ

### Execution
- executor ทั้งหมดเป็น mock · cpu/memory/availability เป็นค่าจาก config (`mocked: true`)
- delivery แบบ at-most-once — ไม่มี retry อัตโนมัติหลัง timeout (ตั้งใจ ดู `docs/routing.md`)
- job event เป็น best effort (คิวเต็มทิ้งได้) สถานะสุดท้ายถามได้เสมอภายใน `job_retention_ms`
- job ไม่ถูกเก็บลงดิสก์ — node restart แล้ว job หาย
- ถ้า connection ของผู้ส่งหลุด job ยังทำต่อจนจบ แต่ event ระหว่างนั้นหาย (ถาม status ได้หลังต่อใหม่)

### UTCP
- ทดสอบกับ Python SDK ทางการตัวเดียว (utcp 1.2.0 / utcp-http 1.1.15) · ใช้เฉพาะ protocol `http` ไม่มี streaming
- เทสต์ SDK ทางการไม่อยู่ใน `cargo test --workspace` อัตโนมัติ (ต้องมี Python)

## สถานะเฟส

ตอนนี้ยังอยู่ในเฟส sandbox (เครื่องเดียว, loopback) จนกว่าระบบจะนิ่ง · การข้ามเครื่องจริงเป็นเฟสถัดไปเสมอ
เฟสที่จะมาก่อน: สร้าง sandbox env บนเว็บให้คนลองใช้และทดสอบ protocol/ระบบต่าง ๆ ด้วยตัวเอง (ยังไม่ได้ออกแบบ)

## Roadmap (เรียงตามสิ่งที่กันพังได้มากสุด)

1. ~~**Secure channel**~~ — **เสร็จใน v0.0.2:** Noise NN + ลายเซ็น Ed25519 บน handshake hash (`docs/protocol.md`)
2. **Tailscale transport** — `AddressPolicy::Tailnet` (100.64.0.0/10 + `fd7a:115c:a1e0::/48`) + ตรวจว่า peer มาจาก tailnet จริง
   (`Transport` trait รองรับแล้ว ไม่ต้องแก้ routing)
3. **ทรัพยากรจริง** — อ่าน cpu/memory จาก host (โหมด `mock` ยังคงไว้สำหรับเทสต์)
4. **`script.execute`** — executor ที่รัน Syn-Script ใน sandbox ของตัวเอง พร้อมสิทธิ์ต่อ module ตาม `docs/language.md` §10 ของ Syn-Script
5. **DB summary ใน sync** + rate limit ต่อ peer → รองรับ cluster ใหญ่ขึ้น
6. **สิทธิ์ละเอียด** — scope ต่อ token / ต่อ agent, เพิกถอน key โดยไม่ต้อง restart
7. **UTCP streaming** (`streamable_http`) สำหรับ progress ของ async job · ทดสอบกับ TypeScript SDK
8. **Idempotency key** — ให้ผู้เรียกระบุ key แล้ว retry ได้อย่างปลอดภัย (เปลี่ยน at-most-once เป็น exactly-once สำหรับงานที่ต้องการ)

### ???????? Lab / relay ?????????
- ?????????????????????????????????? ??????????????????????? card/envelope
- job progress events ??????? relay ??? status/result/cancel ??????
- Lab ????? login ?????????????? tailnet ?????????? IP ???????????????????????
- ?????? trace ???????????????? ???? Save ??????; trace buffer ????????????????????????
- ?????????? node ??? UI ?????????????????????????????? ?????????????????? full mesh
