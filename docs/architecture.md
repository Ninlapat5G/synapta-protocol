# Architecture — Synapta Protocol v0.0.3

## ภาพรวม

```
                 ┌──────────────── node (process เดียว) ────────────────┐
 UTCP client ──▶ │ synapta-gateway  (HTTP, loopback, bearer token)       │
 synapta CLI ──▶ │   /utcp  /tools/{cap}  /status  /jobs  /routing ...   │
                 │            │ NodeHandle                               │
                 │ synapta-core                                          │
                 │   routing ── decide() ล้วน ๆ ──▶ dispatch ──┐          │
                 │   registry (peer state + advert ล่าสุด)     │          │
                 │   discovery: link (ขาออก) · server (ขาเข้า) │          │
                 │              handshake · flooding (advert)  │          │
                 │   execution: Executor · JobManager ◀───────┘ (local)  │
                 │   transport: Transport trait · framing · secure · TCP │
                 └───────────────────────────────────────────────────────┘
                          ▲  TCP 127.0.0.1, Noise-encrypted JSON  ▲
                          └──────── node อื่น (เหมือนกันทุกตัว) ───┘
```

ทุก node เหมือนกัน ไม่มี node พิเศษ สิ่งที่ต่างกันมีแค่ config (capability, ทรัพยากร, seed, trust)

## Crate — ทำไมแยกแค่สาม

| crate | หน้าที่เดียว | เหตุผลที่แยก |
|---|---|---|
| `synapta-core` | ตัว protocol | ไม่พึ่ง HTTP หรือ UTCP — runtime อื่น (Syn-Script, mobile) ฝังได้โดยไม่ลากของไม่ใช้มาด้วย |
| `synapta-gateway` | เปิด node ออกทาง HTTP ภายในเครื่อง (UTCP + ควบคุม) | เป็น adapter ชั้นบน ถอดออกได้ |
| `synapta-node` | โปรแกรมที่รันจริง | แยก binary ออกจาก library |

transport, discovery, routing, execution อยู่เป็น module ใน core — แยกเป็น crate แล้วไม่ได้อะไรเพิ่ม
(spec แนะนำให้แยก `synapta-transport` แต่ตัดสินใจไม่ทำ: abstraction ที่ต้องการคือ trait `Transport` ซึ่งได้ครบใน module)

## Module ใน `synapta-core`

| module | ทำอะไร |
|---|---|
| `protocol` | NodeId, version, **ทุกข้อความบนสาย** (`message.rs` = ที่เดียวของ schema) |
| `transport` | trait `Transport`/`Listener`, `AddressPolicy`, framing (ในที่แจ้ง), `secure` (frame เข้ารหัส), TCP |
| `identity` | key Ed25519, ลายเซ็น, trust store |
| `discovery` | `handshake` (Noise NN + ลายเซ็น Ed25519) · `link` (ต่อออก + heartbeat + reconnect) · `server` (รับเข้า) · `flooding` (advert) · `advert` (เซ็น/ตรวจ) |
| `registry` | มุมมองของ node นี้ต่อ peer อื่น: state ของ link + advert ล่าสุด |
| `routing` | `decide()` (policy ล้วน ไม่มี I/O) · `dispatch` (ส่งงานตามผล + บันทึก) |
| `execution` | trait `Executor`, executor mock, `JobManager`, log ของ job event |
| `node` | ประกอบทุกส่วน, `NodeHandle` (API สาธารณะ), shutdown |

## การตัดสินใจหลัก

### 1. ทุก node ต่อออกหาทุก node — คู่หนึ่งมีสอง connection

node A ส่ง request ไป B ผ่าน connection ที่ **A เปิดเอง** เท่านั้น B ตอบบน connection เดิม
ดังนั้น A↔B มีสองเส้น (A→B, B→A)

- ได้: ไม่ต้องมีกติกา "ใครเก็บ connection" ซึ่งเป็นแหล่ง race ที่พบบ่อย · state `READY` ของ peer แปลว่า
  "request ของฉันไปถึงเขาได้" ตรง ๆ
- เสีย: connection เป็นสองเท่า — ไม่มีผลที่ขนาด PoC

### 2. Liveness มาจาก link ตรงเท่านั้น

advert ถูกส่งต่อหลายทอด จึงอาจเก่า routing จะส่งงานไปเฉพาะ peer ที่ **link ตรงของเรา READY และตอบ heartbeat**
advert ใช้บอกความสามารถและทรัพยากร ไม่ใช้ตัดสินว่ายังมีชีวิต

### 3. Advert เซ็นโดยเจ้าของ (OSPF-inspired)

แต่ละ node ออก advert ของตัวเอง (capability + ทรัพยากร + address) เซ็นด้วย key ตัวเอง แล้ว flood ไปทุก peer
node ที่รับต่อจะส่งต่อเฉพาะ advert ที่ใหม่กว่าที่มี ลำดับใช้ `(incarnation, seq)`

- relay ปลอม/แก้ advert ไม่ได้ (ลายเซ็นไม่ผ่าน) → ปิดช่อง route hijack
- `incarnation` = เวลาเริ่ม process (ms) และเก็บลง `data_dir` เพื่อให้เพิ่มขึ้นเสมอแม้นาฬิกาถอยหลัง →
  node ที่ restart (seq เริ่มใหม่) ยังชนะ advert เก่าของตัวเอง
- address ที่ได้จาก advert ต้องผ่าน `AddressPolicy` ก่อน dial → peer ที่เชื่อใจแต่บั๊ก ทำให้เราไปต่อที่อื่นไม่ได้

### 4. ส่งงานแบบ at-most-once

routing อัตโนมัติจะลองเครื่องถัดไปก็ต่อเมื่อ **พิสูจน์ได้ว่าเครื่องแรกไม่ได้รันอะไร** (link ไม่ READY ตอนส่ง หรือตอบ `busy`)
หลัง timeout / connection หลุด งานอาจรันไปแล้ว จึงรายงาน error แทนการส่งซ้ำ (กันงานซ้ำซ้อนกับงานที่ไม่ idempotent)

### 5. เครื่องปลายทางเป็นคนตัดสินสิทธิ์

`allow` ของแต่ละ capability ถูกประกาศใน advert (ให้ router ข้ามเครื่องที่จะปฏิเสธ) **และ** ถูกบังคับซ้ำที่เครื่องปลายทาง
ด้วย node id ที่พิสูจน์แล้วใน handshake — ไม่ใช่ค่าที่ผู้เรียกบอกมา ทั้งสองอย่างอ่านจากค่าชุดเดียวกัน จึงไม่เพี้ยนจากกัน
(สอดคล้องกับ Syn-Script `docs/language.md` §10: "เครื่องปลายทางเป็นคนกำหนดว่า agent จากเครื่องอื่นใช้ module ไหนได้")

### 6. ไม่มีการส่งต่อหลายทอด

node ที่ได้รับ `invoke` จะรันเองหรือปฏิเสธ ไม่ส่งต่อ → ไม่มี routing loop

## Concurrency และการปิดระบบ

- state แยก lock ตามเรื่อง (registry, job, routing log, dialer) และ **ไม่ถือ lock ข้าม `.await`**
  (คัดลอก snapshot ออกมาก่อนเสมอ)
- background task ทุกตัวอยู่ใน `TaskTracker` และฟัง `CancellationToken` ตัวเดียว → `shutdown()` รอทุกตัวจบภายใน `shutdown_grace_ms`
- job token เป็นลูกของ shutdown token → ปิด node = ยกเลิกทุก job
- ทุก network operation มี timeout (connect, handshake, request, heartbeat, idle)
- reconnect ถอยแบบ exponential · address ที่รู้จาก advert หยุดหลัง `max_reconnect_attempts` จนกว่าจะเห็น advert ใหม่
  · seed ใน config ไม่ยอมแพ้: หลังครบโควตาลองช้าลงทุก `seed_retry_ms` (ค่าเริ่ม 30 s) ตลอดที่ node ยังรัน
- รอเหตุการณ์ด้วย `watch`/`Notify` ไม่มี busy-wait (ที่ poll มีแห่งเดียวคือ `synapta-sandbox` ที่รอสถานะของ process อื่นผ่าน HTTP)

## จุดต่อขยาย

| ต้องการ | ทำที่ |
|---|---|
| Tailscale | implement `Transport` + เพิ่ม `AddressPolicy` ช่วง tailnet — routing/discovery ไม่ต้องแก้ |
| `script.execute` (Syn-Script) / agent จริง | implement `Executor` แล้ว register — protocol ไม่เปลี่ยน |
| policy การเลือกเครื่องแบบอื่น | `routing::decide` (ฟังก์ชันล้วน มีเทสต์ครอบ) |
| ระบบสิทธิ์ละเอียดขึ้น | `NodeState::authorize` ที่เดียว |

## Mock ที่ใช้ (ระบุชัด)

- cpu / memory / availability ของ node มาจาก config (`[resources]`) — advert มี `mocked: true`
- `active_jobs` เป็นค่าจริง
- executor ทั้งห้าเป็น mock ไม่แตะ host: `system.echo`, `system.info`, `math.add`, `test.execute` (จำลอง suite ที่อยู่ใน allow-list),
  `agent.execute` (ตอบข้อความสำเร็จรูป ไม่เรียก AI)
