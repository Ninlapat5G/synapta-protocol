# Protocol Message Specification — v0.0.3 (wire version 3)

ที่มาของจริง: `crates/synapta-core/src/protocol/message.rs` (แก้ไฟล์นั้นต้องแก้เอกสารนี้ด้วย)

## Framing

มีสองชั้น เนื้อในเป็น JSON (UTF-8) ที่มี field `"type"` เหมือนกันทุก frame

**1. ในที่แจ้ง** — ใช้แค่ `hello`, `hello_ack` และ `error` ที่ปฏิเสธสองตัวนี้ (`transport/framing.rs`)

```
┌──────────────┬──────────────────────────────┐
│ length: u32  │ JSON (UTF-8), length ไบต์     │
│ big-endian   │                              │
└──────────────┴──────────────────────────────┘
```

**2. เข้ารหัส** — ทุก frame หลังจากนั้น (`transport/secure.rs`) frame หนึ่งเป็น record หนึ่งตัวขึ้นไป

```
┌──────────────┬─────────────────────────────────────────────────────┐
│ length: u16  │ Noise ciphertext (ChaCha20-Poly1305, tag 16 ไบต์)     │
│ big-endian   │   ถอดแล้ว = flag 1 ไบต์ (1 = record สุดท้าย, 0 = มีต่อ) │
│              │             + JSON ส่วนหนึ่งของ frame                 │
└──────────────┴─────────────────────────────────────────────────────┘
```

- record หนึ่งยาวได้ไม่เกิน 65 535 ไบต์ (เพดานของ Noise) frame ที่ยาวกว่านั้นแบ่งหลาย record
- แต่ละทิศนับ record 0, 1, 2… และใช้เลขนั้นเป็น nonce → record ที่ถูกแก้ ส่งซ้ำ สลับลำดับ หาย หรือมาจาก session อื่น
  ถอดไม่ผ่าน ฝั่งรับปิด connection ทันทีโดยไม่ตอบอะไร
- ทั้งสองชั้น: `length` = 0 → malformed · ขนาด frame รวม > `limits.max_frame_bytes` (ค่าเริ่ม 1 MiB) → `frame_too_large`
  ตรวจก่อนจองหน่วยความจำ (ชั้นเข้ารหัสตรวจทุก record ที่สะสม)
- **ระหว่าง handshake** ทุก frame (ทั้งในที่แจ้งและเข้ารหัส) จำกัดที่ 4 KiB ไม่ว่า config จะตั้งเท่าไร —
  ข้อความ handshake จริงเล็กกว่า 1 KiB และผู้ที่ยังไม่ได้ยืนยันตัวตนไม่ควรทำให้ node จองหน่วยความจำ 1 MiB ต่อ connection ได้
- frame ผิดรูป/ใหญ่เกิน: ฝั่งรับส่ง `Frame::Error` (เข้ารหัสถ้าอยู่ในชั้น 2) แล้วปิด connection — process ไม่ล้ม

## ลำดับบน connection

```
dialer                                    acceptor
  hello        {wire, node_id, noise}                       -->      ในที่แจ้ง
                                          <--  hello_ack {wire, wire_version, node_id, noise}
  ─────────── เข้ารหัสตั้งแต่ตรงนี้ ───────────
                                          <--  hello_proof {signature}
  hello_finish {signature}                                  -->
                                          <--  welcome
  request  {id, request:{method, params}}                   -->
                                          <--  response {id, result | error}
  notify   {method:"advert", params}   (one-way)            -->
                                          <--  event {event:"job_progress"|"job_finished", ...}
```

- ก่อน `welcome` ฝั่ง acceptor รับแค่ `hello` — frame อื่นทั้งหมด (รวม request ที่ถูกต้อง) ได้ `error{code:"not_ready"}` แล้วปิด
- handshake ทั้งหมดต้องเสร็จใน `timeouts.handshake_ms`
- request หลายตัวค้างพร้อมกันบน connection เดียวได้ จับคู่ด้วย `id` (ไม่เกิน `limits.max_inflight_requests_per_connection` → เกินได้ `busy`)
  คำตอบมาไม่เรียงลำดับได้ · `ping` ไม่นับรวมในเพดานนี้ และถูกตอบทันทีเสมอ (ไม่อย่างนั้น link ที่งานเต็มทุกช่องจะถูกตัดเพราะ ping ได้ `busy`)
- acceptor ปิด connection ที่เงียบเกิน `timeouts.idle_timeout_ms` (dialer ส่ง `ping` ทุก `heartbeat_interval_ms` อยู่แล้ว)

## Handshake (แลก key + ยืนยันตัวตนสองทาง)

ที่มา: `discovery/handshake.rs`

1. **แลก key** — Noise `Noise_NN_25519_ChaChaPoly_SHA256` (crate `snow`), prologue `synapta-protocol`
   `hello.noise` / `hello_ack.noise` = ข้อความ Noise ข้อที่ 1 / 2 (hex) คือ X25519 public key ชั่วคราวของแต่ละฝั่ง
   สร้างใหม่ทุก connection → ได้ session key และ handshake hash `h` (ครอบคลุม key ชั่วคราวทั้งสอง)
2. **เลือก version** — `hello.wire` / `hello_ack.wire` = ช่วง wire version ที่แต่ละฝั่งพูดได้ `{min, max}`
   acceptor เลือกตัวสูงสุดที่อยู่ในทั้งสองช่วงใส่ `hello_ack.wire_version` · dialer คำนวณเองแล้วต้องได้ค่าเดียวกัน
   ไม่มีตัวที่ร่วมกัน → `version_mismatch` (ข้อความบอกช่วงของทั้งสองฝั่ง)
3. **ยืนยันตัวตน** — แต่ละฝั่งเซ็น transcript ด้วย Ed25519 key ของ node (key เดิม trust list เดิม):

```
synapta-handshake\n<role>\nwire <wire_version>\ndialer <dialer_id> <min> <max>\nacceptor <acceptor_id> <min> <max>\n<hex(h)>
```

- `hello_proof.signature` = acceptor เซ็น role `acceptor` · `hello_finish.signature` = dialer เซ็น role `dialer`
- ตรวจ: มี wire version ร่วมกัน · node id อยู่ใน trust list · ลายเซ็นผ่านด้วย key ของ id นั้น (`verify_strict`) ·
  ไม่ใช่ตัวเอง · ถ้า dialer รู้ว่าที่ address นั้นควรเป็นใคร (จาก advert) ต้องตรง
- **ทำไมกันคนกลางได้:** คนกลางที่อยากอ่านข้อความต้องเปิด Noise session แยกกับแต่ละฝั่ง → สอง session ได้ `h` ต่างกัน
  ลายเซ็นที่ฝั่งจริงเซ็นจึงใช้กับอีก session ไม่ได้ (มีเทสต์ `relaying_man_in_the_middle_is_refused`)
- role กันเอาลายเซ็นทิศหนึ่งไปใช้อีกทิศ · id และช่วง version อยู่ใน transcript เพราะส่งในที่แจ้ง ไม่ได้อยู่ใน `h`
- **ทำไมกันการหลอกให้ถอย version ได้:** คนกลางที่แก้ช่วง version ใน hello (เช่นตัดตัวใหม่ออกให้เหลือตัวเก่า)
  ทำให้สองฝั่งเซ็น transcript คนละแบบ ลายเซ็นจึงไม่ผ่าน (เทสต์ `an_edited_version_offer_is_refused`)
- key ชั่วคราวทิ้งเมื่อ connection ปิด → ข้อความที่ดักบันทึกไว้ยังอ่านไม่ได้ แม้ key ของ node รั่วทีหลัง (forward secrecy)

**ที่ยังเห็นในที่แจ้ง:** ช่วง wire version, node id ทั้งสองฝั่ง, public key ชั่วคราว — นอกนั้นเข้ารหัสหมด รวมถึง error

**กับ node v0.0.1 / v0.0.2:** ต่อกันไม่ได้ (wire version 1 / 2) — hello แบบเก่ายังอ่านออก (ไม่มี `wire` อ่านเป็น `{0,0}`)
และได้ `version_mismatch` พร้อมข้อความ "older than v0.0.3" ไม่ใช่ malformed

## Version และความเข้ากันได้

ที่มา: `protocol/mod.rs` (`WireRange`), `protocol/message.rs`

- **wire version** คือตัวเลขเดียวที่บอกว่าสอง node คุยกันได้หรือไม่ ขึ้นเลขเฉพาะเมื่อสิ่งที่ส่งบนสายเปลี่ยนแบบเข้ากันไม่ได้
  release ที่ไม่แตะรูปแบบข้อความไม่ต้องขึ้นเลข จึงคุยกับ node รุ่นก่อนได้ · 1 = v0.0.1, 2 = v0.0.2, **3 = v0.0.3**
- **software version** (เลข release เช่น `0.0.3`) อยู่ใน advert และ `system.info` ให้คนอ่าน ไม่ใช้ตัดสินอะไร
- **เพิ่มได้โดยไม่ขึ้นเลข** (ฝั่งรับที่เก่ากว่าไม่หลุด):
  - field ใหม่ในข้อความเดิม — ฝั่งรับไม่สนใจ field ที่ไม่รู้จัก
  - frame type, notification, event ใหม่ — ฝั่งรับข้าม
  - request method ใหม่ — ฝั่งรับตอบ `unsupported_request` (รับประกันว่ายังไม่ได้รัน)
  - error code ใหม่ — ฝั่งรับอ่านเป็น `unknown`
- **ต้องขึ้นเลข:** เปลี่ยนความหมาย/ชนิดของ field เดิม, ค่าใหม่ใน enum อื่น (เช่น `state`, `availability`), เปลี่ยน framing หรือการเข้ารหัส
- **advert** บอกช่วง wire version ของ node · node ที่ไม่มี version ร่วมกันยังถูกเก็บใน registry (เห็นใน status ว่ามีอยู่)
  แต่ไม่ถูก dial เพราะ handshake จะไม่ผ่านอยู่ดี
- **ห้ามเปลี่ยน:** รูปแบบของ `hello` / `hello_ack` (length 4 ไบต์ + JSON, field `wire`, `wire_version`, `node_id`)
  — เป็นช่องทางเดียวที่ node สองรุ่นใช้หาว่าจะพูด version ไหน
- ตอนนี้ทุก build พูดได้ version เดียว (ช่วง `{3,3}`) การรองรับหลาย version พร้อมกันทำได้โดยขยายช่วงและเก็บโค้ดของ version เก่าไว้

## Request methods

| method | params | result |
|---|---|---|
| `ping` | — | `{}` (ตอบทันที ไม่นับในเพดาน request ที่ค้าง) |
| `adverts.sync` | `{adverts: [SignedAdvert]}` | `{adverts: [SignedAdvert]}` — ส่งทั้งหมดที่รู้ ได้ทั้งหมดที่อีกฝั่งรู้ |
| `invoke` | `{capability, input}` | `InvokeResult {executed_by, job_id, output}` |
| `job.submit` | `{capability, input}` | `JobSubmitted {executed_by, job_id}` |
| `job.status` | `{job_id}` | `JobInfo` |
| `job.result` | `{job_id, wait_ms}` | `JobInfo` (รอได้ไม่เกินครึ่งของ `request_ms`) |
| `job.cancel` | `{job_id}` | `JobInfo` |

เครื่องที่ได้รับ `invoke`/`job.submit` จะรันเองหรือปฏิเสธ — ไม่ส่งต่อ
job มองเห็นได้เฉพาะ node ที่ส่งมา node อื่นได้ `job_not_found`

`JobInfo`: `{job_id, executed_by, capability, state, percent, message, output?, error?, elapsed_ms}`
`state`: `running` · `succeeded` · `failed` · `cancelled` · `timed_out`

## Events (acceptor → dialer, เฉพาะ job ที่ dialer ส่งมาบน connection นั้น)

```json
{"type":"event","event":"job_progress","job_id":"…","percent":40,"message":"unit: step 2/5"}
{"type":"event","event":"job_finished","job_id":"…","state":"succeeded"}
```

best effort: คิวเต็มจะทิ้ง event แต่สถานะสุดท้ายยังถามได้ด้วย `job.status`

## Advert (link-state)

```json
{"type":"notify","method":"advert","params":{"payload":"<NodeAdvert เป็น JSON string>","signature":"<hex>"}}
```

`NodeAdvert`:

```json
{
  "node_id": "node-b", "name": "Node B (server)", "node_type": "server",
  "software_version": "0.0.3", "wire": {"min": 3, "max": 3}, "address": "127.0.0.1:47322",
  "incarnation": 1794632449365, "seq": 7,
  "capabilities": [{"name": "math.add", "description": "…", "input_schema": {…}, "output_schema": {…}, "allow": ["*"]}],
  "resources": {"cpu_percent": 20, "memory_available_mb": 16384, "active_jobs": 0,
                "max_concurrent_jobs": 2, "availability": "available", "mocked": true}
}
```

- ลายเซ็นครอบ `"synapta-advert\n" + payload` — เซ็น **string** ไม่ใช่ struct ที่ serialize ใหม่ จึงไม่ขึ้นกับรูปแบบ JSON
- รับเฉพาะเมื่อ: เซ็นด้วย key ของ `node_id` ใน trust list · address ผ่าน `AddressPolicy` ·
  `(incarnation, seq)` ใหม่กว่าที่มี (ช่วง `wire` ไม่ใช่เงื่อนไข — ใช้ตัดสินแค่ว่าจะ dial หรือไม่)
- advert ของตัวเองที่วนกลับมา ทิ้งก่อนตรวจ

## Error

```json
{"code": "target_unavailable", "message": "node-b is Unavailable", "details": null}
```

| code | ความหมาย | HTTP (gateway) |
|---|---|---|
| `bad_request` | เนื้อหา request ผิด | 400 |
| `malformed_frame` / `frame_too_large` | frame ผิดรูป / ใหญ่เกิน | 400 / — |
| `handshake_failed` · `untrusted_peer` · `version_mismatch` · `not_ready` | handshake | — |
| `unauthorized` | ผู้เรียกไม่อยู่ใน `allow` | 403 |
| `target_not_found` | ไม่รู้จัก node นี้ | 404 |
| `capability_not_supported` | node นั้นไม่มี capability นี้ | 404 |
| `target_unavailable` | รู้จักแต่ไม่พร้อม | 503 |
| `no_eligible_node` | routing อัตโนมัติหาเครื่องไม่ได้ | 503 |
| `busy` | เต็ม — รับประกันว่ายังไม่ได้รัน | 429 |
| `timeout` · `connection_lost` | อาจรันไปแล้ว | 504 · 503 |
| `job_not_found` · `cancelled` · `execution_failed` · `shutting_down` · `internal` | | 404 · 500 · 500 · 503 · 500 |
| `unsupported_request` | ฝั่งรับไม่รู้จัก method นี้ (รุ่นเก่ากว่า) — รับประกันว่ายังไม่ได้รัน | 501 |
| `unknown` | code ที่ build นี้ไม่รู้จัก (ส่งมาจาก node รุ่นใหม่กว่า) — build นี้ไม่ส่งเอง | 500 |
