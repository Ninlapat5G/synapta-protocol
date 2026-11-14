# Protocol Message Specification — v0.0.1

ที่มาของจริง: `crates/synapta-core/src/protocol/message.rs` (แก้ไฟล์นั้นต้องแก้เอกสารนี้ด้วย)

## Framing

```
┌──────────────┬──────────────────────────────┐
│ length: u32  │ JSON (UTF-8), length ไบต์     │
│ big-endian   │                              │
└──────────────┴──────────────────────────────┘
```

- `length` = 0 → malformed · `length` > `limits.max_frame_bytes` (ค่าเริ่ม 1 MiB) → `frame_too_large`
  ตรวจก่อนจองหน่วยความจำ
- frame ผิดรูป/ใหญ่เกิน: ฝั่งรับส่ง `Frame::Error` แล้วปิด connection — process ไม่ล้ม
- ทุก frame มี field `"type"`

## ลำดับบน connection

```
dialer                                    acceptor
  hello        {protocol_version, node_id, nonce}          -->
                                          <--  hello_ack {protocol_version, node_id, nonce, signature}
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
- acceptor ปิด connection ที่เงียบเกิน `timeouts.idle_timeout_ms` (dialer ส่ง `ping` ทุก `heartbeat_interval_ms` อยู่แล้ว)

## Handshake (ยืนยันตัวตนสองทาง)

transcript ที่ถูกเซ็น (UTF-8):

```
synapta-handshake/0.0.1/<role>\n<dialer_id>\n<acceptor_id>\n<dialer_nonce>\n<acceptor_nonce>
```

- `hello_ack.signature` = acceptor เซ็น transcript role `acceptor`
- `hello_finish.signature` = dialer เซ็น transcript role `dialer`
- nonce = 32 ไบต์สุ่ม (hex 64 ตัว) ใหม่ทุก connection
- ตรวจ: version ตรงกันพอดี · node id อยู่ใน trust list · ลายเซ็นผ่านด้วย key ของ id นั้น (`verify_strict`) ·
  ไม่ใช่ตัวเอง · ถ้า dialer รู้ว่าที่ address นั้นควรเป็นใคร (จาก advert) ต้องตรง
- role ในข้อความกันการเอาลายเซ็นทิศหนึ่งไปใช้อีกทิศ · nonce กัน replay ข้าม connection

> ข้อจำกัด: handshake ยืนยันตัวตนตอนเปิด connection แต่ frame หลังจากนั้น **ไม่ได้เข้ารหัสหรือ MAC**
> บน loopback รับได้ · บนเครือข่ายจริงต้องพึ่ง WireGuard (Tailscale) หรือเพิ่ม secure channel ใน v0.0.2

## Request methods

| method | params | result |
|---|---|---|
| `ping` | — | `{}` |
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
  "protocol_version": "0.0.1", "address": "127.0.0.1:47322",
  "incarnation": 1794632449365, "seq": 7,
  "capabilities": [{"name": "math.add", "description": "…", "input_schema": {…}, "output_schema": {…}, "allow": ["*"]}],
  "resources": {"cpu_percent": 20, "memory_available_mb": 16384, "active_jobs": 0,
                "max_concurrent_jobs": 2, "availability": "available", "mocked": true}
}
```

- ลายเซ็นครอบ `"synapta-advert\n" + payload` — เซ็น **string** ไม่ใช่ struct ที่ serialize ใหม่ จึงไม่ขึ้นกับรูปแบบ JSON
- รับเฉพาะเมื่อ: เซ็นด้วย key ของ `node_id` ใน trust list · address ผ่าน `AddressPolicy` · version ตรง ·
  `(incarnation, seq)` ใหม่กว่าที่มี
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
