# UTCP Integration

ที่มาของจริง: `crates/synapta-gateway/src/utcp.rs` · `routes.rs`

## ตรวจสอบกับอะไร

ไม่ได้อ้างจากความจำหรือบทความ — อ่าน source ของ **UTCP Python SDK ทางการ** และรันเทสต์กับมันจริง

| package | version |
|---|---|
| `utcp` (PyPI) | 1.2.0 |
| `utcp-http` (PyPI) | 1.1.15 |

พฤติกรรมของ SDK ที่ adapter นี้อิง (อ่านจาก `utcp/data/utcp_manual.py`, `utcp_http/http_communication_protocol.py`,
`utcp_http/_security.py`, `utcp/implementations/*`):

1. manual = `{utcp_version, manual_version, tools[]}` — SDK ตรวจว่ามี `utcp_version` และ `tools` แต่ไม่ตรวจค่า version
2. tool มี `name`, `description`, `inputs` (JSON Schema), `outputs`, `tags`, `tool_call_template`
3. call template แบบ `http`: argument ที่ชื่อตรงกับ `body_field` กลายเป็น JSON body · **argument อื่นทั้งหมดเป็น query string**
4. `http://` อนุญาตเฉพาะ host loopback ที่เขียนไว้ตรง ๆ ใน template (กัน SSRF) — ต่อ `127.0.0.1` ได้
5. `${VAR}` ใน template แทนค่าจาก variables ของ client โดย **เติมชื่อ manual นำหน้า**: manual ชื่อ `synapta` →
   ตัวแปร `synapta_SYNAPTA_API_TOKEN`
6. client ตั้งชื่อ tool เป็น `<manual>.<tool>` → `synapta.math.add` และแยก manual ด้วย `.` ตัวแรก
7. ตอบ HTTP 4xx/5xx → SDK raise exception ที่มี body ของ server อยู่ในข้อความ

## การ map

```
UTCP client                     node-a gateway                       Synapta
call_tool("synapta.math.add",   POST /tools/math.add?target=…        NodeHandle::invoke
  {"body": {"a":2,"b":3},  ──▶   Authorization: Bearer <token>  ──▶  routing.decide → node-b
   "target": "node-c"?})         body: {"a":2,"b":3}                 link.call(invoke) → node-b รัน
                           ◀──   200 {"result":{"sum":5},       ◀──  InvokeResult{executed_by: node-b}
                                      "executed_by":"node-b", ...}
```

- หนึ่ง capability = หนึ่ง tool ไม่ว่าจะมีกี่ node ให้บริการ — routing เลือกตอนเรียก
- `inputs` = `{body: <input schema ของ capability>, target?: string}` (required: `body`) ตามข้อ 3
- `outputs` = `{result, executed_by, job_id, routing}` — ผู้เรียกเห็นเสมอว่าเครื่องไหนรันจริง
- token ไม่ถูกเขียนลง manual — template ใส่ `Bearer ${SYNAPTA_API_TOKEN}`
- ปลายทาง HTTP ตรวจ `Host` ต้องเป็น address loopback ของตัวเอง (กัน DNS rebinding จากหน้าเว็บในเบราว์เซอร์)

### ฝั่ง client ลงทะเบียนแบบนี้

```python
client = await UtcpClient.create(config={
    "variables": {"synapta_SYNAPTA_API_TOKEN": "<เนื้อไฟล์ data/node-a/api-token>"},
    "manual_call_templates": [{
        "name": "synapta", "call_template_type": "http", "http_method": "GET",
        "url": "http://127.0.0.1:48321/utcp",
        "auth": {"auth_type": "api_key", "api_key": "Bearer ${SYNAPTA_API_TOKEN}",
                 "var_name": "Authorization", "location": "header"}
    }]
})
await client.call_tool("synapta.math.add", {"body": {"a": 2, "b": 3}})
```

## ผลทดสอบ

| เทสต์ | วิธี | ผล |
|---|---|---|
| `test_05_utcp_contract` | Rust เล่น HTTP exchange แบบเดียวกับ utcp-http ทุกขั้น (discovery → call → error) | ผ่าน (รันใน `cargo test --workspace`) |
| `test_05_official_utcp_sdk` | รัน `scripts/utcp-official/official_client_check.py` ด้วย SDK ทางการจริง | **ผ่าน** เมื่อมี `SYNAPTA_UTCP_PYTHON` · ไม่มี = `ignored` |

ผลจริงจาก SDK ทางการ (2026-11-14):

```json
{"tools": ["synapta.math.add", "synapta.system.echo", "synapta.system.info", "synapta.test.execute"],
 "auto":     {"executed_by": "node-b", "result": {"sum": 5}, "routing": {"mode": "automatic", "selected": "node-b", …}},
 "explicit": {"executed_by": "node-c", "result": {"node": "node-c", "passed": true, …}, "routing": {"mode": "explicit", …}},
 "refused":  "404, message='Not Found: {\"error\":{\"code\":\"capability_not_supported\", …}}'"}
```

และเทสต์ตรวจ execution counter ว่า node-b รัน `math.add` 1 ครั้ง node-a (ที่เป็นปลายทาง UTCP) ไม่ได้รันเอง

## ข้อจำกัด (ตรงไปตรงมา)

- ทดสอบกับ **Python SDK ตัวเดียว** ยังไม่ได้ลอง TypeScript / Rust SDK
- ใช้เฉพาะ protocol `http` ของ UTCP — ยังไม่มี streaming (`sse`, `streamable_http`) progress ของ async job จึงไม่ได้ส่งผ่าน UTCP
  (ใช้ `/jobs/...` ของ gateway แทน)
- `utcp_version` ประกาศเป็น `1.0.1` ตามตัวอย่างใน RFC ของ utcp.io — SDK 1.2.0 ไม่ตรวจค่านี้ จึงไม่ได้พิสูจน์ความเข้ากันได้ระดับ version
- เทสต์ SDK ทางการไม่ได้รันใน `cargo test --workspace` โดยอัตโนมัติ เพราะต้องมี Python + package ภายนอก
