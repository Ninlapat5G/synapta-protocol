# Test Report — Synapta Protocol v0.0.1

วันที่รัน: 2026-11-14 · Windows 11 · rustc/cargo 1.99.1 · ทุกการทดสอบอยู่บน 127.0.0.1 และโฟลเดอร์ชั่วคราวเท่านั้น

## คำสั่ง

```
cargo test --workspace                      # ทั้งหมด ยกเว้น UTCP SDK ทางการ
SYNAPTA_UTCP_PYTHON=<python ที่ลง utcp> cargo test -p synapta-gateway --test utcp -- --include-ignored
cargo run -p synapta-node --bin synapta-sandbox   # สาธิต 3 process
cargo clippy --workspace --all-targets -- -D warnings ; cargo fmt --all --check
```

## สรุป

| ชุด | จำนวน | ผล |
|---|---|---|
| unit (`synapta-core`) | 50 | ผ่านทั้งหมด |
| acceptance in-process (`synapta-core/tests/acceptance.rs`) | 12 | ผ่านทั้งหมด |
| unit (`synapta-gateway`) | 3 | ผ่านทั้งหมด |
| UTCP + local API (`synapta-gateway/tests/utcp.rs`) | 3 + 1 | 3 ผ่าน · SDK ทางการ: `ignored` ใน `--workspace`, **ผ่าน** เมื่อรันด้วย `SYNAPTA_UTCP_PYTHON` |
| E2E 3 process (`synapta-node/tests/e2e.rs`) | 1 | ผ่าน |
| **รวม `cargo test --workspace`** | **69 ผ่าน, 0 ตก, 1 ignored** | |
| รวมเมื่อเปิด SDK ทางการ | 70 ผ่าน, 0 ตก | |

ความเสถียร: `acceptance` รันติดกัน 15 รอบ และทั้ง workspace (รวม E2E) 5 รอบ — ผ่านทุกรอบ ไม่มี flaky
lint: `clippy -D warnings` สะอาด · `cargo fmt --check` สะอาด

## ผล TEST-01 … TEST-10

แต่ละข้อทดสอบสองระดับ: **in-process** (node แยก instance ใน process เดียว, port จาก OS) และ **3 process** (`synapta-sandbox`)

| Test | in-process | 3 process | หลักฐานที่เทสต์ตรวจ |
|---|---|---|---|
| TEST-01 Peer Discovery | ✅ `test_01_peer_discovery_and_handshake` | ✅ | node-a seed แค่ node-b แต่ READY กับ node-c (เรียนรู้จาก advert) · capability ใน registry ถูก · request ก่อน handshake ได้ `not_ready` และ connection ถูกปิด · execution counter ว่าง |
| TEST-02 Capability Routing | ✅ `test_02_capability_based_routing` | ✅ | `math.add` → `executed_by: node-b`, `sum: 5` · decision: selected node-b, `nodes_without_capability: 2` · counter node-b = 1, node-a ไม่มี |
| TEST-03 Explicit Target | ✅ `test_03_explicit_target` | ✅ | `test.execute` target node-c → node-c รัน · node-b (มี capability เดียวกัน) counter ไม่ขยับ · `mode: explicit` |
| TEST-04 Resource-Aware | ✅ `test_04_resource_aware_routing` | ✅ | job ยาวจริง 2 งานเต็ม node-b (2/2) → auto ไป node-c · node-b `rejection: at_capacity (2/2)`, `load_permille: 1000` · ไม่มี LLM |
| TEST-05 UTCP Cross-Node | ✅ `test_05_utcp_contract` · ✅ `test_05_official_utcp_sdk` (SDK ทางการ) | ✅ | SDK ทางการ discover 4 tool · `synapta.math.add` → node-b `{sum:5}` · target node-c ผ่าน query · error 404 `capability_not_supported` · counter ยืนยัน node-a ไม่ได้รันแทน |
| TEST-06 Invalid Target | ✅ `test_06_invalid_explicit_target` | ✅ | node-x → `target_not_found` · node-c + math.add → `capability_not_supported` · `attempts` ว่าง · ไม่มี node ไหนรัน |
| TEST-07 Peer Failure | ✅ `test_07_peer_failure` · ✅ `test_07b_silent_peer_detected_by_heartbeat_timeout` | ✅ (kill process จริง) | node-b → `UNAVAILABLE` · auto `test.execute` → node-c · explicit node-b → `target_unavailable` · `math.add` → `no_eligible_node` · node-a ยังทำงาน · peer ที่ handshake แล้วเงียบ ถูกจับได้ด้วย heartbeat timeout (`last_error` มี "heartbeat") |
| TEST-08 Async Job | ✅ `test_08_async_job_lifecycle` | ✅ | ได้ job id · status `running` · progress event 4/4 (3 process: 5/5) + `job_finished` · ผล `succeeded` · cancel → `cancelled` และ active_jobs กลับเป็น 0 · หลัง retention status = `job_not_found`, `retained_jobs: 0` |
| TEST-09 Isolation & Security | ✅ `test_09_isolation_and_security` · ✅ `test_09_no_host_command_execution_in_core` · ✅ `local_api_rejects_…` | — | listener ทุกตัว loopback · `0.0.0.0` ถูกปฏิเสธตอน start · node นอก trust list handshake ไม่ผ่าน (`untrusted_peer`) · capability ที่ไม่อนุญาต: router ข้าม + ปลายทางปฏิเสธแม้ส่งตรงข้าม router · malformed JSON / length 4 GB / ขยะ HTTP → error + ปิด connection, node ยังตอบ · สแกน source core ไม่มี `std::process`/`tokio::process`/`Command::new` · local API: token ผิด 401, Host ปลอม 403, body ใหญ่ 413, JSON เสีย 400 |
| TEST-10 Deterministic | ✅ `test_10_deterministic_routing` + unit `decision_does_not_depend_on_peer_order` | ✅ | explain 50 ครั้ง ได้ JSON ตรงกันทุกไบต์ · invoke จริง 10 ครั้งไป node-b ทุกครั้ง · tie-break ด้วย node id · สลับลำดับ peer แล้วผลเหมือนเดิม |

เพิ่มเติม: `restart_after_clock_regression_still_outranks_old_adverts` (incarnation เพิ่มเสมอหลัง restart แม้นาฬิกาถอยหลัง)
และ E2E ตรวจว่าแต่ละ node เขียน log แยกไฟล์เป็น JSON lines และไม่มีคำเตือน trust ปลอม

## ผลจาก `synapta-sandbox` (3 process, port ตาม spec)

```
started node-c: pid 19068 listen 127.0.0.1:47323 api Some(127.0.0.1:48323)
started node-b: pid 29820 listen 127.0.0.1:47322 api Some(127.0.0.1:48322)
started node-a: pid 13204 listen 127.0.0.1:47321 api Some(127.0.0.1:48321)
...
==== summary ====
PASS  TEST-01 discovery + handshake
PASS  TEST-02 capability-based routing
PASS  TEST-03 explicit target
PASS  TEST-04 resource-aware routing
PASS  TEST-05 UTCP cross-node invocation
PASS  TEST-06 invalid explicit target
PASS  TEST-08 async job lifecycle
PASS  TEST-10 deterministic routing
PASS  TEST-07 peer failure (node-b killed)
PASS  graceful shutdown
```

node-b ถูก kill แบบ hard kill — node-a เห็น `UNAVAILABLE` ทันที (TCP reset) · กรณีที่ไม่มี reset (peer ค้าง)
ใช้ heartbeat timeout ซึ่งทดสอบใน `test_07b` (ค่าในเทสต์ 600 ms, ค่าเริ่มต้น 3 s)

## ตัวอย่าง request/response จริง

```
node-a POST /tools/math.add {"a":2,"b":3}
-> 200 {"executed_by":"node-b","job_id":"91e7d5eefbd73da2","result":{"sum":5},
        "routing":{"mode":"automatic","selected":"node-b","policy":"least-loaded-v1",
                   "candidates":[{"node_id":"node-b","eligible":true,"load_permille":0,"cpu_percent":20,"rank":1}],
                   "nodes_without_capability":2,"attempts":[{"node_id":"node-b","outcome":"ok"}]}}

node-a POST /tools/test.execute?target=node-b   (หลัง kill node-b)
-> 503 {"error":{"code":"target_unavailable","message":"node-b is Unavailable"},
        "routing":{"mode":"explicit","candidates":[{"node_id":"node-b","eligible":false,
                   "rejection":{"code":"link_not_ready","state":"UNAVAILABLE"}}]}}
```

routing decision ของ TEST-04 อยู่ใน [`routing.md`](routing.md)

## สิ่งที่พบและแก้ระหว่างทดสอบ

1. **คำเตือน trust ปลอม** — node ได้ advert ของตัวเองวนกลับมาแล้วตรวจลายเซ็นก่อนเช็กว่าเป็นของตัวเอง
   (node ไม่มี key ตัวเองใน trust list) → log `UntrustedPeer` ทุกครั้งที่ sync ไม่กระทบความถูกต้องแต่เป็น alarm ปลอม
   แก้: ทิ้ง advert ของตัวเองก่อนตรวจ · มี regression check ใน E2E
2. **log ระดับผิด** — peer ถูก kill แล้วขึ้น "bad frame" (WARN) ทั้งที่เป็น connection reset → แยกเป็น "connection lost" (INFO)
3. **incarnation จากนาฬิกา** (พบตอน review) — นาฬิกาถอยหลังระหว่าง restart ทำให้ advert ใหม่แพ้ของเก่า → เก็บค่าใน `data_dir`
4. **สิทธิ์ไฟล์** (พบตอน review) — private key / API token บน Unix เขียนแบบใครก็อ่านได้ → 0600

## สิ่งที่ยังไม่ได้ทดสอบ

- เครือข่ายจริงหรือ Tailscale (ห้ามตาม scope)
- Linux/macOS — รันเฉพาะ Windows 11 (โค้ดไม่มีส่วนเฉพาะ OS ยกเว้นสิทธิ์ไฟล์ Unix ที่ยังไม่ได้รันจริง)
- UTCP SDK ภาษาอื่นนอกจาก Python
- โหลดสูง / cluster มากกว่า 3–4 node
