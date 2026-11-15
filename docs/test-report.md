# Test Report — Synapta Protocol v0.0.3

วันที่รัน: 2026-11-14 (ปรับ routing + เทสต์โหลด, secure channel v0.0.2 และบั๊ก + version v0.0.3: 2026-11-15) · Windows 11 · rustc/cargo 1.99.1 · ทุกการทดสอบอยู่บน 127.0.0.1 และโฟลเดอร์ชั่วคราวเท่านั้น

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
| unit (`synapta-core`) | 77 | ผ่านทั้งหมด |
| acceptance in-process (`synapta-core/tests/acceptance.rs`) | 12 | ผ่านทั้งหมด |
| ความเข้ากันได้กับ node รุ่นใหม่กว่า (`synapta-core/tests/compatibility.rs`) | 2 | ผ่านทั้งหมด |
| link ที่ต้องอยู่รอด / กลับมาเอง (`synapta-core/tests/link_resilience.rs`) | 2 | ผ่านทั้งหมด |
| การกระจายงานภายใต้โหลด (`synapta-core/tests/load_balancing.rs`) | 4 | ผ่านทั้งหมด |
| secure channel บนสายจริง (`synapta-core/tests/secure_channel.rs`) | 2 | ผ่านทั้งหมด |
| unit (`synapta-gateway`) | 3 | ผ่านทั้งหมด |
| UTCP + local API (`synapta-gateway/tests/utcp.rs`) | 3 + 1 | 3 ผ่าน · SDK ทางการ: `ignored` ใน `--workspace`, **ผ่าน** เมื่อรันด้วย `SYNAPTA_UTCP_PYTHON` |
| E2E 3 process (`synapta-node/tests/e2e.rs`) | 1 | ผ่าน |
| **รวม `cargo test --workspace`** | **106 ผ่าน, 0 ตก, 1 ignored** | |
| รวมเมื่อเปิด SDK ทางการ | 107 ผ่าน, 0 ตก | |

ความเสถียร: `acceptance` รันติดกัน 15 รอบ และทั้ง workspace (รวม E2E) 5 รอบ — ผ่านทุกรอบ ไม่มี flaky
หลังปรับ routing (v2): ทั้ง workspace 4 รอบติด · E2E 8 รอบติด · `load_balancing` 12 รอบติด — ผ่านทุกรอบ
หลังเพิ่ม secure channel (v0.0.2): ทั้ง workspace 4 รอบติด (รวมเทสต์โหลด) · SDK ทางการผ่าน — ผ่านทุกรอบ
v0.0.3 (บั๊ก + version): ทั้ง workspace 4 รอบติด (106 ผ่านทุกรอบ) · SDK ทางการผ่าน · `synapta-sandbox` build release ผ่านทุกข้อ
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
| TEST-10 Deterministic | ✅ `test_10_deterministic_routing` + unit `decision_does_not_depend_on_peer_order` | ✅ | explain 50 ครั้ง ได้ JSON ตรงกันทุกไบต์ · invoke จริง 10 ครั้ง: explain ทายเครื่องที่รันถูกทุกครั้ง, สลับ B/C ตามลำดับ · เปิด cluster ใหม่แล้วได้ลำดับเดิมทุกตัว · สลับลำดับ peer แล้วผลเหมือนเดิม |

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
        "routing":{"mode":"automatic","selected":"node-b","policy":"least-loaded-v2",
                   "candidates":[{"node_id":"node-b","eligible":true,"load_permille":0,"cpu_percent":20,"rank":1}],
                   "nodes_without_capability":2,"attempts":[{"node_id":"node-b","outcome":"ok"}]}}

node-a POST /tools/test.execute?target=node-b   (หลัง kill node-b)
-> 503 {"error":{"code":"target_unavailable","message":"node-b is Unavailable"},
        "routing":{"mode":"explicit","candidates":[{"node_id":"node-b","eligible":false,
                   "rejection":{"code":"link_not_ready","state":"UNAVAILABLE"}}]}}
```

routing decision ของ TEST-04 อยู่ใน [`routing.md`](routing.md)

## การกระจายงานภายใต้โหลด (`load_balancing.rs`)

node-a เป็นผู้เรียก · node-b และ node-c รับ `test.execute` ได้ทั้งคู่ · วัด "ใครรันกี่งาน" จากคำตอบ
และวัดจำนวนงานที่ **กำลังรันจริง** จากตัว node เองทุก 2 ms (ไม่ใช่จาก advert) เพื่อดูเพดานและอัตราการใช้งาน
รันเทสต์เดียวกันกับโค้ดก่อนแก้ (v1) และหลังแก้ (v2) บนเครื่องเดียวกัน

| สถานการณ์ | v1 (ก่อน) | v2 (หลัง) |
|---|---|---|
| งานสั้น 100 งาน ส่งทีละงาน (B, C ว่างทุกครั้ง) | B 100 · C 0 | **B 50 · C 50** |
| ยิงพร้อมกันทีละ 4 งาน × 10 รอบ (ช่องว่างพอดี 4) | แบ่ง 2+2 ได้ แต่ `busy` เด้ง 20 ครั้ง | **2+2 ทุกรอบ, `busy` 0** |
| โหลดเกินต่อเนื่อง 4 วินาที: 6 ผู้เรียก, B=2 C=2, งาน 20–100 ms | ใช้งาน 70% / 70% · `busy` 465 | **ใช้งาน 82–91%** · `busy` 235–268 |
| เหมือนข้างบน แต่ B=4 C=2, 8 ผู้เรียก | ใช้งาน 75% / 74% · `busy` 584 | **ใช้งาน 82–89%** · `busy` 248–308 · B ได้ ~67% ตามสัดส่วน capacity |

ทุกสถานการณ์ทั้ง v1 และ v2: **ไม่มีเครื่องไหนรันเกิน `max_concurrent_jobs` แม้แต่ครั้งเดียว** (เพดานคุมโดยเครื่องปลายทาง)
และไม่มี error อื่นนอกจาก "คลัสเตอร์เต็ม" (`no_eligible_node` เมื่อทุกช่องมีงานจริง ผู้เรียกรอ 5 ms แล้วลองใหม่)

ตัวเลขโหลดต่อเนื่องเป็นค่าจาก 6 รอบ ช่วงที่แสดงคือต่ำสุด–สูงสุด · `busy` ที่ยังเหลือใน v2 มาจากงานของผู้เรียกที่จบแล้ว
และ advert ที่ออกก่อนงานถัดไปเริ่มแต่มาถึงทีหลัง (ledger ประมาณต่ำได้ทางเดียว — ดู `routing.md`)
แต่ละครั้งเสียแค่ไป-กลับหนึ่งรอบบน loopback และงานไม่ถูกรันซ้ำ

**ทดสอบเครื่องแน่น** (รัน 3 ชุดพร้อมกัน): การแบ่งงานยังถูก ไม่มีเครื่องเกินเพดาน อัตราใช้งานลดเหลือ 71–84% เพราะ CPU เครื่องเต็มเอง
ชุดเทสต์โหลดจึงตั้งเกณฑ์ไว้ที่ ≥ 75% (v1 ได้ 70–75% บนเครื่องว่าง)

## Secure channel (v0.0.2)

| สิ่งที่ต้องกัน | เทสต์ | ผล |
|---|---|---|
| อ่านข้อความบนสาย | `nothing_after_the_hello_is_readable_on_the_wire` — node จริงคุยผ่าน relay ที่บันทึกทุกไบต์ | เห็นแค่ `hello`/`hello_ack` กับ node id · ไม่เจอ input ของงาน, ชื่อ capability, `invoke`, `ping`, `advert`, ลายเซ็น |
| แก้ข้อความ | unit `any_flipped_byte_is_rejected` (พลิกทีละไบต์ทุกตำแหน่ง) · สายจริง `a_corrupted_frame_closes_the_link_and_runs_nothing` | ทุกไบต์ถูกปฏิเสธ · บนสายจริง: ได้ `connection_lost`, node-b ไม่ได้รันอะไร, ต่อใหม่แล้วใช้ได้ต่อ |
| ส่งซ้ำ / สลับลำดับ / record จาก session อื่น | unit `replayed_and_reordered_records_are_rejected`, `record_from_another_session_is_rejected` | ถูกปฏิเสธทั้งหมด |
| คนกลางที่ไม่มี key | unit `relaying_man_in_the_middle_is_refused` — คนกลางเปิด Noise แยกกับสองฝั่งและส่งลายเซ็นต่อ | dialer ปฏิเสธ (`failed to prove its identity`) · acceptor ไม่ยอมรับ session |
| frame ใหญ่เกิน | unit `size_limit_holds_on_both_sides`, `frames_round_trip_including_ones_spanning_many_records` (200 KB) | ฝั่งส่งและรับปฏิเสธ · frame หลาย record ถอดกลับถูก |
| node v0.0.1 / v0.0.2 | unit `hellos_from_older_releases_get_version_mismatch` | ได้ `version_mismatch` (ข้อความ "older than v0.0.3") |

เทสต์เดิมที่ยืนยันตัวตน (trust list, key ผิด, node ผิดที่ address) ยังผ่านบน handshake ใหม่ทั้งหมด

## บั๊กที่แก้ และความเข้ากันได้ (v0.0.3)

บั๊ก 1–3 ยืนยันด้วยเทสต์บนโค้ดเดิมก่อนแก้ทุกข้อ (ผลอยู่ในคอลัมน์ "ก่อนแก้")

| # | ปัญหา | ก่อนแก้ | เทสต์ | หลังแก้ |
|---|---|---|---|---|
| 1 | ping ใช้ช่อง request ร่วมกับงาน — ช่องเต็ม (32) ping ได้ `busy` แล้ว link ถูกตัด | 32 งานช้าพร้อมกัน: สำเร็จ 0, `connection_lost` 32 | `a_link_with_every_request_slot_busy_keeps_its_heartbeat` | สำเร็จครบ link ยัง READY |
| 2 | dialer ยอมแพ้หลังครบโควตา แม้เป็น seed — node ที่หายไปนานกลับมาแล้วไม่ถูกต่อ | B หาย 20 s แล้วกลับที่เดิม: A ไม่ต่อ และลืม B | `a_seed_that_was_away_for_a_long_time_is_found_again` | A ต่อกลับเอง (ยืนยันว่าเทสต์ล้มเมื่อปิดการลองใหม่ของ seed) |
| 3 | ผลของ invoke ค้างในตารางเดียวกับ job — invoke ถี่ ๆ ดันผล job ที่ยังไม่มีคนมารับออก | เพดาน 5 + invoke 10 ครั้ง: ผล job หาย (`job_not_found`) | unit `sync_calls_never_push_out_an_unfetched_async_result`, `a_timed_out_sync_call_leaves_nothing_behind` | ผล job อยู่ครบ invoke ไม่ทิ้งรายการ |
| 4 | ก่อนยืนยันตัวตนรับ frame ได้ถึง 1 MiB (อ่านจากโค้ด) | — | unit `oversized_hello_is_refused_before_it_is_read`, `the_session_gets_the_full_frame_limit` | handshake จำกัด 4 KiB · หลังยืนยันตัวตนได้เพดานเต็ม |

| ความเข้ากันได้ | เทสต์ | ผล |
|---|---|---|
| ต่อรอง version | unit `wire_ranges_meet_at_the_highest_shared_version` | เลือกตัวสูงสุดที่ร่วมกัน · ไม่มีร่วม → ไม่ต่อ |
| คนกลางแก้ช่วง version ใน hello | unit `an_edited_version_offer_is_refused` | ลายเซ็นไม่ผ่าน handshake ล้ม |
| acceptor เลือก version ผิด | unit `acceptor_choosing_another_version_is_refused` | dialer ปฏิเสธ |
| ข้อความชนิดใหม่จาก node รุ่นใหม่กว่า | unit `messages_from_a_newer_version_parse_as_unknown`, `unknown_fields_are_ignored` · สายจริง `messages_from_a_newer_release_do_not_close_the_link` | frame/notify ที่ไม่รู้จักถูกข้าม · request ที่ไม่รู้จักได้ `unsupported_request` ไม่ได้รันอะไร · link ใช้ต่อได้ |
| method ที่รู้จักแต่ params ผิด | unit `known_method_with_bad_params_is_still_an_error` | ยังเป็น error เหมือนเดิม |
| node ที่ไม่มี version ร่วม | สายจริง `a_node_on_another_wire_version_is_known_but_not_dialled` | เก็บ advert ไว้ใน registry แต่ไม่ dial |

## สิ่งที่พบและแก้ระหว่างทดสอบ

1. **คำเตือน trust ปลอม** — node ได้ advert ของตัวเองวนกลับมาแล้วตรวจลายเซ็นก่อนเช็กว่าเป็นของตัวเอง
   (node ไม่มี key ตัวเองใน trust list) → log `UntrustedPeer` ทุกครั้งที่ sync ไม่กระทบความถูกต้องแต่เป็น alarm ปลอม
   แก้: ทิ้ง advert ของตัวเองก่อนตรวจ · มี regression check ใน E2E
2. **log ระดับผิด** — peer ถูก kill แล้วขึ้น "bad frame" (WARN) ทั้งที่เป็น connection reset → แยกเป็น "connection lost" (INFO)
3. **incarnation จากนาฬิกา** (พบตอน review) — นาฬิกาถอยหลังระหว่าง restart ทำให้ advert ใหม่แพ้ของเก่า → เก็บค่าใน `data_dir`
4. **สิทธิ์ไฟล์** (พบตอน review) — private key / API token บน Unix เขียนแบบใครก็อ่านได้ → 0600
5. **เครื่องว่างเท่ากัน B ได้ทุกงาน / ยิงพร้อมกันกองที่ B** (v1) → policy v2: ledger + สลับเครื่องเมื่อโหลดเท่ากัน (ตัวเลขด้านบน)
6. **heartbeat 600 ms ทำ link หลุดตอนเครื่องแน่น** — ค่า 600 ms เป็นค่าของเทสต์ (จับ peer ตายให้เร็ว) พอรันโหลดหนัก 3 ชุดพร้อมกัน
   node ตอบ ping ไม่ทัน link ถูกปิด งานที่ค้างบน link นั้นได้ `ConnectionLost` (ถูกต้องตาม at-most-once ไม่ได้รันซ้ำ)
   เทสต์โหลดเปลี่ยนไปใช้ค่าเดียวกับ production (3 s) แล้วไม่หลุดอีก · **ข้อควรรู้:** เครื่องที่ CPU แน่นมากควรตั้ง `heartbeat_timeout_ms` เผื่อไว้
   v0.0.3 ตรวจซ้ำหลังแก้บั๊ก 1: เทสต์โหลดด้วย 600 ms รันเดี่ยวผ่าน แต่รัน 3 ชุดพร้อมกันยังหลุด → สาเหตุคือ CPU เต็มจริง ไม่ใช่บั๊ก 1
7. **บั๊ก 4 ข้อของ v0.0.3** — ดูตารางด้านบน
8. **`#[serde(other)]` ใช้กับ enum แบบ `{method, params}` ไม่ได้** (พบจากเทสต์ระหว่างทำ v0.0.3) — method ที่ไม่รู้จักแต่มี params
   ถูกอ่านเป็น error แทน `Unknown` · แก้: `Request` / `Notification` ตรวจชื่อ method กับรายการที่รู้จักก่อน (มีเทสต์ว่ารายการครบ)

## สิ่งที่ยังไม่ได้ทดสอบ

- เครือข่ายจริงหรือ Tailscale (ห้ามตาม scope)
- Linux/macOS — รันเฉพาะ Windows 11 (โค้ดไม่มีส่วนเฉพาะ OS ยกเว้นสิทธิ์ไฟล์ Unix ที่ยังไม่ได้รันจริง)
- UTCP SDK ภาษาอื่นนอกจาก Python
- โหลดสูง / cluster มากกว่า 3–4 node
