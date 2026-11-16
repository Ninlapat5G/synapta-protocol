# Routing Policy — `least-loaded-v2`

ที่มาของจริง: `crates/synapta-core/src/routing/mod.rs` (policy) · `routing/ledger.rs` (บัญชีงานที่ตัวเองส่ง) ·
`routing/dispatch.rs` (การส่ง)

`decide()` เป็นฟังก์ชันล้วน: รับ `ClusterView` (snapshot ของ registry + advert ของตัวเอง + ledger) คืน `RoutingDecision`
ไม่มี I/O ไม่มีเวลา ไม่มีการสุ่ม → state เดิมได้ผลเดิมทุกไบต์ ไม่ขึ้นกับลำดับที่ peer ตอบมา
ไม่มี LLM ในเส้นทางนี้

"state" รวมประวัติการส่งของ node ที่เป็นผู้เรียกด้วย (ledger) จึงเรียก `explain` ซ้ำกี่ครั้งก็ได้ผลเดิม
(`explain` ไม่แก้ ledger) และ `explain` บอกล่วงหน้าได้ตรงว่างานถัดไปจะไปเครื่องไหน

## Automatic (ไม่ระบุ target)

1. **ผู้สมัคร** = node ที่ advert ประกาศ capability นั้น (รวมตัวเอง) — node ที่ไม่มีนับไว้ใน `nodes_without_capability`
2. **ตัดออก** ตามลำดับ พร้อมเหตุผล (`rejection.code`):

   | code | เงื่อนไข |
   |---|---|
   | `not_allowed` | `allow` ของ capability ไม่มีผู้เรียก (ตัวเองเรียกตัวเองได้เสมอ) |
   | `link_not_ready` | ไม่มี link `READY` และไม่มีเส้นทางผ่าน relay |
   | `not_available` | `availability` ≠ `available` |
   | `at_capacity` | `active_jobs + unseen_jobs ≥ max_concurrent_jobs` |

3. **จัดอันดับ** ผู้ผ่าน ด้วย key เดียว:

   ```
   (load_permille ↑, hops ↑, cpu_percent ↑, last_dispatch ↑ (ไม่เคยส่ง = มาก่อน), node_id ↑)
   load_permille = (active_jobs + unseen_jobs) × 1000 / max_concurrent_jobs   (จำนวนเต็ม — เทียบได้แน่นอน)
   ```

   - `load_permille` เป็นสัดส่วนต่อ capacity เครื่องที่รับได้ 4 งานจึงได้งานประมาณสองเท่าของเครื่องที่รับได้ 2
   - `last_dispatch` — ลำดับครั้งล่าสุดที่ผู้เรียกส่งงานไปเครื่องนั้น โหลดเท่ากันจะไปเครื่องที่ส่งไปนานที่สุด
     เครื่องว่างจึงสลับกันรับงาน แทนที่เครื่องชื่อขึ้นก่อนจะได้ไปทั้งหมด
   - `node_id` ไม่ซ้ำกัน ปิดท้ายให้ลำดับสมบูรณ์เสมอ (ใช้จริงเฉพาะตอนยังไม่เคยส่งไปทั้งสองเครื่อง)
4. เลือกอันดับ 1 ส่งตรงหรือผ่าน relay ไปเครื่องนั้น
5. **ถ้าเครื่องนั้นปฏิเสธ** — ไปอันดับถัดไปได้เฉพาะกรณีที่พิสูจน์ได้ว่ายังไม่ได้รัน:
   link ไม่ READY ตอนจะส่ง (ไม่ได้ส่ง) หรือตอบ `busy` (ปฏิเสธก่อนรัน)
   กรณีอื่น (timeout, connection หลุด, executor error) รายงาน error ทันที — at-most-once

ไม่มีผู้ผ่านเลย → `no_eligible_node` พร้อมรายการผู้สมัครและเหตุผลของแต่ละตัว

## Ledger — งานของตัวเองที่ advert ยังไม่เห็น (`unseen_jobs`)

advert ช้ากว่าความจริงได้ถึง `advert_min_interval_ms` + เวลาเดินทาง ถ้าเชื่อ advert อย่างเดียว:

- ยิงงานพร้อมกันหลายงาน ทุกงานเห็น advert เดียวกันที่บอกว่าเครื่องแรก "ว่าง" → กองไปที่เดียว แล้วโดน `busy` เด้งกลับ
- งานของเราเพิ่งจบ ช่องว่างแล้ว แต่ advert ยังบอกว่าเต็ม → ปฏิเสธเองทั้งที่รับได้

ผู้เรียกจึงจดสิ่งที่รู้แน่เกี่ยวกับงาน **ของตัวเอง** แล้วแก้ตัวเลข `active_jobs` ของ advert:

| สถานะงานที่ส่งไป | advert ที่ถืออยู่ | ผล |
|---|---|---|
| ส่งแล้ว ยังไม่ได้คำตอบ | ยังเป็นตัวเดิมตอนส่ง (ออกก่อนส่ง จึงไม่มีงานนี้) | `+1` |
| ส่งแล้ว ยังไม่ได้คำตอบ | ใหม่กว่าตอนส่ง (นับงานนี้แล้ว) | `0` |
| `invoke` จบแล้ว | ได้รับระหว่างส่งกับจบ (นับงานที่จบไปแล้ว) | `-1` |
| `invoke` จบแล้ว | advert ใหม่มาหลังจบ | ลบออกจาก ledger |
| `job.submit` ได้รับแล้ว (งานยังวิ่ง) | ยังเป็นตัวเดิมตอนส่ง | `+1` จนกว่า advert ใหม่มา แล้วลบ |
| ถูกปฏิเสธ / error / timeout | — | ลบทิ้ง ไม่อ้างอะไร |

ค่าใน decision คือค่าที่ใช้จริงหลังกันไม่ให้ต่ำกว่า 0 (`unseen_jobs` แสดงเฉพาะเมื่อไม่เป็น 0)

**ทิศของความคลาดเคลื่อน** — ledger ใช้ "เวลาที่ได้รับ advert" แทน "เวลาที่ออก advert"
advert ที่ออกก่อนงานเริ่มแต่มาถึงหลังส่ง จะถูกนับว่ารวมงานนั้นแล้ว ค่าที่ประมาณจึงพลาดได้แค่ทาง **ต่ำ** ไม่มีทางสูง
ผลเสียที่แย่สุดคือส่งไปแล้วได้ `busy` แล้ว routing ไปเครื่องถัดไป ไม่มีงานรันซ้ำ
และไม่มีเครื่องไหนถูกข้ามเพราะคิดว่าเต็มทั้งที่ไม่เต็ม

**สิ่งที่ ledger ไม่เห็น** — งานจากผู้เรียกคนอื่น ส่วนนั้นยังอาศัย advert และการตรวจ capacity ของเครื่องปลายทาง (`busy`)

## Explicit (ระบุ `target`)

1. ไม่รู้จัก node นั้น → `target_not_found`
2. node นั้นไม่ได้ประกาศ capability → `capability_not_supported`
3. `allow` ไม่มีผู้เรียก → `unauthorized`
4. ไม่มี link/เส้นทาง relay หรือ availability ไม่พร้อม → `target_unavailable`
5. เต็ม (นับ `unseen_jobs` ด้วย) → `busy`
6. ผ่าน → ส่งไปเครื่องนั้นเท่านั้น

**ไม่มีกรณีไหนที่เปลี่ยนไปเครื่องอื่น** แม้จะมีเครื่องอื่นรองรับ capability เดียวกัน
งานแบบ explicit ก็ลง ledger ด้วย (เป็นโหลดของเครื่องนั้นจริง)

## Decision record

ทุกการตัดสินใจถูกบันทึกเป็น log `target=synapta::routing` (JSON) และเก็บใน ring buffer
(`limits.routing_log_size`, ค่าเริ่ม 100) — ดูได้ที่ `GET /routing/decisions` หรือ `synapta … decisions`
ดูล่วงหน้าโดยไม่รัน: `GET /routing/explain?capability=…&target=…`

ตัวอย่างจริง (TEST-04: node-b เต็ม 2/2)

```json
{
  "capability": "test.execute", "mode": "automatic", "caller": "node-a", "policy": "least-loaded-v2",
  "selected": "node-c",
  "candidates": [
    {"node_id": "node-b", "local": false, "eligible": false, "load_permille": 1000, "cpu_percent": 20,
     "last_dispatch": 4, "rejection": {"code": "at_capacity", "active_jobs": 2, "max_concurrent_jobs": 2}},
    {"node_id": "node-c", "local": false, "eligible": true, "load_permille": 0, "cpu_percent": 20,
     "last_dispatch": 2, "rank": 1}
  ],
  "nodes_without_capability": 1,
  "attempts": [{"node_id": "node-c", "outcome": "ok"}]
}
```

## ความสดของข้อมูล

ทรัพยากรมาจาก advert ซึ่งออกใหม่เมื่อจำนวน job เปลี่ยน (รวบให้ไม่ถี่กว่า `advert_min_interval_ms`) และทุก `advert_refresh_ms`
ledger ปิดช่องว่างสำหรับงานของผู้เรียกเอง ส่วนที่เหลือ (งานของคนอื่น, advert ที่ออกก่อนงานเริ่ม)
เครื่องปลายทางตรวจ capacity เองอีกรอบ — ถ้าเต็มจะตอบ `busy` แล้ว routing อัตโนมัติไปเครื่องถัดไป (ปลอดภัยเพราะยังไม่ได้รัน)

ผลวัดภายใต้โหลดต่อเนื่อง อยู่ใน [`test-report.md`](test-report.md) หัวข้อ "การกระจายงานภายใต้โหลด"

## เส้นทางข้ามวง (v0.0.4)

`routing/paths.rs` หา shortest path ด้วย BFS ใช้ READY link ของผู้เรียกและคู่ neighbors ที่ทั้งสองฝั่งประกาศตรงกัน
เส้นทางยาวเท่ากันตัดสินด้วย node id ผลคงเดิมเมื่อ snapshot เดิม
CandidateReport มี `hops` (0=ตัวเอง, 1=ตรง, มากกว่า 1=relay) และ `via` สำหรับ first hop
โหลดเท่ากันจึงเลือก hop น้อยก่อน CPU/ประวัติการส่งงาน
ระบุ target แล้วยังคงส่งถึงเครื่องนั้นเท่านั้น ไม่มีการเปลี่ยนปลายทางเมื่อสะพานดับ
unreachable/ttl_exceeded ก่อนรันอนุญาต fallback เฉพาะการเลือกอัตโนมัติ
