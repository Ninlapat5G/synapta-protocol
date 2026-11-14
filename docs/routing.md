# Routing Policy — `least-loaded-v1`

ที่มาของจริง: `crates/synapta-core/src/routing/mod.rs` (policy) · `routing/dispatch.rs` (การส่ง)

`decide()` เป็นฟังก์ชันล้วน: รับ `ClusterView` (snapshot ของ registry + advert ของตัวเอง) คืน `RoutingDecision`
ไม่มี I/O ไม่มีเวลา ไม่มีการสุ่ม → state เดิมได้ผลเดิมทุกไบต์ ไม่ขึ้นกับลำดับที่ peer ตอบมา
ไม่มี LLM ในเส้นทางนี้

## Automatic (ไม่ระบุ target)

1. **ผู้สมัคร** = node ที่ advert ประกาศ capability นั้น (รวมตัวเอง) — node ที่ไม่มีนับไว้ใน `nodes_without_capability`
2. **ตัดออก** ตามลำดับ พร้อมเหตุผล (`rejection.code`):

   | code | เงื่อนไข |
   |---|---|
   | `not_allowed` | `allow` ของ capability ไม่มีผู้เรียก (ตัวเองเรียกตัวเองได้เสมอ) |
   | `link_not_ready` | link ตรงไปเครื่องนั้นไม่ `READY` |
   | `not_available` | `availability` ≠ `available` |
   | `at_capacity` | `active_jobs ≥ max_concurrent_jobs` |

3. **จัดอันดับ** ผู้ผ่าน ด้วย key เดียว:

   ```
   (load_permille ↑, cpu_percent ↑, node_id ↑)
   load_permille = active_jobs × 1000 / max_concurrent_jobs   (จำนวนเต็ม — เทียบได้แน่นอน)
   ```

   `node_id` ไม่ซ้ำกัน จึงเป็น tie-breaker ที่ทำให้ลำดับสมบูรณ์เสมอ
4. เลือกอันดับ 1 ส่งตรงไปเครื่องนั้น
5. **ถ้าเครื่องนั้นปฏิเสธ** — ไปอันดับถัดไปได้เฉพาะกรณีที่พิสูจน์ได้ว่ายังไม่ได้รัน:
   link ไม่ READY ตอนจะส่ง (ไม่ได้ส่ง) หรือตอบ `busy` (ปฏิเสธก่อนรัน)
   กรณีอื่น (timeout, connection หลุด, executor error) รายงาน error ทันที — at-most-once

ไม่มีผู้ผ่านเลย → `no_eligible_node` พร้อมรายการผู้สมัครและเหตุผลของแต่ละตัว

## Explicit (ระบุ `target`)

1. ไม่รู้จัก node นั้น → `target_not_found`
2. node นั้นไม่ได้ประกาศ capability → `capability_not_supported`
3. `allow` ไม่มีผู้เรียก → `unauthorized`
4. link ไม่ READY หรือ availability ไม่พร้อม → `target_unavailable`
5. เต็ม → `busy`
6. ผ่าน → ส่งไปเครื่องนั้นเท่านั้น

**ไม่มีกรณีไหนที่เปลี่ยนไปเครื่องอื่น** แม้จะมีเครื่องอื่นรองรับ capability เดียวกัน

## Decision record

ทุกการตัดสินใจถูกบันทึกเป็น log `target=synapta::routing` (JSON) และเก็บใน ring buffer
(`limits.routing_log_size`, ค่าเริ่ม 100) — ดูได้ที่ `GET /routing/decisions` หรือ `synapta … decisions`
ดูล่วงหน้าโดยไม่รัน: `GET /routing/explain?capability=…&target=…`

ตัวอย่างจริง (TEST-04: node-b เต็ม 2/2)

```json
{
  "capability": "test.execute", "mode": "automatic", "caller": "node-a", "policy": "least-loaded-v1",
  "selected": "node-c",
  "candidates": [
    {"node_id": "node-b", "local": false, "eligible": false, "load_permille": 1000, "cpu_percent": 20,
     "rejection": {"code": "at_capacity", "active_jobs": 2, "max_concurrent_jobs": 2}},
    {"node_id": "node-c", "local": false, "eligible": true, "load_permille": 0, "cpu_percent": 20, "rank": 1}
  ],
  "nodes_without_capability": 1,
  "attempts": [{"node_id": "node-c", "outcome": "ok"}]
}
```

## ความสดของข้อมูล

ทรัพยากรมาจาก advert ซึ่งออกใหม่เมื่อจำนวน job เปลี่ยน (รวบให้ไม่ถี่กว่า `advert_min_interval_ms`) และทุก `advert_refresh_ms`
จึงมีช่วงสั้น ๆ ที่มุมมองช้ากว่าความจริง เครื่องปลายทางตรวจ capacity เองอีกรอบ — ถ้าเต็มจะตอบ `busy`
แล้ว routing อัตโนมัติไปเครื่องถัดไป (ปลอดภัยเพราะยังไม่ได้รัน)
