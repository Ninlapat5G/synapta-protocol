# คู่มือให้ agent คุม Synapta Lab

สำหรับ agent (เช่น ซิน) ที่ต้องเปิด/ปิด Lab และสั่งงานผ่าน HTTP โดยไม่ต้องเปิดหน้าเว็บ
Lab คือตัวจำลองในหน่วยความจำ: node, ผัง และ trace **หายเมื่อปิดโปรแกรม**

ทุกตัวอย่างใช้ `http://127.0.0.1:7878` (พอร์ตเริ่มต้น) — ใช้พอร์ตตามที่เปิดจริง

## 1. เปิด / ปิด / ดูสถานะ

รันในโฟลเดอร์ที่มี `synapta-lab.exe`:

| ต้องการ | คำสั่ง | ผล |
|---|---|---|
| เปิด (ไม่มีหน้าต่าง) | `Start-Lab-Hidden.cmd` | พิมพ์ URL + pid · ถ้ารันอยู่แล้วบอก "already running" (exit 0) ไม่เปิดซ้ำ |
| เปิดพอร์ตอื่น / ไม่เปิดทาง Tailscale | `Start-Lab-Hidden.cmd -Port 7990 -NoTailscale` | |
| สถานะ | `powershell -NoProfile -File Lab-Control.ps1 status` | exit 0 = รันอยู่ · exit 3 = ไม่ได้รัน |
| ปิด | `Stop-Lab.cmd` | **บันทึกผังลง `lab-saved\topology-<เวลา>.json` ก่อน** แล้วค่อยปิด (ถ้าไม่มี node ไม่บันทึก) |

- เปิดไม่ขึ้น (พอร์ตชน ฯลฯ) → exit 1 และเหตุผลอยู่ใน `lab.err`
- ปิดได้เฉพาะ Lab ของโฟลเดอร์นี้ (ตรวจ path ของ exe กับ `lab.pid`) ไม่ฆ่าโปรเซสอื่น
- **ก่อนปิดให้ถามเจ้าของ** ถ้าเขากำลังทดลองอยู่ — trace และสถานะการทำงานหายแม้ผังจะถูกเก็บ
- เปิดเครื่องใหม่แล้ว Lab ไม่ขึ้นเอง ต้องสั่งเปิดใหม่ · โหลดผังเดิมกลับด้วยข้อ 4

## 2. กติกาของ API

- ทุกคำขอที่ **เปลี่ยนค่า** (POST/PUT/DELETE) ต้องมี header `Content-Type: application/json` ไม่งั้นได้ 415
- body เป็น JSON เสมอ · ข้อผิดพลาดคืน `{"error":"..."}` พร้อมรหัส 400/404/409
- การเรียกงาน (`invoke`/`jobs`) ที่ **ล้มเหลวยังได้ 200** — ดูที่คีย์ `failed` (พร้อมรายงาน routing) ไม่ใช่รหัส HTTP; สำเร็จอยู่ใต้คีย์ `ok`
- node ที่ดับอยู่หรือไม่มี → 409 `... does not exist or is powered off`
- ชื่อ node (`id`) ใส่ใน URL ได้ตรง ๆ เช่น `r1-n1`

ตัวอย่างใน PowerShell:

```powershell
$j = @{ 'Content-Type' = 'application/json' }
Invoke-RestMethod http://127.0.0.1:7878/api/scenarios/rings -Method Post -Headers $j -Body '{"rings":2,"per_ring":3}'
```

ตัวอย่างใน bash/curl: `curl -s -X POST -H 'Content-Type: application/json' -d '{...}' URL`

## 3. สร้างวงเครือข่ายเร็ว ๆ — scenarios

`POST /api/scenarios/<ชื่อ>` (body ว่างได้ ใช้ค่าเริ่มต้น) · รายชื่ออยู่ที่ `GET /api/info` → `scenarios`

| ชื่อ | body | ทำอะไร |
|---|---|---|
| `rings` | `{"rings":3,"per_ring":4,"capabilities":["math.add","test.execute","system.echo"]}` | สร้างหลายวง ต่อกันเป็นผัง ตอบ `{"nodes":N,"started_in_ms":..}` · node ตั้งชื่อ `r1-n1`, `r1-n2`, `r2-n1` ... (ล้างของเดิมก่อน) |
| `burst` | `{"from":"r1-n1","count":10,"capability":"math.add","input":{"a":1,"b":2},"target":null}` | ยิงงานจำนวนมากจาก node หนึ่ง ตอบ `{"calls","elapsed_ms","errors","ran_on":{node:จำนวน}}` |
| `crash_during_jobs` | `{"from":"r1-n1","jobs":4}` | ส่งงานยาว ~4 วินาทีแล้วทำให้ node ล่ม ดูว่างานถูกจัดการอย่างไร |
| `cut_bridge` | `{"bridge":"r1-n1"}` (ไม่ใส่ = node แรกที่อยู่สองวง) | ถอด node ที่เป็นสะพานออกจากวงสุดท้าย ดูว่าสองวงแยกกันไหม |

เพดาน: scenario จำกัด 5,000 node และ 10,000 งานต่อครั้ง · วงใหญ่มากอาจไม่ผ่าน (ดู `web-sandbox-plan.md`)
**หลังสร้างวงให้รอ 2-3 วินาที** ให้ node ค้นหากันครบก่อนสั่งงาน

## 4. สั่งงานรายตัว

| ทำอะไร | คำขอ |
|---|---|
| ดู node ทั้งหมด | `GET /api/nodes` → รายการ `{setup:{id,rings,capabilities,...}, links, running, address, jobs_run, resources}` |
| ดูสถานะละเอียดของ node | `GET /api/nodes/<id>` |
| เพิ่ม node (ตัวเดียว หรืออาร์เรย์) | `POST /api/nodes` body `{"id":"x1","capabilities":[{"name":"system.echo"}],"rings":["ring-1"]}` · ใส่แค่ `id` ก็ได้ · ตอบ `{"failed":[...]}` |
| แก้ node | `PUT /api/nodes/<id>` body เป็น setup ทั้งก้อน (`id` ต้องตรง URL) |
| ลบ node | `DELETE /api/nodes/<id>` |
| เปลี่ยนวงของ node | `PUT /api/nodes/<id>/rings` body `["ring-2"]` |
| ดับ node | `POST /api/nodes/<id>/power` body `{"off":"graceful"}` (ปิดสาย peer เห็นทันที) หรือ `{"off":"crash"}` (เงียบหายเหมือนดึงปลั๊ก peer รู้จาก heartbeat) |
| เปิด node คืน | `POST /api/nodes/<id>/power` body `{"on":true}` |
| เรียกงานแล้วรอผล | `POST /api/nodes/<id>/invoke` body `{"capability":"math.add","input":{"a":2,"b":3},"target":null}` → `{"ok":{"executed_by","output":{"sum":5},"routing":{...}}}` |
| ส่งงานยาว (ไม่รอ) | `POST /api/nodes/<id>/jobs` body เหมือน invoke → `ok.job_id` + `ok.executed_by` |
| ถามผลงาน | `GET /api/nodes/<caller>/jobs/<executor>/<job_id>?wait_ms=3000` (รอได้นานสุดตามเพดานของระบบ) |
| ยกเลิกงาน | `POST /api/nodes/<caller>/jobs/<executor>/<job_id>/cancel` |

- `target` ว่าง = ให้ routing เลือกเอง · ใส่ `"target":"r2-n1"` = บังคับปลายทาง
- capability ที่มีให้เรียก: `GET /api/info` → `catalog` (เช่น `math.add`, `system.echo`, `test.execute`)
- `test.execute` ใช้ `input` แบบ `{"suite":"smoke","steps":20,"step_ms":200}`

## 5. ดู "ทำไม routing เลือกเครื่องนั้น"

- `GET /api/nodes/<id>/explain?capability=math.add[&target=<node>]` — จัดอันดับผู้สมัครตอนนี้ (`rank`, `hops`, `load_permille`, `eligible`)
- ผลของ `invoke`/`jobs` มี `routing.attempts` (ลองเครื่องไหนบ้าง ผลอะไร) และ `routing.candidates` ในตัว
- `GET /api/nodes/<id>/routes` — ตาราง route ของ node · `GET /api/nodes/<id>/decisions` — การตัดสินใจล่าสุด

## 6. trace — เห็นว่าอะไรเกิดขึ้นบนสาย

- `GET /api/trace?after=<seq>&limit=<n>` → `{"events":[{seq,at_us,kind,node,peer,...}],"last":<seq ล่าสุด>}`
  ดึงต่อโดยส่ง `after` = `last` ของรอบก่อน (เริ่มที่ 0)
- `DELETE /api/trace` ล้าง trace · `PUT /api/trace/heartbeats` body `true|false` เปิด/ปิดการบันทึก heartbeat (ท่วม trace ได้เร็วในวงใหญ่)
- trace เก็บสูงสุด 200,000 เหตุการณ์ ของเก่าถูกทิ้ง — ถ้า `skipped` เป็น `true` แปลว่าที่ขอหายไปแล้ว

## 7. บันทึก / โหลดผัง

- บันทึก: `GET /api/topology` → `{"nodes":[...],"layout":{...}}` เก็บเป็นไฟล์ได้เลย (`Stop-Lab.cmd` ทำให้อัตโนมัติ)
- โหลด: `PUT /api/topology` ส่งไฟล์เดิมกลับไปทั้งก้อน (ใช้ `--data-binary @ไฟล์` ใน curl) · **ไฟล์ต้องไม่มี BOM**
- โหลดแทนผังปัจจุบันทั้งหมด · โหลดสำเร็จตอบ `{}` แล้วรอ 2-3 วินาทีให้ node ค้นหากัน
- `POST /api/reset` ล้างทุก node (**ไม่ถามซ้ำ** — บันทึกผังก่อนถ้ายังต้องการ)

## 8. ตรวจว่าใช้ได้

1. `Start-Lab-Hidden.cmd` → ได้ `started on ...` หรือ `already running`
2. `GET /api/info` ได้ `version` และ `catalog`
3. `POST /api/scenarios/rings` ตามด้วยรอ 3 วินาที → `POST /api/nodes/r1-n1/invoke` ได้ `ok.output.sum`

## เรื่องที่ Lab ทำไม่ได้ (อย่าสัญญาเกินจริง)

- node ไม่เชื่อมเครื่องจริง ท่อเป็นในหน่วยความจำ (handshake เข้ารหัสและ protocol ทำจริง)
- ไม่มีสิทธิ์/ล็อกอิน: ใครเข้า URL ได้ก็สั่งได้ จึงฟังเฉพาะ loopback และ IP Tailscale ของเครื่อง
- ผลทดสอบวงใหญ่และข้อจำกัดที่ทราบอยู่ใน `docs/web-sandbox-plan.md` และ `docs/test-report.md`
