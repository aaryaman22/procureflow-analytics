# ProcureFlow Analytics — Complete Manual QA Guide

Step-by-step validation document. Execute top to bottom before adding this project to your resume.

**Defaults assumed:**
- App: `http://localhost:8080`
- PostgreSQL: `localhost:5432`, database `event_analytics`, user `postgres`
- Kafka: `localhost:9092`
- Main topic: `procurement-events`
- DLQ topic: `procurement-events-dlq`
- Kafka install path: `$env:KAFKA_HOME` (set this to your Kafka folder)

```powershell
# Set once per session
$BaseUrl = "http://localhost:8080"
$env:KAFKA_HOME = "C:\path\to\kafka"   # UPDATE THIS
$PgDb = "event_analytics"
$PgUser = "postgres"
New-Item -ItemType Directory -Force -Path qa-payloads | Out-Null
```

**Tip:** Create a `qa-payloads\` folder for JSON files to avoid PowerShell escaping issues.

---

## How to read each test

| Field | Meaning |
|-------|---------|
| **Purpose** | What capability you are proving |
| **Expected behavior** | What the system should do |
| **Command** | Exact steps to run |
| **Expected response** | HTTP body / exit code |
| **Expected DB state** | PostgreSQL after test |
| **Expected Kafka behavior** | Topic messages / offsets |
| **Expected logs** | Spring Boot log lines to look for |
| **Failure indicates** | What a wrong result means |

**Wait rule:** After every ingest (API or Kafka), wait **2–5 seconds** for consumer processing before checking DB or analytics.

---

# SECTION 0 — Environment Verification

---

## TEST 0.1 — PostgreSQL running

**Purpose:** Confirm database is reachable before any test.

**Expected behavior:** Connection succeeds; `event_analytics` database exists.

**Command:**
```powershell
psql -U $PgUser -d $PgDb -c "SELECT 1 AS ok;"
```

**Expected response:**
```
 ok
----
  1
(1 row)
```

**Expected DB state:** N/A

**Expected Kafka behavior:** N/A

**Expected logs:** N/A

**Failure indicates:** Postgres not running, wrong credentials, or database not created.

---

## TEST 0.2 — Kafka broker running

**Purpose:** Confirm Kafka accepts connections.

**Expected behavior:** Broker responds to topic list command.

**Command:**
```powershell
& "$env:KAFKA_HOME\bin\windows\kafka-topics.bat" --list --bootstrap-server localhost:9092
```

**Expected response:** Command exits 0; lists topics (may be empty on first run).

**Expected DB state:** N/A

**Expected Kafka behavior:** Broker reachable.

**Expected logs:** N/A

**Failure indicates:** Kafka not started or wrong bootstrap server.

---

## TEST 0.3 — Kafka topics exist

**Purpose:** Confirm main and DLQ topics are created.

**Expected behavior:** Both topics listed.

**Command:**
```powershell
& "$env:KAFKA_HOME\bin\windows\kafka-topics.bat" --create --if-not-exists --topic procurement-events --bootstrap-server localhost:9092
& "$env:KAFKA_HOME\bin\windows\kafka-topics.bat" --create --if-not-exists --topic procurement-events-dlq --bootstrap-server localhost:9092
& "$env:KAFKA_HOME\bin\windows\kafka-topics.bat" --list --bootstrap-server localhost:9092
```

**Expected response:** List includes `procurement-events` and `procurement-events-dlq`.

**Expected DB state:** N/A

**Expected Kafka behavior:** Topics exist with at least 1 partition.

**Expected logs:** N/A

**Failure indicates:** Topic misconfiguration; consumer/producer will fail.

---

## TEST 0.4 — Spring Boot running

**Purpose:** Confirm application started successfully.

**Expected behavior:** App listens on port 8080; no startup stack traces.

**Command:**
```powershell
cd C:\Users\HP\Desktop\project\event-analytics
.\mvnw.cmd spring-boot:run
```

(In a separate terminal for remaining tests.)

**Expected response:** Log line similar to:
```
Started EventAnalyticsApplication
Tomcat started on port 8080
```

**Expected DB state:** `events` table exists (created by Hibernate if empty).

**Expected Kafka behavior:** Consumer joins group `event-analytics-group`.

**Expected logs:**
```
Started EventAnalyticsApplication
```

**Failure indicates:** Bad DB password, Kafka unreachable at startup, or port 8080 in use.

---

## TEST 0.5 — Health endpoint

**Purpose:** Verify Actuator health and DB connectivity.

**Expected behavior:** HTTP 200, status UP.

**Command:**
```powershell
curl.exe -s -w "`nHTTP_STATUS:%{http_code}`n" "$BaseUrl/actuator/health"
```

**Expected response:**
```json
{"status":"UP"}
HTTP_STATUS:200
```

**Expected DB state:** Unchanged

**Expected Kafka behavior:** N/A (Kafka health not included in current implementation)

**Expected logs:** N/A

**Failure indicates:** DB down, app unhealthy, or actuator misconfigured.

---

## TEST 0.6 — Baseline DB snapshot (optional reset)

**Purpose:** Establish clean baseline or record starting counts.

**Expected behavior:** Know starting row count before functional tests.

**Command:**
```powershell
psql -U $PgUser -d $PgDb -c "SELECT COUNT(*) AS total FROM events;"
psql -U $PgUser -d $PgDb -c "SELECT event_type, COUNT(*) FROM events GROUP BY event_type ORDER BY event_type;"
```

**Expected response:** Current counts (may be 0).

**Expected DB state:** Document baseline in your notes.

**Expected Kafka behavior:** N/A

**Expected logs:** N/A

**Failure indicates:** `events` table missing — Hibernate ddl-auto failed.

---

# SECTION 1 — Event Ingestion Tests

---

## TEST 1.1 — Happy path (valid event)

**Purpose:** Validate full pipeline: HTTP → Kafka → Consumer → PostgreSQL.

**Expected behavior:** 202 Accepted; one new row; Kafka message published and consumed.

**Command:**
```powershell
@'
{
  "event_id": "11111111-1111-1111-1111-111111111101",
  "event_type": "RFP_CREATED",
  "aggregate_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
  "user_id": "qa-user-1",
  "payload": { "title": "QA Happy Path RFP" },
  "occurred_at": "2026-06-23T10:00:00Z"
}
'@ | Set-Content -Encoding utf8 qa-payloads\happy-path.json

curl.exe -s -w "`nHTTP_STATUS:%{http_code}`n" -X POST "$BaseUrl/api/v1/events" -H "Content-Type: application/json" -d "@qa-payloads/happy-path.json"
```

Wait 3 seconds, then:
```powershell
psql -U $PgUser -d $PgDb -c "SELECT event_id, event_type, aggregate_id, user_id, payload FROM events WHERE event_id = '11111111-1111-1111-1111-111111111101';"
```

**Expected response:**
```json
{"eventId":"11111111-1111-1111-1111-111111111101","message":"Event accepted"}
HTTP_STATUS:202
```

**Expected DB state:** Exactly 1 row with matching `event_id`, `event_type = RFP_CREATED`, JSON payload contains `"title"`.

**Expected Kafka behavior:** One message on `procurement-events` with key `aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa`.

**Expected logs (in order):**
```
Received event ingestion request ... eventId=11111111-...
Accepting event for publishing ... eventId=11111111-...
Publishing event to Kafka ... eventId=11111111-...
Successfully published event to Kafka ... eventId=11111111-...
Event persisted successfully ... action=PERSISTED ... eventId=11111111-...
```

**Failure indicates:**
- 202 but no DB row → consumer not running or Kafka issue
- 4xx/5xx → validation or server error
- No publish log → producer failure

---

## TEST 1.2 — Validation: missing `event_id`

**Purpose:** Confirm `@NotNull` validation on `event_id`.

**Expected behavior:** HTTP 400; no Kafka publish; no DB insert.

**Command:**
```powershell
@'
{
  "event_type": "RFP_CREATED",
  "aggregate_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
  "user_id": "qa-user",
  "payload": {},
  "occurred_at": "2026-06-23T10:00:00Z"
}
'@ | Set-Content -Encoding utf8 qa-payloads\missing-event-id.json

curl.exe -s -w "`nHTTP_STATUS:%{http_code}`n" -X POST "$BaseUrl/api/v1/events" -H "Content-Type: application/json" -d "@qa-payloads/missing-event-id.json"
```

**Expected response:**
```
HTTP_STATUS:400
```
Body contains `"status":400` and field error referencing `eventId` or `event_id`.

**Expected DB state:** No new row.

**Expected Kafka behavior:** No new message.

**Expected logs:** No `Publishing event to Kafka` for this request.

**Failure indicates:** Validation not enforced; missing `@Valid` or dependency.

---

## TEST 1.3 — Validation: missing `event_type`

**Purpose:** Confirm `@NotNull` on `event_type`.

**Command:**
```powershell
@'
{
  "event_id": "11111111-1111-1111-1111-111111111102",
  "aggregate_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
  "user_id": "qa-user",
  "payload": {},
  "occurred_at": "2026-06-23T10:00:00Z"
}
'@ | Set-Content -Encoding utf8 qa-payloads\missing-event-type.json

curl.exe -s -w "`nHTTP_STATUS:%{http_code}`n" -X POST "$BaseUrl/api/v1/events" -H "Content-Type: application/json" -d "@qa-payloads/missing-event-type.json"
```

**Expected response:** `HTTP_STATUS:400`, error on `eventType`.

**Expected DB state:** No new row.

**Expected Kafka behavior:** No new message.

**Expected logs:** No publish log.

**Failure indicates:** Validation bypass.

---

## TEST 1.4 — Validation: missing `aggregate_id`

**Command:**
```powershell
@'
{
  "event_id": "11111111-1111-1111-1111-111111111103",
  "event_type": "RFP_CREATED",
  "user_id": "qa-user",
  "payload": {},
  "occurred_at": "2026-06-23T10:00:00Z"
}
'@ | Set-Content -Encoding utf8 qa-payloads\missing-aggregate-id.json

curl.exe -s -w "`nHTTP_STATUS:%{http_code}`n" -X POST "$BaseUrl/api/v1/events" -H "Content-Type: application/json" -d "@qa-payloads/missing-aggregate-id.json"
```

**Expected response:** `HTTP_STATUS:400`

**Expected DB state:** No new row.

**Expected Kafka behavior:** No message.

**Expected logs:** No publish log.

**Failure indicates:** Validation bypass.

---

## TEST 1.5 — Validation: missing `user_id`

**Command:**
```powershell
@'
{
  "event_id": "11111111-1111-1111-1111-111111111104",
  "event_type": "RFP_CREATED",
  "aggregate_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
  "payload": {},
  "occurred_at": "2026-06-23T10:00:00Z"
}
'@ | Set-Content -Encoding utf8 qa-payloads\missing-user-id.json

curl.exe -s -w "`nHTTP_STATUS:%{http_code}`n" -X POST "$BaseUrl/api/v1/events" -H "Content-Type: application/json" -d "@qa-payloads/missing-user-id.json"
```

**Expected response:** `HTTP_STATUS:400`

**Expected DB state:** No new row.

**Expected Kafka behavior:** No message.

**Expected logs:** No publish log.

**Failure indicates:** Validation bypass.

---

## TEST 1.6 — Validation: missing `occurred_at`

**Command:**
```powershell
@'
{
  "event_id": "11111111-1111-1111-1111-111111111105",
  "event_type": "RFP_CREATED",
  "aggregate_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
  "user_id": "qa-user",
  "payload": {}
}
'@ | Set-Content -Encoding utf8 qa-payloads\missing-occurred-at.json

curl.exe -s -w "`nHTTP_STATUS:%{http_code}`n" -X POST "$BaseUrl/api/v1/events" -H "Content-Type: application/json" -d "@qa-payloads/missing-occurred-at.json"
```

**Expected response:** `HTTP_STATUS:400`

**Expected DB state:** No new row.

**Expected Kafka behavior:** No message.

**Expected logs:** No publish log.

**Failure indicates:** Validation bypass.

---

## TEST 1.7 — Validation: missing `payload` field

**Purpose:** Confirm `@NotNull` on payload map.

**Command:**
```powershell
@'
{
  "event_id": "11111111-1111-1111-1111-111111111106",
  "event_type": "RFP_CREATED",
  "aggregate_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
  "user_id": "qa-user",
  "occurred_at": "2026-06-23T10:00:00Z"
}
'@ | Set-Content -Encoding utf8 qa-payloads\missing-payload.json

curl.exe -s -w "`nHTTP_STATUS:%{http_code}`n" -X POST "$BaseUrl/api/v1/events" -H "Content-Type: application/json" -d "@qa-payloads/missing-payload.json"
```

**Expected response:** `HTTP_STATUS:400`

**Expected DB state:** No new row.

**Expected Kafka behavior:** No message.

**Expected logs:** No publish log.

**Failure indicates:** Validation bypass.

---

## TEST 1.8 — Empty payload `{}` (valid)

**Purpose:** Confirm empty object is allowed (`@NotNull` but not `@NotEmpty`).

**Expected behavior:** 202 Accepted; row inserted with `{}` payload.

**Command:**
```powershell
@'
{
  "event_id": "11111111-1111-1111-1111-111111111107",
  "event_type": "RFP_CREATED",
  "aggregate_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
  "user_id": "qa-user",
  "payload": {},
  "occurred_at": "2026-06-23T10:00:00Z"
}
'@ | Set-Content -Encoding utf8 qa-payloads\empty-payload.json

curl.exe -s -w "`nHTTP_STATUS:%{http_code}`n" -X POST "$BaseUrl/api/v1/events" -H "Content-Type: application/json" -d "@qa-payloads/empty-payload.json"
```

**Expected response:** `HTTP_STATUS:202`

**Expected DB state:** Row exists; `payload = {}`.

**Expected Kafka behavior:** Message published and consumed.

**Expected logs:** `PERSISTED` for this `event_id`.

**Failure indicates:** Over-strict validation on payload.

---

## TEST 1.9 — Invalid UUID

**Purpose:** Confirm malformed UUID rejected at HTTP layer.

**Command:**
```powershell
curl.exe -s -w "`nHTTP_STATUS:%{http_code}`n" -X POST "$BaseUrl/api/v1/events" -H "Content-Type: application/json" -d "{\"event_id\":\"not-a-uuid\",\"event_type\":\"RFP_CREATED\",\"aggregate_id\":\"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa\",\"user_id\":\"qa-user\",\"payload\":{},\"occurred_at\":\"2026-06-23T10:00:00Z\"}"
```

**Expected response:** `HTTP_STATUS:400` (JSON parse / type conversion error)

**Expected DB state:** No new row.

**Expected Kafka behavior:** No message.

**Expected logs:** No publish log.

**Failure indicates:** Weak input parsing.

---

## TEST 1.10 — Invalid event type (API)

**Purpose:** Confirm unknown enum rejected on HTTP ingest.

**Command:**
```powershell
curl.exe -s -w "`nHTTP_STATUS:%{http_code}`n" -X POST "$BaseUrl/api/v1/events" -H "Content-Type: application/json" -d "{\"event_id\":\"11111111-1111-1111-1111-111111111108\",\"event_type\":\"INVALID_TYPE\",\"aggregate_id\":\"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa\",\"user_id\":\"qa-user\",\"payload\":{},\"occurred_at\":\"2026-06-23T10:00:00Z\"}"
```

**Expected response:** `HTTP_STATUS:400`

**Expected DB state:** No new row.

**Expected Kafka behavior:** No message.

**Expected logs:** No publish log.

**Failure indicates:** Enum validation missing.

---

## TEST 1.11 — Malformed JSON (API)

**Purpose:** Confirm bad JSON rejected before controller logic.

**Command:**
```powershell
curl.exe -s -w "`nHTTP_STATUS:%{http_code}`n" -X POST "$BaseUrl/api/v1/events" -H "Content-Type: application/json" -d "{ this is not json"
```

**Expected response:** `HTTP_STATUS:400`

**Expected DB state:** No new row.

**Expected Kafka behavior:** No message.

**Expected logs:** No ingest log.

**Failure indicates:** Request reached business logic incorrectly.

---

## TEST 1.12 — Future timestamp (known gap)

**Purpose:** Document current behavior — no future-date validation exists.

**Expected behavior:** **Currently accepted** (202). Record as known limitation.

**Command:**
```powershell
@'
{
  "event_id": "11111111-1111-1111-1111-111111111109",
  "event_type": "RFP_CREATED",
  "aggregate_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
  "user_id": "qa-user",
  "payload": { "note": "future timestamp test" },
  "occurred_at": "2099-12-31T23:59:59Z"
}
'@ | Set-Content -Encoding utf8 qa-payloads\future-timestamp.json

curl.exe -s -w "`nHTTP_STATUS:%{http_code}`n" -X POST "$BaseUrl/api/v1/events" -H "Content-Type: application/json" -d "@qa-payloads/future-timestamp.json"
```

**Expected response:** `HTTP_STATUS:202` (current implementation)

**Expected DB state:** Row with `occurred_at` in 2099.

**Expected Kafka behavior:** Message consumed normally.

**Expected logs:** `PERSISTED`

**Failure indicates:** If 400 — someone added validation (update your resume notes accordingly).

---

## TEST 1.13 — Very large payload (known gap)

**Purpose:** Stress payload handling; documents no size limit today.

**Command:**
```powershell
$big = "x" * 500000
@{
  event_id = "11111111-1111-1111-1111-111111111110"
  event_type = "RFP_CREATED"
  aggregate_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
  user_id = "qa-user"
  payload = @{ data = $big }
  occurred_at = "2026-06-23T10:00:00Z"
} | ConvertTo-Json -Depth 5 | Set-Content -Encoding utf8 qa-payloads\large-payload.json

curl.exe -s -w "`nHTTP_STATUS:%{http_code}`n" -X POST "$BaseUrl/api/v1/events" -H "Content-Type: application/json" -d "@qa-payloads/large-payload.json"
```

**Expected response:** `HTTP_STATUS:202` or `413` depending on Tomcat default limits (~2MB may pass).

**Expected DB state:** Row inserted if 202.

**Expected Kafka behavior:** Large message on topic if accepted.

**Expected logs:** `PERSISTED` if successful.

**Failure indicates:** Unexpected crash or OOM — production would need payload limits.

---

# SECTION 2 — Idempotency Tests

---

## TEST 2.1 — Duplicate API submission (same `event_id`)

**Purpose:** Prove duplicate HTTP submits do not create duplicate DB rows.

**Expected behavior:** Both return 202; only one DB row; second consumed as duplicate.

**Command:**
```powershell
@'
{
  "event_id": "22222222-2222-2222-2222-222222222201",
  "event_type": "VENDOR_INVITED",
  "aggregate_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
  "user_id": "qa-user",
  "payload": { "vendor": "Acme" },
  "occurred_at": "2026-06-23T11:00:00Z"
}
'@ | Set-Content -Encoding utf8 qa-payloads\idempotent-api.json

curl.exe -s -w "`nHTTP_STATUS:%{http_code}`n" -X POST "$BaseUrl/api/v1/events" -H "Content-Type: application/json" -d "@qa-payloads/idempotent-api.json"
Start-Sleep -Seconds 3
curl.exe -s -w "`nHTTP_STATUS:%{http_code}`n" -X POST "$BaseUrl/api/v1/events" -H "Content-Type: application/json" -d "@qa-payloads/idempotent-api.json"
Start-Sleep -Seconds 3
psql -U $PgUser -d $PgDb -c "SELECT COUNT(*) AS cnt FROM events WHERE event_id = '22222222-2222-2222-2222-222222222201';"
```

**Expected response:** Both `HTTP_STATUS:202`

**Expected DB state:** `cnt = 1`

**Expected Kafka behavior:** **Two messages** on `procurement-events` (known design — dedup is consumer-side only).

**Expected logs:**
- First: `PERSISTED`
- Second: `Duplicate event detected — skipping persistence` with `action=SKIP_DUPLICATE`

**Failure indicates:**
- `cnt > 1` → idempotency broken
- No SKIP_DUPLICATE log on second → consumer not processing or check failed

---

## TEST 2.2 — Duplicate Kafka message (bypass API)

**Purpose:** Prove consumer idempotency when same message delivered twice from Kafka.

**Command:**
```powershell
$msg = '{"event_id":"22222222-2222-2222-2222-222222222202","event_type":"PROPOSAL_SUBMITTED","aggregate_id":"cccccccc-cccc-cccc-cccc-cccccccccccc","user_id":"qa-user","payload":{"amount":1000},"occurred_at":"2026-06-23T12:00:00Z"}'
$msg | & "$env:KAFKA_HOME\bin\windows\kafka-console-producer.bat" --bootstrap-server localhost:9092 --topic procurement-events
Start-Sleep -Seconds 3
$msg | & "$env:KAFKA_HOME\bin\windows\kafka-console-producer.bat" --bootstrap-server localhost:9092 --topic procurement-events
Start-Sleep -Seconds 3
psql -U $PgUser -d $PgDb -c "SELECT COUNT(*) FROM events WHERE event_id = '22222222-2222-2222-2222-222222222202';"
```

**Expected response:** N/A (CLI)

**Expected DB state:** `COUNT = 1`

**Expected Kafka behavior:** Two identical records on topic; consumer skips second.

**Expected logs:** One `PERSISTED`, one `SKIP_DUPLICATE`

**Failure indicates:** Idempotency logic or unique constraint missing.

---

## TEST 2.3 — Concurrent duplicate (race simulation)

**Purpose:** Validate DB unique constraint backstop when two messages processed close together.

**Expected behavior:** At most one row; at least one `PERSISTED` and possibly one `SKIP_DUPLICATE` (unique constraint path).

**Command:**
```powershell
# Send same event twice rapidly via API from two PowerShell jobs
$body = @'
{
  "event_id": "22222222-2222-2222-2222-222222222203",
  "event_type": "CONTRACT_AWARDED",
  "aggregate_id": "dddddddd-dddd-dddd-dddd-dddddddddddd",
  "user_id": "qa-user",
  "payload": { "amount": 5000 },
  "occurred_at": "2026-06-23T13:00:00Z"
}
'@ | Set-Content -Encoding utf8 qa-payloads\race.json

$job1 = Start-Job { param($u,$f) curl.exe -s -X POST "$u/api/v1/events" -H "Content-Type: application/json" -d "@$f" } -ArgumentList $BaseUrl, "$PWD\qa-payloads\race.json"
$job2 = Start-Job { param($u,$f) curl.exe -s -X POST "$u/api/v1/events" -H "Content-Type: application/json" -d "@$f" } -ArgumentList $BaseUrl, "$PWD\qa-payloads\race.json"
Wait-Job $job1,$job2 | Out-Null
Receive-Job $job1,$job2
Start-Sleep -Seconds 5
psql -U $PgUser -d $PgDb -c "SELECT COUNT(*) FROM events WHERE event_id = '22222222-2222-2222-2222-222222222203';"
```

**Expected response:** Both jobs return 202 JSON.

**Expected DB state:** `COUNT = 1` (never 2)

**Expected Kafka behavior:** Two publishes.

**Expected logs:** Mix of `PERSISTED` and `SKIP_DUPLICATE` (reason may be `event_id already exists` or `unique constraint on event_id`).

**Failure indicates:** `COUNT = 2` — race protection failed (critical bug).

---

# SECTION 3 — Analytics Tests

---

## TEST 3.1 — Seed data for analytics

**Purpose:** Insert known event mix for predictable analytics assertions.

**Command:**
```powershell
# 3x RFP_CREATED, 2x VENDOR_INVITED, 1x PROPOSAL_SUBMITTED, 1x PROPOSAL_APPROVED, 1x CONTRACT_AWARDED
$types = @(
  "RFP_CREATED","RFP_CREATED","RFP_CREATED",
  "VENDOR_INVITED","VENDOR_INVITED",
  "PROPOSAL_SUBMITTED",
  "PROPOSAL_APPROVED",
  "CONTRACT_AWARDED"
)
$i = 1
foreach ($t in $types) {
  $id = [guid]::NewGuid().ToString()
  $agg = [guid]::NewGuid().ToString()
  @"
{"event_id":"$id","event_type":"$t","aggregate_id":"$agg","user_id":"seed-user","payload":{"seed":$i},"occurred_at":"2026-06-23T14:00:00Z"}
"@ | Set-Content -Encoding utf8 "qa-payloads\seed-$i.json"
  curl.exe -s -X POST "$BaseUrl/api/v1/events" -H "Content-Type: application/json" -d "@qa-payloads/seed-$i.json" | Out-Null
  $i++
  Start-Sleep -Milliseconds 300
}
Start-Sleep -Seconds 5
```

**Expected response:** All 202.

**Expected DB state:** 8 new rows (plus any from prior tests).

**Expected Kafka behavior:** 8 messages consumed.

**Expected logs:** 8x `PERSISTED`.

**Failure indicates:** Ingest broken — fix before analytics tests.

---

## TEST 3.2 — Event counts endpoint

**Purpose:** Verify SQL aggregation via API.

**Command:**
```powershell
curl.exe -s "$BaseUrl/api/v1/analytics/event-counts"
psql -U $PgUser -d $PgDb -c "SELECT event_type, COUNT(*) AS cnt FROM events GROUP BY event_type ORDER BY event_type;"
```

**Expected response:** JSON with all 5 enum keys; counts match SQL query (includes ALL rows in DB, not just seed data).

Example shape:
```json
{
  "RFP_CREATED": <n>,
  "VENDOR_INVITED": <n>,
  "PROPOSAL_SUBMITTED": <n>,
  "PROPOSAL_APPROVED": <n>,
  "CONTRACT_AWARDED": <n>
}
```

**Expected DB state:** Unchanged by GET.

**Expected Kafka behavior:** N/A

**Expected logs:** `Received event counts request`, `Fetched event counts by type`

**Failure indicates:** API/SQL mismatch, missing zero-fill, or wrong enum keys.

**Pass criteria:** Each API value **exactly equals** SQL `cnt` for that type.

---

## TEST 3.3 — Funnel analytics + conversion rate

**Purpose:** Verify funnel stages and `conversionRate = (CONTRACT_AWARDED / RFP_CREATED) * 100`.

**Command:**
```powershell
curl.exe -s "$BaseUrl/api/v1/analytics/funnel"
psql -U $PgUser -d $PgDb -c "SELECT event_type, COUNT(*) FROM events WHERE event_type IN ('RFP_CREATED','VENDOR_INVITED','PROPOSAL_SUBMITTED','CONTRACT_AWARDED') GROUP BY event_type ORDER BY event_type;"
```

**Expected response:**
```json
{
  "RFP_CREATED": <n>,
  "VENDOR_INVITED": <n>,
  "PROPOSAL_SUBMITTED": <n>,
  "CONTRACT_AWARDED": <n>,
  "conversionRate": <CONTRACT_AWARDED / RFP_CREATED * 100>
}
```
Note: `PROPOSAL_APPROVED` is **not** in funnel response (by design).

**Expected DB state:** Unchanged.

**Expected Kafka behavior:** N/A

**Expected logs:** `Received procurement funnel request`, `Fetched procurement funnel`

**Failure indicates:** Wrong formula, wrong stages, or missing `conversionRate`.

**Manual verify:**
```
conversionRate == (CONTRACT_AWARDED count / RFP_CREATED count) * 100
(or 0.0 if RFP_CREATED is 0)
```

---

## TEST 3.4 — Divide-by-zero (empty or zero RFP_CREATED)

**Purpose:** Confirm funnel returns `conversionRate: 0.0` when no RFP_CREATED events.

**Command:** (Only run on a **fresh empty DB**, or compute expected value if not empty)

```powershell
# Option A: fresh DB only
curl.exe -s "$BaseUrl/api/v1/analytics/funnel"
```

**Expected response (empty DB):**
```json
{
  "RFP_CREATED": 0,
  "VENDOR_INVITED": 0,
  "PROPOSAL_SUBMITTED": 0,
  "CONTRACT_AWARDED": 0,
  "conversionRate": 0.0
}
```

**Expected DB state:** No RFP_CREATED rows (fresh DB scenario).

**Expected Kafka behavior:** N/A

**Expected logs:** Normal funnel fetch logs.

**Failure indicates:** NaN, 500 error, or division exception.

---

## TEST 3.5 — Analytics on populated DB (zeros still present)

**Purpose:** Confirm all funnel keys returned even when some stages are 0.

**Command:**
```powershell
curl.exe -s "$BaseUrl/api/v1/analytics/event-counts" | ConvertFrom-Json | Format-List
```

**Expected response:** All 5 keys present (none missing).

**Expected DB state:** Unchanged.

**Expected Kafka behavior:** N/A

**Expected logs:** Normal.

**Failure indicates:** Incomplete map construction in `AnalyticsService`.

---

# SECTION 4 — Dead Letter Queue Tests

---

## TEST 4.1 — Invalid JSON → DLQ

**Purpose:** Prove deserialization failures route to DLQ without crashing consumer.

**Command:**
```powershell
# Publish invalid JSON directly to main topic
"{ not valid json" | & "$env:KAFKA_HOME\bin\windows\kafka-console-producer.bat" --bootstrap-server localhost:9092 --topic procurement-events
Start-Sleep -Seconds 5
# Read DLQ (run in separate terminal; Ctrl+C after seeing message)
& "$env:KAFKA_HOME\bin\windows\kafka-console-consumer.bat" --bootstrap-server localhost:9092 --topic procurement-events-dlq --from-beginning --timeout-ms 10000
```

**Expected response:** DLQ consumer prints malformed JSON string.

**Expected DB state:** No row for this message.

**Expected Kafka behavior:** Message on `procurement-events-dlq`; consumer continues processing later messages.

**Expected logs:**
```
SEND_TO_DLQ ... action=SEND_TO_DLQ ... failureType=...
```

**Failure indicates:** Consumer stuck, no DLQ message, or app crash loop.

---

## TEST 4.2 — Unknown enum via Kafka → DLQ

**Purpose:** Invalid `event_type` cannot deserialize to `EventRequest`.

**Command:**
```powershell
'{"event_id":"33333333-3333-3333-3333-333333333301","event_type":"NOT_A_REAL_TYPE","aggregate_id":"eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee","user_id":"qa","payload":{},"occurred_at":"2026-06-23T15:00:00Z"}' | & "$env:KAFKA_HOME\bin\windows\kafka-console-producer.bat" --bootstrap-server localhost:9092 --topic procurement-events
Start-Sleep -Seconds 5
& "$env:KAFKA_HOME\bin\windows\kafka-console-consumer.bat" --bootstrap-server localhost:9092 --topic procurement-events-dlq --from-beginning --timeout-ms 10000
```

**Expected response:** DLQ contains the original JSON line.

**Expected DB state:** No insert for that `event_id`.

**Expected Kafka behavior:** DLQ message added.

**Expected logs:** `SEND_TO_DLQ`

**Failure indicates:** Deserialization error not handled; consumer not recovering.

---

## TEST 4.3 — Consumer failure (PostgreSQL stopped)

**Purpose:** Simulate persistence failure → DLQ via `EventConsumer` catch block.

**Steps:**
1. Publish a **new valid** event via API (note `event_id`).
2. **Stop PostgreSQL** immediately or before consumer processes (or stop Postgres, then publish via Kafka console).
3. Publish valid event directly to Kafka:

```powershell
'{"event_id":"33333333-3333-3333-3333-333333333302","event_type":"RFP_CREATED","aggregate_id":"ffffffff-ffff-ffff-ffff-ffffffffffff","user_id":"qa","payload":{"dlq":"test"},"occurred_at":"2026-06-23T16:00:00Z"}' | & "$env:KAFKA_HOME\bin\windows\kafka-console-producer.bat" --bootstrap-server localhost:9092 --topic procurement-events
```

4. With Postgres **down**, wait 5 seconds.
5. Check logs and DLQ.
6. **Restart PostgreSQL** and app if needed.

**Expected response:** N/A

**Expected DB state:** No row for `33333333-3333-3333-3333-333333333302` while DB is down.

**Expected Kafka behavior:** Message appears on `procurement-events-dlq` after failure.

**Expected logs:**
```
CONSUME_FAILED ... action=CONSUME_FAILED
SEND_TO_DLQ ... eventId=33333333-3333-3333-3333-333333333302
```

**Failure indicates:** Silent message loss, infinite retry loop, or consumer not running.

**Note:** Current design sends **transient** DB failures to DLQ permanently (known limitation — document in resume as trade-off).

---

## TEST 4.4 — Read all DLQ messages

**Purpose:** Operational verification of DLQ contents after tests 4.1–4.3.

**Command:**
```powershell
& "$env:KAFKA_HOME\bin\windows\kafka-console-consumer.bat" --bootstrap-server localhost:9092 --topic procurement-events-dlq --from-beginning --timeout-ms 15000
```

**Expected response:** Messages from invalid JSON, unknown enum, and DB failure tests (if executed).

**Expected DB state:** N/A

**Expected Kafka behavior:** DLQ retains messages per topic retention policy.

**Expected logs:** N/A

**Failure indicates:** DLQ routing broken across failure types.

---

## TEST 4.5 — Duplicate does NOT go to DLQ

**Purpose:** Confirm expected duplicates are skipped, not DLQ'd.

**Command:** Re-run TEST 2.1 and watch logs/DLQ.

**Expected behavior:** `SKIP_DUPLICATE` log; **no** new DLQ message for that duplicate.

**Failure indicates:** Incorrect failure classification.

---

# SECTION 5 — Persistence & Recovery Tests

---

## TEST 5.1 — Application restart

**Purpose:** Verify PostgreSQL durability independent of app lifecycle.

**Command:**
```powershell
psql -U $PgUser -d $PgDb -c "SELECT COUNT(*) AS before_restart FROM events;"
# Stop Spring Boot (Ctrl+C), then restart:
# .\mvnw.cmd spring-boot:run
Start-Sleep -Seconds 15
psql -U $PgUser -d $PgDb -c "SELECT COUNT(*) AS after_restart FROM events;"
curl.exe -s "$BaseUrl/actuator/health"
```

**Expected response:** `before_restart = after_restart`; health UP.

**Expected DB state:** All rows preserved.

**Expected Kafka behavior:** Consumer rejoins group; no duplicate processing if offsets committed (may reprocess uncommitted — watch logs).

**Expected logs:** Clean startup; possible replay with `SKIP_DUPLICATE` for uncommitted offsets.

**Failure indicates:** Data loss (should not happen) or consumer offset issues.

---

## TEST 5.2 — Kafka restart

**Purpose:** Verify system recovers when broker restarts.

**Command:**
```powershell
# Stop Kafka broker, wait, restart Kafka
curl.exe -s -w "`nHTTP_STATUS:%{http_code}`n" -X POST "$BaseUrl/api/v1/events" -H "Content-Type: application/json" -d "@qa-payloads/happy-path.json"
```

**Expected behavior while Kafka down:** **Known gap:** API may still return **202** even if publish fails asynchronously (fire-and-forget). Check producer error logs.

**Expected response after Kafka back:** Subsequent ingests succeed with publish logs.

**Expected DB state:** Events only for messages actually consumed.

**Expected logs when Kafka down:**
```
Failed to publish event to Kafka
```

**Failure indicates:** App crash without recovery, or silent data loss without log.

---

## TEST 5.3 — PostgreSQL restart

**Purpose:** Verify app and consumer recover after DB outage.

**Command:**
```powershell
# Restart PostgreSQL service
curl.exe -s "$BaseUrl/actuator/health"
curl.exe -s "$BaseUrl/api/v1/analytics/event-counts"
```

**Expected response:** Health returns UP after DB is back; analytics returns JSON (not 500).

**Expected DB state:** Prior rows intact.

**Expected Kafka behavior:** Consumer may DLQ messages during outage (see TEST 4.3).

**Expected logs:** Temporary DB errors during outage; recovery logs after restart.

**Failure indicates:** App requires manual intervention to reconnect permanently.

---

# SECTION 6 — Repository Verification SQL

Run these after testing to validate data integrity.

```powershell
# Total events
psql -U $PgUser -d $PgDb -c "SELECT COUNT(*) AS total_events FROM events;"

# Events by type
psql -U $PgUser -d $PgDb -c "SELECT event_type, COUNT(*) AS cnt FROM events GROUP BY event_type ORDER BY event_type;"

# Duplicate event_ids (MUST return 0 rows)
psql -U $PgUser -d $PgDb -c "SELECT event_id, COUNT(*) AS cnt FROM events GROUP BY event_id HAVING COUNT(*) > 1;"

# Most recent 10 events
psql -U $PgUser -d $PgDb -c "SELECT id, event_id, event_type, aggregate_id, occurred_at, created_at FROM events ORDER BY created_at DESC LIMIT 10;"

# Manual analytics verification (compare to API)
psql -U $PgUser -d $PgDb -c "SELECT event_type, COUNT(*) FROM events GROUP BY event_type ORDER BY event_type;"

# Funnel stage counts (manual)
psql -U $PgUser -d $PgDb -c "SELECT event_type, COUNT(*) FROM events WHERE event_type IN ('RFP_CREATED','VENDOR_INVITED','PROPOSAL_SUBMITTED','CONTRACT_AWARDED') GROUP BY event_type ORDER BY event_type;"

# Conversion rate manual calc
psql -U $PgUser -d $PgDb -c "SELECT ROUND(100.0 * SUM(CASE WHEN event_type = 'CONTRACT_AWARDED' THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN event_type = 'RFP_CREATED' THEN 1 ELSE 0 END), 0), 2) AS conversion_rate_pct FROM events;"
```

**Pass criteria:**
- Duplicate query returns **zero rows**
- API `/event-counts` matches SQL group-by
- Funnel counts match filtered SQL
- `conversionRate` matches manual SQL (or 0 when RFP_CREATED = 0)

---

# SECTION 7 — Performance Smoke Tests

---

## TEST 7.1 — Insert 100 events

**Purpose:** Basic throughput smoke test; analytics still responds.

**Command:**
```powershell
1..100 | ForEach-Object {
  $id = [guid]::NewGuid().ToString()
  $agg = [guid]::NewGuid().ToString()
  $types = @("RFP_CREATED","VENDOR_INVITED","PROPOSAL_SUBMITTED","PROPOSAL_APPROVED","CONTRACT_AWARDED")
  $t = $types[$_ % 5]
  @"
{"event_id":"$id","event_type":"$t","aggregate_id":"$agg","user_id":"perf-user","payload":{"i":$_},"occurred_at":"2026-06-23T17:00:00Z"}
"@ | Set-Content -Encoding utf8 qa-payloads\perf-$_.json
  curl.exe -s -o NUL -X POST "$BaseUrl/api/v1/events" -H "Content-Type: application/json" -d "@qa-payloads/perf-$_.json"
}
Start-Sleep -Seconds 30
Measure-Command { curl.exe -s "$BaseUrl/api/v1/analytics/event-counts" | Out-Null }
Measure-Command { curl.exe -s "$BaseUrl/api/v1/analytics/funnel" | Out-Null }
psql -U $PgUser -d $PgDb -c "SELECT COUNT(*) FROM events;"
```

**Expected response:** Analytics endpoints return 200 in reasonable time (< 2s locally for 100–500 rows).

**Expected DB state:** Row count increased by 100 (minus any duplicate event_ids if re-run).

**Expected Kafka behavior:** Consumer lag spikes then clears.

**Expected logs:** Many `PERSISTED` lines; no unhandled exception storms.

**Failure indicates:** Consumer too slow, DB pool exhaustion, or analytics timeout.

---

## TEST 7.2 — Insert 1000 events (optional stress)

**Purpose:** Resume-grade smoke test at higher volume.

**Command:** Same as 7.1 but `1..1000` and `Start-Sleep -Seconds 120` before analytics.

**Expected response:** Analytics still 200; latency noted in your checklist.

**Expected DB state:** +1000 rows (unique event_ids).

**Expected Kafka behavior:** Temporary lag acceptable; must eventually drain.

**Expected logs:** No OOM; no repeated DLQ storms.

**Failure indicates:** Not ready to claim high-throughput without optimization (document honestly on resume).

---

# SECTION 8 — Production Readiness Checklist

Mark each item after executing tests above.

| # | Feature | Pass | Fail | Notes |
|---|---------|:----:|:----:|-------|
| 1 | PostgreSQL connectivity | [ ] | [ ] | TEST 0.1 |
| 2 | Kafka broker connectivity | [ ] | [ ] | TEST 0.2 |
| 3 | Topics `procurement-events` + DLQ exist | [ ] | [ ] | TEST 0.3 |
| 4 | Spring Boot starts cleanly | [ ] | [ ] | TEST 0.4 |
| 5 | Actuator health UP | [ ] | [ ] | TEST 0.5 |
| 6 | Happy path ingest (202 → DB row) | [ ] | [ ] | TEST 1.1 |
| 7 | Validation rejects missing required fields | [ ] | [ ] | TEST 1.2–1.7 |
| 8 | Empty payload `{}` accepted | [ ] | [ ] | TEST 1.8 |
| 9 | Invalid UUID / enum / JSON rejected (API) | [ ] | [ ] | TEST 1.9–1.11 |
| 10 | Duplicate API submit → one DB row | [ ] | [ ] | TEST 2.1 |
| 11 | Duplicate Kafka message → one DB row | [ ] | [ ] | TEST 2.2 |
| 12 | Concurrent duplicate race → one DB row | [ ] | [ ] | TEST 2.3 |
| 13 | `/analytics/event-counts` matches SQL | [ ] | [ ] | TEST 3.2 |
| 14 | `/analytics/funnel` stages + conversionRate | [ ] | [ ] | TEST 3.3 |
| 15 | Divide-by-zero → `conversionRate: 0.0` | [ ] | [ ] | TEST 3.4 |
| 16 | Invalid JSON (Kafka) → DLQ | [ ] | [ ] | TEST 4.1 |
| 17 | Unknown enum (Kafka) → DLQ | [ ] | [ ] | TEST 4.2 |
| 18 | DB failure → DLQ + log | [ ] | [ ] | TEST 4.3 |
| 19 | Duplicates do NOT go to DLQ | [ ] | [ ] | TEST 4.5 |
| 20 | Data survives app restart | [ ] | [ ] | TEST 5.1 |
| 21 | System recovers after Kafka restart | [ ] | [ ] | TEST 5.2 |
| 22 | System recovers after Postgres restart | [ ] | [ ] | TEST 5.3 |
| 23 | No duplicate `event_id` rows in DB | [ ] | [ ] | Section 6 SQL |
| 24 | 100-event smoke test passes | [ ] | [ ] | TEST 7.1 |
| 25 | 1000-event smoke test (optional) | [ ] | [ ] | TEST 7.2 |

---

## Known limitations to disclose on resume (honest QA findings)

Document these if asked in interviews — they show critical thinking:

1. **202 returned before Kafka ack confirmed** — ingest may report success while publish fails (TEST 5.2).
2. **Duplicate API calls publish duplicate Kafka messages** — dedup is consumer-side only.
3. **Funnel counts events, not distinct RFPs** — conversion rate is a volume metric, not strict funnel semantics.
4. **Transient DB errors go to DLQ** — no retry before DLQ.
5. **No authentication** on any endpoint.
6. **Future timestamps and large payloads accepted** — no business validation yet.

---

## Suggested test execution order (one session)

```
0.1 → 0.2 → 0.3 → 0.4 → 0.5 → 0.6
1.1 → 1.2–1.11 → 1.8 → 1.12–1.13
2.1 → 2.2 → 2.3
3.1 → 3.2 → 3.3 → 3.4
4.1 → 4.2 → 4.3 → 4.4 → 4.5
5.1 → 5.2 → 5.3
Section 6 SQL
7.1 → (7.2 optional)
Section 8 checklist
```

**Minimum resume-ready bar:** All Section 0, TEST 1.1, 2.1, 2.2, 3.2, 3.3, 4.1, 4.2, Section 6 duplicate query = 0 rows, and checklist items 1–6, 10–11, 13–14, 16–17, 23.

---

*ProcureFlow Analytics QA Guide — aligned with `POST /api/v1/events`, `GET /api/v1/analytics/event-counts`, `GET /api/v1/analytics/funnel`, topics `procurement-events` / `procurement-events-dlq`.*
