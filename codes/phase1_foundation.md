# Phase 1 — Foundation (Fleet Telematics Lakehouse)

Project: Fleet Telematics Lakehouse on Microsoft Fabric | SIC 8 / 2026 | Big Data Engineering, Project 3
Phase: Foundation and Data Generator (Foundation part) — **CLOSED**

---

## 1. Goal of the phase

Prepare the Fabric environment and verify, before building anything on top of it, how the Eventstream behaves (Day-1 checks from section 13 of the project guide), so the rest of the pipeline is built on confirmed facts.

---

## 2. What was set up

### 2.1 Fabric workspace
- Workspace: `SIC8-Fleet-Telematics`
- Trial capacity: 31 days left (as of 2026-10-23). The whole plan must finish before it ends.

### 2.2 Lakehouse `lh_fleet`

| Item | Content |
| :--- | :--- |
| Tables | schemas `dbo` and `bronze` |
| Files/reference | landing folder for reference CSVs (vehicles, drivers, routes, ...) |
| Files/landing/offline | landing folder for offline JSONL bulk files |
| Files/evidence | folder for run evidence (counts, logs, screenshots) |

A SQL analytics endpoint was created automatically with the Lakehouse.

### 2.3 Eventstream `es_fleet_telemetry`

- Sources (5 custom endpoints, Kafka protocol): `gps`, `engine`, `fuel`, `temperature`, `device_status`
- Stream: `es_fleet_telemetry-stream`
- Operators: none
- Destination used for the test: `to_bronze` (Lakehouse, JSON) writing to `bronze.telemetry_raw`

### 2.4 Test scripts (Python, confluent-kafka)
- `test_all.py`: sends one test message to each of the 5 endpoints and prints OK/FAIL.
- `test_malformed.py`: sends a truncated JSON message (`event_id = BAD-TEST-001`) to check how the destination treats malformed input.
- Connection strings / keys are kept in `.env` (must be listed in `.gitignore`, never committed).

---

## 3. Day-1 tests and findings

### 3.1 Lakehouse destination test (`to_bronze`, JSON)

| Check | Result |
| :--- | :--- |
| Can one Eventstream carry all signals? | YES |
| Are JSON fields split into columns? | YES |
| Are the 5 signals separable downstream? | YES (via `signal_type`) |
| Schema behaviour | Fixed from the first message (gps). The table had 16 columns and 25 rows. |
| Measurements of engine / fuel / temperature / device_status | LOST (rows exist but their own fields such as `engine_rpm`, `fuel_level_liters`, `temperature_value` are not in the table; gps columns are NULL) |
| Raw text column (`raw_value`) | NO |
| `kafka_offset` | NO (only `PartitionId`) |
| Arrival timestamp | `EventEnqueuedUtcTime` (closest equivalent) |
| Extra columns added by the platform | `EventProcessedUtcTime`, `PartitionId`, `EventEnqueuedUtcTime` |

### 3.2 Malformed message test
- Sent a truncated JSON (`event_id = BAD-TEST-001`).
- Query used:
  ```sql
  SELECT COUNT(*) AS total,
         SUM(CASE WHEN event_id = 'BAD-TEST-001' THEN 1 ELSE 0 END) AS bad_found
  FROM bronze.telemetry_raw;
  ```
- Result: `total = 25`, `bad_found = 0`. No row was added.
- Conclusion: the destination silently drops malformed messages. The original text is not kept anywhere we can recover it.
- Note: to prove delivery to the Eventstream (not just "queued"), the producer uses a delivery callback that prints `DELIVERED offset: ...`.

---

## 4. Decision

The `to_bronze` destination **cannot be used as the real Bronze layer** because it:
1. fixes the schema from the first message and loses the other signals' fields,
2. drops malformed messages with no trace,
3. has no raw text and no stream offsets.

**Decision:** write a **Spark raw-sink job** (Structured Streaming from the Eventstream Kafka endpoint) into `bronze.telemetry_raw` with this schema:

| Column | Purpose |
| :--- | :--- |
| `raw_value` | exact text received |
| `source_stream` | gps / engine / fuel / temperature / device_status |
| `source_type` | STREAM or OFFLINE_FILE |
| `kafka_partition`, `kafka_offset` | position in the stream |
| `source_file`, `file_line` | origin of file rows |
| `arrival_time`, `ingestion_time`, `ingestion_date` | timing and partition |

The `to_bronze` destination was only an experiment.

---

## 5. Day-1 checks status (from guide section 13)

| Check | Status |
| :--- | :--- |
| One Eventstream with several endpoints keeps signals separable | Confirmed |
| Raw sink keeps exact raw message, offsets, arrival timestamp | Confirmed NOT possible with the Lakehouse destination, so a Spark raw-sink job is required |
| Throughput (max msgs/sec) | Not tested yet (needs the generator) |
| foreachBatch silence check + MERGE into silver_alerts | Not tested yet |
| Power BI refresh / Direct Lake lag | Not tested yet |
| Airflow Job / Great Expectations availability | Not tested yet |

---

## 6. Phase-closing checklist

| # | Task | Done |
| :--- | :--- | :--- |
| 1 | Run modified `test_malformed.py` and record `DELIVERED` | ☑ |
| 2 | Deactivate `to_bronze` (toggle in the Eventstream). Destination was removed entirely; the Eventstream now has no destination | ☑ |
| 3 | Delete or rename the test table `bronze.telemetry_raw` (to `bronze.telemetry_raw_test` if kept as evidence) | ☑ |
| 4 | Regenerate the gps key, update `.env`, rerun `test_all.py` (5 x OK) | ☑ |
| 5 | Confirm `.env` is in `.gitignore` | ☑ |
| 6 | Save this documentation | ☑ |
| 7 | Close open notebooks/queries to free Spark capacity | ☑ |

---

## 7. Open items and risks

- **Spark capacity on trial:** `HTTP 430 TooManyRequestsForCapacity` was observed. The streaming raw-sink job holds a session permanently, so close idle notebooks. Fallback if it repeats: write a raw copy from the generator into Lakehouse Files.
- **Trial time:** about 31 days left, the plan must finish before that.
- **Raw-sink job:** cannot be tested properly until the generator sends real data.

---

## 8. Next step

Generator step 1: `config.yaml`, `config.py`, `rng.py`, and a reproducibility test (same seed gives the same output).
