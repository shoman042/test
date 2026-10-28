# Fleet Telematics Batch Orchestration in Microsoft Fabric

**Workspace:** SIC8-Fleet-Telematics
**Pipeline:** `pl_fleet_batch` (Data Factory data pipeline)
**Implementation date:** October 28, 2026

---

## 1. Goal

Automate the batch steps in order (Bronze → Silver → Alerts → Gold) instead of running the notebooks by hand, with an automatic schedule that runs twice a day.

**Decision:** We chose a **Data Pipeline** (Data Factory) over an Apache Airflow Job. Both can run notebooks, but the pipeline is simpler, built into Fabric, and supports retry, timeout, schedule and If Condition without writing DAG code.

---

## 2. Workspace Components

| Item | Type | Role |
|---|---|---|
| `eh_fleet` | Eventhouse + KQL Database | Receives the streaming data (`bronze_*_raw` tables and the `alert_events` table) |
| `es_gps` / `es_engine` / `es_fuel` / `es_temperature` / `es_device_status` | Eventstream | Continuous streaming into KQL. **Not part of the pipeline** |
| `lh_fleet` | Lakehouse | Storage (schemas: `bronze`, `silver`, `gold`, `ref`, `ops`) |
| `env_fleet` | Environment | Holds the custom `fleet_shared` library |
| `nb_p2_bronze_loader` | Notebook | KQL → Bronze |
| `nb_silver_setup` | Notebook | Prepares reference data and watermarks |
| `nb_p2_batch_job_1` / `nb_p2_batch_job_1_clean` | Notebook | Bronze → Silver + Gate 1 |
| `nb_p4_fact_alerts_load` | Notebook | KQL alerts → `gold.fact_alerts` |
| `nb_p3_batch_job_2` | Notebook | Silver → Gold (dimensions, facts, dashboard tables) |

---

## 3. Execution Order

```
bronze_load → silver_setup → Wait1 (120 s) → batch_job_1 → If Condition1 (Gate 1) → Wait1_copy1 (120 s) → alerts_load → batch_job_2
```

| Activity | Notebook | Timeout | Retry |
|---|---|---|---|
| `bronze_load` | `nb_p2_bronze_loader` | 0:30:00 (1:00:00 recommended) | 1 |
| `silver_setup` | `nb_silver_setup` | 0:15:00 | 1 |
| `batch_job_1` | `nb_p2_batch_job_1_clean` | 1:00:00 (1:30:00 recommended) | 1 |
| `alerts_load` | `nb_p4_fact_alerts_load` | 0:30:00 | 1 |
| `batch_job_2` | `nb_p3_batch_job_2` | 1:00:00 | 1 |

- All activities are connected with **On success**.
- Retry interval: Fixed, 60 seconds. (Consider raising Retry to 2 or using Increasing Delay if the capacity problem continues.)
- No notebook takes parameters.
- All retries are safe (idempotent):
  - Bronze has a retry guard that deletes the run's window before rewriting it.
  - `batch_job_1` advances the watermark as its last step.
  - `alerts_load` uses MERGE.
  - Gold uses overwrite.

### Wait activities (Spark capacity workaround)

Two **Wait** activities of **120 seconds** each were added:

| Activity | Position | Purpose |
|---|---|---|
| `Wait1` | Between `silver_setup` and `batch_job_1` | Gives Spark capacity time to free up before the next session starts |
| `Wait1_copy1` | Between `If Condition1` and `alerts_load` | Same purpose |

- Before adding them, `batch_job_1` and `alerts_load` failed on their first attempt (after ~20 s) and only succeeded on retry. With the Waits, the run completed with **no failed attempts**.
- A Wait activity does not use Spark, so it does not compete for capacity.
- Cost: about 4 extra minutes per run, which is small next to the retries it replaces.
- Suggestion: rename `Wait1` to `wait_before_batch_job_1` and `Wait1_copy1` to `wait_before_alerts_load` for clarity.

### Gate 1 (If Condition1)

After `batch_job_1`, an If Condition reads the notebook's exit value:

```
@equals(json(activity('batch_job_1').output.result.exitValue).status, 'PASS')
```

- **True:** empty. The pipeline continues to `alerts_load`.
- **False:** a Fail activity with the message:
  ```
  @concat('Gate 1 failed: ', activity('batch_job_1').output.result.exitValue)
  ```
  and an error code such as `GATE1_FAILED`.

Reason: the notebook itself does not fail when Gate 1 returns FAIL (Silver is still written), so the If Condition is what stops Gold.

---

## 4. Notebook Details

### `nb_p2_bronze_loader`
- Reads the KQL (`eh_fleet`) tables `bronze_{signal}_raw` for 5 signals (gps, engine, fuel, temperature, device_status) through the Kusto Spark connector, using a token from `mssparkutils.credentials.getToken`.
- Incremental: based on `ingestion_time()` and the watermark table `ops.ops_bronze_load_watermark`. The upper bound is the current time minus 2 minutes (`SAFETY_LAG_MIN`).
- Writes to `bronze.bronze_{signal}_raw` (Delta, partitioned by `ingestion_date`).
- First load: gps 30,895 / engine 30,926 / fuel 5,176 / temperature 7,197 / device_status 0.

### `nb_silver_setup`
- Prepares the schemas and the `ref.*` tables from the files under `Files/reference` (overwritten on every run), and initializes `ops.ops_watermark` if it is empty.
- Safe to rerun. If the reference files are static, it can be removed from the schedule after the first run.

### `nb_p2_batch_job_1_clean`
- Bronze → Silver for gps, engine, fuel and temperature.
- Performs validation, DLQ handling, parsing, deduplication, DQ flags (`dq_flags` as STRING), MERGE and logging.
- Computes **Gate 1** and returns it as JSON through `mssparkutils.notebook.exit`.
- Depends on:
  - The `fleet_shared` library from `env_fleet`.
  - `Files/evidence/run_manifest.json` and `Files/evidence/run_summary.json` in `lh_fleet`.

### `nb_p4_fact_alerts_load`
- Reads the KQL `alert_events` table and takes the latest state per `alert_key` (`arg_max`).
- Joins with `ref.ref_trips` (route, cargo type) and with `gold.dim_vehicle` when it exists.
- MERGE into `gold.fact_alerts`: rows with `source_system = 'KQL'` are updated, and rows written by batch jobs are never touched.
- First load: 6 alerts (COLD_CHAIN_BREACH 1, EXCESSIVE_IDLING 2, ROUTE_DEVIATION 3).

### `nb_p3_batch_job_2` (Gold)
- Builds the dimensions, the facts (`fact_trip_performance`, `fact_cold_chain`, `dim_vehicle`, ...) and the dashboard tables.
- Contains **asserts** that fail the notebook (and therefore the pipeline) when the data is wrong. We kept them on purpose as a quality gate.

---

## 5. Problems Found and Fixed

| # | Notebook | Problem | Fix |
|---|---|---|---|
| 1 | Original `nb_p2_batch_job_1` | A development notebook with dangerous cells: Cell 51 wipes all Silver tables, the DLQ and the log and resets the watermarks; Cell 9 drops `silver_temperature` and recreates it with `dq_flags ARRAY<STRING>`; Cell 48 raises a `NameError`; Cell 39 runs gps on its own | Created a clean copy, `nb_p2_batch_job_1_clean`, with only 14 code cells (identical character for character to the original). Removed exploration, DROP/DELETE cells and tests |
| 2 | `nb_p3_batch_job_2` | `CREATE TABLE` for `gold.fact_trip_performance` and `gold.fact_cold_chain` had no `IF NOT EXISTS`, so the second run would fail | Added `IF NOT EXISTS` |
| 3 | `nb_p2_bronze_loader` | `session.stop()` in Cell 3, then another `stop()` in Cell 4 on a dead session | Deleted Cell 4 |
| 4 | All notebooks | `session.stop()` at the end | Preferably replace with `mssparkutils.notebook.exit("done")` |
| 5 | The pipeline | `alerts_load` and `batch_job_2` existed twice (inside the True branch and outside it) | Deleted the ones inside True and left that branch empty |

---

## 6. First Successful Run

- **Pipeline status:** Succeeded.
- **Total duration:** about 16 minutes (7:22 PM to 7:38 PM).

| Activity | Duration |
|---|---|
| bronze_load | 2m 55s |
| silver_setup | 1m 21s |
| batch_job_1 | 1m 49s (after a failed attempt of 19s) |
| If Condition1 | under 1s |
| alerts_load | 1m 20s (after a failed attempt of 20s) |
| batch_job_2 | 3m 39s |

**Note:** `batch_job_1` and `alerts_load` failed on the first attempt after about 20 seconds and succeeded on retry. This looks like a Spark capacity shortage at session start rather than a code problem (confirm by checking the Output message of the failed attempt).

### Second run (with the Wait activities)

- **Pipeline status:** Succeeded, with no failed attempts and no retries.
- **Total duration:** about 14 minutes (9:22 PM to about 9:36 PM).

| Activity | Duration |
|---|---|
| bronze_load | 1m 39s |
| silver_setup | 1m 6s |
| Wait1 | 2m 1s |
| batch_job_1 | 1m 38s |
| If Condition1 | under 1s |
| Wait1_copy1 | 2m 1s |
| alerts_load | 1m 16s |
| batch_job_2 | 3m 55s |

This second successful run also confirms that rerunning the pipeline is safe (the idempotency check we planned).

---

## 7. Schedule

| Setting | Value |
|---|---|
| Schedule type | Fixed |
| Repeat | Daily |
| Times | 12:00 PM and 12:00 AM (twice a day) |
| Start | 10/28/2026 |
| End | 08/28/2027 |
| Time zone | (UTC+02:00) Cairo |
| Parameters | None |

- Add an email address under **Failure notifications** (Schedule tab of the pipeline).
- The 12-hour gap means runs will not overlap. (To our knowledge Fabric has no setting that prevents overlapping runs.)

---

## 8. Permissions

- The schedule runs under the identity of whoever created or last edited it. That identity needs:
  - **Contributor** on the workspace.
  - **Read** access to the KQL database `eh_fleet` (Bronze and Alerts use its token).
- Different owners on the notebooks do not matter as long as this identity has Contributor.

---

## 9. Known Notes and Limitations

1. **The Eventstreams must stay Running** at all times, because the pipeline only reads what has already been collected in KQL.
2. **`vehicle_key` in `gold.fact_alerts`:** `alerts_load` runs before `batch_job_2`, so on the first run `vehicle_key` is NULL. On later runs it is filled from the previous run's `dim_vehicle`. Alternative: move `alerts_load` after `batch_job_2` (there is no reverse dependency).
3. **`lateness_sec`** is tied to the `run_manifest.json` of a single run. If Bronze holds data from other runs the values are inaccurate, and the notebook prints MISMATCH without stopping.
4. **Spark capacity:** open interactive sessions consume the same capacity. Close sessions you do not need before scheduled runs. The two 120 s Wait activities are a workaround for this; if failures come back, increase the Wait time or Retry count.
5. **`alerts_load`** reads the whole `alert_events` table every time (no watermark). Acceptable at the current volume.
6. **Gate 1:** on FAIL, Silver has already been written, and the pipeline stops before `alerts_load` and `batch_job_2`.

---

## 10. Quick Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| An activity fails after about 20s and succeeds on retry | Spark capacity | Increase the Wait activities (for example to 180 s), increase Retry, use Increasing Delay, and close open sessions |
| `batch_job_1` fails in the manifest cell | `run_manifest.json` or `run_summary.json` is missing | Check `Files/evidence` in `lh_fleet` |
| `batch_job_2` fails with "table already exists" | `IF NOT EXISTS` missing in Cell 3 | Add it |
| The If Condition fails or the Fail activity runs | Gate 1 returned FAIL | Open the `batch_job_1` output and read the exitValue |
| The If Condition expression errors | The `output.result.exitValue` path or the activity name is wrong | Check the Output in Monitor and confirm the name is `batch_job_1` |
| Bronze is slow or exceeds its timeout | 12 hours of data per run | Increase the timeout |

---

## 11. Follow-up Checklist

- [ ] Confirm the first scheduled run started on time.
- [ ] Review the duration of Bronze and `batch_job_1` and adjust the timeouts if needed.
- [ ] Add the Failure notifications email.
- [ ] Replace `session.stop()` with `notebook.exit` in the remaining notebooks.
- [ ] Check the Spark capacity message in the Output of the failed attempts (from the first run, before the Waits were added).
- [ ] Watch the first few scheduled runs to confirm the 120 s Waits are enough, since one clean run is not proof.
- [ ] Make a final decision on the order of `alerts_load` and `batch_job_2` (the `vehicle_key` issue).
