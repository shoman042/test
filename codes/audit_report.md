# Phase 1 audit

```
[FAIL] P1-9  README.md missing (how to run each mode, config keys, outputs, limits)
[PASS] P1-1  Git repo found
[PASS] P1-1  .env is listed in .gitignore
[PASS] P1-3  config.yaml: has seed, A, four modes, anomaly/fault/offline settings
[PASS] P1-5  Generator scripts: ['generate_reference_data.py', 'generate_stream.py']
[PASS] P1-8  Wheel found: fleet_telematics_shared-1.0.0-py3-none-any.whl
[PASS] P1-2  Day-1 findings note found
[PASS] P1-4  vehicles_initial.csv: columns match section 3 (20 rows)
[PASS] P1-4  drivers.csv: columns match section 3 (13 rows)
[PASS] P1-4  routes.csv: columns match section 3 (3 rows)
[PASS] P1-4  route_checkpoints.csv: columns match section 3 (59 rows)
[PASS] P1-4  cargo_types.csv: columns match section 3 (4 rows)
[PASS] P1-4  trips.csv: columns match section 3 (21 rows)
[PASS] P1-4  fuel_prices.csv: columns match section 3 (2 rows)
[PASS] P1-4  business_rules.csv: columns match section 3 (32 rows)
[PASS] P1-4  business_rules.csv covers all required rules (32 rules)
[PASS] P1-4  All 3 routes have 15-25 checkpoints
[PASS] P1-7  1 change file(s) found
[PASS] P1-7  V002 retrofit present
[PASS] P1-7  V007 registration correction present
[PASS] P1-7  V010 device swap present
[PASS] P1-4  trips: all ids exist in reference tables
[PASS] P1-4  Every vehicle has at least one trip
[PASS] P1-4  No overlapping trips per vehicle
[PASS] P1-4  No overlapping trips per driver
[PASS] P1-4  planned_end_time = start + expected duration for all trips
[PASS] P1-4  Cold cargo only on refrigeration-capable vehicle versions
[PASS] P1-6  run_manifest.json has all required keys
[PASS] P1-6  injected_events_log.csv has all required columns
[INFO] -     Project root: D:\SIC\Materials\GraduationProject\phase1
[INFO] P1-4  Reference folder used: D:\SIC\Materials\GraduationProject\phase1\reference
FAIL=1  WARN=0  PASS=28
```
