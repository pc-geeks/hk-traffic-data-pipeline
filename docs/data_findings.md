# What the real data looks like (and where it differs from the specification)

Source: Transport Department, *Traffic Data of Strategic / Major Roads* (data.gov.hk). Findings below come from
one real snapshot of each file downloaded on 2026-10-29 (kept in `tests/fixtures/real/`) and from the sizes of the
daily archive packages for 2026-10-28. They are checked by tests in `tests/test_parsing.py`.

## What the downloaded packages really look like (5-day download, 2026-10-26 to 10-30)

- The daily packages are named `<64-character hash>-<YYYYMMDD>.zip`, not "Raw Data ...": files must be identified by content
  (XML root tag / CSV header). A first version that relied on the file name found nothing.
- Inside, files sit in a folder named after the URL-encoded source URL, plus a `data-dictionary.pdf` that only contains a link.
- Member names carry the archive minute (`20261026-0004-rawSpeedVol-all.xml`). **The content lags the label**: the archive of
  day D contains data stamped 23:53 - 23:58 of day D-1, so the last minutes of a day live in the next day's package. Gold tables
  are therefore built on the observation date, reading one extra archive day.
- No duplicate (detector, lane, 30-second period) keys across 5 days; about 2,000 of the 2,880 possible 30-second periods
  per day are present.
- Raw files per day: 1,047 / 1,065 / 1,035 / 983 / 990 (68 - 74% of 1,440). Lane rows per file: about 4,125 on 26 - 28 October,
  3,830 on the 29th and 3,696 on the 30th.

## Archive packages (2026-10-28)

| Package | Files | Zip size |
|---|---|---|
| Traffic Speed, Volume and Road Occupancy (Raw Data) | 1,035 | 37.2 MB |
| Locations of Traffic Detectors (monthly, 2026-04) | 1 | 42.6 KB |

A full day would be 1,440 raw files (one per minute), so the archive contains about **72% of the raw files**.
Gaps are real and the pipeline treats them as a fact of life (`low_coverage` hours instead of silently averaging over nothing).

Volume estimate from the single sample file (an estimate, not a measurement of a full day): a raw file is ~690 KB of
XML with 4,140 lane rows, so a day is roughly 700 MB of XML and ~4.3 M lane rows (~130 M per month).

## Specification versus reality

| Item | Specification (2022) | Real file (2026) |
|---|---|---|
| Lane speed standard deviation | `sd` | `<s.d.>` |
| Direction | numeric code | text such as "South East", 12 values with trailing spaces |
| Detector CSV id column | `Device_ID` | `AID_ID_Number`, file starts with a UTF-8 BOM |
| Detector ids | `AIDxxxxx` | also `TDS90018`, `TDSCPRPS10003`, ... |
| Lane ids | Fast / Middle / Slow | also "Middle Lane 1" .. "Middle Lane 4" |

## Data-quality traps found in the raw sample (4,140 lane rows)

| Finding | Count | How the pipeline handles it |
|---|---|---|
| `valid = N` rows still carry plausible-looking numbers | 22 | flag `invalid_flag`, never used in metrics |
| Valid rows with speed above 150 km/h (max 237, single vehicle) | 13 | flag `out_of_range` |
| Valid rows with volume 0 but a speed value | 366 | flag `no_vehicles`: speed is meaningless without vehicles |
| Volume 0 with high occupancy (a car sitting on the detector, e.g. AID09101 slow lane at 21%) | 1 | flag `standstill_candidate`, counted as congestion |
| Detector reported in only one of the two 30-second periods | 1 (AID09402) | kept, coverage is computed from periods actually seen |
| Detectors in the location file that are silent in this snapshot | 43 of 807 | quality gate checks the share reporting |

Speeds are averaged **weighted by volume**: a plain mean lets one slow lane carrying one car count as much as a busy lane.

## Detector location file (807 detectors)

| Finding | Detail | How the pipeline handles it |
|---|---|---|
| `Road_EN` is not a road name but a location description: `<corridor> near <place> - <direction>` | 767 of 807 rows follow the pattern, 40 deviate (no "near", no dash, direction glued to the place, "(1)" suffixes) | `corridor`, `location` and `travel_direction` are parsed; rows without "near" inherit a corridor seen elsewhere (e.g. "Lung Cheung Road Beacon Heights" -> "Lung Cheung Road") |
| Result of the parsing | 81 corridors; largest: Tuen Mun Road (75 detectors), North Lantau Highway (45), Yuen Long Highway (36) | `dim_detector.corridor` |
| Travel direction | Eastbound 223, Westbound 215, Northbound 188, Southbound 172, unknown 9 | `travel_direction` (NULL when absent) |
| District spelled two ways | "Central & Western" (13) and "Central and Western" (1) | normalised to "Central and Western" (18 districts) |
| The three harbour crossings are not in this dataset | only "Western Harbour Centre" matches "harbour" | no harbour-tunnel comparison is attempted |
| Tunnels that are present | Lion Rock, Aberdeen, Tate's Cairn, Tseung Kwan O, Shing Mun, Tai Lam (via Tsing Long Highway) | usable in the tidal-flow page |
