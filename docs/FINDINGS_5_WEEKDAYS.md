# Findings from five real weekdays (Mon 2026-10-26 to Fri 2026-10-30)

Produced by running this pipeline on the real daily archives. Everything below was computed with SQL on the serving
layer (see `sql/analytics.sql` for the style of query). It is a **5-weekday sample**: no weekend, no public holiday,
no weather or incident records were used. Interpretations are marked as inference.

## What was processed

| | Raw detector data |
|---|---|
| Files | 5,120 (983 - 1,065 per day) |
| Rows | 20.4 million lane readings |
| Detector-hours in gold | 91,549 (779 detectors with data, out of 807 in the location file) |

The whole run (unzip, silver for all days, quality gate, gold) took about 13 minutes on a 1-core machine with 4 GB of RAM.
The quality gate passed for all five days.

## 1. The weekday rhythm

- Overnight the network runs at 78 - 80 km/h (volume-weighted). The slowest hour is **18:00 at 65.3 km/h**
  (3.68% of lane readings congested), then 08:00 at 67.9 km/h (3.63%).
- Using only hours with enough data, against a midday level of 73.1 km/h (11:00 - 14:59):
  morning peak (07:00 - 09:59) is **4.3% slower**, evening peak (17:00 - 19:59) is **6.4% slower**.
- The evening peak is the worse one: it is the peak hour for 18 of the 50 most congested detectors, against 15 for 07:00 - 09:59.
- Friday evening was the slowest of the five: 67.2 km/h and 3.09% congested, against 68.9 km/h and 1.78% on Monday.
  (One Friday is not a trend.)

## 2. Congestion is concentrated

- The 5 most congested corridors hold **31%** of all congested lane readings, the top 10 hold **51%** (80 corridors in total).
- 12 of 779 detectors are congested at least 10% of the time, 49 at least 5%, and 627 (80%) less than 1% of the time.
- Corridors with at least 8 detectors, ranked by share of congested readings: Lion Rock Tunnel Road 5.7% (64 km/h average),
  Kwun Tong Road 3.1% (54 km/h), Waterloo Road 2.9% (48 km/h), Ting Kok Road 2.5%, Princess Margaret Road 2.1%.
- **Kwun Tong Road at Kai Tai Court** is a persistent bottleneck: congested 48% of the time, 11 km/h average, worst at 08:00,
  observed on all five days.
- **TDS10012 on Lion Rock Tunnel Road (World-wide Gardens)** ranks first (55% congested, 11 km/h) but has only 3 reliable days
  and its worst hour is 13:00, which does not look like a rush-hour bottleneck. A neighbouring detector (TDS10013) stopped
  reporting on 29 - 30 October. This may be roadworks or a faulty detector: check Transport Department notices before quoting it.

## 3. Tidal flow exists on the main corridors (3 corridors had both directions with enough traffic)

| Corridor | Direction | Morning (07-09) vs midday | Evening (17-19) vs midday |
|---|---|---|---|
| Tuen Mun Road | Eastbound | **7.9% slower** | 1.0% slower |
| Tuen Mun Road | Westbound | 0.8% faster | **6.2% slower** |
| Yuen Long Highway | Eastbound | **12.0% slower** | 3.0% slower |
| Yuen Long Highway | Westbound | 4.7% slower | **7.9% slower** |
| North Lantau Highway | Eastbound | 3.3% faster | 6.4% slower |
| North Lantau Highway | Westbound | 1.8% faster | 0.3% faster |

On Tuen Mun Road one direction jams in the morning and the opposite direction in the evening. Inference: commuters heading
towards the urban area in the morning and back in the evening. North Lantau Highway runs close to free flow (90 - 99 km/h).
The pattern is shown for these three corridors only: other corridors have detectors in one direction only, or too little volume.

## 4. Anomalies

Rule: an hour is flagged when its speed is at least 3 standard deviations and at least 25% below the usual speed of the same
detector in the same hour on the other four days. 14 to 82 detector-hours per day were flagged (0.11 - 0.62% of the 13,330
comparable detector-hours per day).

- The strongest event: **Island Eastern Corridor at the ICAC Headquarters, Friday 30 October**, 8.0 km/h at 18:00 (usual 75.9,
  89% below) and 10.9 km/h at 19:00 (usual 86.8, 87% below). On the same evening 4 detectors on the corridor were flagged at
  18:00 and 5 at 19:00. Several detectors flagged together on one corridor in the same hour points to a real event rather than a
  faulty detector (inference, **not checked against news or incident reports**).
- Other clusters of 4 detectors in one hour: West Kowloon Highway (30 Oct, 17:00 and 18:00), Lung Cheung Road (28 Oct, 17:00),
  Shing Mun Tunnel Road (28 Oct, 18:00), Kwun Tong Bypass (29 Oct, 19:00).
- Friday 30 October has by far the most flags (82). The baseline mixes Monday to Friday and Friday evenings looked slower in
  general (section 1), so Friday is probably over-flagged. A per-weekday baseline needs several weeks of data.

## 5. Data trust (what I could not rely on)

- **The archive is incomplete**: 68 - 74% of the 1,440 raw files per day exist.
- **Gaps are not uniform**: 13.6% of detector-hours at 13:00, 9.4% at 12:00 and 9.3% at 23:00 have too little data, against about 5%
  in most hours. The worst day-hours were 29 Oct 17:00 (44% coverage) and 30 Oct 13:00 (45%).
- **Quality dropped on 29 - 30 October**: lane rows per file fell from about 4,125 to 3,830 and 3,696, and invalid raw rows rose
  from 0.21% (28 Oct) to 0.83% (29 Oct) and 1.21% (30 Oct). The increase is spread widely: the 30 detectors with the biggest
  increase explain only about a third (35%) of it.
- **Six detectors went silent on 29 - 30 October**: TDS91050, TDS91055, TDS91042, TDS90057 (Yuen Long Highway), TDS10013
  (Lion Rock Tunnel Road), AID05206 (Tsuen Wan Road).
- **One detector is essentially broken**: TDS90018 (Tuen Mun Road) has 68% of its rows flagged bad. Two more jumped on 29 - 30 October:
  TDSIEC20001 (Wan Chai) from about 10 to 665 bad rows a day and TDSTCKR10003 (Tai Chung Kiu Road) from 24 to 590.
- 28 of the 807 detectors in the location file delivered no data at all in the five days.
- The source data itself contains a typo: the corridor "Gloucestor Road" (should be Gloucester).

## Limitations

Five weekdays only (no weekend, holiday, typhoon or rain analysis); thresholds are heuristics (20 km/h, 15% occupancy,
z <= -3, 25% drop, 50% coverage); the anomaly baseline does not model the day of the week; no finding has been checked
against news, incident logs or weather records; hotspot ranking over five days may be dominated by roadworks or faulty detectors.
