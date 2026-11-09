# Checklist before publishing

Built offline (no access to data.gov.hk). Since then the pipeline HAS been run on five real weekdays
(2026-10-26 to 10-30) supplied as zip packages: that found and fixed opaque zip names, the midnight boundary of the archive
and a missing column in the anomaly table. What is still unverified is listed below.

## 1. Re-run it on your own machine (most important)

(The 5-day run was done in a 1-core sandbox. Re-run here so the numbers in the repo come from this machine.)

- [ ] Download both daily zips for one day from the dataset page ("Download Historical Data") into `data/landing/`.
- [ ] Open a zip: how are the XML files named? Does each contain exactly one snapshot (one `<date>`, two periods)?
      The pipeline reads the date/time from INSIDE each file, so file names do not matter, but confirm there are no
      sub-folders or other formats.
- [ ] Download the dimension zips (`Locations of Traffic Detectors`, `Road Network Segments`) and run
      `hktraffic dims --detectors <zip or csv>`.
- [ ] `hktraffic run-day --date YYYY-MM-DD`. Compare "files" in the output with the zip listing.
- [ ] Look at the flags: are `invalid_flag` / `out_of_range` shares plausible over a whole day? Are there hours
      with very few files (the archive has ~28% missing minutes: where are the gaps)?

## 2. Check the analytics make sense

- [ ] After at least 5 weekdays, run `gold` for them and look at `hotspots`: do the top detectors sit on roads you
      recognise as congested (Cross-Harbour Tunnel approaches, Tuen Mun Road, ...)? Spot-check 3 against the road name.
- [ ] Look at the anomalies for a day with a known event (a typhoon signal, a big accident reported in the news).
      Public holidays are NOT modelled: a holiday will look anomalous against a weekday baseline.
- [ ] The thresholds (20 km/h, 15% occupancy, z <= -3, coverage 50%) are starting heuristics, not standards.
      Change them in `config.py` and write down what you chose and why.

## 3. Spark versus single process

- [ ] `hktraffic benchmark --date YYYY-MM-DD` on your own machine, with the full day. Record the numbers (files,
      MB, cores, seconds). On a machine with few cores Spark may be slower: that is a legitimate finding.
      Fill the results table in the README with YOUR numbers; do not keep placeholders.

## 3b. Power BI

- [ ] Build the report with powerbi/BUILD_GUIDE.md. The DAX and theme were NOT run in Power BI Desktop: fix names/types where needed.
- [ ] Cross-check three numbers against SQL (guide section 5).
- [ ] Replace every placeholder page title with a real, measured sentence.
- [ ] Add screenshots to docs/images and to the README, fill in the "Findings" section, delete the HTML comment.
- [ ] Do not use "Publish to web" unless the data terms allow public redistribution.

## 4. Infrastructure

- [ ] `docker compose up -d --build`: the Airflow image (with Java and PySpark) was not built in the dev sandbox.
- [ ] The DAG waits for zips in `data/landing/`. If you automate the download, respect the API: a day is ~1,000 requests.

## 5. Legal and hygiene

- [ ] Read the data.gov.hk terms for this dataset and keep the attribution in the README.
- [ ] The files in `tests/fixtures/real/` are real open data (about 1.3 MB). Keep attribution next to them.
- [ ] Push to GitHub and confirm CI passes.
