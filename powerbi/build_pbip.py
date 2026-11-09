"""Generate the HK Traffic Power BI project (PBIP) from the exported CSVs.

Open powerbi/HKTraffic.pbip in Power BI Desktop. The model parameter DataPath
points at powerbi/bi_data.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DATA_PATH = "G:/MyGithub/self-learn-autumn/hk-traffic/hk-traffic-pipeline/hk-traffic-pipeline/powerbi/bi_data"
PBIP = ROOT / "HKTraffic.pbip"
REPORT = ROOT / "HKTraffic.Report"
MODEL = ROOT / "HKTraffic.SemanticModel"

VISUAL_SCHEMA = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.9.0/schema.json"
PAGE_SCHEMA = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/2.1.0/schema.json"

M = "_Measures"


def hid(text: str, n: int = 20) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()[:n]


def page_id(text: str) -> str:
    return "ReportSection" + hid(text, 24)


def filter_id(text: str) -> str:
    return "Filter" + hid(text, 24)


def guid() -> str:
    return str(uuid.uuid4())


def tmdl_name(name: str) -> str:
    if name.replace("_", "").isalnum() and not name[0].isdigit():
        return name
    return "'" + name.replace("'", "''") + "'"


def lit(value: str) -> dict:
    return {"expr": {"Literal": {"Value": value}}}


def solid(color: str) -> dict:
    return {"solid": {"color": lit(f"'{color}'")}}


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def col_proj(table: str, column: str, display: str | None = None) -> dict:
    item = {
        "field": {
            "Column": {
                "Expression": {"SourceRef": {"Entity": table}},
                "Property": column,
            }
        },
        "queryRef": f"{table}.{column}",
        "nativeQueryRef": display or column,
    }
    if display:
        item["displayName"] = display
    return item


def measure_proj(measure: str, display: str | None = None) -> dict:
    item = {
        "field": {
            "Measure": {
                "Expression": {"SourceRef": {"Entity": M}},
                "Property": measure,
            }
        },
        "queryRef": f"{M}.{measure}",
        "nativeQueryRef": display or measure,
    }
    if display:
        item["displayName"] = display
    return item


def agg_proj(table: str, column: str, function: int, display: str) -> dict:
    names = {0: "Sum", 1: "Avg", 2: "Min", 3: "Max", 5: "CountNonNull"}
    name = names[function]
    return {
        "field": {
            "Aggregation": {
                "Expression": {
                    "Column": {
                        "Expression": {"SourceRef": {"Entity": table}},
                        "Property": column,
                    }
                },
                "Function": function,
            }
        },
        "queryRef": f"{name}({table}.{column})",
        "nativeQueryRef": display,
        "displayName": display,
    }


def projections(role_map: dict) -> dict:
    return {role: {"projections": items} for role, items in role_map.items()}


def chrome(title: str, subtitle: str | None = None) -> dict:
    container = {
        "title": [
            {
                "properties": {
                    "show": lit("true"),
                    "text": lit("'" + title.replace("'", "''") + "'"),
                    "fontSize": lit("12D"),
                    "bold": lit("true"),
                }
            }
        ],
        "background": [
            {
                "properties": {
                    "show": lit("true"),
                    "color": solid("#FFFFFF"),
                    "transparency": lit("0D"),
                }
            }
        ],
        "border": [
            {
                "properties": {
                    "show": lit("true"),
                    "color": solid("#E5E7EB"),
                    "radius": lit("6D"),
                }
            }
        ],
        "visualHeader": [{"properties": {"show": lit("false")}}],
        "padding": [
            {
                "properties": {
                    "top": lit("4D"),
                    "bottom": lit("4D"),
                    "left": lit("6D"),
                    "right": lit("6D"),
                }
            }
        ],
    }
    if subtitle:
        container["subTitle"] = [
            {
                "properties": {
                    "show": lit("true"),
                    "text": lit("'" + subtitle.replace("'", "''") + "'"),
                    "fontSize": lit("9D"),
                }
            }
        ]
    return container


def visual(
    name: str,
    visual_type: str,
    x: float,
    y: float,
    w: float,
    h: float,
    z: int,
    tab: int,
    query_state: dict | None = None,
    objects: dict | None = None,
    container: dict | None = None,
    filters: list | None = None,
    sort: list | None = None,
    extra_visual: dict | None = None,
) -> dict:
    body: dict = {"visualType": visual_type, "drillFilterOtherVisuals": True}
    if query_state is not None:
        query: dict = {"queryState": projections(query_state)}
        if sort:
            query["sortDefinition"] = {"sort": sort, "isDefaultSort": True}
        body["query"] = query
    if objects:
        body["objects"] = objects
    if container:
        body["visualContainerObjects"] = container
    if extra_visual:
        body.update(extra_visual)
    payload = {
        "$schema": VISUAL_SCHEMA,
        "name": name,
        "position": {
            "x": x,
            "y": y,
            "z": z,
            "height": h,
            "width": w,
            "tabOrder": tab,
        },
        "visual": body,
    }
    if filters:
        payload["filterConfig"] = {"filters": filters}
    return payload


def sort_by(table: str, column: str, direction: str = "Ascending", kind: str = "Column") -> dict:
    field: dict
    if kind == "Measure":
        field = {"Measure": {"Expression": {"SourceRef": {"Entity": table}}, "Property": column}}
    else:
        field = {"Column": {"Expression": {"SourceRef": {"Entity": table}}, "Property": column}}
    return {"field": field, "direction": direction}


def categorical_filter(name: str, table: str, column: str, literal: str) -> dict:
    return {
        "name": filter_id(name),
        "field": {
            "Column": {
                "Expression": {"SourceRef": {"Entity": table}},
                "Property": column,
            }
        },
        "type": "Categorical",
        "howCreated": "User",
        "filter": {
            "Version": 2,
            "From": [{"Name": "f", "Entity": table, "Type": 0}],
            "Where": [
                {
                    "Condition": {
                        "In": {
                            "Expressions": [
                                {
                                    "Column": {
                                        "Expression": {"SourceRef": {"Source": "f"}},
                                        "Property": column,
                                    }
                                }
                            ],
                            "Values": [[{"Literal": {"Value": literal}}]],
                        }
                    }
                }
            ],
        },
    }


def gradient(measure: str, stops: list[tuple[str, str]]) -> dict:
    keys = ["min", "mid", "max"]
    rule = {
        keys[i]: {
            "color": {"Literal": {"Value": f"'{color}'"}},
            "value": {"Literal": {"Value": value}},
        }
        for i, (color, value) in enumerate(stops)
    }
    return {
        "properties": {
            "backColor": {
                "solid": {
                    "color": {
                        "expr": {
                            "FillRule": {
                                "Input": {
                                    "Measure": {
                                        "Expression": {"SourceRef": {"Entity": M}},
                                        "Property": measure,
                                    }
                                },
                                "FillRule": {"linearGradient3": rule},
                            }
                        }
                    }
                }
            }
        },
        "selector": {"metadata": f"{M}.{measure}"},
    }


def card(name: str, measure: str, title: str, x, y, w, h, z, tab, color="#0F766E", plain=False) -> dict:
    return visual(
        name,
        "card",
        x,
        y,
        w,
        h,
        z,
        tab,
        {"Values": [measure_proj(measure)]},
        objects={
            "labels": [
                {
                    "properties": {
                        "fontSize": lit("14D" if plain else "22D"),
                        "color": solid(color),
                    }
                }
            ],
            "categoryLabels": [{"properties": {"show": lit("false")}}],
        },
        container={
            "title": [{"properties": {"show": lit("false")}}],
            "background": [{"properties": {"show": lit("false")}}],
            "border": [{"properties": {"show": lit("false")}}],
            "visualHeader": [{"properties": {"show": lit("false")}}],
        }
        if plain
        else chrome(title),
    )


def slicer(name, table, column, title, x, y, w, h, z, tab, mode="Dropdown", default_literal=None, single=False) -> dict:
    objects: dict = {
        "data": [{"properties": {"mode": lit(f"'{mode}'")}}],
        "header": [
            {
                "properties": {
                    "show": lit("true"),
                    "text": lit("'" + title.replace("'", "''") + "'"),
                    "fontSize": lit("10D"),
                }
            }
        ],
    }
    if single:
        objects["selection"] = [{"properties": {"singleSelect": lit("true")}}]
    if default_literal:
        objects["general"] = [
            {
                "properties": {
                    "filter": {
                        "filter": categorical_filter(name + "default", table, column, default_literal)["filter"]
                    }
                }
            }
        ]
    return visual(
        name,
        "slicer",
        x,
        y,
        w,
        h,
        z,
        tab,
        {"Values": [col_proj(table, column)]},
        objects=objects,
        container=chrome(title) | {"title": [{"properties": {"show": lit("false")}}]},
    )


def title_button(name: str, text: str, x, y, w, h, z, tab, size="16px") -> dict:
    paragraph = json.dumps(
        [
            {
                "textRuns": [
                    {
                        "value": text,
                        "textStyle": {
                            "fontFamily": "Segoe UI",
                            "fontSize": size,
                            "fontWeight": "bold",
                            "color": "#111827",
                        },
                    }
                ]
            }
        ],
        ensure_ascii=False,
    )
    return visual(
        name,
        "textbox",
        x,
        y,
        w,
        h,
        z,
        tab,
        objects={
            "general": [
                {
                    "properties": {
                        "paragraphs": lit("'" + paragraph.replace("'", "''") + "'")
                    }
                }
            ]
        },
        container={
            "background": [{"properties": {"show": lit("false")}}],
            "border": [{"properties": {"show": lit("false")}}],
            "visualHeader": [{"properties": {"show": lit("false")}}],
        },
    )


def page_navigator(name: str, x, y, w, h, z, tab) -> dict:
    return visual(
        name,
        "pageNavigator",
        x,
        y,
        w,
        h,
        z,
        tab,
        container={
            "background": [{"properties": {"show": lit("false")}}],
            "border": [{"properties": {"show": lit("false")}}],
            "visualHeader": [{"properties": {"show": lit("false")}}],
        },
    )


def page_shell(key: str, sentence: str) -> list[tuple[str, dict]]:
    return [
        (hid(key + "title"), title_button(hid(key + "title"), sentence, 12, 6, 1256, 36, 9000, 0)),
    ]


# -------------------------------------------------------------------------------------- model
TABLES: dict[str, list[dict]] = {
    "bi_detector_hour": [
        {"name": "detector_id", "type": "string"},
        {"name": "corridor", "type": "string"},
        {"name": "location", "type": "string"},
        {"name": "travel_direction", "type": "string"},
        {"name": "district", "type": "string"},
        {"name": "latitude", "type": "double", "format": "0.00000", "category": "Latitude"},
        {"name": "longitude", "type": "double", "format": "0.00000", "category": "Longitude"},
        {"name": "obs_date", "type": "dateTime", "format": "yyyy-MM-dd", "date": True},
        {"name": "hour", "type": "int64", "format": "0"},
        {"name": "iso_dow", "type": "int64", "format": "0"},
        {"name": "weekday", "type": "string", "sort": "iso_dow"},
        {"name": "day_type", "type": "string"},
        {"name": "wavg_speed", "type": "double", "format": "0.0"},
        {"name": "volume", "type": "int64", "format": "#,0", "sum": "sum"},
        {"name": "avg_occupancy", "type": "double", "format": "0.00"},
        {"name": "congested_share", "type": "double", "format": "0.00%"},
        {"name": "congested_rows", "type": "int64", "format": "#,0", "sum": "sum"},
        {"name": "usable_rows", "type": "int64", "format": "#,0", "sum": "sum"},
        {"name": "lane_rows", "type": "int64", "format": "#,0", "sum": "sum"},
        {"name": "bad_rows", "type": "int64", "format": "#,0", "sum": "sum"},
        {"name": "coverage", "type": "double", "format": "0.00%"},
        {"name": "low_coverage", "type": "int64", "format": "0"},
    ],
    "bi_corridor_hour": [
        {"name": "corridor", "type": "string"},
        {"name": "travel_direction", "type": "string"},
        {"name": "obs_date", "type": "dateTime", "format": "yyyy-MM-dd", "date": True},
        {"name": "hour", "type": "int64", "format": "0"},
        {"name": "iso_dow", "type": "int64", "format": "0"},
        {"name": "weekday", "type": "string", "sort": "iso_dow"},
        {"name": "day_type", "type": "string"},
        {"name": "detectors", "type": "int64", "format": "#,0", "sum": "sum"},
        {"name": "wavg_speed", "type": "double", "format": "0.0"},
        {"name": "volume", "type": "int64", "format": "#,0", "sum": "sum"},
        {"name": "congested_share", "type": "double", "format": "0.00%"},
    ],
    "bi_hotspot": [
        {"name": "rank", "type": "int64", "format": "0"},
        {"name": "detector_id", "type": "string"},
        {"name": "corridor", "type": "string"},
        {"name": "location", "type": "string"},
        {"name": "travel_direction", "type": "string"},
        {"name": "district", "type": "string"},
        {"name": "latitude", "type": "double", "format": "0.00000", "category": "Latitude"},
        {"name": "longitude", "type": "double", "format": "0.00000", "category": "Longitude"},
        {"name": "mean_congested_share", "type": "double", "format": "0.00%"},
        {"name": "mean_speed_kmh", "type": "double", "format": "0.0"},
        {"name": "days_observed", "type": "int64", "format": "0"},
        {"name": "hours_observed", "type": "int64", "format": "#,0"},
        {"name": "peak_hour", "type": "int64", "format": "0"},
        {"name": "peak_hour_share", "type": "double", "format": "0.00%"},
    ],
    "bi_anomaly": [
        {"name": "obs_date", "type": "dateTime", "format": "yyyy-MM-dd", "date": True},
        {"name": "hour", "type": "int64", "format": "0"},
        {"name": "detector_id", "type": "string"},
        {"name": "corridor", "type": "string"},
        {"name": "location", "type": "string"},
        {"name": "travel_direction", "type": "string"},
        {"name": "district", "type": "string"},
        {"name": "latitude", "type": "double", "format": "0.00000", "category": "Latitude"},
        {"name": "longitude", "type": "double", "format": "0.00000", "category": "Longitude"},
        {"name": "wavg_speed", "type": "double", "format": "0.0"},
        {"name": "baseline_mean", "type": "double", "format": "0.0"},
        {"name": "baseline_std", "type": "double", "format": "0.00"},
        {"name": "baseline_days", "type": "int64", "format": "0"},
        {"name": "drop_share", "type": "double", "format": "0.00%"},
        {"name": "z_score", "type": "double", "format": "0.00"},
        {"name": "is_anomaly", "type": "int64", "format": "0"},
        {"name": "volume", "type": "int64", "format": "#,0", "sum": "sum"},
    ],
    "bi_coverage_hour": [
        {"name": "obs_date", "type": "dateTime", "format": "yyyy-MM-dd", "date": True},
        {"name": "hour", "type": "int64", "format": "0"},
        {"name": "detectors_reporting", "type": "int64", "format": "#,0", "sum": "sum"},
        {"name": "avg_coverage", "type": "double", "format": "0.00%"},
        {"name": "low_coverage_share", "type": "double", "format": "0.00%"},
    ],
    "bi_detector_quality": [
        {"name": "detector_id", "type": "string"},
        {"name": "corridor", "type": "string"},
        {"name": "location", "type": "string"},
        {"name": "district", "type": "string"},
        {"name": "latitude", "type": "double", "format": "0.00000", "category": "Latitude"},
        {"name": "longitude", "type": "double", "format": "0.00000", "category": "Longitude"},
        {"name": "lane_rows", "type": "int64", "format": "#,0", "sum": "sum"},
        {"name": "bad_rows", "type": "int64", "format": "#,0", "sum": "sum"},
        {"name": "bad_share", "type": "double", "format": "0.00%"},
    ],
    "bi_pipeline_run": [
        {"name": "day", "type": "dateTime", "format": "yyyy-MM-dd", "date": True},
        {"name": "kind", "type": "string"},
        {"name": "files", "type": "int64", "format": "#,0", "sum": "sum"},
        {"name": "rows", "type": "int64", "format": "#,0", "sum": "sum"},
        {"name": "detectors_reporting", "type": "int64", "format": "#,0", "sum": "sum"},
        {"name": "ok_rows", "type": "int64", "format": "#,0", "sum": "sum"},
        {"name": "invalid_rows", "type": "int64", "format": "#,0", "sum": "sum"},
        {"name": "out_of_range_rows", "type": "int64", "format": "#,0", "sum": "sum"},
        {"name": "no_vehicle_rows", "type": "int64", "format": "#,0", "sum": "sum"},
        {"name": "standstill_rows", "type": "int64", "format": "#,0", "sum": "sum"},
        {"name": "parse_error_rows", "type": "int64", "format": "#,0", "sum": "sum"},
    ],
}

M_TYPE = {
    "string": "type text",
    "int64": "Int64.Type",
    "double": "type number",
    "dateTime": "type date",
}

MEASURES: list[tuple[str, str, str, str]] = [
    (
        "Avg Speed (km/h)",
        """VAR _t =
    FILTER (
        bi_detector_hour,
        bi_detector_hour[low_coverage] = 0 && NOT ISBLANK ( bi_detector_hour[wavg_speed] )
    )
RETURN
    DIVIDE (
        SUMX ( _t, bi_detector_hour[wavg_speed] * bi_detector_hour[volume] ),
        SUMX ( _t, bi_detector_hour[volume] )
    )""",
        "0.0",
        "Speed",
    ),
    (
        "Congested Share",
        """CALCULATE (
    DIVIDE ( SUM ( bi_detector_hour[congested_rows] ), SUM ( bi_detector_hour[usable_rows] ) ),
    bi_detector_hour[low_coverage] = 0
)""",
        "0.0%",
        "Speed",
    ),
    (
        "Total Volume",
        "CALCULATE ( SUM ( bi_detector_hour[volume] ), bi_detector_hour[low_coverage] = 0 )",
        "#,0",
        "Speed",
    ),
    (
        "Detectors Reporting",
        "DISTINCTCOUNT ( bi_detector_hour[detector_id] )",
        "#,0",
        "Speed",
    ),
    (
        "Speed Midday Weekday",
        """CALCULATE (
    [Avg Speed (km/h)],
    bi_detector_hour[day_type] = "weekday",
    bi_detector_hour[hour] IN { 11, 12, 13, 14 }
)""",
        "0.0",
        "Speed",
    ),
    (
        "Speed Morning Peak Weekday",
        """CALCULATE (
    [Avg Speed (km/h)],
    bi_detector_hour[day_type] = "weekday",
    bi_detector_hour[hour] IN { 7, 8, 9 }
)""",
        "0.0",
        "Speed",
    ),
    (
        "Speed Evening Peak Weekday",
        """CALCULATE (
    [Avg Speed (km/h)],
    bi_detector_hour[day_type] = "weekday",
    bi_detector_hour[hour] IN { 17, 18, 19 }
)""",
        "0.0",
        "Speed",
    ),
    (
        "Evening Peak Slowdown %",
        "DIVIDE ( [Speed Midday Weekday] - [Speed Evening Peak Weekday], [Speed Midday Weekday] )",
        "0.0%",
        "Speed",
    ),
    (
        "Morning Peak Slowdown %",
        "DIVIDE ( [Speed Midday Weekday] - [Speed Morning Peak Weekday], [Speed Midday Weekday] )",
        "0.0%",
        "Speed",
    ),
    (
        "Weekday Speed",
        'CALCULATE ( [Avg Speed (km/h)], bi_detector_hour[day_type] = "weekday" )',
        "0.0",
        "Speed",
    ),
    (
        "Weekend Speed",
        'CALCULATE ( [Avg Speed (km/h)], bi_detector_hour[day_type] = "weekend" )',
        "0.0",
        "Speed",
    ),
    (
        "Corridor Speed (km/h)",
        """VAR _t = FILTER ( bi_corridor_hour, NOT ISBLANK ( bi_corridor_hour[wavg_speed] ) )
RETURN
    DIVIDE (
        SUMX ( _t, bi_corridor_hour[wavg_speed] * bi_corridor_hour[volume] ),
        SUMX ( _t, bi_corridor_hour[volume] )
    )""",
        "0.0",
        "Corridor",
    ),
    (
        "Corridor Volume",
        "SUM ( bi_corridor_hour[volume] )",
        "#,0",
        "Corridor",
    ),
    (
        "Corridor Congested Share",
        "AVERAGE ( bi_corridor_hour[congested_share] )",
        "0.0%",
        "Corridor",
    ),
    (
        "Anomaly Count",
        "CALCULATE ( COUNTROWS ( bi_anomaly ), bi_anomaly[is_anomaly] = 1 )",
        "#,0",
        "Anomaly",
    ),
    (
        "Avg Anomaly Drop %",
        "CALCULATE ( AVERAGE ( bi_anomaly[drop_share] ), bi_anomaly[is_anomaly] = 1 )",
        "0.0%",
        "Anomaly",
    ),
    (
        "Bad Row Share",
        "DIVIDE ( SUM ( bi_detector_hour[bad_rows] ), SUM ( bi_detector_hour[lane_rows] ) )",
        "0.00%",
        "Trust",
    ),
    (
        "Avg Hour Coverage",
        "AVERAGE ( bi_coverage_hour[avg_coverage] )",
        "0.0%",
        "Trust",
    ),
    (
        "Low Coverage Hours %",
        "AVERAGE ( bi_coverage_hour[low_coverage_share] )",
        "0.0%",
        "Trust",
    ),
    (
        "Raw File Completeness",
        """VAR _days = CALCULATE ( DISTINCTCOUNT ( bi_pipeline_run[day] ), bi_pipeline_run[kind] = "raw" )
RETURN
    DIVIDE ( CALCULATE ( SUM ( bi_pipeline_run[files] ), bi_pipeline_run[kind] = "raw" ), _days * 1440 )""",
        "0.0%",
        "Trust",
    ),
    (
        "Title Speed",
        '"Average speed " & FORMAT ( [Avg Speed (km/h)], "0" ) & " km/h, " & FORMAT ( [Congested Share], "0.0%" ) & " of lane readings congested"',
        "",
        "Title",
    ),
    (
        "Top Corridor Congestion",
        """VAR _rank =
    RANKX (
        CALCULATETABLE ( VALUES ( bi_hotspot[corridor] ), REMOVEFILTERS ( bi_hotspot[corridor] ) ),
        CALCULATE ( AVERAGE ( bi_hotspot[mean_congested_share] ) ),
        ,
        DESC,
        Dense
    )
RETURN
    IF ( _rank <= 10, AVERAGE ( bi_hotspot[mean_congested_share] ) )""",
        "0.0%",
        "Hotspot",
    ),
    (
        "Top Detector Bad Share",
        """VAR _rank =
    RANKX (
        CALCULATETABLE (
            VALUES ( bi_detector_quality[detector_id] ),
            REMOVEFILTERS ( bi_detector_quality[detector_id] )
        ),
        CALCULATE ( MAX ( bi_detector_quality[bad_share] ) ),
        ,
        DESC,
        Dense
    )
RETURN
    IF ( _rank <= 10, MAX ( bi_detector_quality[bad_share] ) )""",
        "0.0%",
        "Trust",
    ),
]


def column_block(column: dict) -> str:
    name = tmdl_name(column["name"])
    lines = [f"\tcolumn {name}", f"\t\tdataType: {column['type']}"]
    if column.get("format"):
        lines.append(f"\t\tformatString: {json.dumps(column['format'])}")
    if column.get("hidden"):
        lines.append("\t\tisHidden")
    lines.append(f"\t\tlineageTag: {guid()}")
    lines.append(f"\t\tsummarizeBy: {column.get('sum', 'none')}")
    source = column.get("source", column["name"])
    lines.append(f"\t\tsourceColumn: {source}")
    if column.get("sort"):
        lines.append(f"\t\tsortByColumn: {tmdl_name(column['sort'])}")
    if column.get("category"):
        lines.append(f"\t\tdataCategory: {column['category']}")
    if column.get("date"):
        lines.append("\t\tannotation UnderlyingDateTimeDataType = Date")
    lines.append("")
    return "\n".join(lines)


def m_partition(table: str) -> str:
    pairs = ", ".join(
        f'{{"{column["name"]}", {M_TYPE[column["type"]]}}}' for column in TABLES[table]
    )
    return f"""\tpartition {tmdl_name(table)} = m
\t\tmode: import
\t\tsource =
\t\t\t\tlet
\t\t\t\t    Source = Csv.Document(File.Contents(Text.Combine({{#"DataPath", "{table}.csv"}}, "/")), [Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.Csv]),
\t\t\t\t    Promoted = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
\t\t\t\t    Cleaned = Table.ReplaceValue(Promoted, "", null, Replacer.ReplaceValue, Table.ColumnNames(Promoted)),
\t\t\t\t    Typed = Table.TransformColumnTypes(Cleaned, {{{pairs}}})
\t\t\t\tin
\t\t\t\t    Typed
"""


def import_table(name: str) -> str:
    body = [f"table {tmdl_name(name)}", f"\tlineageTag: {guid()}", ""]
    body.extend(column_block(column) for column in TABLES[name])
    body.append(m_partition(name))
    body.append("\tannotation PBI_ResultType = Table")
    body.append("")
    return "\n".join(body)


def calculated_table(name: str, columns: list[dict], dax: str) -> str:
    lines = [f"table {tmdl_name(name)}", f"\tlineageTag: {guid()}", ""]
    for column in columns:
        lines.append(column_block(column))
    lines.append(f"\tpartition {tmdl_name(name)} = calculated")
    lines.append("\t\tmode: import")
    lines.append("\t\tsource = ```")
    for dax_line in dax.splitlines():
        lines.append("\t\t\t\t" + dax_line if dax_line else "")
    lines.append("\t\t\t\t```")
    lines.append("")
    lines.append("\tannotation PBI_ResultType = Table")
    lines.append("")
    return "\n".join(lines)


def measures_table() -> str:
    lines = [
        "table '_Measures'",
        f"\tlineageTag: {guid()}",
        "",
        column_block({"name": "Dummy", "type": "string", "hidden": True, "source": "[Dummy]"}),
    ]
    for name, expression, fmt, folder in MEASURES:
        lines.append(f"\tmeasure {tmdl_name(name)} = ```")
        for dax_line in expression.splitlines():
            lines.append("\t\t\t" + dax_line if dax_line else "")
        lines.append("\t\t\t```")
        if fmt:
            lines.append(f"\t\tformatString: {json.dumps(fmt)}")
        lines.append(f"\t\tdisplayFolder: {folder}")
        lines.append(f"\t\tlineageTag: {guid()}")
        lines.append("")
    lines.append("\tpartition '_Measures' = calculated")
    lines.append("\t\tmode: import")
    lines.append('\t\tsource = ```')
    lines.append('\t\t\t\tROW ( "Dummy", BLANK () )')
    lines.append("\t\t\t\t```")
    lines.append("")
    lines.append("\tannotation PBI_ResultType = Table")
    lines.append("")
    return "\n".join(lines)


def relationships() -> str:
    pairs = [
        ("bi_detector_hour", "obs_date", "Date", "Date"),
        ("bi_corridor_hour", "obs_date", "Date", "Date"),
        ("bi_anomaly", "obs_date", "Date", "Date"),
        ("bi_coverage_hour", "obs_date", "Date", "Date"),
        ("bi_detector_hour", "hour", "Hour", "Hour"),
        ("bi_corridor_hour", "hour", "Hour", "Hour"),
        ("bi_anomaly", "hour", "Hour", "Hour"),
        ("bi_coverage_hour", "hour", "Hour", "Hour"),
    ]
    blocks = []
    for src, src_col, dst, dst_col in pairs:
        blocks.append(
            "\n".join(
                [
                    f"relationship {guid()}",
                    "\tfromCardinality: many",
                    "\ttoCardinality: one",
                    "\tcrossFilteringBehavior: oneDirection",
                    f"\tfromColumn: {src}.{src_col}",
                    f"\ttoColumn: {dst}.{dst_col}",
                    "",
                ]
            )
        )
    return "\n".join(blocks)


def write_model() -> None:
    definition = MODEL / "definition"
    tables = definition / "tables"
    if MODEL.exists():
        shutil.rmtree(MODEL)
    tables.mkdir(parents=True)
    (definition / "database.tmdl").write_text(
        "database\n\tcompatibilityLevel: 1702\n\tcompatibilityMode: powerBI\n",
        encoding="utf-8",
    )
    (definition / "expressions.tmdl").write_text(
        "\n".join(
            [
                f'expression DataPath = "{DATA_PATH}" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true]',
                f"\tlineageTag: {guid()}",
                "",
                "\tannotation PBI_ResultType = Text",
                "",
            ]
        ),
        encoding="utf-8",
    )
    query_order = ["DataPath", *TABLES.keys()]
    model_lines = [
        "model Model",
        "\tculture: en-US",
        "\tdefaultPowerBIDataSourceVersion: powerBI_V3",
        "\tsourceQueryCulture: en-US",
        "\tdataAccessOptions",
        "\t\tlegacyRedirects",
        "\t\treturnErrorValuesAsNull",
        "",
        "\tannotation PBI_QueryOrder = " + json.dumps(query_order, separators=(",", ":")),
        "",
        "\tannotation __PBI_TimeIntelligenceEnabled = 0",
        "",
        "ref table Date",
        "ref table Hour",
        "ref table '_Measures'",
    ]
    for name in TABLES:
        model_lines.append(f"ref table {name}")
    model_lines.append("")
    (definition / "model.tmdl").write_text("\n".join(model_lines), encoding="utf-8")
    (definition / "relationships.tmdl").write_text(relationships(), encoding="utf-8")
    (tables / "Date.tmdl").write_text(
        calculated_table(
            "Date",
            [
                {
                    "name": "Date",
                    "type": "dateTime",
                    "format": "yyyy-MM-dd",
                    "source": "[Date]",
                    "date": True,
                }
            ],
            "CALENDAR ( MIN ( bi_detector_hour[obs_date] ), MAX ( bi_detector_hour[obs_date] ) )",
        ),
        encoding="utf-8",
    )
    (tables / "Hour.tmdl").write_text(
        calculated_table(
            "Hour",
            [
                {"name": "Hour", "type": "int64", "format": "0", "source": "[Hour]"},
                {
                    "name": "Hour Label",
                    "type": "string",
                    "source": "[Hour Label]",
                    "sort": "Hour",
                },
            ],
            'SELECTCOLUMNS (\n    GENERATESERIES ( 0, 23, 1 ),\n    "Hour", [Value],\n    "Hour Label", FORMAT ( [Value], "00" ) & ":00"\n)',
        ),
        encoding="utf-8",
    )
    (tables / "_Measures.tmdl").write_text(measures_table(), encoding="utf-8")
    for name in TABLES:
        (tables / f"{name}.tmdl").write_text(import_table(name), encoding="utf-8")
    write_json(
        MODEL / "definition.pbism",
        {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/semanticModel/definitionProperties/1.0.0/schema.json",
            "version": "4.2",
            "settings": {"qnaEnabled": False},
        },
    )
    write_json(
        MODEL / ".platform",
        {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
            "metadata": {"type": "SemanticModel", "displayName": "HK Traffic"},
            "config": {"version": "2.0", "logicalId": str(uuid.uuid4())},
        },
    )


# -------------------------------------------------------------------------------------- report
def save_visuals(page: str, visuals: list[tuple[str, dict]]) -> None:
    for name, payload in visuals:
        write_json(REPORT / "definition" / "pages" / page / "visuals" / name / "visual.json", payload)


def build_pages() -> list[tuple[str, str, dict]]:
    rhythm = page_id("rhythm")
    hotspots = page_id("hotspots")
    tidal = page_id("tidal")
    anomalies = page_id("anomalies")
    trust = page_id("trust")
    pages = []

    rhythm_visuals = page_shell("rhythm", "工作日晚高峰比午间慢 5.9%（18:00 最慢，65 km/h）")
    rhythm_visuals.append(
        (
            hid("rhythm speed card dyn"),
            card(
                hid("rhythm speed card dyn"),
                "Title Speed",
                "动态标题",
                12,
                44,
                1256,
                32,
                2000,
                2,
                "#334155",
                plain=True,
            ),
        )
    )
    kpis = [
        ("Avg Speed (km/h)", "Avg Speed (km/h)", "#0F766E"),
        ("Congested Share", "Congested Share", "#D97706"),
        ("Evening Peak Slowdown %", "Evening Peak Slowdown %", "#DC2626"),
        ("Detectors Reporting", "Detectors Reporting", "#1D4ED8"),
    ]
    for index, (measure, title, color) in enumerate(kpis):
        rhythm_visuals.append(
            (
                hid("rhythm kpi " + measure),
                card(hid("rhythm kpi " + measure), measure, title, 12 + index * 316, 90, 304, 78, 3000, 3 + index, color),
            )
        )
    rhythm_visuals += [
        (hid("rhythm date"), slicer(hid("rhythm date"), "Date", "Date", "日期", 12, 176, 240, 64, 4000, 8, "Between")),
        (hid("rhythm hour"), slicer(hid("rhythm hour"), "Hour", "Hour Label", "小时", 264, 176, 200, 64, 4000, 9)),
        (hid("rhythm district"), slicer(hid("rhythm district"), "bi_detector_hour", "district", "地区", 476, 176, 240, 64, 4000, 10)),
        (
            hid("rhythm line"),
            visual(
                hid("rhythm line"),
                "lineChart",
                12,
                250,
                1256,
                210,
                5000,
                11,
                {
                    "Category": [col_proj("Hour", "Hour Label", "小时")],
                    "Y": [measure_proj("Avg Speed (km/h)", "速度 km/h")],
                    "Series": [col_proj("bi_detector_hour", "day_type", "日类型")],
                    "Tooltips": [measure_proj("Total Volume", "流量")],
                },
                objects={"legend": [{"properties": {"show": lit("true"), "position": lit("'Top'")}}]},
                container=chrome("速度按小时（km/h）", "样本只有 5 个工作日，图例里不会出现周末"),
            ),
        ),
        (
            hid("rhythm matrix"),
            visual(
                hid("rhythm matrix"),
                "pivotTable",
                12,
                470,
                1256,
                240,
                5000,
                12,
                {
                    "Rows": [col_proj("bi_detector_hour", "weekday", "星期")],
                    "Columns": [col_proj("Hour", "Hour Label", "小时")],
                    "Values": [measure_proj("Congested Share", "拥堵占比")],
                },
                objects={
                    "values": [
                        gradient(
                            "Congested Share",
                            [("#16A34A", "0D"), ("#F59E0B", "0.03D"), ("#DC2626", "0.08D")],
                        )
                    ]
                },
                container=chrome("拥堵占比：星期 × 小时", "绿是顺畅，红是拥堵。只统计覆盖率达标的小时"),
            ),
        ),
    ]
    pages.append((rhythm, "一天的节奏", {"visuals": rhythm_visuals}))

    hot_visuals = page_shell("hot", "前 5 条走廊占了 32% 的拥堵读数，前 10 占 51%")
    hot_visuals += [
        (hid("hot district"), slicer(hid("hot district"), "bi_hotspot", "district", "地区", 12, 50, 250, 56, 4000, 2)),
        (
            hid("hot map"),
            visual(
                hid("hot map"),
                "scatterChart",
                12,
                114,
                760,
                594,
                5000,
                3,
                {
                    "Category": [col_proj("bi_hotspot", "detector_id", "探测器")],
                    "X": [agg_proj("bi_hotspot", "longitude", 1, "经度")],
                    "Y": [agg_proj("bi_hotspot", "latitude", 1, "纬度")],
                    "Size": [agg_proj("bi_hotspot", "mean_congested_share", 1, "拥堵占比")],
                    "Series": [col_proj("bi_hotspot", "peak_hour", "最堵小时")],
                    "Tooltips": [
                        col_proj("bi_hotspot", "corridor", "走廊"),
                        col_proj("bi_hotspot", "location", "位置"),
                        col_proj("bi_hotspot", "district", "地区"),
                        agg_proj("bi_hotspot", "mean_speed_kmh", 1, "速度 km/h"),
                    ],
                },
                container=chrome("探测器位置（经度 × 纬度）", "Azure 地图需要登录，所以用散点图。点越大越堵。TDS10012 只有 3 天数据"),
            ),
        ),
        (
            hid("hot bar"),
            visual(
                hid("hot bar"),
                "clusteredBarChart",
                784,
                50,
                484,
                300,
                5000,
                4,
                {
                    "Category": [col_proj("bi_hotspot", "corridor", "走廊")],
                    "Y": [measure_proj("Top Corridor Congestion", "平均拥堵占比")],
                },
                objects={"labels": [{"properties": {"show": lit("true")}}]},
                container=chrome("走廊平均拥堵占比（前 10）"),
                sort=[sort_by(M, "Top Corridor Congestion", "Descending", "Measure")],
            ),
        ),
        (
            hid("hot table"),
            visual(
                hid("hot table"),
                "tableEx",
                784,
                360,
                484,
                348,
                5000,
                5,
                {
                    "Values": [
                        col_proj("bi_hotspot", "rank", "排名"),
                        col_proj("bi_hotspot", "corridor", "走廊"),
                        col_proj("bi_hotspot", "location", "位置"),
                        col_proj("bi_hotspot", "district", "地区"),
                        col_proj("bi_hotspot", "peak_hour", "最堵小时"),
                        col_proj("bi_hotspot", "mean_speed_kmh", "速度 km/h"),
                    ]
                },
                container=chrome("热点明细", "按排名。783 个探测器，表格可滚动"),
                sort=[sort_by("bi_hotspot", "rank", "Ascending")],
            ),
        ),
    ]
    pages.append((hotspots, "拥堵热点", {"visuals": hot_visuals}))

    tidal_filter = [categorical_filter("tidal weekday", "bi_corridor_hour", "day_type", "'weekday'")]
    tidal_visuals = page_shell("tidal", "Tuen Mun Road 早上东行更慢，晚上西行更慢")
    tidal_visuals += [
        (
            hid("tidal slicer"),
            slicer(
                hid("tidal slicer"),
                "bi_corridor_hour",
                "corridor",
                "走廊",
                12,
                50,
                360,
                64,
                4000,
                2,
                default_literal="'Tuen Mun Road'",
                single=True,
            ),
        ),
        (
            hid("tidal speed"),
            visual(
                hid("tidal speed"),
                "lineChart",
                12,
                124,
                1256,
                286,
                5000,
                3,
                {
                    "Category": [col_proj("Hour", "Hour Label", "小时")],
                    "Y": [measure_proj("Corridor Speed (km/h)", "速度 km/h")],
                    "Series": [col_proj("bi_corridor_hour", "travel_direction", "方向")],
                    "Tooltips": [
                        col_proj("bi_corridor_hour", "corridor", "走廊"),
                        measure_proj("Corridor Volume", "流量"),
                    ],
                },
                objects={"legend": [{"properties": {"show": lit("true"), "position": lit("'Top'")}}]},
                container=chrome("走廊速度（km/h，工作日）", "有两个方向的还有 Yuen Long Highway、North Lantau Highway。过海隧道不在这份数据里"),
                filters=tidal_filter,
            ),
        ),
        (
            hid("tidal volume"),
            visual(
                hid("tidal volume"),
                "lineChart",
                12,
                420,
                1256,
                288,
                5000,
                4,
                {
                    "Category": [col_proj("Hour", "Hour Label", "小时")],
                    "Y": [measure_proj("Corridor Volume", "流量")],
                    "Series": [col_proj("bi_corridor_hour", "travel_direction", "方向")],
                    "Tooltips": [measure_proj("Corridor Speed (km/h)", "速度 km/h")],
                },
                objects={"legend": [{"properties": {"show": lit("true"), "position": lit("'Top'")}}]},
                container=chrome("走廊流量（工作日）", "看流量是否和速度一起随方向翻转"),
                filters=tidal_filter,
            ),
        ),
    ]
    pages.append((tidal, "潮汐车流", {"visuals": tidal_visuals, "filters": tidal_filter}))

    anomaly_filter = [categorical_filter("anomaly flag", "bi_anomaly", "is_anomaly", "1L")]
    anomaly_visuals = page_shell("anomaly", "11月5日 16:00，Lion Rock Tunnel Road 近 Fung Shing Court 比平时慢 89%")
    anomaly_visuals += [
        (
            hid("anomaly count"),
            card(hid("anomaly count"), "Anomaly Count", "Anomaly Count", 12, 50, 300, 78, 3000, 2, "#DC2626"),
        ),
        (
            hid("anomaly drop"),
            card(hid("anomaly drop"), "Avg Anomaly Drop %", "Avg Anomaly Drop %", 324, 50, 300, 78, 3000, 3, "#D97706"),
        ),
        (
            hid("anomaly date"),
            slicer(hid("anomaly date"), "Date", "Date", "日期", 636, 50, 250, 78, 4000, 4, "Between"),
        ),
        (
            hid("anomaly table"),
            visual(
                hid("anomaly table"),
                "tableEx",
                12,
                140,
                760,
                568,
                5000,
                5,
                {
                    "Values": [
                        col_proj("bi_anomaly", "obs_date", "日期"),
                        col_proj("bi_anomaly", "hour", "小时"),
                        col_proj("bi_anomaly", "corridor", "走廊"),
                        col_proj("bi_anomaly", "location", "位置"),
                        col_proj("bi_anomaly", "wavg_speed", "速度 km/h"),
                        col_proj("bi_anomaly", "baseline_mean", "平时速度"),
                        col_proj("bi_anomaly", "drop_share", "降幅"),
                    ]
                },
                container=chrome("异常小时", "只显示 is_anomaly = 1，按降幅从大到小。还没有对照新闻和天气"),
                sort=[sort_by("bi_anomaly", "drop_share", "Descending")],
            ),
        ),
        (
            hid("anomaly scatter"),
            visual(
                hid("anomaly scatter"),
                "clusteredColumnChart",
                784,
                140,
                484,
                568,
                5000,
                6,
                {
                    "Category": [col_proj("Hour", "Hour Label", "小时")],
                    "Series": [col_proj("bi_anomaly", "obs_date", "日期")],
                    "Y": [measure_proj("Anomaly Count", "异常次数")],
                },
                objects={"legend": [{"properties": {"show": lit("true"), "position": lit("'Top'")}}]},
                container=chrome("异常出现在哪一天、哪一小时", "每个柱子是这一天、这个小时被标成异常的探测器数"),
            ),
        ),
    ]
    pages.append((anomalies, "异常事件", {"visuals": anomaly_visuals, "filters": anomaly_filter}))

    trust_visuals = page_shell("trust", "原始归档大约到了 72%–76%，TDS90018 的坏数据占 66%")
    trust_kpis = [
        ("Raw File Completeness", "Raw File Completeness", "#1D4ED8"),
        ("Bad Row Share", "Bad Row Share", "#DC2626"),
        ("Avg Hour Coverage", "Avg Hour Coverage", "#0F766E"),
    ]
    for index, (measure, title, color) in enumerate(trust_kpis):
        trust_visuals.append(
            (
                hid("trust kpi " + measure),
                card(hid("trust kpi " + measure), measure, title, 12 + index * 422, 50, 406, 78, 3000, 2 + index, color),
            )
        )
    trust_visuals += [
        (
            hid("trust matrix"),
            visual(
                hid("trust matrix"),
                "pivotTable",
                12,
                140,
                1256,
                250,
                5000,
                6,
                {
                    "Rows": [col_proj("bi_coverage_hour", "obs_date", "日期")],
                    "Columns": [col_proj("Hour", "Hour Label", "小时")],
                    "Values": [measure_proj("Avg Hour Coverage", "覆盖率")],
                },
                objects={
                    "values": [
                        gradient(
                            "Avg Hour Coverage",
                            [("#DC2626", "0.4D"), ("#F59E0B", "0.7D"), ("#16A34A", "0.95D")],
                        )
                    ]
                },
                container=chrome("每小时覆盖率", "红是缺口。11月3日 18:00 最低，约 61%"),
            ),
        ),
        (
            hid("trust bar"),
            visual(
                hid("trust bar"),
                "clusteredBarChart",
                12,
                400,
                620,
                308,
                5000,
                7,
                {
                    "Category": [col_proj("bi_detector_quality", "detector_id", "探测器")],
                    "Y": [measure_proj("Top Detector Bad Share", "坏数据占比")],
                    "Tooltips": [
                        col_proj("bi_detector_quality", "corridor", "走廊"),
                        col_proj("bi_detector_quality", "location", "位置"),
                        col_proj("bi_detector_quality", "district", "地区"),
                    ],
                },
                objects={"labels": [{"properties": {"show": lit("true")}}]},
                container=chrome("坏数据占比最高的探测器（前 10）", "TDS90018 在 Tuen Mun Road"),
                sort=[sort_by(M, "Top Detector Bad Share", "Descending", "Measure")],
            ),
        ),
        (
            hid("trust table"),
            visual(
                hid("trust table"),
                "tableEx",
                644,
                400,
                624,
                308,
                5000,
                8,
                {
                    "Values": [
                        col_proj("bi_pipeline_run", "day", "日期"),
                        col_proj("bi_pipeline_run", "kind", "类型"),
                        col_proj("bi_pipeline_run", "files", "文件数"),
                        col_proj("bi_pipeline_run", "rows", "行数"),
                        col_proj("bi_pipeline_run", "ok_rows", "正常行"),
                        col_proj("bi_pipeline_run", "invalid_rows", "无效行"),
                        col_proj("bi_pipeline_run", "out_of_range_rows", "超范围"),
                        col_proj("bi_pipeline_run", "standstill_rows", "停车"),
                        col_proj("bi_pipeline_run", "parse_error_rows", "解析错误"),
                    ]
                },
                container=chrome("每日管道运行", "完整一天应有 1440 个原始文件"),
                sort=[sort_by("bi_pipeline_run", "day", "Ascending")],
            ),
        ),
    ]
    pages.append((trust, "数据可信度", {"visuals": trust_visuals}))
    return pages


def write_report(pages: list[tuple[str, str, dict]]) -> None:
    if REPORT.exists():
        shutil.rmtree(REPORT)
    theme = json.loads((ROOT / "theme.json").read_text(encoding="utf-8"))
    theme["name"] = "HKTraffic"
    write_json(REPORT / "StaticResources" / "RegisteredResources" / "HKTraffic.json", theme)
    write_json(
        REPORT / "definition.pbir",
        {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/2.0.0/schema.json",
            "version": "4.0",
            "datasetReference": {"byPath": {"path": "../HKTraffic.SemanticModel"}},
        },
    )
    write_json(
        REPORT / ".platform",
        {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
            "metadata": {"type": "Report", "displayName": "HK Traffic"},
            "config": {"version": "2.0", "logicalId": str(uuid.uuid4())},
        },
    )
    write_json(
        REPORT / "definition" / "version.json",
        {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/versionMetadata/1.0.0/schema.json",
            "version": "2.0.0",
        },
    )
    write_json(
        REPORT / "definition" / "report.json",
        {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/report/3.1.0/schema.json",
            "themeCollection": {
                "customTheme": {
                    "name": "HKTraffic",
                    "reportVersionAtImport": {"visual": "2.4.0", "report": "3.1.0", "page": "2.1.0"},
                    "type": "RegisteredResources",
                }
            },
            "resourcePackages": [
                {
                    "name": "RegisteredResources",
                    "type": "RegisteredResources",
                    "items": [{"name": "HKTraffic", "path": "HKTraffic.json", "type": "CustomTheme"}],
                }
            ],
            "settings": {
                "useStylableVisualContainerHeader": True,
                "exportDataMode": "AllowSummarized",
                "defaultDrillFilterOtherVisuals": True,
                "allowChangeFilterTypes": True,
                "useEnhancedTooltips": True,
            },
        },
    )
    write_json(
        REPORT / "definition" / "pages" / "pages.json",
        {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/pagesMetadata/1.0.0/schema.json",
            "pageOrder": [page for page, _, _ in pages],
            "activePageName": pages[0][0],
        },
    )
    for page, display, spec in pages:
        page_json = {
            "$schema": PAGE_SCHEMA,
            "name": page,
            "displayName": display,
            "displayOption": "FitToPage",
            "height": 720,
            "width": 1280,
            "objects": {
                "background": [
                    {
                        "properties": {
                            "color": solid("#F8FAFC"),
                            "transparency": lit("0D"),
                        }
                    }
                ]
            },
        }
        if spec.get("filters"):
            page_json["filterConfig"] = {"filters": spec["filters"]}
        write_json(REPORT / "definition" / "pages" / page / "page.json", page_json)
        save_visuals(page, spec["visuals"])
    write_json(
        PBIP,
        {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/pbip/pbipProperties/1.0.0/schema.json",
            "version": "1.0",
            "artifacts": [{"report": {"path": "HKTraffic.Report"}}],
            "settings": {"enableAutoRecovery": True},
        },
    )


def main() -> None:
    write_model()
    write_report(build_pages())
    print(f"Wrote {PBIP}")


if __name__ == "__main__":
    main()
