import pandas as pd
import xml.etree.ElementTree as ET
import re

# ==============================
# USER CONFIGURATION
# ==============================

INPUT_CSV = "Data/edgepi_all_150ishruns_260311.csv"
OUTPUT_XES = "output_distributing.xes"
MAPPING_FILE = "event_name_mapping.csv"

ID_MEASUREMENT = "opcua.0.vars.Objects.ServerInterfaces.Event-Log_Dis.ID"

EVENT_REGEX = r"^opcua\.0\.vars\.Objects\.ServerInterfaces\.Event-Log_Dis\.([A-Za-z0-9_]+)$"

# ==============================
# LOAD DATA
# ==============================

df = pd.read_csv(INPUT_CSV, skiprows=3)

df.columns = [
    "", "result", "table", "_start", "_stop",
    "_time", "_value", "_field", "_measurement"
]

df["_time"] = pd.to_datetime(df["_time"])
df = df.sort_values("_time")

# ==============================
# LOAD NAME MAPPING
# ==============================

mapping_df = pd.read_csv(MAPPING_FILE)

# Create dictionary:
# { "C2MB1_true": "Magazin 1 Valve extended", ... }
event_name_mapping = dict(
    zip(mapping_df["original_name"], mapping_df["new_name"])
)

# ==============================
# FILTER EVENTS VIA REGEX
# ==============================

pattern = re.compile(EVENT_REGEX)

events = df[df["_measurement"].str.match(pattern, na=False)].copy()
events["variable_name"] = events["_measurement"].str.extract(pattern)

events["original_event_name"] = (
    events["variable_name"] + "_" +
    events["_value"].astype(str).str.lower()
)

# keep only events present in mapping
events = events[events["original_event_name"].isin(event_name_mapping)]

events["final_event_name"] = events["original_event_name"].map(event_name_mapping)

# If no mapping exists → keep original name
events["final_event_name"] = events["final_event_name"].fillna(
    events["original_event_name"]
)

# ==============================
# PROCESS IDS
# ==============================

ids = df[df["_measurement"] == ID_MEASUREMENT].copy()
ids = ids.sort_values("_time")

if len(ids) == 0:
    raise ValueError("No ID entries found. Check ID_MEASUREMENT name.")

trace_intervals = []

for i in range(len(ids)):
    start_time = ids.iloc[i]["_time"]
    trace_id = str(ids.iloc[i]["_value"])

    if i < len(ids) - 1:
        end_time = ids.iloc[i + 1]["_time"]
    else:
        end_time = df["_time"].max()

    trace_intervals.append((trace_id, start_time, end_time))

# ==============================
# BUILD XES
# ==============================

log = ET.Element("log", {
    "xes.version": "1.0",
    "xes.features": "nested-attributes",
    "xmlns": "http://www.xes-standard.org/"
})

for trace_id, start_time, end_time in trace_intervals:

    trace = ET.SubElement(log, "trace")
    ET.SubElement(trace, "string", key="concept:name", value=trace_id)

    trace_events = events[
        (events["_time"] >= start_time) &
        (events["_time"] < end_time)
    ]

    for _, row in trace_events.iterrows():
        event = ET.SubElement(trace, "event")

        ET.SubElement(
            event,
            "string",
            key="concept:name",
            value=row["final_event_name"]
        )

        ET.SubElement(
            event,
            "date",
            key="time:timestamp",
            value=row["_time"].isoformat()
        )

# ==============================
# WRITE FILE
# ==============================

tree = ET.ElementTree(log)
tree.write(OUTPUT_XES, encoding="utf-8", xml_declaration=True)

print(f"XES file successfully written to {OUTPUT_XES}")
