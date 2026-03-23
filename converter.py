import pandas as pd
import xml.etree.ElementTree as ET
import argparse
import re

# ==============================
# USER CONFIGURATION
# ==============================

station = "distributing"

parser = argparse.ArgumentParser(description="This is a script to remove all the unnecessary information in a csv file")

parser.add_argument("infile", help="File to read from")
parser.add_argument("mapping", help="File to read mapping from")
parser.add_argument("outfile", help="File the script writes the cleaned up data to")

args = parser.parse_args(); 

INPUT_CSV = args.infile
OUTPUT_XES = args.outfile
MAPPING_FILE = args.mapping
EVENT_REGEX = r"([A-Za-z][0-9]+[A-Za-z]+\d)$"
ID_MEASUREMENT = "ID"


try: 
    df = pd.read_csv(INPUT_CSV, skiprows=3)
except: 
    print("Opening infile was not possible")
    exit()

# Rename columns properly
df.columns = [
    "unnamed", "_time", "_value", "_measurement"
]

# Convert timestamps
df["_time"] = pd.to_datetime(df["_time"])

# Sort by time
df = df.sort_values("_time")

# ==============================
# LOAD NAME MAPPING
# ==============================


try: 
    mapping_df = pd.read_csv(MAPPING_FILE)
except: 
    print("Opening mapping file was not possible")
    exit()

# Create dictionary:
# { "C2MB1_true": "Magazin 1 Valve extended", ... }
event_name_mapping = dict(
    zip(mapping_df["original_name"], mapping_df["new_name"])
)

# ==============================
# SPLIT EVENTS AND IDS
# ==============================
pattern = re.compile(EVENT_REGEX)

events = df[df["_measurement"].str.match(pattern, na=False)].copy()
events["variable_name"] = events["_measurement"].str.extract(pattern)

# Build original event name
events["original_event_name"] = (
    events["variable_name"] + "_" +
    events["_value"].astype(str).str.lower()
)

# keep only events present in mapping
events = events[events["original_event_name"].isin(event_name_mapping)]

events["final_event_name"] = events["original_event_name"].map(event_name_mapping)

ids = df[df["_measurement"] == ID_MEASUREMENT].copy()

ids = ids.sort_values("_time")

if len(ids) == 0:
    raise ValueError("No ID entries found. Check ID_MEASUREMENT name.")

# ==============================
# CREATE TRACE INTERVALS
# ==============================

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
# BUILD XES STRUCTURE
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

try: 

    tree = ET.ElementTree(log)
    tree.write(OUTPUT_XES, encoding="utf-8", xml_declaration=True)

    print(f"XES file successfully written to {OUTPUT_XES}")

except: 
    print("An error occured writing the outfile")
    exit()
