import pandas as pd
import xml.etree.ElementTree as ET
import argparse

def main(): 
    parser = argparse.ArgumentParser(description="This is a script to remove all the unnecessary information in a csv file")

    parser.add_argument("infile", help="File to read from")
    parser.add_argument("outfile", help="File the script writes the cleaned up data to")

    args = parser.parse_args(); 

    INPUT_CSV = args.infile
    OUTPUT_CSV = args.outfile

    try: 
        input_f = pd.read_csv(INPUT_CSV, skiprows=3)
    except: 
        print("The parsed infile cannot be opened")
        exit()

    input_f["table"] = pd.to_numeric(input_f["table"], errors="coerce")
    input_f = input_f[input_f["table"].notna()]

    input_f["_measurement"] = input_f["_measurement"].str.split(".").str[-1]

    input_f.columns = [
    "", "result", "table", "_start", "_stop",
    "_time", "_value", "_field", "_measurement"
    ]

    selected_columns = ["_time", "_value", "_measurement"]
    filtered_df = input_f[selected_columns]

    

    try: 
        filtered_df.to_csv(OUTPUT_CSV)
    except: 
        print("The parse output file is not valid")
        exit()


if __name__ == "__main__": 
    main()