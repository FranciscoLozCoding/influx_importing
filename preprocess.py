import pandas as pd
import re
import os
import yaml
from dateutil.parser import parse
import subprocess

#                   NOTE: CHANGE THE VARIABLES TO YOUR PREPROCESSING NEEDS
########################################################################################################
# 1) Specify the directory containing the CSV files
input_directory = 'import_2'

# 2) Specify the directory where the generated CSV files will go
output_directory = 'import_2/preprocessed'  

# 3) Specify the columns to keep
columns_to_keep = [
    'Date',
    'Time',
    'Uncorrected Out (cm/hr)',
    'Uncorrected In (cm/hr)',
    'Internal Battery Voltage (V)'
]

# 4) specify the columns to rename
rename_cols = {
 'Uncorrected Out (cm/hr)': 'uncorrected_outer',
 'Uncorrected In (cm/hr)': 'uncorrected_inner',
 'Internal Battery Voltage (V)': 'battery_voltage',
}

# 5) specify pivot id vars
piv_ids = ['_time']

# 6) specify pivot value vars
piv_vals = ['uncorrected_outer', 'uncorrected_inner', 'battery_voltage']

# 7) specify additional columns and their values for each file
additional_columns = {
    'SX61NA0A.csv': {
        'job': 'import',
        'result': '',
        '_field': 'value',
        'serial_number_tag': 'SX61NA0A', 
        'applicationId': '5996eb03-7675-488b-830e-13c77c50cc3a', 
        'applicationName': 'default', 
        'deviceProfileId': '45c51c47-014e-4347-832a-4c8505cc90cb',
        'deviceProfileName': 'SFM1x',
        'host': '0000dca632e9bafd.ws-rpi',
        'node': '000048b02d35a87e',
        'plugin' : 'registry.sagecontinuum.org/flozano/lorawan-listener:0.0.14',
        'task': 'lorawan-listener',
        'tenantId': '52f14cd4-c6f1-4fbd-8f87-4025e1d49242',
        'tenantName': 'ChirpStack',
        'vsn': 'W08E',
        'zone': 'shield',
        'devAddr': '019132ff',
        'devEui': '8c1f6460e80000f1',
        'deviceName': 'Sap Flow Meter SX61NA0A',
    },
    'SX61NA0D.csv': {
        'job': 'import',
        'result': '',
        '_field': 'value',
        'serial_number_tag': 'SX61NA0D', 
        'applicationId': '5996eb03-7675-488b-830e-13c77c50cc3a', 
        'applicationName': 'default', 
        'deviceProfileId': '45c51c47-014e-4347-832a-4c8505cc90cb',
        'deviceProfileName': 'SFM1x',
        'host': '0000dca632e9bafd.ws-rpi',
        'node': '000048b02d35a87e',
        'plugin' : 'registry.sagecontinuum.org/flozano/lorawan-listener:0.0.14',
        'task': 'lorawan-listener',
        'tenantId': '52f14cd4-c6f1-4fbd-8f87-4025e1d49242',
        'tenantName': 'ChirpStack',
        'vsn': 'W08E',
        'zone': 'shield',
        'devAddr': '0118f2cf',
        'devEui': '8c1c6460d80000f7',
        'deviceName': 'Sap Flow Meter SX61NA0D',
    },
    'SX61NA0E.csv': {
        'job': 'import',
        'result': '',
        '_field': 'value',
        'serial_number_tag': 'SX61NA0E', 
        'applicationId': '5996eb03-7675-488b-830e-13c77c50cc3a', 
        'applicationName': 'default', 
        'deviceProfileId': '45c51c47-014e-4347-832a-4c8505cc90cb',
        'deviceProfileName': 'SFM1x',
        'host': '0000dca632e9bafd.ws-rpi',
        'node': '000048b02d35a87e',
        'plugin' : 'registry.sagecontinuum.org/flozano/lorawan-listener:0.0.14',
        'task': 'lorawan-listener',
        'tenantId': '52f14cd4-c6f1-4fbd-8f87-4025e1d49242',
        'tenantName': 'ChirpStack',
        'vsn': 'W08E',
        'zone': 'shield',
        'devAddr': '001119da',
        'devEui': '8c1f6460e80000d4',
        'deviceName': 'Sap Flow Meter SX61NA0E',
    },
    'SX61NA0H.csv': {
        'job': 'import',
        'result': '',
        '_field': 'value',
        'serial_number_tag': 'SX61NA0H', 
        'applicationId': '5996eb03-7675-488b-830e-13c77c50cc3a', 
        'applicationName': 'default', 
        'deviceProfileId': '45c51c47-014e-4347-832a-4c8505cc90cb',
        'deviceProfileName': 'SFM1x',
        'host': '0000dca632e9bafd.ws-rpi',
        'node': '000048b02d35a87e',
        'plugin' : 'registry.sagecontinuum.org/flozano/lorawan-listener:0.0.14',
        'task': 'lorawan-listener',
        'tenantId': '52f14cd4-c6f1-4fbd-8f87-4025e1d49242',
        'tenantName': 'ChirpStack',
        'vsn': 'W08E',
        'zone': 'shield',
        'devAddr': '005eb353',
        'devEui': '8c1f6460e80000d9',
        'deviceName': 'Sap Flow Meter SX61NA0H',
    },
    'SX61NA0P.csv': {
        'job': 'import',
        'result': '',
        '_field': 'value',
        'serial_number_tag': 'SX61NA0P', 
        'applicationId': '5996eb03-7675-488b-830e-13c77c50cc3a', 
        'applicationName': 'default', 
        'deviceProfileId': '45c51c47-014e-4347-832a-4c8505cc90cb',
        'deviceProfileName': 'SFM1x',
        'host': '0000dca632e9bafd.ws-rpi',
        'node': '000048b02d35a87e',
        'plugin' : 'registry.sagecontinuum.org/flozano/lorawan-listener:0.0.14',
        'task': 'lorawan-listener',
        'tenantId': '52f14cd4-c6f1-4fbd-8f87-4025e1d49242',
        'tenantName': 'ChirpStack',
        'vsn': 'W08E',
        'zone': 'shield',
        'devAddr': '00f179d4',
        'devEui': '8c1f6460e80000d2',
        'deviceName': 'Sap Flow Meter SX61NA0P',
    },
    'SX61NA0T.csv': {
        'job': 'import',
        'result': '',
        '_field': 'value',
        'serial_number_tag': 'SX61NA0T', 
        'applicationId': '5996eb03-7675-488b-830e-13c77c50cc3a', 
        'applicationName': 'default',  
        'deviceProfileId': '45c51c47-014e-4347-832a-4c8505cc90cb',
        'deviceProfileName': 'SFM1x',
        'host': '0000dca632e9bafd.ws-rpi',
        'node': '000048b02d35a87e',
        'plugin' : 'registry.sagecontinuum.org/flozano/lorawan-listener:0.0.14',
        'task': 'lorawan-listener',
        'tenantId': '52f14cd4-c6f1-4fbd-8f87-4025e1d49242',
        'tenantName': 'ChirpStack',
        'vsn': 'W08E',
        'zone': 'shield',
        'devAddr': '001b4ef6',
        'devEui': '8c1f6460e80000f9',
        'deviceName': 'Sap Flow Meter SX61NA0T',
    },
    'SX61NA0W.csv': {
        'job': 'import',
        'result': '',
        '_field': 'value',
        'serial_number_tag': 'SX61NA0W', 
        'applicationId': '5996eb03-7675-488b-830e-13c77c50cc3a', 
        'applicationName': 'default', 
        'deviceProfileId': '45c51c47-014e-4347-832a-4c8505cc90cb',
        'deviceProfileName': 'SFM1x',
        'host': '0000dca632e9bafd.ws-rpi',
        'node': '000048b02d35a87e',
        'plugin' : 'registry.sagecontinuum.org/flozano/lorawan-listener:0.0.14',
        'task': 'lorawan-listener',
        'tenantId': '52f14cd4-c6f1-4fbd-8f87-4025e1d49242',
        'tenantName': 'ChirpStack',
        'vsn': 'W08E',
        'zone': 'shield',
        'devAddr': '01750544',
        'devEui': '8c1f6460e80000e1',
        'deviceName': 'Sap Flow Meter SX61NA0W',
    },
    'SX61NA08.csv': {
        'job': 'import',
        'result': '',
        '_field': 'value',
        'serial_number_tag': 'SX61NA08', 
        'applicationId': '5996eb03-7675-488b-830e-13c77c50cc3a', 
        'applicationName': 'default', 
        'deviceProfileId': '45c51c47-014e-4347-832a-4c8505cc90cb',
        'deviceProfileName': 'SFM1x',
        'host': '0000dca632e9bafd.ws-rpi',
        'node': '000048b02d35a87e',
        'plugin' : 'registry.sagecontinuum.org/flozano/lorawan-listener:0.0.14',
        'task': 'lorawan-listener',
        'tenantId': '52f14cd4-c6f1-4fbd-8f87-4025e1d49242',
        'tenantName': 'ChirpStack',
        'vsn': 'W08E',
        'zone': 'shield',
        'devAddr': '00eff963',
        'devEui': '8c1f6460e80000a6',
        'deviceName': 'Sap Flow Meter SX61NA08',
    },
    # Add entries for more files as needed
}
########################################################################################################

def safe_parse_date(date_str):
    """Try parsing the date, if form is invalid print the original form"""
    try:
        return parse(date_str, dayfirst=True).strftime('%Y-%m-%d')
    except Exception as e:
        print(f"Error parsing date: {date_str} - {e}")

def match_dt(dt_str):
    """Function to check if the datetime string matches the exact format"""
    # Define the regex pattern for the exact datetime format
    pattern = re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,9})?Z$')
    return bool(pattern.match(dt_str))

def enforce_data_types(df, datatypes):
    """Function to enforce column data types"""

    for col, dtype in datatypes.items():
        if col in df.columns:
            if dtype == 'string':
                df[col] = df[col].astype(str)
            elif dtype == 'double':
                df[col] = pd.to_numeric(df[col], errors='coerce')
            elif dtype == 'dateTime:RFC3339':
                # Check each value in the column for regex match
                invalid_values = df[~df[col].apply(match_dt)][col]
                if not invalid_values.empty:
                    print(f"Error: The following values in column '{col}' are not in dateTime:RFC3339 format:")
                    print(invalid_values.tolist())
                    raise ValueError(f"Datetime values are not in RFC3339.")
            else:
                print(f"Error: Unexpected 'datatype' in meta.yml, specify new datatype to enforce")
                raise ValueError(f"Unexpected 'datatype' in meta.yml")
    return df

def validate_columns(df, datatypes):
    """Function to validate columns against meta.yml"""
    expected_columns = set(datatypes.keys())
    actual_columns = set(df.columns)
    unexpected_columns = actual_columns - expected_columns

    # Log errors for unexpected columns
    if unexpected_columns:
        print(f"Error: Unexpected columns please specify 'group' and 'datatype' in meta.yml: {unexpected_columns}")
        raise ValueError(f"Unexpected columns found: {unexpected_columns}")
    
def generate_csv_headers(group, datatype, defaults, df):
    """Function to generate header lines using meta.yml"""
    
    # Filter out the columns in 'datatype' that are not present in 'df.columns'
    filtered_columns = [col for col in datatype.keys() if col in df.columns]
    
    # Generate the group, datatype, and default lines based on filtered columns
    group_line = "#group," + ",".join(["true" if group.get(col, False) else "false" for col in filtered_columns])
    datatype_line = "#datatype," + ",".join([datatype.get(col, "") for col in filtered_columns])
    default_line = "#default,_result," + ",".join([defaults.get(col, "") for col in filtered_columns])

    ## Reorder columns to match meta.yml
    import_df = df[[col for col in datatypes.keys() if col in df.columns]]
    
    return group_line, datatype_line, default_line, import_df

# Path to the meta.yml file
meta_file = 'meta.yml'

# Load meta.yml
with open(meta_file, 'r') as file:
    meta = yaml.safe_load(file)

# Extract group and datatype information from the YAML structure
columns = meta.get("columns", {})
group = {key: value['group'] for key, value in columns.items()}
datatypes = {key: value['datatype'] for key, value in columns.items()}
defaults = {key: value['default'] for key, value in columns.items()}

# Ensure the output directory exists
os.makedirs(output_directory, exist_ok=True)

# List to store all dataframes for final concatenation
all_dataframes = []

# Process each CSV file in the input directory
for filename in os.listdir(input_directory):
    if filename.endswith('.csv'):
        input_file = os.path.join(input_directory, filename)
        try:
            # Read the CSV file and select the specified columns
            df = pd.read_csv(input_file)
            df.columns = df.columns.str.strip()
            filtered_df = df[columns_to_keep]

            # Rename columns
            filtered_df = filtered_df.rename(columns=rename_cols)

            # Convert 'Date' from day/month/year to year/month/day using dateutil.parser
            filtered_df['Date'] = filtered_df['Date'].apply(safe_parse_date)

            # Combine 'Date' and 'Time' into a new '_time' column
            filtered_df['_time'] = filtered_df.apply(lambda row: parse(f"{row['Date']} {row['Time']}").isoformat() + 'Z', axis=1)

            # Pivot the table
            df_melted = filtered_df.melt(
                id_vars=piv_ids,
                value_vars=piv_vals,
                var_name="_measurement",
                value_name="_value"
            )

            # Remove rows where the 'value' field is empty
            df_melted = df_melted.dropna(subset=['_value'])

            # Add additional columns if specified in the dictionary
            if filename in additional_columns:
                for col, value in additional_columns[filename].items():
                    df_melted[col] = value

            # Validate columns and enforce data types based on meta.yml
            validate_columns(df_melted, datatypes)
            enforce_data_types(df_melted, datatypes)

            # Save the transformed and filtered data to a new CSV file in the output directory
            output_file = os.path.join(output_directory, filename)
            df_melted.to_csv(output_file, index=False)
            print(f"Preprocessed data saved to {output_file}")

            # Append to the list of all dataframes
            all_dataframes.append(df_melted)
        except FileNotFoundError:
            print(f"Error: File '{input_file}' not found.")
        except KeyError as e:
            print(f"Error: Missing expected column in '{filename}'. Details: {e}")

# Concatenate all dataframes and save to CSV file
if all_dataframes:
    df = pd.concat(all_dataframes, ignore_index=True)
    df.to_csv(os.path.join(output_directory, 'all.csv'), index=False)
    print("All data combined and saved to all.csv")

    # Format a new csv to import into influx

    ## Generate headers
    group_line, datatype_line, default_line, import_df = generate_csv_headers(group, datatypes, defaults, df)

    ## Add an empty column to the first position in import_df
    import_df.insert(0, "", "")

    ## Save the transformed data with headers
    output_file = os.path.join(output_directory, 'import.csv')
    with open(output_file, "w") as out_csv:
        out_csv.write(group_line + "\n")
        out_csv.write(datatype_line + "\n")
        out_csv.write(default_line + "\n")
        import_df.to_csv(out_csv, index=False)

    # Test the import.csv file in a influx import dry run

    ## Build the dry-run command
    command = [ "influx", "write", "dryrun", "--file", output_file]

    try:
        # Run the command
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        
        # Check and process the logged errors
        if result.stderr:
            print("Dry-run logged errors:")
            print(result.stderr)
            print(f"\nImport file for Influx has errors review and fix errors, DO NOT import {output_file} until fixed!")
        else:
            print("Influx import dry-run successful!")
            print(f"Import file for Influx saved to {output_file}, ready for import!")
    except subprocess.CalledProcessError as e:
        print("Process exited during dry-run:")
        print(e.stderr)