# Importing Influx Data

This directory holds tools to import data into Influx.

- **meta.yml**: This holds metadata for columns being imported into Influx. If the column is NOT listed here preprocessing will FAIL.
- **preprocess.py**: This script will format the data correctly so that influx can process it correctly.

## Preprocessing the Data

Before importing the data into Influx it needs to be preprocessed so that Influx can correctly import it.
1) You need to modify the variables in `preprocess.py` in the section where it says `CHANGE THE VARIABLES TO YOUR PREPROCESSING NEEDS`.
2) Once that is done, Run `preprocess.py` to do this.
3) You have now generated `import.csv`, this holds the data correctly formatted so that Influx can process it correctly.

## Importing into Influx

After running preprocess.py run the following command to import the output file `import.csv` into influx.

```sh
influx write --bucket waggle --file import.csv --debug
```