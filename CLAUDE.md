# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Data pipeline for downloading and processing FR Y-9 Bank Holding Company financial data (1986-present). Downloads raw CSV data from Chicago Fed (historical) and FFIEC (recent), parses it by filer type, and converts to parquet format.

## Commands

```bash
# Setup
pip install -r requirements.txt

# Download raw data from Chicago Fed (1986 Q3 - 2021 Q1)
python 01_download_data.py
# For 2021 Q2+, manually download ZIPs from FFIEC to data/raw/

# Optional: Add variable descriptions as parquet metadata
python 02_download_dictionary.py  # Download MDRM data dictionary
python 03_parse_dictionary.py     # Parse dictionary for FR Y-9 variables

# Parse CSV to parquet (auto-extracts ZIPs, splits by filer type)
python 04_parse_data.py

# Verify/summarize parsed data
python 05_summarize.py

# Cleanup to conserve disk space
python 06_cleanup.py --extracted  # Remove extracted CSVs (keeps ZIPs)
python 06_cleanup.py --raw        # Remove all raw files
```

### Script Options
- `04_parse_data.py --workers N` - limit parallel workers (for low-memory systems)
- `04_parse_data.py --no-parallel` - disable parallelization
- `01_download_data.py --start-year YYYY --end-year YYYY` - download specific date range

## Architecture

### Data Flow
```
01_download_data.py    04_parse_data.py              05_summarize.py
Chicago Fed/FFIEC → data/raw/*.csv → data/processed/{y_9c,y_9lp,y_9sp}/*.parquet → summary
```

### Filer Types
The pipeline separates data by regulatory filing type based on which column prefix has the most populated fields:
- **y_9c/** - FR Y-9C filers (BHCK#### columns) - quarterly, ~350-400 institutions
- **y_9lp/** - FR Y-9LP filers (BHCP#### columns) - quarterly, ~60-70 large/complex institutions
- **y_9sp/** - FR Y-9SP filers (BHSP#### columns) - semi-annual (Q2/Q4 only), ~3,400-5,500 smaller institutions

### Key Implementation Details
- `01_download_data.py`: Downloads from Chicago Fed only (1986 Q3 - 2021 Q1); FFIEC requires manual download
- `04_parse_data.py`: Auto-detects CSV delimiter (comma for Chicago Fed, caret for FFIEC); applies dictionary metadata if available
- `03_parse_dictionary.py`: Filters MDRM to BHCK/BHCP/BHSP variables; outputs data_dictionary.parquet
- Filer classification logic is in `process_fry9c_csv()` - classifies by counting non-null values per prefix
- RSSD9001 is renamed to RSSD_ID; REPORTING_PERIOD is derived from filename

## Data Notes

- Q1/Q3 have fewer filers (no Y-9SP) - this is expected per Federal Reserve filing requirements
- 2021 Q2+ may require manual ZIP download from FFIEC if automated download fails
- Variable counts vary by era (~1,200 in 1986, ~2,500 recent)
