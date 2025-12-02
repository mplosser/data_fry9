# FR Y-9C Bank Holding Company Data Pipeline

Automated pipeline for downloading and processing FR Y-9 data (1986-2025).

## Overview
Downloads raw data, separates by filer type, and converts to parquet format. Automatically handles ZIP file extraction for manually downloaded FFIEC data.

**Data Coverage:**
- **Quarters**: 1986 Q3 - Present
- **Filer Types**:
  - FR Y-9C: Quarterly (Q1, Q2, Q3, Q4) - ~350-400 filers
  - FR Y-9LP: Quarterly (Q1, Q2, Q3, Q4) - ~60-70 filers
  - FR Y-9SP: Semi-annual (Q2, Q4 only) - ~3,400-5,500 filers
- **Variables**: 120-1,600 per filer type (after efficient filtering by prefix)

## Quick Start

**Simple 3-step process**:
1. `pip install -r requirements.txt` - Install dependencies
2. `python download.py` - Download data
3. `python parse.py` - Parse to parquet (with automatic ZIP extraction)

### Setup

Install Python dependencies for the tools:

```bash
pip install -r requirements.txt
```

### 1. Download Data

```bash
# Download all available Chicago Fed data (1986 Q3 - 2021 Q1)
python download.py

# Output: CSV files in data/raw/
```

**Note**: 2021 Q2+ requires manual download from [FFIEC](https://www.ffiec.gov/npw/FinancialReport/FinancialDataDownload). Simply download the ZIP files to `data/raw/`.

### 2. Parse to Parquet

```bash
# Simple usage with defaults (data/raw -> data/processed)
python parse.py

# Limit workers for low-memory systems
python parse.py --workers 4
```

**Note**: The parse script automatically extracts any `BHCF*.zip` files found in `data/raw/` before parsing.

### 3. Verify Data

```bash
# Generate quarterly breakdown summary (uses data/processed by default)
python summarize.py
```

The summary script provides:
- Quarterly breakdown showing filer counts and variable counts for each type (Y-9C, Y-9LP, Y-9SP)
- Clear visualization of semi-annual filing pattern for Y-9SP (Q2 & Q4 only shown as data, Q1 & Q3 shown as dashes)
- Summary statistics for each filer type (quarters, average filers, average variables, total size)
- Overall coverage statistics across all filer types

## Data Sources

FR Y-9C data comes from different sources depending on the period:

| Period | Source | Format | Automation |
|--------|--------|--------|------------|
| 1986 Q3 - 2021 Q1 | [Chicago Fed](https://www.chicagofed.org/banking/financial-institution-reports/bhc-data) | CSV (bhcfYYQQ.csv) | ✅ Automated download |
| 2021 Q2 - Present | [FFIEC NIC](https://www.ffiec.gov/npw/FinancialReport/FinancialDataDownload) | ZIP (BHCFYYYYMMDD.zip) | ⚠️ Manual download, ✅ Auto extraction |

## Output Format

Data is organized by filer type in separate subdirectories under `data/processed/`:

```
data/processed/
├── y_9c/                    # FR Y-9C filers (quarterly)
│   ├── 1986Q3.parquet
│   ├── 1986Q4.parquet
│   ├── ...
│   └── 2025Q3.parquet
├── y_9lp/                   # FR Y-9LP filers (quarterly)
│   ├── 1986Q3.parquet
│   ├── 1986Q4.parquet
│   ├── ...
│   └── 2025Q3.parquet
└── y_9sp/                   # FR Y-9SP filers (semi-annual)
    ├── 1986Q4.parquet       # Q2 & Q4 only
    ├── 1987Q2.parquet
    ├── 1987Q4.parquet
    ├── ...
    └── 2025Q2.parquet
```

**File Structure (each parquet file):**
- **Rows**: One per bank holding company of that filer type
- **Columns**:
  - `RSSD_ID` - RSSD identifier (integer)
  - `REPORTING_PERIOD` - Quarter end date (datetime)
  - Variable columns specific to filer type:
    - **y_9c/**: BHCK#### variables only (~1,600 columns)
    - **y_9lp/**: BHCP#### variables only (~174 columns)
    - **y_9sp/**: BHSP#### variables only (~126 columns)

**Coverage by Filer Type:**
- **FR Y-9C** (y_9c/): ~350-400 filers per quarter, all quarters (Q1, Q2, Q3, Q4)
- **FR Y-9LP** (y_9lp/): ~60-70 filers per quarter, all quarters (Q1, Q2, Q3, Q4)
- **FR Y-9SP** (y_9sp/): ~3,400-5,500 filers per quarter, semi-annual only (Q2, Q4)

## Pipeline Scripts

### Core Scripts

| Script | Purpose | Input | Output | Performance |
|--------|---------|-------|--------|-------------|
| `download.py` | Download Chicago Fed data (1986-2021) | URLs | CSV files | ~2-3 min |
| `parse.py` | Extract ZIPs & parse CSV to parquet (parallel) | ZIP/CSV files | Parquet files by type | ~1-2 min (160 files) |
| `summarize.py` | Generate quarterly breakdown by filer type | Parquet files | Summary table | ~5-10 sec (390+ files) |
| `cleanup.py` | Remove raw/processed files to conserve space | File paths | - | Instant |

**Parallelization Options** (available for `parse.py`, `summarize.py`):
- **Default**: Uses all CPU cores for parallel processing
- `--workers N`: Specify number of parallel workers (e.g., `--workers 4`)
- `--no-parallel`: Disable parallel processing (slower but uses less memory)
- **Expected speedup**: 4-8x on multi-core CPUs

**Examples**:
```bash
# Default settings (data/raw -> data/processed)
# Automatically extracts any ZIP files before parsing
python parse.py

# Limit to 4 workers
python parse.py --workers 4

# Disable parallelization
python parse.py --no-parallel

# Custom directories
python parse.py --input-dir /path/to/csvs --output-dir /path/to/output
```

**Cleanup Options** (`cleanup.py`):
```bash
# Remove extracted CSVs only (keeps ZIPs as source)
python cleanup.py --extracted

# Remove all raw files (CSVs and ZIPs)
python cleanup.py --raw

# Remove processed parquet files
python cleanup.py --processed

# Preview what would be deleted (dry run)
python cleanup.py --raw --dry-run
```

## Repository Structure

```
data_fry9/
├── README.md                     # This file
├── CLAUDE.md                     # Claude Code guidance
├── requirements.txt              # Python dependencies
├── .gitignore                    # Git exclusions
│
├── download.py                   # Download FR Y-9C data
├── parse.py                      # CSV to parquet (parallel)
├── summarize.py                  # Data audit
└── cleanup.py                    # Remove files to save space
```

## Data Quality Notes

### Quarterly Filing Patterns and Filer Types

The number of BHCs varies significantly by quarter due to regulatory filing requirements. There are **three types of filers**:

| Form Type | Description | Filing Frequency | Typical Count | Column Prefix | Output Directory |
|-----------|-------------|------------------|---------------|---------------|------------------|
| **FR Y-9C** | Standard quarterly report | Quarterly (all 4 quarters) | ~350-400 | BHCK#### | y_9c/ |
| **FR Y-9LP** | Large/complex institutions | Quarterly (all 4 quarters) | ~60-70 | BHCP#### | y_9lp/ |
| **FR Y-9SP** | Smaller institutions | Semi-annual (Q2 & Q4 only) | ~3,400-5,500 | BHSP#### | y_9sp/ |

**Filing patterns by quarter:**
- **Q1 & Q3**: Only Y-9C (~350-400) and Y-9LP (~60-70) file quarterly
- **Q2 & Q4**: All filer types - Y-9C, Y-9LP, and Y-9SP (~4,000-6,000 total)

This pattern is consistent across all years from 1986 onwards and is **not an error** - it reflects the Federal Reserve's filing requirements.

**Data Organization**: The parse script automatically classifies each BHC into one of these three types based on which column prefix (BHCK/BHCP/BHSP) has the most populated fields, then saves each filer type to its own subdirectory with only the relevant variables for that type.

### File Format Changes

- **Pre-2021 Q2** (Chicago Fed): Comma-delimited CSV files
- **2021 Q2+** (FFIEC): Caret (^) delimited CSV files
- The `parse.py` script automatically detects the correct delimiter

### Variable Coverage

- Variable names and reporting requirements evolve over time
- Earlier quarters (1986-1990s) have fewer variables (~1,200)
- Recent quarters have more detailed breakdowns (~2,500 variables)
- Not all BHCs report all variables (smaller BHCs have fewer required fields)

## Additional Resources

- **FR Y-9C Form**: https://www.federalreserve.gov/apps/reportingforms/Report/Index/FR_Y-9C
- **Chicago Fed BHC Data**: https://www.chicagofed.org/banking/financial-institution-reports/bhc-data
- **FFIEC NIC** (2021 Q2+): https://www.ffiec.gov/npw/FinancialReport/FinancialDataDownload
- **Federal Reserve MDRM**: Variable definitions and documentation
