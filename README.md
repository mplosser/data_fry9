# FR Y-9C Bank Holding Company Data Pipeline

Automated pipeline for downloading and processing FR Y-9C (Consolidated Financial Statements for Bank Holding Companies) data (1986-2025).

## Overview
Downloads raw data and converts it to parquet format. Automatically handles ZIP file extraction for manually downloaded FFIEC data.

**Data Coverage:**
- **Quarters**: 1986 Q3 - Present
- **Frequency**: Quarterly
- **Entities**: 200-500 bank holding companies per quarter
- **Variables**: 1,900-2,500 variables per quarter

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

**Note**: 2021 Q2+ requires manual download from [FFIEC](https://www.ffiec.gov/npw/FinancialReport/FinancialDataDownload). Simply download the ZIP files to `data/raw/` - extraction is automatic.

### 2. Parse to Parquet

```bash
# Simple usage with defaults (data/raw -> data/processed)
python parse.py

# Limit workers for low-memory systems
python parse.py --workers 4
```

**Note**: The parse script automatically extracts any `BHCF*.zip` files found in `data/raw/` before parsing.

**Note on FILER_TYPE**: If you parsed data before this feature was added, you'll need to delete the old parquet files and reparse to get the FILER_TYPE column:
```bash
rm -rf data/processed/*.parquet
python parse.py
```

### 3. Verify Data

```bash
# Generate summary (uses data/processed by default)
python summarize.py

# Save summary to CSV
python summarize.py --output-csv fry9c_summary.csv
```

## Data Sources

FR Y-9C data comes from different sources depending on the period:

| Period | Source | Format | Automation |
|--------|--------|--------|------------|
| 1986 Q3 - 2021 Q1 | [Chicago Fed](https://www.chicagofed.org/banking/financial-institution-reports/bhc-data) | CSV (bhcfYYQQ.csv) | ✅ Automated download |
| 2021 Q2 - Present | [FFIEC NIC](https://www.ffiec.gov/npw/FinancialReport/FinancialDataDownload) | ZIP (BHCFYYYYMMDD.zip) | ⚠️ Manual download, ✅ Auto extraction |

## Output Format

All data is saved as parquet files in `data/processed/`:

```
data/processed/
├── 1986Q3.parquet
├── 1986Q4.parquet
├── ...
├── 2025Q3.parquet
```

**File Structure:**
- **Rows**: One per bank holding company
- **Columns**:
  - `RSSD_ID` - RSSD identifier (integer)
  - `REPORTING_PERIOD` - Quarter end date (datetime)
  - `FILER_TYPE` - Filing form type (FR_Y9C, FR_Y9LP, or FR_Y9SP)
  - RSSD#### - Identification codes
  - BHCK#### - FR Y-9C variables (standard quarterly)
  - BHCP#### - FR Y-9LP variables (large/complex quarterly)
  - BHSP#### - FR Y-9SP variables (smaller semi-annual)
  - BHCA#### - Regulatory capital items
  - Additional variables (alphabetical)

**Coverage:**
- ~160 quarters (1986 Q3 - 2025+)
- **Q1 & Q3**: 400-500 BHCs (large institutions filing quarterly)
- **Q2 & Q4**: 3,900-6,000 BHCs (all institutions - semi-annual + quarterly filers)
- 1,900-2,500 variables per quarter
- Complete data with no filtering or transformations

**Note**: The variation in BHC counts reflects regulatory filing requirements - larger institutions ($1B+ assets) file quarterly, while smaller institutions file semi-annually (Q2 and Q4 only).

## Pipeline Scripts

### Core Scripts

| Script | Purpose | Input | Output | Performance |
|--------|---------|-------|--------|-------------|
| `download.py` | Download Chicago Fed data (1986-2021) | URLs | CSV files | ~2-3 min |
| `parse.py` | Extract ZIPs & parse CSV to parquet (parallel) | ZIP/CSV files | Parquet files | ~1-2 min (160 files) |
| `summarize.py` | Summarize parsed data | Parquet files | Summary table | ~5-10 sec (160 files) |

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

## Repository Structure

```
data_fry9/
├── README.md                     # This file
├── requirements.txt              # Python dependencies
├── .gitignore                    # Git exclusions
│
├── download.py                   # Download FR Y-9C data
├── parse.py                      # CSV to parquet (parallel)
└── summarize.py                  # Data audit
```

## Data Quality Notes

### Quarterly Filing Patterns and Filer Types

The number of BHCs varies significantly by quarter due to regulatory filing requirements. There are **three types of filers**:

| Form Type | Description | Filing Frequency | Typical Count | Column Prefix |
|-----------|-------------|------------------|---------------|---------------|
| **FR Y-9C** | Standard quarterly report | Quarterly (all 4 quarters) | ~380-400 | BHCK#### |
| **FR Y-9LP** | Large/complex institutions | Quarterly (all 4 quarters) | ~60-70 | BHCP#### |
| **FR Y-9SP** | Smaller institutions | Semi-annual (Q2 & Q4 only) | ~3,400-5,500 | BHSP#### |

**Resulting patterns by quarter:**
- **Q1 & Q3**: ~400-500 BHCs total (Y-9C + Y-9LP quarterly filers only)
- **Q2 & Q4**: ~4,000-6,000 BHCs total (Y-9C + Y-9LP + Y-9SP all filers)

This pattern is consistent across all years from 1986 onwards and is **not an error** - it reflects the Federal Reserve's filing requirements.

**FILER_TYPE Column**: The parse script automatically classifies each BHC into one of these three types based on which column prefix (BHCK/BHCP/BHSP) has the most populated fields in their filing.

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
