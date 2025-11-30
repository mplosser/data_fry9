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

**Simple 3-step process**: Install dependencies → Download data → Parse to parquet (with automatic ZIP extraction)

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
# Parse with parallelization (recommended)
# Automatically extracts any ZIP files before parsing
python parse.py \
    --input-dir data/raw \
    --output-dir data/processed

# Limit workers for low-memory systems
python parse.py \
    --input-dir data/raw \
    --output-dir data/processed \
    --workers 4
```

**Note**: The parse script automatically extracts any `BHCF*.zip` files found in the input directory before parsing.

### 3. Verify Data

```bash
# Generate summary
python summarize.py --input-dir data/processed

# Save summary to CSV
python summarize.py \
    --input-dir data/processed \
    --output-csv fry9c_summary.csv
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
  - RSSD#### - Identification codes
  - BHCK#### - Balance sheet items
  - BHCP#### - Income statement items
  - BHCA#### - Regulatory capital items
  - Additional variables (alphabetical)

**Coverage:**
- ~160 quarters (1986 Q3 - 2025+)
- 200-500 BHCs per quarter
- 1,900-2,500 variables per quarter
- Complete data with no filtering or transformations

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
# Default parallel processing (all CPU cores)
# Automatically extracts any ZIP files before parsing
python parse.py \
    --input-dir data/raw \
    --output-dir data/processed

# Limit to 4 workers
python parse.py \
    --input-dir data/raw \
    --output-dir data/processed \
    --workers 4

# Disable parallelization
python parse.py \
    --input-dir data/raw \
    --output-dir data/processed \
    --no-parallel
```

## Repository Structure

```
data_fry9c/
├── README.md                     # This file
├── requirements.txt              # Python dependencies
├── .gitignore                    # Git exclusions
│
├── download.py                   # Download FR Y-9C data
├── parse.py                      # CSV to parquet (parallel)
└── summarize.py                  # Data audit
```

## Additional Resources

- **FR Y-9C Form**: https://www.federalreserve.gov/apps/reportingforms/Report/Index/FR_Y-9C
- **Chicago Fed BHC Data**: https://www.chicagofed.org/banking/financial-institution-reports/bhc-data
- **FFIEC NIC** (2021 Q2+): https://www.ffiec.gov/npw/FinancialReport/FinancialDataDownload
- **Federal Reserve MDRM**: Variable definitions and documentation
