"""
Parse FR Y-9C data from CSV files to parquet format.

This script processes downloaded FR Y-9C CSV files and converts them to
standardized parquet format with parallelization support.

Features:
- Automatically extracts ZIP files (BHCF*.zip) before parsing
- Applies data dictionary metadata to parquet columns (if available)
- Handles CSV format: bhcfYYQQ.csv (e.g., bhcf2103.csv for 2021 Q1)
- Removes separator rows that must be removed
- RSSD9001 is the bank holding company identifier

Usage:
    # Parse all files with default settings (uses data/raw -> data/processed)
    python 04_parse_data.py

    # Specify custom directories
    python 04_parse_data.py \\
        --input-dir data/raw \\
        --output-dir data/processed

    # Specify number of workers
    python 04_parse_data.py --workers 8

    # Disable parallelization
    python 04_parse_data.py --no-parallel
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import argparse
import sys
import zipfile
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing
import re


# Global cache for data dictionary
_DATA_DICTIONARY = None
_DICTIONARY_PATH = None


def load_data_dictionary(dict_path: Path) -> dict:
    """
    Load the data dictionary for variable descriptions.

    Args:
        dict_path: Path to data_dictionary.parquet

    Returns:
        Dictionary mapping variable names to descriptions
    """
    global _DATA_DICTIONARY, _DICTIONARY_PATH

    # Return cached if already loaded
    if _DATA_DICTIONARY is not None and _DICTIONARY_PATH == dict_path:
        return _DATA_DICTIONARY

    if not dict_path.exists():
        return {}

    try:
        df = pd.read_parquet(dict_path)
        # Create mapping: Variable -> ItemName (short description)
        _DATA_DICTIONARY = dict(zip(df['Variable'], df['ItemName']))
        _DICTIONARY_PATH = dict_path
        return _DATA_DICTIONARY
    except Exception:
        return {}


def write_parquet_with_metadata(df: pd.DataFrame, output_path: Path, dict_path: Path = None):
    """
    Write DataFrame to parquet with column descriptions as metadata.

    Args:
        df: DataFrame to write
        output_path: Path for output parquet file
        dict_path: Path to data dictionary (optional)
    """
    # Load dictionary if available
    var_descriptions = {}
    if dict_path:
        var_descriptions = load_data_dictionary(dict_path)

    # Convert to pyarrow Table
    table = pa.Table.from_pandas(df, preserve_index=False)

    # Add column metadata if dictionary available
    if var_descriptions:
        # Build new schema with field metadata
        new_fields = []
        for field in table.schema:
            col_name = field.name
            if col_name in var_descriptions:
                # Add description as field metadata
                metadata = {b'description': var_descriptions[col_name].encode('utf-8')}
                new_field = field.with_metadata(metadata)
            else:
                new_field = field
            new_fields.append(new_field)

        # Create new schema and cast table
        new_schema = pa.schema(new_fields)
        table = table.cast(new_schema)

    # Write parquet
    pq.write_table(table, output_path, compression='snappy')


def extract_zip_files(input_dir: Path) -> list:
    """
    Extract ZIP files in the input directory.

    Looks for BHCF*.zip files and extracts the TXT files, renaming them to CSV format.

    Args:
        input_dir: Directory containing ZIP files

    Returns:
        List of extracted CSV file paths
    """
    zip_files = list(input_dir.glob('BHCF*.zip')) + list(input_dir.glob('bhcf*.zip'))

    if not zip_files:
        return []

    extracted_files = []

    for zip_path in zip_files:
        try:
            # Extract quarter info from ZIP filename
            # Format: BHCF20210630.zip -> bhcf2106.csv
            match = re.search(r'bhcf(\d{4})(\d{2})(\d{2})', zip_path.name.lower())

            if not match:
                print(f"Skipping {zip_path.name}: Cannot parse filename")
                continue

            year = int(match.group(1))
            month = int(match.group(2))

            # Determine quarter from month
            quarter_months = {'03': '03', '06': '06', '09': '09', '12': '12'}
            month_str = f"{month:02d}"

            if month_str not in quarter_months:
                print(f"Skipping {zip_path.name}: Invalid month {month_str}")
                continue

            # Generate CSV filename: bhcfYYQQ.csv
            year_short = year % 100
            csv_filename = f"bhcf{year_short:02d}{month_str}.csv"
            csv_path = input_dir / csv_filename

            # Skip if CSV already exists
            if csv_path.exists():
                print(f"Skipping {zip_path.name}: {csv_filename} already exists")
                continue

            # Extract ZIP file
            print(f"Extracting {zip_path.name}...")
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # Find the BHCF TXT file (case-insensitive)
                bhcf_file = None
                for file in zip_ref.namelist():
                    if file.upper().startswith('BHCF') and file.upper().endswith('.TXT'):
                        bhcf_file = file
                        break

                if not bhcf_file:
                    print(f"  WARNING: No BHCF*.TXT file found in {zip_path.name}")
                    continue

                # Extract and rename to .csv
                with zip_ref.open(bhcf_file) as source:
                    with open(csv_path, 'wb') as target:
                        target.write(source.read())

                csv_size_mb = csv_path.stat().st_size / (1024 * 1024)
                print(f"  Extracted to {csv_filename} ({csv_size_mb:.2f} MB)")
                extracted_files.append(csv_path)

        except zipfile.BadZipFile:
            print(f"ERROR: {zip_path.name} is not a valid ZIP file")
        except Exception as e:
            print(f"ERROR extracting {zip_path.name}: {e}")

    return extracted_files


def extract_quarter_from_filename(filename):
    """
    Extract year and quarter from filename.

    Format: bhcfYYQQ.csv
    - YY: 2-digit year (e.g., 21 for 2021)
    - QQ: Quarter end month (03, 06, 09, 12)

    Returns:
        Tuple of (year, quarter, quarter_str) or (None, None, None)
    """
    # Match bhcfYYQQ pattern
    match = re.search(r'bhcf(\d{2})(\d{2})', filename.lower())

    if not match:
        return None, None, None

    year_code = match.group(1)
    month_code = match.group(2)

    # Convert 2-digit year to 4-digit
    year_2digit = int(year_code)
    year = 2000 + year_2digit if year_2digit < 50 else 1900 + year_2digit

    # Convert month to quarter
    quarter_map = {'03': 1, '06': 2, '09': 3, '12': 4}
    quarter = quarter_map.get(month_code)

    if quarter is None:
        return None, None, None

    quarter_str = f"{year}Q{quarter}"

    return year, quarter, quarter_str


def process_fry9c_csv(csv_path):
    """
    Parse FR Y-9C CSV file and split by filer type.

    Handles:
    - Auto-detection of delimiter (comma for old Chicago Fed, caret for new FFIEC)
    - Separator row removal
    - Column name standardization (uppercase)
    - RSSD9001 → RSSD_ID renaming
    - REPORTING_PERIOD addition
    - Splitting by filer type (Y-9C, Y-9LP, Y-9SP)
    - Retaining only relevant variables for each filer type

    Args:
        csv_path: Path to CSV file

    Returns:
        Dictionary with keys 'y_9c', 'y_9lp', 'y_9sp' containing DataFrames
        Each DataFrame contains only relevant columns for that filer type
    """
    # Auto-detect delimiter
    # Chicago Fed files (pre-2021 Q2) use comma
    # FFIEC files (2021 Q2+) use caret ^
    with open(csv_path, 'r', encoding='utf-8', errors='ignore') as f:
        first_line = f.readline()
        delimiter = '^' if '^' in first_line else ','

    # Read CSV with detected delimiter
    # Try UTF-8 first, fallback to latin-1 for older files
    # If parsing errors occur, skip bad lines
    try:
        df = pd.read_csv(csv_path, delimiter=delimiter, low_memory=False, dtype=str, encoding='utf-8')
    except UnicodeDecodeError:
        df = pd.read_csv(csv_path, delimiter=delimiter, low_memory=False, dtype=str, encoding='latin-1')
    except pd.errors.ParserError:
        # Some files have malformed rows - skip them (python engine doesn't support low_memory)
        df = pd.read_csv(csv_path, delimiter=delimiter, dtype=str, encoding='latin-1',
                        on_bad_lines='skip', engine='python')

    # Remove separator row (second row with "--------")
    if len(df) > 0:
        # Check first column for separator
        first_col = df.columns[0]
        df = df[df[first_col] != '--------']

    # Ensure uppercase column names
    df.columns = [str(col).upper().strip() for col in df.columns]

    # Rename RSSD9001 to RSSD_ID for consistency
    if 'RSSD9001' in df.columns:
        df = df.rename(columns={'RSSD9001': 'RSSD_ID'})
    elif 'RSSD_ID' not in df.columns:
        raise ValueError(f"RSSD identifier column not found in {csv_path.name}")

    # Convert RSSD_ID to integer
    df['RSSD_ID'] = pd.to_numeric(df['RSSD_ID'], errors='coerce')
    df = df.dropna(subset=['RSSD_ID'])
    df['RSSD_ID'] = df['RSSD_ID'].astype(int)

    # Add REPORTING_PERIOD from filename
    filename = csv_path.stem
    year, quarter, quarter_str = extract_quarter_from_filename(filename)

    if year is None:
        raise ValueError(f"Could not extract quarter from filename: {filename}")

    # Create reporting period (quarter end date)
    reporting_period = pd.Timestamp(year=year, month=quarter*3, day=1) + pd.offsets.QuarterEnd(0)
    df['REPORTING_PERIOD'] = reporting_period

    # Identify columns by prefix
    # BHCK = FR Y-9C (quarterly filers)
    # BHCP = FR Y-9LP (quarterly, large/complex)
    # BHSP = FR Y-9SP (semi-annual, smaller institutions)
    bhck_cols = [c for c in df.columns if c.startswith('BHCK')]
    bhcp_cols = [c for c in df.columns if c.startswith('BHCP')]
    bhsp_cols = [c for c in df.columns if c.startswith('BHSP')]

    # Count non-null values for each prefix per row to determine filer type
    df['bhck_count'] = df[bhck_cols].notna().sum(axis=1)
    df['bhcp_count'] = df[bhcp_cols].notna().sum(axis=1)
    df['bhsp_count'] = df[bhsp_cols].notna().sum(axis=1)

    def classify_filer(row):
        counts = {'FR_Y9C': row['bhck_count'], 'FR_Y9LP': row['bhcp_count'], 'FR_Y9SP': row['bhsp_count']}
        max_type = max(counts, key=counts.get)
        if counts[max_type] > 0:
            return max_type
        return 'UNKNOWN'

    df['FILER_TYPE'] = df.apply(classify_filer, axis=1)
    df = df.drop(columns=['bhck_count', 'bhcp_count', 'bhsp_count'])

    # Split by filer type and retain only relevant columns
    result = {}

    # Y-9C filers: keep BHCK columns
    y9c_df = df[df['FILER_TYPE'] == 'FR_Y9C'].copy()
    if len(y9c_df) > 0:
        metadata_cols = ['RSSD_ID', 'REPORTING_PERIOD']
        relevant_cols = [c for c in bhck_cols if c in y9c_df.columns]
        y9c_df = y9c_df[metadata_cols + relevant_cols]
        result['y_9c'] = y9c_df

    # Y-9LP filers: keep BHCP columns
    y9lp_df = df[df['FILER_TYPE'] == 'FR_Y9LP'].copy()
    if len(y9lp_df) > 0:
        metadata_cols = ['RSSD_ID', 'REPORTING_PERIOD']
        relevant_cols = [c for c in bhcp_cols if c in y9lp_df.columns]
        y9lp_df = y9lp_df[metadata_cols + relevant_cols]
        result['y_9lp'] = y9lp_df

    # Y-9SP filers: keep BHSP columns
    y9sp_df = df[df['FILER_TYPE'] == 'FR_Y9SP'].copy()
    if len(y9sp_df) > 0:
        metadata_cols = ['RSSD_ID', 'REPORTING_PERIOD']
        relevant_cols = [c for c in bhsp_cols if c in y9sp_df.columns]
        y9sp_df = y9sp_df[metadata_cols + relevant_cols]
        result['y_9sp'] = y9sp_df

    return result


def process_file_wrapper(args_tuple):
    """
    Wrapper function for parallel processing.

    Args:
        args_tuple: (file_path_str, output_dir_str, dict_path_str or None)

    Returns:
        Tuple of (status, quarter_str, message)
    """
    file_path_str, output_dir_str, dict_path_str = args_tuple

    file_path = Path(file_path_str)
    output_dir = Path(output_dir_str)
    dict_path = Path(dict_path_str) if dict_path_str else None

    try:
        # Extract quarter from filename
        year, quarter, quarter_str = extract_quarter_from_filename(file_path.name)

        if quarter_str is None:
            return ('error', None, f"Could not extract quarter from {file_path.name}")

        # Process CSV - returns dictionary of DataFrames by filer type
        filer_dfs = process_fry9c_csv(file_path)

        if not filer_dfs:
            return ('error', quarter_str, "No data found for any filer type")

        # Save separate parquet files for each filer type
        results = []
        for filer_type, df in filer_dfs.items():
            # Create subdirectory for filer type
            filer_output_dir = output_dir / filer_type
            filer_output_dir.mkdir(parents=True, exist_ok=True)

            # Save parquet file with metadata
            output_path = filer_output_dir / f"{quarter_str}.parquet"
            write_parquet_with_metadata(df, output_path, dict_path)

            results.append(f"{filer_type}: {len(df):,} filers, {len(df.columns)-2} vars")

        message = " | ".join(results)
        return ('success', quarter_str, message)

    except Exception as e:
        import traceback
        error_msg = f"Error processing {file_path.name}: {str(e)}\n{traceback.format_exc()}"
        return ('error', None, error_msg)


def main():
    parser = argparse.ArgumentParser(
        description='Parse FR Y-9C data from CSV files to parquet format',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Parse with default settings (data/raw -> data/processed)
  # Automatically extracts any ZIP files found
  python 04_parse_data.py

  # Limit to 4 workers
  python 04_parse_data.py --workers 4

  # Disable parallelization
  python 04_parse_data.py --no-parallel

  # Use custom directories
  python 04_parse_data.py \\
      --input-dir /path/to/csvs \\
      --output-dir /path/to/output

Features:
  - Automatically extracts BHCF*.zip files to CSV before parsing
  - Handles both manually downloaded ZIPs and automated downloads
  - Parallel processing for faster extraction and parsing

Output Format:
  - Quarterly parquet files: {YEAR}Q{Q}.parquet
  - Columns: RSSD_ID, REPORTING_PERIOD, ... (alphabetical)
  - All numeric values preserved as-is
        """
    )

    parser.add_argument(
        '--input-dir',
        type=str,
        default='data/raw',
        help='Directory containing CSV files (default: data/raw)'
    )

    parser.add_argument(
        '--output-dir',
        type=str,
        default='data/processed',
        help='Directory to save parquet files (default: data/processed)'
    )

    parser.add_argument(
        '--workers',
        type=int,
        default=None,
        help='Number of parallel workers (default: all CPUs)'
    )

    parser.add_argument(
        '--no-parallel',
        action='store_true',
        help='Disable parallel processing'
    )

    parser.add_argument(
        '--start-year',
        type=int,
        help='Only process files from this year onwards'
    )

    parser.add_argument(
        '--end-year',
        type=int,
        help='Only process files up to this year'
    )

    args = parser.parse_args()

    # Setup paths
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)

    if not input_dir.exists():
        print(f"ERROR: Input directory does not exist: {input_dir}")
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)

    # Extract ZIP files first (if any)
    print("Checking for ZIP files to extract...")
    extracted_files = extract_zip_files(input_dir)
    if extracted_files:
        print(f"Extracted {len(extracted_files)} ZIP file(s)\n")
    else:
        print("No ZIP files found to extract\n")

    # Find CSV files
    files_to_process = (
        list(input_dir.glob('bhcf*.csv')) +
        list(input_dir.glob('BHCF*.csv'))
    )

    # Filter by year if specified
    if args.start_year or args.end_year:
        filtered_files = []
        for f in files_to_process:
            year, quarter, quarter_str = extract_quarter_from_filename(f.name)
            if year is None:
                continue
            if args.start_year and year < args.start_year:
                continue
            if args.end_year and year > args.end_year:
                continue
            filtered_files.append(f)
        files_to_process = filtered_files

    files_to_process.sort()

    if not files_to_process:
        print("No CSV files found to process")
        return 1

    # Determine worker count
    if args.no_parallel:
        workers = 1
    elif args.workers:
        workers = args.workers
    else:
        workers = multiprocessing.cpu_count()

    # Check for data dictionary
    dict_path = output_dir / 'data_dictionary.parquet'
    dict_path_str = str(dict_path) if dict_path.exists() else None

    print("="*80)
    print("FR Y-9C DATA PARSING")
    print("="*80)
    print(f"Input directory: {input_dir}")
    print(f"Output directory: {output_dir}")
    print(f"Files to process: {len(files_to_process)}")
    print(f"Parallel workers: {workers}")
    if dict_path_str:
        print(f"Data dictionary: {dict_path} (metadata will be applied)")
    else:
        print("Data dictionary: Not found (run 03_parse_dictionary.py to add metadata)")
    print("="*80)

    # Process files
    successful = []
    skipped = []
    failed = []

    if workers == 1:
        # Sequential processing
        print("\nProcessing sequentially...")
        for file_path in files_to_process:
            status, quarter_str, message = process_file_wrapper((str(file_path), str(output_dir), dict_path_str))

            if status == 'success':
                successful.append(quarter_str)
                print(f"[{quarter_str}] {message}")
            elif status == 'skipped':
                skipped.append(quarter_str)
                print(f"[{quarter_str}] {message}")
            else:
                failed.append(quarter_str if quarter_str else file_path.name)
                print(f"[ERROR] {message}")

    else:
        # Parallel processing
        print(f"\nProcessing in parallel with {workers} workers...")

        with ProcessPoolExecutor(max_workers=workers) as executor:
            # Submit all tasks
            future_to_file = {
                executor.submit(process_file_wrapper, (str(f), str(output_dir), dict_path_str)): f
                for f in files_to_process
            }

            # Process results as they complete
            completed = 0
            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                completed += 1

                try:
                    status, quarter_str, message = future.result()

                    if status == 'success':
                        successful.append(quarter_str)
                        print(f"[{quarter_str}] {message}")
                    elif status == 'skipped':
                        skipped.append(quarter_str)
                        print(f"[{quarter_str}] {message}")
                    else:
                        failed.append(quarter_str if quarter_str else file_path.name)
                        print(f"[ERROR] {message}")

                except Exception as e:
                    print(f"[ERROR] Unexpected error processing {file_path.name}: {e}")
                    failed.append(file_path.name)

                # Progress update
                if completed % 10 == 0 or completed == len(files_to_process):
                    print(f"  Progress: {completed}/{len(files_to_process)} files processed")

    # Summary
    print("\n" + "="*80)
    print("PARSING SUMMARY")
    print("="*80)
    print(f"Successfully processed: {len(successful)} files")
    if successful:
        successful_sorted = sorted(successful)
        print(f"  Quarters: {successful_sorted[0]} to {successful_sorted[-1]}")

    if skipped:
        print(f"\nSkipped (already exist): {len(skipped)} files")
        if len(skipped) <= 20:
            print(f"  Quarters: {sorted(skipped)}")

    if failed:
        print(f"\nFailed: {len(failed)} files")
        print(f"  {failed}")

    print("\n" + "="*80)
    print("NEXT STEPS")
    print("="*80)
    print("\nVerify the parsed data:")
    print(f"  python 05_summarize.py --input-dir {output_dir}")
    print("\nOutput structure:")
    print(f"  {output_dir}/y_9c/    - FR Y-9C filers (BHCK variables)")
    print(f"  {output_dir}/y_9lp/   - FR Y-9LP filers (BHCP variables)")
    print(f"  {output_dir}/y_9sp/   - FR Y-9SP filers (BHSP variables)")

    return 0 if not failed else 1


if __name__ == '__main__':
    sys.exit(main())
