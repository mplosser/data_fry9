"""
Parse MDRM data dictionary for FR Y-9 variables.

This script processes the raw MDRM CSV and creates a filtered dictionary
containing only variables relevant to FR Y-9 reports (BHCK, BHCP, BHSP).

The output can be used to add metadata/descriptions to parquet files.

Usage:
    python 03_parse_dictionary.py
    python 03_parse_dictionary.py --input-dir data/raw --output-dir data/processed
"""

import argparse
import sys
import re
import html
from pathlib import Path

import pandas as pd


# FR Y-9 related mnemonics
FR_Y9_MNEMONICS = ['BHCK', 'BHCP', 'BHSP']


def clean_description(text: str) -> str:
    """
    Clean up description text.

    - Decode HTML entities
    - Remove carriage returns and normalize whitespace
    - Truncate if too long

    Args:
        text: Raw description text

    Returns:
        Cleaned description
    """
    if pd.isna(text):
        return ""

    # Decode HTML entities (&#x0D; etc.)
    text = html.unescape(str(text))

    # Replace common HTML-encoded characters
    text = text.replace('&#x0D;', ' ')
    text = text.replace('\r\n', ' ')
    text = text.replace('\r', ' ')
    text = text.replace('\n', ' ')

    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text).strip()

    return text


def parse_mdrm(input_path: Path, output_dir: Path) -> bool:
    """
    Parse MDRM CSV and create filtered dictionary for FR Y-9 variables.

    Args:
        input_path: Path to MDRM.csv
        output_dir: Directory to save processed dictionary

    Returns:
        True if successful, False otherwise
    """
    if not input_path.exists():
        print(f"ERROR: MDRM file not found: {input_path}")
        print("Run 'python 02_download_dictionary.py' first")
        return False

    print(f"Reading MDRM dictionary: {input_path}")

    # Read CSV (skip first row which just says "PUBLIC")
    df = pd.read_csv(
        input_path,
        skiprows=1,
        encoding='latin-1',
        low_memory=False,
        dtype=str
    )

    print(f"  Total entries: {len(df):,}")
    print(f"  Unique mnemonics: {df['Mnemonic'].nunique()}")

    # Filter to FR Y-9 related mnemonics
    df_filtered = df[df['Mnemonic'].isin(FR_Y9_MNEMONICS)].copy()
    print(f"\nFiltering to FR Y-9 mnemonics ({', '.join(FR_Y9_MNEMONICS)})...")
    print(f"  Entries after filter: {len(df_filtered):,}")

    # Create variable name (Mnemonic + Item Code)
    df_filtered['Variable'] = df_filtered['Mnemonic'] + df_filtered['Item Code'].str.strip()

    # Parse dates for sorting
    df_filtered['EndDateParsed'] = pd.to_datetime(
        df_filtered['End Date'],
        format='%m/%d/%Y %I:%M:%S %p',
        errors='coerce'
    )

    # Keep only the most recent entry for each variable (latest End Date)
    # This handles cases where a variable has multiple historical definitions
    df_filtered = df_filtered.sort_values('EndDateParsed', ascending=False)
    df_deduped = df_filtered.drop_duplicates(subset=['Variable'], keep='first').copy()

    print(f"  Unique variables: {len(df_deduped):,}")

    # Clean descriptions
    print("\nCleaning descriptions...")
    df_deduped['Description'] = df_deduped['Description'].apply(clean_description)
    df_deduped['ItemName'] = df_deduped['Item Name'].apply(clean_description)

    # Create output dataframe with relevant columns
    output_df = df_deduped[[
        'Variable',
        'Mnemonic',
        'Item Code',
        'ItemName',
        'Description',
        'Start Date',
        'End Date',
        'Reporting Form'
    ]].copy()

    output_df = output_df.rename(columns={
        'Item Code': 'ItemCode',
        'Start Date': 'StartDate',
        'End Date': 'EndDate',
        'Reporting Form': 'ReportingForm'
    })

    # Sort by variable name
    output_df = output_df.sort_values('Variable').reset_index(drop=True)

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Save as parquet (efficient for metadata lookups)
    parquet_path = output_dir / 'data_dictionary.parquet'
    output_df.to_parquet(parquet_path, index=False)
    parquet_size = parquet_path.stat().st_size / 1024
    print(f"\nSaved: {parquet_path} ({parquet_size:.1f} KB)")

    # Also save as CSV for human readability
    csv_path = output_dir / 'data_dictionary.csv'
    output_df.to_csv(csv_path, index=False)
    csv_size = csv_path.stat().st_size / 1024
    print(f"Saved: {csv_path} ({csv_size:.1f} KB)")

    # Print summary by mnemonic
    print("\n" + "=" * 60)
    print("DICTIONARY SUMMARY")
    print("=" * 60)

    for mnemonic in FR_Y9_MNEMONICS:
        count = len(output_df[output_df['Mnemonic'] == mnemonic])
        filer_type = {
            'BHCK': 'FR Y-9C',
            'BHCP': 'FR Y-9LP',
            'BHSP': 'FR Y-9SP'
        }.get(mnemonic, mnemonic)
        print(f"  {mnemonic} ({filer_type}): {count:,} variables")

    print(f"\n  Total: {len(output_df):,} variables")

    # Show sample entries
    print("\n" + "=" * 60)
    print("SAMPLE ENTRIES")
    print("=" * 60)

    for mnemonic in FR_Y9_MNEMONICS:
        sample = output_df[output_df['Mnemonic'] == mnemonic].head(2)
        for _, row in sample.iterrows():
            name = row['ItemName'][:50] + '...' if len(row['ItemName']) > 50 else row['ItemName']
            print(f"  {row['Variable']}: {name}")
    print()

    return True


def main():
    parser = argparse.ArgumentParser(
        description='Parse MDRM data dictionary for FR Y-9 variables',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python 03_parse_dictionary.py
  python 03_parse_dictionary.py --input-dir data/raw --output-dir data/processed

The script filters the MDRM to include only FR Y-9 related variables:
  - BHCK: FR Y-9C variables
  - BHCP: FR Y-9LP variables
  - BHSP: FR Y-9SP variables

Output files:
  - data_dictionary.parquet: Efficient format for metadata lookups
  - data_dictionary.csv: Human-readable format
        """
    )

    parser.add_argument(
        '--input-dir',
        type=str,
        default='data/raw',
        help='Directory containing MDRM.csv (default: data/raw)'
    )

    parser.add_argument(
        '--output-dir',
        type=str,
        default='data/processed',
        help='Directory to save processed dictionary (default: data/processed)'
    )

    args = parser.parse_args()

    print("=" * 60)
    print("MDRM DATA DICTIONARY PARSING")
    print("=" * 60)

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    input_path = input_dir / 'MDRM.csv'

    success = parse_mdrm(input_path, output_dir)

    if success:
        print("=" * 60)
        print("SUCCESS")
        print("=" * 60)
        print("\nNext step: Parse data with dictionary metadata")
        print("  python 04_parse_data.py")
        return 0
    else:
        return 1


if __name__ == "__main__":
    sys.exit(main())
