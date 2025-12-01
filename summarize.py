"""
Summarize FR Y-9C parquet files.

This script scans all FR Y-9C parquet files and generates a summary showing:
- Coverage by quarter
- Number of BHCs per quarter
- Number of variables per quarter
- File sizes
- Overall statistics

Usage:
    # Summarize with default settings (uses data/processed)
    python summarize.py

    # Save summary to CSV
    python summarize.py --output-csv fry9c_summary.csv

    # Disable parallelization
    python summarize.py --no-parallel
"""

import pandas as pd
import argparse
import sys
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing


def analyze_file(file_path_str):
    """
    Analyze a single parquet file.

    Args:
        file_path_str: Path to parquet file as string

    Returns:
        Dictionary with file info or None if error
    """
    file_path = Path(file_path_str)

    try:
        # Read parquet file
        df = pd.read_parquet(file_path)

        # Extract quarter from filename (e.g., "2021Q1.parquet")
        quarter_str = file_path.stem

        # Get reporting period
        if 'REPORTING_PERIOD' in df.columns:
            reporting_period = df['REPORTING_PERIOD'].iloc[0]
        else:
            # Parse from filename as fallback
            year = int(quarter_str[:4])
            quarter = int(quarter_str[5])
            reporting_period = pd.Timestamp(year=year, month=quarter*3, day=1) + pd.offsets.QuarterEnd(0)

        # Get file size
        file_size_mb = file_path.stat().st_size / (1024 * 1024)

        # Get filer type counts
        filer_counts = {}
        if 'FILER_TYPE' in df.columns:
            filer_counts = df['FILER_TYPE'].value_counts().to_dict()

        # Count metadata columns (RSSD_ID, REPORTING_PERIOD, FILER_TYPE)
        metadata_cols = 3 if 'FILER_TYPE' in df.columns else 2

        return {
            'quarter': quarter_str,
            'date': reporting_period,
            'bhcs': len(df),
            'variables': len(df.columns) - metadata_cols,
            'total_columns': len(df.columns),
            'size_mb': file_size_mb,
            'file': file_path.name,
            'y9c': filer_counts.get('FR_Y9C', 0),
            'y9lp': filer_counts.get('FR_Y9LP', 0),
            'y9sp': filer_counts.get('FR_Y9SP', 0),
        }

    except Exception as e:
        print(f"Error processing {file_path.name}: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(
        description='Summarize FR Y-9C parquet files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate summary with defaults (uses data/processed)
  python summarize.py

  # Save to CSV
  python summarize.py --output-csv fry9c_summary.csv

  # Disable parallelization (for low-memory systems)
  python summarize.py --no-parallel

  # Use custom directory
  python summarize.py --input-dir /path/to/parquet
        """
    )

    parser.add_argument(
        '--input-dir',
        type=str,
        default='data/processed',
        help='Directory containing parquet files (default: data/processed)'
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
        '--output-csv',
        type=str,
        help='Save summary to CSV file'
    )

    args = parser.parse_args()

    # Setup
    input_dir = Path(args.input_dir)

    if not input_dir.exists():
        print(f"ERROR: Directory does not exist: {input_dir}")
        sys.exit(1)

    # Find parquet files
    parquet_files = sorted(input_dir.glob('*.parquet'))

    if not parquet_files:
        print(f"No parquet files found in {input_dir}")
        return 1

    # Determine worker count
    if args.no_parallel:
        workers = 1
    elif args.workers:
        workers = args.workers
    else:
        workers = multiprocessing.cpu_count()

    print("="*80)
    print("FR Y-9C DATA SUMMARY")
    print("="*80)
    print(f"Directory: {input_dir}")
    print(f"Files found: {len(parquet_files)}")
    print(f"Parallel workers: {workers}")
    print("="*80)

    # Analyze files
    results = []

    if workers == 1:
        # Sequential processing
        print("\nAnalyzing files sequentially...")
        for file_path in parquet_files:
            result = analyze_file(str(file_path))
            if result:
                results.append(result)
                print(f"  Processed {result['quarter']}")

    else:
        # Parallel processing
        print(f"\nProcessing files in parallel with {workers} workers...")

        with ProcessPoolExecutor(max_workers=workers) as executor:
            future_to_file = {
                executor.submit(analyze_file, str(f)): f
                for f in parquet_files
            }

            completed = 0
            for future in as_completed(future_to_file):
                completed += 1

                try:
                    result = future.result()
                    if result:
                        results.append(result)

                    # Progress update
                    if completed % 20 == 0 or completed == len(parquet_files):
                        print(f"  Processed {completed}/{len(parquet_files)} files...")

                except Exception as e:
                    print(f"  Error: {e}")

    if not results:
        print("\nNo valid data found")
        return 1

    # Create summary DataFrame
    df_summary = pd.DataFrame(results)
    df_summary = df_summary.sort_values('quarter')

    # Print summary table
    print()
    # Check if filer type data is available
    has_filer_types = 'y9c' in df_summary.columns and df_summary['y9c'].sum() > 0

    if has_filer_types:
        print(f"{'Quarter':<8} {'Date':<12} {'Total':>6} {'Y-9C':>6} {'Y-9LP':>6} {'Y-9SP':>6} {'Vars':>6} {'MB':>8}")
        print("-" * 8 + " " + "-" * 12 + " " + "-" * 6 + " " + "-" * 6 + " " + "-" * 6 + " " + "-" * 6 + " " + "-" * 6 + " " + "-" * 8)

        for _, row in df_summary.iterrows():
            print(f"{row['quarter']:<8} {row['date'].strftime('%Y-%m-%d'):<12} "
                  f"{row['bhcs']:>6,} {row['y9c']:>6,} {row['y9lp']:>6,} {row['y9sp']:>6,} "
                  f"{row['variables']:>6,} {row['size_mb']:>8.1f}")
    else:
        print(f"{'Quarter':<8} {'Date':<12} {'BHCs':>7} {'Variables':>10} {'Size (MB)':>10}")
        print("-" * 8 + " " + "-" * 12 + " " + "-" * 7 + " " + "-" * 10 + " " + "-" * 10)

        for _, row in df_summary.iterrows():
            print(f"{row['quarter']:<8} {row['date'].strftime('%Y-%m-%d'):<12} "
                  f"{row['bhcs']:>7,} {row['variables']:>10,} {row['size_mb']:>10.1f}")

    # Overall statistics
    print("\n" + "="*80)
    print("OVERALL STATISTICS")
    print("="*80)
    print(f"Total quarters: {len(df_summary)}")
    print(f"Date range: {df_summary['date'].min().strftime('%Y-%m-%d')} to {df_summary['date'].max().strftime('%Y-%m-%d')}")
    print(f"BHCs (avg): {df_summary['bhcs'].mean():,.0f}")
    print(f"BHCs (min): {df_summary['bhcs'].min():,}")
    print(f"BHCs (max): {df_summary['bhcs'].max():,}")

    if has_filer_types:
        print(f"\nFiler Type Breakdown (avg per quarter):")
        print(f"  FR Y-9C  (Quarterly):      {df_summary['y9c'].mean():>6.0f}")
        print(f"  FR Y-9LP (Quarterly):      {df_summary['y9lp'].mean():>6.0f}")
        print(f"  FR Y-9SP (Semi-annual):    {df_summary['y9sp'].mean():>6.0f}")

    print(f"\nVariables (avg): {df_summary['variables'].mean():.0f}")
    print(f"Variables (min): {df_summary['variables'].min()}")
    print(f"Variables (max): {df_summary['variables'].max()}")
    print(f"Total size: {df_summary['size_mb'].sum():.1f} MB")
    print("="*80)

    # Save to CSV if requested
    if args.output_csv:
        df_summary.to_csv(args.output_csv, index=False)
        print(f"\nSummary saved to: {args.output_csv}")

    return 0


if __name__ == '__main__':
    sys.exit(main())
