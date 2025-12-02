"""
Summarize FR Y-9 parquet files by filer type.

This script scans all FR Y-9 parquet files (organized by filer type) and generates:
- Quarterly breakdown by filer type (Y-9C, Y-9LP, Y-9SP)
- Number of filers per quarter by type
- Number of variables per filer type
- File sizes
- Overall statistics across all filer types

Usage:
    # Summarize with default settings (uses data/processed)
    python summarize.py

    # Disable parallelization
    python summarize.py --no-parallel
"""

import pandas as pd
import argparse
import sys
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing


def analyze_file(args_tuple):
    """
    Analyze a single parquet file.

    Args:
        args_tuple: (file_path_str, filer_type)

    Returns:
        Dictionary with file info or None if error
    """
    file_path_str, filer_type = args_tuple
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

        # Count metadata columns (RSSD_ID, REPORTING_PERIOD)
        metadata_cols = 2

        return {
            'quarter': quarter_str,
            'date': reporting_period,
            'filer_type': filer_type,
            'filers': len(df),
            'variables': len(df.columns) - metadata_cols,
            'total_columns': len(df.columns),
            'size_mb': file_size_mb,
            'file': file_path.name,
        }

    except Exception as e:
        print(f"Error processing {file_path.name}: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(
        description='Summarize FR Y-9 parquet files by filer type',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate summary with defaults (uses data/processed)
  python summarize.py

  # Disable parallelization (for low-memory systems)
  python summarize.py --no-parallel

  # Use custom directory
  python summarize.py --input-dir /path/to/parquet

Note: Expects subdirectories y_9c/, y_9lp/, y_9sp/ under input directory
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


    args = parser.parse_args()

    # Setup
    input_dir = Path(args.input_dir)

    if not input_dir.exists():
        print(f"ERROR: Directory does not exist: {input_dir}")
        sys.exit(1)

    # Find parquet files in subdirectories (y_9c, y_9lp, y_9sp)
    filer_types = ['y_9c', 'y_9lp', 'y_9sp']
    files_to_process = []

    for filer_type in filer_types:
        filer_dir = input_dir / filer_type
        if filer_dir.exists():
            for pq_file in sorted(filer_dir.glob('*.parquet')):
                files_to_process.append((str(pq_file), filer_type))

    if not files_to_process:
        print(f"No parquet files found in {input_dir}/{{y_9c,y_9lp,y_9sp}}")
        print("Note: Expected directory structure with subdirectories for each filer type")
        return 1

    # Determine worker count
    if args.no_parallel:
        workers = 1
    elif args.workers:
        workers = args.workers
    else:
        workers = multiprocessing.cpu_count()

    print("="*80)
    print("FR Y-9 DATA SUMMARY")
    print("="*80)
    print(f"Directory: {input_dir}")
    print(f"Files found: {len(files_to_process)}")
    print(f"Parallel workers: {workers}")
    print("="*80)

    # Analyze files
    results = []

    if workers == 1:
        # Sequential processing
        print("\nAnalyzing files sequentially...")
        for file_args in files_to_process:
            result = analyze_file(file_args)
            if result:
                results.append(result)
                print(f"  Processed {result['filer_type']}/{result['quarter']}")

    else:
        # Parallel processing
        print(f"\nProcessing files in parallel with {workers} workers...")

        with ProcessPoolExecutor(max_workers=workers) as executor:
            future_to_file = {
                executor.submit(analyze_file, f): f
                for f in files_to_process
            }

            completed = 0
            for future in as_completed(future_to_file):
                completed += 1

                try:
                    result = future.result()
                    if result:
                        results.append(result)

                    # Progress update
                    if completed % 20 == 0 or completed == len(files_to_process):
                        print(f"  Processed {completed}/{len(files_to_process)} files...")

                except Exception as e:
                    print(f"  Error: {e}")

    if not results:
        print("\nNo valid data found")
        return 1

    # Create summary DataFrame
    df_summary = pd.DataFrame(results)
    df_summary = df_summary.sort_values(['quarter', 'filer_type'])

    # Pivot to show filers by quarter and type
    pivot_filers = df_summary.pivot(index='quarter', columns='filer_type', values='filers')
    pivot_vars = df_summary.pivot(index='quarter', columns='filer_type', values='variables')

    # Get date for each quarter (from first filer type)
    dates = df_summary.groupby('quarter')['date'].first()

    # Combine into display dataframe
    display_df = pd.DataFrame({
        'Date': dates,
        'Y-9C': pivot_filers.get('y_9c', 0).fillna(0).astype(int),
        'Y-9LP': pivot_filers.get('y_9lp', 0).fillna(0).astype(int),
        'Y-9SP': pivot_filers.get('y_9sp', 0).fillna(0).astype(int),
        'Y-9C_vars': pivot_vars.get('y_9c', 0).fillna(0).astype(int),
        'Y-9LP_vars': pivot_vars.get('y_9lp', 0).fillna(0).astype(int),
        'Y-9SP_vars': pivot_vars.get('y_9sp', 0).fillna(0).astype(int),
    })

    # Print quarterly breakdown
    print("\n" + "="*80)
    print("QUARTERLY BREAKDOWN BY FILER TYPE")
    print("="*80)
    print(f"{'Quarter':<8} {'Date':<12} {'Y-9C':>7} {'Y-9LP':>7} {'Y-9SP':>7} | {'Y-9C':>5} {'Y-9LP':>5} {'Y-9SP':>5}")
    print(f"{'':8} {'':12} {'Filers':>7} {'Filers':>7} {'Filers':>7} | {'Vars':>5} {'Vars':>5} {'Vars':>5}")
    print("-" * 8 + " " + "-" * 12 + " " + "-" * 7 + " " + "-" * 7 + " " + "-" * 7 + " | " + "-" * 5 + " " + "-" * 5 + " " + "-" * 5)

    for quarter, row in display_df.iterrows():
        y9c = f"{row['Y-9C']:,}" if row['Y-9C'] > 0 else "-"
        y9lp = f"{row['Y-9LP']:,}" if row['Y-9LP'] > 0 else "-"
        y9sp = f"{row['Y-9SP']:,}" if row['Y-9SP'] > 0 else "-"
        y9c_v = f"{row['Y-9C_vars']:,}" if row['Y-9C_vars'] > 0 else "-"
        y9lp_v = f"{row['Y-9LP_vars']:,}" if row['Y-9LP_vars'] > 0 else "-"
        y9sp_v = f"{row['Y-9SP_vars']:,}" if row['Y-9SP_vars'] > 0 else "-"

        print(f"{quarter:<8} {row['Date'].strftime('%Y-%m-%d'):<12} "
              f"{y9c:>7} {y9lp:>7} {y9sp:>7} | {y9c_v:>5} {y9lp_v:>5} {y9sp_v:>5}")

    # Overall statistics
    print("\n" + "="*80)
    print("SUMMARY STATISTICS")
    print("="*80)
    print(f"Total quarters: {len(display_df)}")
    print(f"Date range: {dates.min().strftime('%Y-%m-%d')} to {dates.max().strftime('%Y-%m-%d')}")
    print(f"Total files: {len(df_summary)}")

    print(f"\nFiler Type Breakdown:")
    for filer_type in ['y_9c', 'y_9lp', 'y_9sp']:
        df_type = df_summary[df_summary['filer_type'] == filer_type]
        if len(df_type) > 0:
            filer_names = {
                'y_9c': 'FR Y-9C',
                'y_9lp': 'FR Y-9LP',
                'y_9sp': 'FR Y-9SP'
            }
            print(f"  {filer_names[filer_type]:<10} {len(df_type):>3} quarters, "
                  f"avg {df_type['filers'].mean():>6.0f} filers, "
                  f"avg {df_type['variables'].mean():>5.0f} vars, "
                  f"{df_type['size_mb'].sum():>6.1f} MB")

    print(f"\nTotal size: {df_summary['size_mb'].sum():.1f} MB")
    print("="*80)

    return 0


if __name__ == '__main__':
    sys.exit(main())
