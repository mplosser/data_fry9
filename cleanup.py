"""
Cleanup utility for FR Y-9C data pipeline.

This script helps conserve disk space by removing intermediate or source files
after parsing is complete.

Usage:
    # Remove extracted CSVs (keeps ZIPs as source)
    python cleanup.py --extracted

    # Remove all raw files (CSVs and ZIPs)
    python cleanup.py --raw

    # Remove processed parquet files
    python cleanup.py --processed

    # Dry run (show what would be deleted)
    python cleanup.py --extracted --dry-run
"""

import argparse
import sys
import re
from pathlib import Path


def get_extracted_csvs(raw_dir: Path) -> list:
    """
    Find CSV files that were extracted from ZIPs.

    These are CSVs where a corresponding ZIP file exists.
    ZIP format: BHCF20210630.zip -> CSV: bhcf2106.csv

    Args:
        raw_dir: Directory containing raw files

    Returns:
        List of CSV paths that have corresponding ZIPs
    """
    extracted = []

    zip_files = list(raw_dir.glob('BHCF*.zip')) + list(raw_dir.glob('bhcf*.zip'))

    for zip_path in zip_files:
        # Extract date from ZIP filename
        match = re.search(r'bhcf(\d{4})(\d{2})(\d{2})', zip_path.name.lower())
        if not match:
            continue

        year = int(match.group(1))
        month = match.group(2)

        # Generate corresponding CSV filename
        year_short = year % 100
        csv_filename = f"bhcf{year_short:02d}{month}.csv"
        csv_path = raw_dir / csv_filename

        if csv_path.exists():
            extracted.append(csv_path)

    return extracted


def get_all_raw_files(raw_dir: Path) -> list:
    """
    Find all raw data files (CSVs and ZIPs).

    Args:
        raw_dir: Directory containing raw files

    Returns:
        List of all raw file paths
    """
    files = []
    files.extend(raw_dir.glob('bhcf*.csv'))
    files.extend(raw_dir.glob('BHCF*.csv'))
    files.extend(raw_dir.glob('bhcf*.zip'))
    files.extend(raw_dir.glob('BHCF*.zip'))
    return list(files)


def get_processed_files(processed_dir: Path) -> list:
    """
    Find all processed parquet files.

    Args:
        processed_dir: Directory containing processed files

    Returns:
        List of all parquet file paths
    """
    files = []
    for subdir in ['y_9c', 'y_9lp', 'y_9sp']:
        subdir_path = processed_dir / subdir
        if subdir_path.exists():
            files.extend(subdir_path.glob('*.parquet'))
    return list(files)


def format_size(total_bytes: int) -> str:
    """Format bytes as human-readable size."""
    if total_bytes >= 1024 * 1024 * 1024:
        return f"{total_bytes / (1024**3):.2f} GB"
    elif total_bytes >= 1024 * 1024:
        return f"{total_bytes / (1024**2):.2f} MB"
    elif total_bytes >= 1024:
        return f"{total_bytes / 1024:.2f} KB"
    return f"{total_bytes} bytes"


def delete_files(files: list, dry_run: bool = False) -> tuple:
    """
    Delete files and return statistics.

    Args:
        files: List of file paths to delete
        dry_run: If True, don't actually delete

    Returns:
        Tuple of (deleted_count, total_bytes)
    """
    deleted_count = 0
    total_bytes = 0

    for file_path in files:
        if file_path.exists():
            size = file_path.stat().st_size
            total_bytes += size

            if not dry_run:
                file_path.unlink()
            deleted_count += 1

    return deleted_count, total_bytes


def main():
    parser = argparse.ArgumentParser(
        description='Cleanup utility for FR Y-9C data pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Remove extracted CSVs only (keeps ZIPs as source)
  python cleanup.py --extracted

  # Remove all raw files (CSVs and ZIPs)
  python cleanup.py --raw

  # Remove processed parquet files
  python cleanup.py --processed

  # Preview what would be deleted
  python cleanup.py --extracted --dry-run

  # Clean everything
  python cleanup.py --raw --processed
        """
    )

    parser.add_argument(
        '--extracted',
        action='store_true',
        help='Remove extracted CSV files (keeps ZIPs as source)'
    )

    parser.add_argument(
        '--raw',
        action='store_true',
        help='Remove all raw files (CSVs and ZIPs)'
    )

    parser.add_argument(
        '--processed',
        action='store_true',
        help='Remove processed parquet files'
    )

    parser.add_argument(
        '--raw-dir',
        type=str,
        default='data/raw',
        help='Directory containing raw files (default: data/raw)'
    )

    parser.add_argument(
        '--processed-dir',
        type=str,
        default='data/processed',
        help='Directory containing processed files (default: data/processed)'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be deleted without actually deleting'
    )

    args = parser.parse_args()

    # Check that at least one cleanup option was specified
    if not (args.extracted or args.raw or args.processed):
        parser.print_help()
        print("\nError: Specify at least one of --extracted, --raw, or --processed")
        return 1

    raw_dir = Path(args.raw_dir)
    processed_dir = Path(args.processed_dir)

    print("=" * 60)
    print("FR Y-9C CLEANUP UTILITY")
    print("=" * 60)

    if args.dry_run:
        print("DRY RUN - No files will be deleted\n")

    total_deleted = 0
    total_freed = 0

    # Handle extracted CSVs
    if args.extracted and not args.raw:  # --raw supersedes --extracted
        if not raw_dir.exists():
            print(f"Raw directory not found: {raw_dir}")
        else:
            files = get_extracted_csvs(raw_dir)
            if files:
                print(f"\nExtracted CSVs ({len(files)} files):")
                for f in sorted(files)[:10]:
                    print(f"  {f.name}")
                if len(files) > 10:
                    print(f"  ... and {len(files) - 10} more")

                count, size = delete_files(files, args.dry_run)
                total_deleted += count
                total_freed += size

                action = "Would delete" if args.dry_run else "Deleted"
                print(f"\n{action}: {count} extracted CSV file(s), {format_size(size)}")
            else:
                print("\nNo extracted CSV files found (no CSVs with matching ZIPs)")

    # Handle all raw files
    if args.raw:
        if not raw_dir.exists():
            print(f"Raw directory not found: {raw_dir}")
        else:
            files = get_all_raw_files(raw_dir)
            if files:
                csv_files = [f for f in files if f.suffix.lower() == '.csv']
                zip_files = [f for f in files if f.suffix.lower() == '.zip']

                print(f"\nAll raw files ({len(files)} files):")
                print(f"  CSV files: {len(csv_files)}")
                print(f"  ZIP files: {len(zip_files)}")

                count, size = delete_files(files, args.dry_run)
                total_deleted += count
                total_freed += size

                action = "Would delete" if args.dry_run else "Deleted"
                print(f"\n{action}: {count} raw file(s), {format_size(size)}")
            else:
                print("\nNo raw files found")

    # Handle processed files
    if args.processed:
        if not processed_dir.exists():
            print(f"Processed directory not found: {processed_dir}")
        else:
            files = get_processed_files(processed_dir)
            if files:
                print(f"\nProcessed parquet files ({len(files)} files):")
                for subdir in ['y_9c', 'y_9lp', 'y_9sp']:
                    subdir_files = [f for f in files if f.parent.name == subdir]
                    if subdir_files:
                        print(f"  {subdir}/: {len(subdir_files)} files")

                count, size = delete_files(files, args.dry_run)
                total_deleted += count
                total_freed += size

                action = "Would delete" if args.dry_run else "Deleted"
                print(f"\n{action}: {count} parquet file(s), {format_size(size)}")
            else:
                print("\nNo processed parquet files found")

    # Summary
    print("\n" + "=" * 60)
    if args.dry_run:
        print(f"TOTAL: Would delete {total_deleted} file(s), freeing {format_size(total_freed)}")
    else:
        print(f"TOTAL: Deleted {total_deleted} file(s), freed {format_size(total_freed)}")
    print("=" * 60)

    return 0


if __name__ == '__main__':
    sys.exit(main())
