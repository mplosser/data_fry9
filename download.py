"""
Download FR Y-9C Bank Holding Company Financial Statements

This script downloads quarterly FR Y-9C (Consolidated Financial Statements for
Holding Companies) data from Chicago Fed (1986 Q3 through 2021 Q1).

For 2021 Q2+, manual download from FFIEC is required:
https://www.ffiec.gov/npw/FinancialReport/FinancialDataDownload

File Format: CSV (comma-delimited)

Usage:
    # Download all available quarters (1986 Q3 - 2021 Q1)
    python download.py

    # Download specific date range
    python download.py --start-year 2010 --start-quarter 1 --end-year 2020 --end-quarter 4

    # Custom output directory
    python download.py --output-dir data/raw
"""

import argparse
import logging
import sys
import time
from pathlib import Path
from typing import List, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class FRY9CDownloader:
    """Download FR Y-9C Bank Holding Company financial data from Chicago Fed."""

    CHICAGO_FED_BASE_URL = "https://www.chicagofed.org/~/media/others/banking/financial-institution-reports/bhc-data"

    # Data availability (Chicago Fed only)
    MIN_YEAR = 1986
    MIN_QUARTER = 3  # Q3 1986
    MAX_YEAR = 2021
    MAX_QUARTER = 1  # Q1 2021

    # Quarter month mappings
    QUARTER_MONTHS = {1: '03', 2: '06', 3: '09', 4: '12'}

    def __init__(self, output_dir: str, delay_seconds: float = 0.5):
        """
        Initialize the FR Y-9C downloader.

        Args:
            output_dir: Directory to save downloaded files
            delay_seconds: Delay between downloads to be respectful to server
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.delay_seconds = delay_seconds

        # Configure requests session with retry logic
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Create a requests session with retry logic."""
        session = requests.Session()

        retry_strategy = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # Set user agent
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

        return session

    def _format_quarter_code(self, year: int, quarter: int) -> str:
        """
        Format year and quarter into BHCF filename code.

        Args:
            year: Year (1986-2021)
            quarter: Quarter (1-4)

        Returns:
            Filename code (e.g., '8609' for 1986 Q3, '2103' for 2021 Q1)
        """
        year_str = str(year)[-2:]  # Last 2 digits of year
        month_str = self.QUARTER_MONTHS[quarter]
        return f"{year_str}{month_str}"

    def download_quarter(self, year: int, quarter: int) -> bool:
        """
        Download FR Y-9C data for a specific quarter from Chicago Fed.

        Args:
            year: Year (1986-2021)
            quarter: Quarter (1-4)

        Returns:
            True if successful, False otherwise
        """
        # Validate inputs
        if quarter not in [1, 2, 3, 4]:
            logger.error(f"Invalid quarter: {quarter}. Must be 1-4.")
            return False

        # Check if within available range
        if year > self.MAX_YEAR or (year == self.MAX_YEAR and quarter > self.MAX_QUARTER):
            logger.warning(f"Data for {year} Q{quarter} not available from Chicago Fed (max: {self.MAX_YEAR} Q{self.MAX_QUARTER})")
            logger.info("For 2021 Q2+, download manually from: https://www.ffiec.gov/npw/FinancialReport/FinancialDataDownload")
            return False

        if year < self.MIN_YEAR or (year == self.MIN_YEAR and quarter < self.MIN_QUARTER):
            logger.warning(f"Data not available for {year} Q{quarter} (starts {self.MIN_YEAR} Q{self.MIN_QUARTER})")
            return False

        # Generate filename
        quarter_code = self._format_quarter_code(year, quarter)
        filename = f"bhcf{quarter_code}.csv"
        output_path = self.output_dir / filename
        url = f"{self.CHICAGO_FED_BASE_URL}/{filename}"

        # Skip if file already exists
        if output_path.exists():
            logger.info(f"File already exists: {filename}")
            return True

        logger.info(f"Downloading: {year} Q{quarter} from Chicago Fed ({filename})")

        try:
            response = self.session.get(url, timeout=60, stream=True)
            response.raise_for_status()

            # Download file
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            actual_size_mb = output_path.stat().st_size / (1024 * 1024)
            logger.info(f"  Downloaded: {filename} ({actual_size_mb:.2f} MB)")
            return True

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.error(f"  File not found (404): {url}")
            else:
                logger.error(f"  HTTP error: {e}")
            return False
        except Exception as e:
            logger.error(f"  Error downloading {filename}: {e}")
            return False

    def generate_quarter_list(
        self,
        start_year: int,
        start_quarter: int,
        end_year: int,
        end_quarter: int
    ) -> List[Tuple[int, int]]:
        """
        Generate list of (year, quarter) tuples for download.

        Args:
            start_year: Starting year
            start_quarter: Starting quarter (1-4)
            end_year: Ending year
            end_quarter: Ending quarter (1-4)

        Returns:
            List of (year, quarter) tuples
        """
        quarters = []

        for year in range(start_year, end_year + 1):
            # Determine quarter range for this year
            if year == start_year:
                q_start = start_quarter
            else:
                q_start = 1

            if year == end_year:
                q_end = end_quarter
            else:
                q_end = 4

            # Add quarters for this year
            for quarter in range(q_start, q_end + 1):
                # Skip if before minimum date
                if year == self.MIN_YEAR and quarter < self.MIN_QUARTER:
                    continue

                quarters.append((year, quarter))

        return quarters

    def download_range(
        self,
        start_year: int = None,
        start_quarter: int = 1,
        end_year: int = None,
        end_quarter: int = None
    ) -> dict:
        """
        Download FR Y-9C data for a range of quarters.

        Args:
            start_year: Starting year (default: MIN_YEAR)
            start_quarter: Starting quarter (default: MIN_QUARTER)
            end_year: Ending year (default: MAX_YEAR)
            end_quarter: Ending quarter (default: MAX_QUARTER)

        Returns:
            Dictionary with download statistics
        """
        # Set defaults
        if start_year is None:
            start_year = self.MIN_YEAR
            start_quarter = self.MIN_QUARTER

        if end_year is None:
            end_year = self.MAX_YEAR
            end_quarter = self.MAX_QUARTER

        # Generate list of quarters
        quarters = self.generate_quarter_list(start_year, start_quarter, end_year, end_quarter)

        logger.info(f"Downloading {len(quarters)} quarters: {start_year} Q{start_quarter} to {end_year} Q{end_quarter}")

        results = {
            'successful': [],
            'failed': [],
            'skipped': []
        }

        for i, (year, quarter) in enumerate(quarters, 1):
            logger.info(f"[{i}/{len(quarters)}] Processing {year} Q{quarter}")

            success = self.download_quarter(year, quarter)

            if success:
                results['successful'].append((year, quarter))
            else:
                results['failed'].append((year, quarter))

            # Respectful delay between downloads
            if i < len(quarters):
                time.sleep(self.delay_seconds)

        return results


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description='Download FR Y-9C Bank Holding Company financial data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download all available quarters (1986 Q3 - 2021 Q1)
    python download.py

  # Download specific date range
    python download.py --start-year 2010 --start-quarter 1 --end-year 2020 --end-quarter 4

  # Custom output directory
    python download.py --output-dir data/raw

  # Enable debug logging
    python download.py --verbose

Data Source:
  Chicago Fed: 1986 Q3 - 2021 Q1 (CSV files, comma-delimited)

Note:
  For 2021 Q2+, manual download from FFIEC is required:
  https://www.ffiec.gov/npw/FinancialReport/FinancialDataDownload
        """
    )

    parser.add_argument(
        '--start-year',
        type=int,
        help='Starting year (default: 1986)'
    )

    parser.add_argument(
        '--start-quarter',
        type=int,
        default=1,
        choices=[1, 2, 3, 4],
        help='Starting quarter (1-4, default: 1)'
    )

    parser.add_argument(
        '--end-year',
        type=int,
        help='Ending year (default: 2021)'
    )

    parser.add_argument(
        '--end-quarter',
        type=int,
        default=4,
        choices=[1, 2, 3, 4],
        help='Ending quarter (1-4, default: 4)'
    )

    parser.add_argument(
        '--output-dir',
        type=str,
        default='data/raw',
        help='Directory to save downloaded files (default: data/raw)'
    )

    parser.add_argument(
        '--delay',
        type=float,
        default=0.5,
        help='Delay between downloads in seconds (default: 0.5)'
    )

    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose (debug) logging'
    )

    args = parser.parse_args()

    # Set logging level
    if args.verbose:
        logger.setLevel(logging.DEBUG)

    # Create downloader and run
    logger.info("=" * 60)
    logger.info("FR Y-9C DATA DOWNLOAD")
    logger.info("=" * 60)
    logger.info(f"Output directory: {Path(args.output_dir).absolute()}")
    logger.info("Source: Chicago Fed (1986 Q3 - 2021 Q1)")

    downloader = FRY9CDownloader(
        output_dir=args.output_dir,
        delay_seconds=args.delay
    )

    results = downloader.download_range(
        start_year=args.start_year,
        start_quarter=args.start_quarter,
        end_year=args.end_year,
        end_quarter=args.end_quarter
    )

    # Print summary
    logger.info("=" * 60)
    logger.info("DOWNLOAD SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Successful: {len(results['successful'])} quarters")
    logger.info(f"Failed: {len(results['failed'])} quarters")

    if results['successful']:
        first_q = results['successful'][0]
        last_q = results['successful'][-1]
        logger.info(f"\nSuccessfully downloaded: {first_q[0]} Q{first_q[1]} through {last_q[0]} Q{last_q[1]}")

    if results['failed']:
        logger.warning("\nFailed quarters:")
        for year, quarter in results['failed']:
            logger.warning(f"  - {year} Q{quarter}")

    logger.info(f"\nFiles saved to: {Path(args.output_dir).absolute()}")
    logger.info("=" * 60)

    return 0 if not results['failed'] else 1


if __name__ == "__main__":
    sys.exit(main())
