"""
Download FR Y-9C Bank Holding Company Financial Statements

This script downloads quarterly FR Y-9C (Consolidated Financial Statements for
Holding Companies) data from two sources:
- Chicago Fed: 1986 Q3 through 2021 Q1 (automated CSV downloads)
- FFIEC: 2021 Q2+ (automated ZIP downloads with extraction)

File Format: CSV (comma-delimited for recent data)

Usage:
    # Download all available quarters
    python download.py

    # Download specific date range
    python download.py --start-year 2010 --start-quarter 1 --end-year 2025 --end-quarter 2

    # Download recent years only (includes FFIEC data)
    python download.py --start-year 2021 --start-quarter 1

    # Custom output directory
    python download.py --output-dir data/raw
"""

import argparse
import logging
import sys
import time
import zipfile
from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Optional

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
    """Download FR Y-9C Bank Holding Company financial data."""

    # Chicago Fed hosts data through 2021 Q1
    CHICAGO_FED_BASE_URL = "https://www.chicagofed.org/~/media/others/banking/financial-institution-reports/bhc-data"

    # FFIEC hosts data from 2021 Q2+
    FFIEC_BASE_URL = "https://www.ffiec.gov/npw/StaticData/BulkPWSDataDownload"

    # Data availability
    MIN_YEAR = 1986
    MIN_QUARTER = 3  # Q3 1986
    CHICAGO_FED_MAX_YEAR = 2021
    CHICAGO_FED_MAX_QUARTER = 1  # Q1 2021

    # Current year/quarter (can be updated)
    CURRENT_YEAR = datetime.now().year
    CURRENT_QUARTER = (datetime.now().month - 1) // 3 + 1

    # Quarter month mappings
    QUARTER_MONTHS = {1: '03', 2: '06', 3: '09', 4: '12'}
    QUARTER_MONTH_DAYS = {1: '0331', 2: '0630', 3: '0930', 4: '1231'}

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

    def _format_quarter_code_chicago(self, year: int, quarter: int) -> str:
        """
        Format year and quarter into Chicago Fed BHCF filename code.

        Args:
            year: Year (1986-2021)
            quarter: Quarter (1-4)

        Returns:
            Filename code (e.g., '8609' for 1986 Q3, '2103' for 2021 Q1)
        """
        year_str = str(year)[-2:]  # Last 2 digits of year
        month_str = self.QUARTER_MONTHS[quarter]
        return f"{year_str}{month_str}"

    def _format_quarter_code_ffiec(self, year: int, quarter: int) -> str:
        """
        Format year and quarter into FFIEC filename code.

        Args:
            year: Year (2021+)
            quarter: Quarter (1-4)

        Returns:
            Filename code (e.g., '20210630' for 2021 Q2)
        """
        month_day = self.QUARTER_MONTH_DAYS[quarter]
        return f"{year}{month_day}"

    def download_quarter_chicago_fed(self, year: int, quarter: int) -> bool:
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

        # Check if within Chicago Fed range
        if year > self.CHICAGO_FED_MAX_YEAR or \
           (year == self.CHICAGO_FED_MAX_YEAR and quarter > self.CHICAGO_FED_MAX_QUARTER):
            return False

        # Generate filename
        quarter_code = self._format_quarter_code_chicago(year, quarter)
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

    def download_quarter_ffiec(self, year: int, quarter: int) -> bool:
        """
        Download FR Y-9C data for a specific quarter from FFIEC.

        Args:
            year: Year (2021+)
            quarter: Quarter (1-4)

        Returns:
            True if successful, False otherwise
        """
        # Validate inputs
        if quarter not in [1, 2, 3, 4]:
            logger.error(f"Invalid quarter: {quarter}. Must be 1-4.")
            return False

        # Check if data should be available
        current_year = self.CURRENT_YEAR
        current_quarter = self.CURRENT_QUARTER

        if year > current_year or (year == current_year and quarter > current_quarter):
            logger.warning(f"Data for {year} Q{quarter} may not be available yet")

        # Generate filenames
        quarter_code = self._format_quarter_code_ffiec(year, quarter)
        zip_filename = f"BHCF{quarter_code}.zip"
        csv_filename = f"bhcf{year % 100:02d}{self.QUARTER_MONTHS[quarter]}.csv"

        zip_path = self.output_dir / zip_filename
        output_path = self.output_dir / csv_filename
        url = f"{self.FFIEC_BASE_URL}/{zip_filename}"

        # Skip if CSV already exists
        if output_path.exists():
            logger.info(f"File already exists: {csv_filename}")
            return True

        logger.info(f"Downloading: {year} Q{quarter} from FFIEC ({zip_filename})")

        try:
            # Download ZIP file
            response = self.session.get(url, timeout=120, stream=True)
            response.raise_for_status()

            # Save ZIP file
            with open(zip_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            zip_size_mb = zip_path.stat().st_size / (1024 * 1024)
            logger.info(f"  Downloaded ZIP: {zip_filename} ({zip_size_mb:.2f} MB)")

            # Extract CSV from ZIP
            logger.info(f"  Extracting ZIP file...")
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # List contents
                file_list = zip_ref.namelist()
                logger.debug(f"  ZIP contains: {file_list}")

                # Find the BHCF CSV file (case-insensitive)
                bhcf_file = None
                for file in file_list:
                    if file.upper().startswith('BHCF') and file.upper().endswith('.TXT'):
                        bhcf_file = file
                        break

                if not bhcf_file:
                    logger.error(f"  No BHCF*.TXT file found in ZIP")
                    return False

                # Extract and rename to .csv
                logger.info(f"  Extracting: {bhcf_file}")
                with zip_ref.open(bhcf_file) as source:
                    with open(output_path, 'wb') as target:
                        target.write(source.read())

            csv_size_mb = output_path.stat().st_size / (1024 * 1024)
            logger.info(f"  Extracted: {csv_filename} ({csv_size_mb:.2f} MB)")

            # Remove ZIP file to save space
            zip_path.unlink()
            logger.debug(f"  Removed ZIP file")

            return True

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.error(f"  File not found (404): {url}")
                logger.info(f"  Note: Data for {year} Q{quarter} may require manual download from:")
                logger.info(f"  https://www.ffiec.gov/npw/FinancialReport/FinancialDataDownload?selectedyear={year}")
            else:
                logger.error(f"  HTTP error: {e}")
            return False
        except zipfile.BadZipFile:
            logger.error(f"  Downloaded file is not a valid ZIP file")
            if zip_path.exists():
                zip_path.unlink()
            return False
        except Exception as e:
            logger.error(f"  Error downloading/extracting {zip_filename}: {e}")
            if zip_path.exists():
                zip_path.unlink()
            return False

    def download_quarter(self, year: int, quarter: int) -> bool:
        """
        Download FR Y-9C data for a specific quarter from appropriate source.

        Args:
            year: Year (1986-current)
            quarter: Quarter (1-4)

        Returns:
            True if successful, False otherwise
        """
        # Validate year range
        if year < self.MIN_YEAR:
            logger.error(f"Year {year} is before minimum year ({self.MIN_YEAR})")
            return False

        if year == self.MIN_YEAR and quarter < self.MIN_QUARTER:
            logger.warning(f"Data not available for {year} Q{quarter} (starts {self.MIN_YEAR} Q{self.MIN_QUARTER})")
            return False

        # Route to appropriate source
        if year < self.CHICAGO_FED_MAX_YEAR or \
           (year == self.CHICAGO_FED_MAX_YEAR and quarter <= self.CHICAGO_FED_MAX_QUARTER):
            return self.download_quarter_chicago_fed(year, quarter)
        else:
            return self.download_quarter_ffiec(year, quarter)

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
        end_quarter: int = 4
    ) -> dict:
        """
        Download FR Y-9C data for a range of quarters.

        Args:
            start_year: Starting year (default: MIN_YEAR)
            start_quarter: Starting quarter (default: 1)
            end_year: Ending year (default: CURRENT_YEAR)
            end_quarter: Ending quarter (default: CURRENT_QUARTER)

        Returns:
            Dictionary with download statistics
        """
        # Set defaults
        if start_year is None:
            start_year = self.MIN_YEAR
            start_quarter = self.MIN_QUARTER

        if end_year is None:
            end_year = self.CURRENT_YEAR
            end_quarter = self.CURRENT_QUARTER

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
  # Download all available quarters (1986 Q3 through latest)
    python download.py

  # Download specific date range
    python download.py --start-year 2010 --start-quarter 1 --end-year 2025 --end-quarter 2

  # Download recent years only
    python download.py --start-year 2021 --start-quarter 1

  # Custom output directory
    python download.py --output-dir data/raw

  # Enable debug logging
    python download.py --verbose

Data Sources:
  - 1986 Q3 - 2021 Q1: Chicago Fed (CSV files)
  - 2021 Q2+: FFIEC (ZIP files with TXT, extracted to CSV)

Data Format:
  - Chicago Fed: CSV files delimited by comma
  - FFIEC: TXT files delimited by caret (^), renamed to CSV
  - Each file contains all FR Y-9C variables for that quarter
  - Files are named: bhcfYYQQ.csv (e.g., bhcf2103.csv for 2021 Q1)

Note:
  If FFIEC downloads fail (403/404), you may need to manually download from:
  https://www.ffiec.gov/npw/FinancialReport/FinancialDataDownload?selectedyear=YYYY
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
        help=f'Ending year (default: {datetime.now().year})'
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
    logger.info(f"Sources: Chicago Fed (1986-2021 Q1), FFIEC (2021 Q2+)")

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
        logger.warning(f"\nFailed quarters:")
        for year, quarter in results['failed']:
            logger.warning(f"  - {year} Q{quarter}")
        logger.info("\nFor failed quarters, try manual download from:")
        failed_years = set(year for year, _ in results['failed'])
        for year in sorted(failed_years):
            logger.info(f"  {year}: https://www.ffiec.gov/npw/FinancialReport/FinancialDataDownload?selectedyear={year}")

    logger.info(f"\nFiles saved to: {Path(args.output_dir).absolute()}")
    logger.info("=" * 60)

    return 0 if not results['failed'] else 1


if __name__ == "__main__":
    sys.exit(main())
