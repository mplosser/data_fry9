"""
Download MDRM (Micro Data Reference Manual) data dictionary.

This script downloads the Federal Reserve's MDRM data dictionary which contains
variable definitions, descriptions, and metadata for FR Y-9 reporting forms.

Source: https://www.federalreserve.gov/apps/mdrm/download_mdrm.htm

Usage:
    python 02_download_dictionary.py
    python 02_download_dictionary.py --output-dir data/raw
"""

import argparse
import sys
import zipfile
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


MDRM_URL = "https://www.federalreserve.gov/apps/mdrm/pdf/MDRM.zip"


def create_session() -> requests.Session:
    """Create a requests session with retry logic."""
    session = requests.Session()

    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    })

    return session


def download_mdrm(output_dir: Path) -> bool:
    """
    Download and extract the MDRM data dictionary.

    Args:
        output_dir: Directory to save the dictionary files

    Returns:
        True if successful, False otherwise
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    zip_path = output_dir / "mdrm.zip"
    csv_path = output_dir / "MDRM.csv"

    # Check if already downloaded
    if csv_path.exists():
        print(f"MDRM dictionary already exists: {csv_path}")
        return True

    print(f"Downloading MDRM dictionary from Federal Reserve...")
    print(f"  URL: {MDRM_URL}")

    try:
        session = create_session()
        response = session.get(MDRM_URL, timeout=60, stream=True)
        response.raise_for_status()

        # Save ZIP file
        with open(zip_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        zip_size_mb = zip_path.stat().st_size / (1024 * 1024)
        print(f"  Downloaded: mdrm.zip ({zip_size_mb:.2f} MB)")

        # Extract CSV from ZIP
        print("  Extracting...")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Find the CSV file (case-insensitive)
            csv_file = None
            for name in zip_ref.namelist():
                if name.upper().endswith('.CSV'):
                    csv_file = name
                    break

            if not csv_file:
                print("  ERROR: No CSV file found in ZIP")
                return False

            # Extract CSV
            with zip_ref.open(csv_file) as source:
                with open(csv_path, 'wb') as target:
                    target.write(source.read())

        csv_size_mb = csv_path.stat().st_size / (1024 * 1024)
        print(f"  Extracted: MDRM.csv ({csv_size_mb:.2f} MB)")

        # Clean up ZIP
        zip_path.unlink()
        print("  Cleaned up ZIP file")

        return True

    except requests.exceptions.HTTPError as e:
        print(f"  HTTP error: {e}")
        return False
    except zipfile.BadZipFile:
        print("  ERROR: Downloaded file is not a valid ZIP")
        if zip_path.exists():
            zip_path.unlink()
        return False
    except Exception as e:
        print(f"  ERROR: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description='Download MDRM data dictionary from Federal Reserve',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python 02_download_dictionary.py
  python 02_download_dictionary.py --output-dir data/raw

The MDRM (Micro Data Reference Manual) contains variable definitions
and descriptions for all Federal Reserve reporting forms including FR Y-9.
        """
    )

    parser.add_argument(
        '--output-dir',
        type=str,
        default='data/raw',
        help='Directory to save dictionary files (default: data/raw)'
    )

    args = parser.parse_args()

    print("=" * 60)
    print("MDRM DATA DICTIONARY DOWNLOAD")
    print("=" * 60)

    output_dir = Path(args.output_dir)

    success = download_mdrm(output_dir)

    if success:
        print("\n" + "=" * 60)
        print("SUCCESS")
        print("=" * 60)
        print(f"\nDictionary saved to: {output_dir / 'MDRM.csv'}")
        print("\nNext step: Parse the dictionary")
        print("  python 03_parse_dictionary.py")
        return 0
    else:
        print("\nDownload failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
