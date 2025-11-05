"""
Command-line interface for CMR collections processing.

This module provides the CLI entry point for fetching and analyzing
CMR collections and generating tiles URLs for granules.
"""

import argparse
import logging
from pathlib import Path
from typing import Optional

import earthaccess
import pandas as pd

from .api import fetch_cmr_collections, fetch_granule_by_id
from .metadata import extract_random_granule_info, extract_granule_tiling_info

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def process_granule_by_id(granule_id: str, auth: Optional[any] = None) -> None:
    """
    Process a specific granule and generate tiles URL.

    Args:
        granule_id: Granule concept ID
        auth: Optional earthaccess authentication object
    """
    print(f"Generating tiles URL for granule ID: {granule_id}")

    try:
        granule = fetch_granule_by_id(granule_id)
        if not granule:
            print(f"\n✗ Failed to fetch granule {granule_id}")
            return

        granule_tiling_info = extract_granule_tiling_info(granule)
        if not granule_tiling_info:
            print(f"\n✗ Failed to extract tiling info for granule {granule_id}")
            return

        tiles_url = granule_tiling_info.generate_tiles_url_for_granule()
        if tiles_url:
            print(f"\n✓ Success! Tiles URL generated:")
            print(f"{tiles_url}")

            # Optionally test tiling if auth is available
            if auth and granule_tiling_info.backend:
                print("\nTesting tile generation...")
                try:
                    granule_tiling_info.test_tiling(auth)
                    print("✓ Tile generation test passed")
                except Exception as e:
                    print(f"✗ Tile generation test failed: {e}")
        else:
            print(f"\n✗ Failed to generate tiles URL for granule {granule_id}")
    except Exception as e:
        logger.error(f"Error processing granule {granule_id}: {e}")
        print(f"\n✗ Error: {e}")


def process_collections(
    page_size: int = 100,
    concept_id: Optional[str] = None,
    auth: Optional[any] = None,
    verbose: bool = False,
    output_file: str = "tiling_results.parquet"
) -> None:
    """
    Process collections and extract random granule information.

    Args:
        page_size: Number of collections to retrieve
        concept_id: Optional specific collection concept ID
        auth: Optional earthaccess authentication object
        verbose: Whether to print detailed output (default: False)
        output_file: Path to the output parquet file (default: tiling_results.parquet)
    """
    if concept_id:
        print(f"Fetching specific collection {concept_id} from CMR in UMM JSON format...\n")
        collections = fetch_cmr_collections(page_size=1, concept_id=concept_id)
    else:
        print("Fetching collections from CMR in UMM JSON format...\n")
        collections = fetch_cmr_collections(page_size=page_size)

    if not collections:
        print("No collections retrieved.")
        return

    print(f"Retrieved {len(collections)} collections\n")
    if verbose:
        print("=" * 80)

    for idx, collection in enumerate(collections, 1):
        try:
            ginfo = extract_random_granule_info(collection)

            if verbose:
                print(f"Collection {idx}:")
                if ginfo:
                    print(f"  Collection Concept ID: {ginfo.collection_concept_id}")
                    print(f"  Granule Concept ID: {ginfo.concept_id}")
                    print(f"  Backend: {ginfo.backend}")
                    print(f"  Format: {ginfo.format}")
                    print(f"  Extension: {ginfo.extension}")
                    print(f"  Data URL: {ginfo.data_url}")
                    if ginfo.data_variables:
                        print(f"  Data Variables: {', '.join(ginfo.data_variables[:5])}")
                        if len(ginfo.data_variables) > 5:
                            print(f"    ... and {len(ginfo.data_variables) - 5} more")
                    if ginfo.tiles_url:
                        print(f"  Tiles URL: {ginfo.tiles_url}")
                        print("Testing tile generation:")
                        print(ginfo.test_tiling(auth))
                else:
                    print("  Failed to extract granule information")
                print("-" * 80)
            else:
                # Non-verbose mode: only print collection concept ID
                if ginfo:
                    print(f"Processing collection {ginfo.collection_concept_id}")
                    # Test tiling but don't print verbose output
                    if ginfo.tiles_url:
                        ginfo.test_tiling(auth)
                else:
                    print(f"Processing collection {idx}: Failed to extract granule information")

            # Append tiling info to parquet file
            if ginfo:
                report_dict = ginfo.to_report_dict()
                df = pd.DataFrame([report_dict])

                # Append to parquet file
                output_path = Path(output_file)
                if output_path.exists():
                    # Append to existing file
                    existing_df = pd.read_parquet(output_path)
                    combined_df = pd.concat([existing_df, df], ignore_index=True)
                    combined_df.to_parquet(output_path, index=False)
                else:
                    # Create new file
                    df.to_parquet(output_path, index=False)

        except Exception as e:
            logger.error(f"Error processing collection {idx}: {e}")
            if verbose:
                print(f"  Error: {e}")
                print("-" * 80)
            else:
                print(f"Error processing collection {idx}: {e}")


def main():
    """
    Main function to fetch and display collection metadata or generate tiles URL for specific granule.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Fetch and display CMR collection metadata or generate tiles URL for specific granule'
    )
    parser.add_argument(
        '--collection',
        type=str,
        help='Specific collection concept ID to search for'
    )
    parser.add_argument(
        '--granule-id',
        type=str,
        help='Specific granule concept ID to generate tiles URL for'
    )
    parser.add_argument(
        '--page-size',
        type=int,
        default=100,
        help='Number of collections to retrieve (default: 100)'
    )
    parser.add_argument(
        '--no-auth',
        action='store_true',
        help='Skip earthaccess authentication (some features may not work)'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose output and logging'
    )
    parser.add_argument(
        '--output-file',
        type=str,
        default='tiling_results.parquet',
        help='Path to output parquet file (default: tiling_results.parquet)'
    )
    args = parser.parse_args()

    # Configure logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Initialize authentication unless explicitly disabled
    auth = None
    if not args.no_auth:
        try:
            print("Authenticating with earthaccess...")
            auth = earthaccess.login()
            print("✓ Authentication successful\n")
        except Exception as e:
            logger.warning(f"Authentication failed: {e}")
            print(f"⚠ Warning: Authentication failed. Some features may not work.\n")

    # Handle granule-specific mode
    if args.granule_id:
        process_granule_by_id(args.granule_id, auth)
        return

    # Handle collection mode
    process_collections(
        page_size=args.page_size,
        concept_id=args.collection,
        auth=auth,
        verbose=args.verbose,
        output_file=args.output_file
    )


if __name__ == "__main__":
    main()
