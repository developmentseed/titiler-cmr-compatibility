"""
Command-line interface for CMR collections processing.

This module provides the CLI entry point for fetching and analyzing
CMR collections and generating tiles URLs for granules.
"""

import argparse
import logging
from typing import Optional

import earthaccess

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
    auth: Optional[any] = None
) -> None:
    """
    Process collections and extract random granule information.

    Args:
        page_size: Number of collections to retrieve
        concept_id: Optional specific collection concept ID
        auth: Optional earthaccess authentication object
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
    print("=" * 80)

    for idx, collection in enumerate(collections, 1):
        try:
            ginfo = extract_random_granule_info(collection)

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
            else:
                print("  Failed to extract granule information")

            print("-" * 80)
        except Exception as e:
            logger.error(f"Error processing collection {idx}: {e}")
            print(f"  Error: {e}")
            print("-" * 80)


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
        help='Enable verbose logging'
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
        auth=auth
    )


if __name__ == "__main__":
    main()
