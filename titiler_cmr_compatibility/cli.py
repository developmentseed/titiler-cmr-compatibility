"""
Command-line interface for CMR collections processing.

This module provides the CLI entry point for fetching and analyzing
CMR collections and generating tiles URLs for granules.
"""

import argparse
import logging
from pathlib import Path
from typing import Optional, Dict, Any, List
from multiprocessing import Pool, cpu_count
from functools import partial
import math

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


def _process_single_collection(auth: earthaccess.Auth, index_and_collection: [int, Dict[str, Any]], verbose: bool = False) -> Optional[Dict[str, Any]]:
    """
    Worker function to process a single collection.

    This function is designed to be used with multiprocessing.Pool.

    Args:
        collection: Collection metadata dictionary

    Returns:
        Dictionary with granule tiling info or None if processing failed
    """
    idx, collection = index_and_collection
    collection_concept_id = collection.get("meta", {}).get("concept-id", "")
    print(f"Processing collection {idx} {collection_concept_id}")
    ginfo = extract_random_granule_info(collection)

    if verbose:
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
        print("-" * 80)
    else:
        if ginfo.tiles_url:
            ginfo.test_tiling(auth)
    return ginfo.to_report_dict()


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


def _append_batch_to_parquet(results: List[Dict[str, Any]], output_file: str) -> None:
    """
    Safely append a batch of results to a parquet file.

    Args:
        results: List of result dictionaries
        output_file: Path to the output parquet file
    """
    if not results:
        return

    df = pd.DataFrame(results)
    output_path = Path(output_file)

    if output_path.exists():
        # Append to existing file
        existing_df = pd.read_parquet(output_path)
        combined_df = pd.concat([existing_df, df], ignore_index=True)
        combined_df.to_parquet(output_path, index=False)
    else:
        # Create new file
        df.to_parquet(output_path, index=False)


def process_collections_parallel(
    auth: earthaccess.Auth,
    total_collections: Optional[int] = None,
    output_file: str = "tiling_results.parquet",
    num_workers: int = 4,
    batch_size: int = 100
) -> None:
    """
    Process collections in parallel using multiprocessing and batch operations.

    Args:
        total_collections: Total number of collections to process (None for all)
        concept_id: Optional specific collection concept ID
        auth: Optional earthaccess authentication object
        verbose: Whether to print detailed output (default: False)
        output_file: Path to the output parquet file (default: tiling_results.parquet)
        num_workers: Number of parallel worker processes (default: 4)
        batch_size: Number of collections to fetch and process per batch (default: 100)
    """
    print("Fetching collection count from CMR...\n")
    # First get total count
    _, total_hits = fetch_cmr_collections(page_size=1)
    total_to_process = min(total_collections, total_hits) if total_collections else total_hits
    print(f"Total collections available: {total_hits}")
    print(f"Will process: {total_to_process} collections\n")

    if total_to_process == 0:
        print("No collections to process.")
        return

    # Calculate number of batches
    num_batches = math.ceil(total_to_process / batch_size)
    print(f"Processing in {num_batches} batches of {batch_size} collections each")
    print(f"Using {num_workers} parallel workers per batch\n")

    total_processed = 0
    total_successful = 0

    # Process in batches
    for batch_num in range(1, num_batches + 1):
        print(f"\n{'='*80}")
        print(f"Batch {batch_num}/{num_batches}")
        print(f"{'='*80}")

        # Fetch collections for this batch
        try:
            collections, _ = fetch_cmr_collections(page_size=batch_size, page_num=batch_num)
        except Exception as e:
            logger.error(f"Error fetching batch {batch_num}: {e}")
            print(f"Error fetching batch {batch_num}: {e}")
            continue

        if not collections:
            print(f"No collections retrieved for batch {batch_num}")
            continue

        print(f"Retrieved {len(collections)} collections for this batch")

        # Process collections in parallel
        with Pool(processes=num_workers) as pool:
            try:
                results = pool.map(partial(_process_single_collection, auth), enumerate(collections))
            finally:
                pool.close()
                pool.join()

        # Filter out None results
        successful_results = [r for r in results if r is not None]

        total_processed += len(collections)
        total_successful += len(successful_results)

        print(f"Processed {len(collections)} collections: {len(successful_results)} successful, {len(collections) - len(successful_results)} failed")

        # Append batch results to parquet file
        if successful_results:
            try:
                _append_batch_to_parquet(successful_results, output_file)
                print(f"Appended {len(successful_results)} results to {output_file}")
            except Exception as e:
                logger.error(f"Error appending batch {batch_num} to parquet: {e}")
                print(f"Error saving batch results: {e}")

        # Print progress
        print(f"\nProgress: {total_processed}/{total_to_process} collections processed ({total_successful} successful)")

        # Check if we've processed enough
        if total_processed >= total_to_process:
            break

    print(f"\n{'='*80}")
    print(f"Processing complete!")
    print(f"Total processed: {total_processed}")
    print(f"Total successful: {total_successful}")
    print(f"Results saved to: {output_file}")
    print(f"{'='*80}")


def process_collections(
    page_size: int = 100,
    concept_id: Optional[str] = None,
    auth: Optional[any] = None,
    verbose: bool = False,
    output_file: Optional[str] = None
) -> None:
    """
    Process collections and extract random granule information (sequential version).

    Args:
        page_size: Number of collections to retrieve
        concept_id: Optional specific collection concept ID
        auth: Optional earthaccess authentication object
        verbose: Whether to print detailed output (default: False)
        output_file: Path to the output parquet file (default: tiling_results.parquet)
    """
    if concept_id:
        print(f"Fetching specific collection {concept_id} from CMR in UMM JSON format...\n")
        collections, _ = fetch_cmr_collections(page_size=1, concept_id=concept_id)
    else:
        print("Fetching collections from CMR in UMM JSON format...\n")
        collections, _ = fetch_cmr_collections(page_size=page_size)

    if not collections:
        print("No collections retrieved.")
        return

    print(f"Retrieved {len(collections)} collections\n")
    if verbose:
        print("=" * 80)

    for idx, collection in enumerate(collections, 1):
        try:
            report_dict = _process_single_collection(idx, auth, collection, verbose)
            # Append tiling info to parquet file
            if report_dict:
                _append_batch_to_parquet([report_dict], output_file)

        except Exception as e:
            logger.error(f"Error processing collection {idx}: {e}")
            raise e
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
        '--total-collections',
        type=int,
        default=None,
        help='Total number of collections to process (default: all available). Use with --parallel.'
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
    parser.add_argument(
        '--parallel',
        action='store_true',
        help='Enable parallel processing mode'
    )
    parser.add_argument(
        '--num-workers',
        type=int,
        default=4,
        help='Number of parallel worker processes (default: 4). Use with --parallel.'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=100,
        help='Number of collections to process per batch (default: 100). Use with --parallel.'
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
    if args.parallel:
        # Use parallel processing mode
        process_collections_parallel(
            total_collections=args.total_collections,
            auth=auth,
            output_file=args.output_file,
            num_workers=args.num_workers,
            batch_size=args.batch_size
        )
    else:
        # Use sequential processing mode
        process_collections(
            page_size=args.page_size,
            concept_id=args.collection,
            auth=auth,
            verbose=args.verbose,
            output_file=args.output_file
        )


if __name__ == "__main__":
    main()
