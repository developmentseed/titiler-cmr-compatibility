"""
Command-line interface for CMR collections processing.

This module provides the CLI entry point for fetching and analyzing
CMR collections and generating tiles URLs for granules.
"""

import argparse
import logging

from pathlib import Path
from typing import Optional, Dict, Any, List
from multiprocessing import Pool
from multiprocessing.pool import TimeoutError as PoolTimeoutError
from functools import partial
import math

import earthaccess
import pandas as pd

from .api import fetch_cmr_collections, fetch_granule_by_id
from .metadata import extract_random_granule_info, extract_granule_tiling_info
from .tiling import GranuleTilingInfo, IncompatibilityReason
from .lithops_processing import (
    create_collection_directories,
    process_all_collections,
    get_unprocessed_collections,
    download_results_from_s3
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def _print_and_test(ginfo: GranuleTilingInfo, auth: earthaccess.Auth):
    print(f"  Granule Concept ID: {ginfo.concept_id}")
    print(f"  Backend: {ginfo.backend}")
    print(f"  Format: {ginfo.format}")
    print(f"  Extension: {ginfo.extension}")
    print(f"  Data URL: {ginfo.data_url}")
    if type(ginfo.data_variables) == list:
        print(f"  Data Variables: {', '.join(ginfo.data_variables)}")
    if ginfo.tiles_url:
        print(f"  Tiles URL: {ginfo.tiles_url}")
        print("Testing tile generation:")
        print(ginfo.test_tiling(auth))
    if ginfo.incompatible_reason:
        print(f"  Incompatible reason: {ginfo.incompatible_reason}")
    if ginfo.error_message:
        print(f"Error message: {ginfo.error_message}")
    print("-" * 80)
    return ginfo

def _minimal_ginfo(collection_id: str, error_message: str, incompatible_reason: Optional[IncompatibilityReason] = None):
    return GranuleTilingInfo(
        collection_concept_id=collection_id,
        error_message=error_message,
        incompatible_reason=incompatible_reason
    )

def _process_single_collection(
    auth: earthaccess.Auth,
    index_and_collection: [int, Dict[str, Any]],
    access_type: Optional[str] = "direct",
    print_collection_info: bool = False,
) -> Optional[Dict[str, Any]]:
    """
    Worker function to process a single collection.

    This function is designed to be used with multiprocessing.Pool.

    Args:
        auth: earthaccess authentication object
        index_and_collection: Tuple of (index, collection_metadata)
        verbose: Whether to print detailed output

    Returns:
        Dictionary with granule tiling info or None if processing failed
    """
    idx, collection = index_and_collection
    collection_concept_id = collection.get("meta", {}).get("concept-id", "")
    ginfo = None

    try:
        logger.info(f"[Worker {idx}] Starting collection {idx}: {collection_concept_id}")
        ginfo = extract_random_granule_info(collection=collection, access_type=access_type)
    except Exception as e:
        logger.error(f"[Worker {idx}] Error extracting granule info {collection_concept_id}: {e}", exc_info=True)
        return _minimal_ginfo(collection_concept_id, str(e), IncompatibilityReason.FAILED_TO_EXTRACT).to_report_dict()

    if ginfo is None:
        error_message = f"[Worker {idx}] No granule info returned for {collection_concept_id}"
        logger.warning(error_message)
        return _minimal_ginfo(collection_concept_id, error_message, IncompatibilityReason.NO_GRANULE_FOUND).to_report_dict()

    try:
        if print_collection_info:
            ginfo = _print_and_test(ginfo, auth)
        else:
            if ginfo.tiles_url:
                ginfo.test_tiling(auth)
        logger.info(f"[Worker {idx}] Completed collection {collection_concept_id} using granule {ginfo.concept_id}.")
    except Exception as e:
        logger.error(f"[Worker {idx}] Error processing collection {collection_concept_id}: {e}", exc_info=True)
        return _minimal_ginfo(collection_concept_id, str(e), IncompatibilityReason.TILE_GENERATION_FAILED).to_report_dict()
    return ginfo.to_report_dict()


def process_granule_by_id(granule_id: str, auth: Optional[any] = None, access_type: Optional[str] = "direct") -> None:
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

        granule_tiling_info = extract_granule_tiling_info(granule=granule, access_type=access_type)
        if not granule_tiling_info:
            print(f"\n✗ Failed to extract tiling info for granule {granule_id}")
            return

        _print_and_test(granule_tiling_info, auth)
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
    batch_size: int = 100,
    timeout_per_collection: int = 180,
    access_type: str = "direct"
) -> None:
    """
    Process collections in parallel using multiprocessing and batch operations.

    Args:
        auth: earthaccess authentication object
        total_collections: Total number of collections to process (None for all)
        output_file: Path to the output parquet file (default: tiling_results.parquet)
        num_workers: Number of parallel worker processes (default: 4)
        batch_size: Number of collections to fetch and process per batch (default: 100)
        timeout_per_collection: Maximum seconds to wait for each collection (default: 180)
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
    print(f"Using {num_workers} parallel workers per batch")
    print(f"Timeout per collection: {timeout_per_collection}s\n")

    total_processed = 0
    total_successful = 0
    total_timeouts = 0

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
            continue

        if not collections:
            print(f"No collections retrieved for batch {batch_num}")
            continue

        print(f"Retrieved {len(collections)} collections for this batch")

        # Process collections in parallel with timeout
        results = []
        timeouts = []

        with Pool(processes=num_workers) as pool:
            try:
                # Use imap_unordered for better control
                worker_func = partial(_process_single_collection, auth, access_type=access_type)
                async_results = [
                    pool.apply_async(worker_func, (item,))
                    for item in enumerate(collections)
                ]

                # Collect results with timeout
                for i, async_result in enumerate(async_results):
                    collection_id = collections[i].get("meta", {}).get("concept-id", "Unknown")
                    try:
                        result = async_result.get(timeout=timeout_per_collection)
                        results.append(result)
                    except PoolTimeoutError:
                        error_message = f"Collection {i} ({collection_id}) timed out after {timeout_per_collection}s"
                        logger.error(error_message)
                        timeouts.append(collection_id)
                        results.append(_minimal_ginfo(collection_id, error_message, IncompatibilityReason.TIMEOUT).to_report_dict())
                    except Exception as e:
                        error_message = f"Error getting result for collection {i} ({collection_id}): {e}"
                        logger.error(error_message)
                        results.append(_minimal_ginfo(collection_id, error_message).to_report_dict())
            finally:
                pool.terminate()
                pool.join()

        # Filter out None results
        successful_results = [r for r in results if r is not None]

        total_processed += len(collections)
        total_successful += len(successful_results)
        total_timeouts += len(timeouts)

        print(f"Processed {len(collections)} collections: {len(successful_results)} successful, "
              f"{len(timeouts)} timed out, "
              f"{len(collections) - len(successful_results) - len(timeouts)} failed")

        # Append batch results to parquet file
        if successful_results:
            try:
                _append_batch_to_parquet(successful_results, output_file)
                print(f"Appended {len(successful_results)} results to {output_file}")
            except Exception as e:
                logger.error(f"Error appending batch {batch_num} to parquet: {e}")

        # Print progress
        print(f"\nProgress: {total_processed}/{total_to_process} collections processed "
              f"({total_successful} successful, {total_timeouts} timed out)")

        # Check if we've processed enough
        if total_processed >= total_to_process:
            break

    print(f"\n{'='*80}")
    print(f"Processing complete!")
    print(f"Total processed: {total_processed}")
    print(f"Total successful: {total_successful}")
    print(f"Total timed out: {total_timeouts}")
    print(f"Results saved to: {output_file}")
    print(f"{'='*80}")


def process_collections(
    page_size: int = 100,
    concept_id: Optional[str] = None,
    auth: Optional[any] = None,
    access_type: Optional[str] = "direct"
) -> None:
    """
    Process collections and extract random granule information (sequential version).

    Args:
        page_size: Number of collections to retrieve
        concept_id: Optional specific collection concept ID
        auth: Optional earthaccess authentication object
        verbose: Whether to print detailed output (default: False)
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
    print("=" * 80)

    for idx, collection in enumerate(collections, 1):
        try:
            _process_single_collection(
                auth=auth,
                index_and_collection=[idx, collection],
                access_type=access_type,
                print_collection_info=True
            )
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
        '--collection-id',
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
        '--debug',
        action='store_true',
        help='Print debug logging'
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
        default=25,
        help='Number of collections to process per batch (default: 100). Use with --parallel.'
    )
    parser.add_argument(
        '--timeout',
        type=int,
        default=30,
        help='Timeout in seconds for processing each collection (default: 30). Use with --parallel.'
    )
    parser.add_argument(
        '--access-type',
        type=str,
        default="direct",
        help='Access method to use when determining granule url (default: "direct" for S3 links).'
    )
    parser.add_argument(
        '--lithops',
        action='store_true',
        help='Use Lithops for distributed processing'
    )
    parser.add_argument(
        '--s3-bucket',
        type=str,
        help='S3 bucket name for Lithops processing (required with --lithops)'
    )
    parser.add_argument(
        '--s3-prefix',
        type=str,
        default='collections',
        help='S3 prefix for collection directories (default: collections)'
    )
    parser.add_argument(
        '--lithops-setup',
        action='store_true',
        help='Setup phase: create S3 directories for collections (use with --lithops)'
    )
    parser.add_argument(
        '--lithops-process',
        action='store_true',
        help='Process phase: process all collections using Lithops (use with --lithops)'
    )
    parser.add_argument(
        '--lithops-reprocess',
        action='store_true',
        help='Reprocess only unprocessed collections (use with --lithops)'
    )
    parser.add_argument(
        '--lithops-download',
        action='store_true',
        help='Download all results from S3 (use with --lithops)'
    )
    args = parser.parse_args()

    # Configure logging level
    if args.debug:
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
        process_granule_by_id(args.granule_id, auth, access_type=args.access_type)
        return

    # Handle Lithops mode
    if args.lithops:
        if not args.s3_bucket:
            print("Error: --s3-bucket is required when using --lithops")
            return

        if args.lithops_setup:
            # Setup phase: create collection directories in S3
            print(f"Creating collection directories in S3 bucket: {args.s3_bucket}")
            print(f"S3 prefix: {args.s3_prefix}\n")

            concept_ids = create_collection_directories(
                bucket=args.s3_bucket,
                prefix=args.s3_prefix,
                total_collections=args.total_collections,
                page_size=args.batch_size
            )

            print(f"\n✓ Created {len(concept_ids)} collection directories in S3")
            print(f"  Location: s3://{args.s3_bucket}/{args.s3_prefix}/")

        elif args.lithops_process:
            # Process phase: process all collections
            print(f"Processing collections using Lithops")
            print(f"S3 bucket: {args.s3_bucket}")
            print(f"S3 prefix: {args.s3_prefix}\n")

            results = process_all_collections(
                bucket=args.s3_bucket,
                prefix=args.s3_prefix,
                access_type=args.access_type
            )

            completed = sum(1 for r in results if r.get('status') == 'completed')
            failed = sum(1 for r in results if r.get('status') == 'failed')

            print(f"\n✓ Processing complete!")
            print(f"  Total: {len(results)} collections")
            print(f"  Completed: {completed}")
            print(f"  Failed: {failed}")

        elif args.lithops_reprocess:
            # Reprocess only unprocessed collections
            print(f"Finding unprocessed collections in S3 bucket: {args.s3_bucket}")

            unprocessed = get_unprocessed_collections(
                bucket=args.s3_bucket,
                prefix=args.s3_prefix
            )

            if not unprocessed:
                print("✓ All collections have been processed!")
                return

            print(f"\nFound {len(unprocessed)} unprocessed collections")
            print("Processing them now...\n")

            results = process_all_collections(
                bucket=args.s3_bucket,
                prefix=args.s3_prefix,
                access_type=args.access_type,
                collection_ids=unprocessed
            )

            completed = sum(1 for r in results if r.get('status') == 'completed')
            failed = sum(1 for r in results if r.get('status') == 'failed')

            print(f"\n✓ Reprocessing complete!")
            print(f"  Total: {len(results)} collections")
            print(f"  Completed: {completed}")
            print(f"  Failed: {failed}")

        elif args.lithops_download:
            # Download results from S3
            print(f"Downloading results from S3 bucket: {args.s3_bucket}")

            download_results_from_s3(
                bucket=args.s3_bucket,
                prefix=args.s3_prefix,
                output_file=args.output_file
            )

            print(f"\n✓ Results downloaded to: {args.output_file}")

        else:
            print("Error: When using --lithops, you must specify one of:")
            print("  --lithops-setup      Create collection directories in S3")
            print("  --lithops-process    Process all collections")
            print("  --lithops-reprocess  Reprocess unprocessed collections")
            print("  --lithops-download   Download results from S3")

    # Handle collection mode (non-Lithops)
    elif args.parallel:
        # Use parallel processing mode
        process_collections_parallel(
            auth=auth,
            total_collections=args.total_collections,
            num_workers=args.num_workers,
            batch_size=args.batch_size,
            timeout_per_collection=args.timeout,
            access_type=args.access_type
        )
    else:
        # Use sequential processing mode
        process_collections(
            page_size=args.page_size,
            concept_id=args.collection_id,
            auth=auth,
            access_type=args.access_type
        )


if __name__ == "__main__":
    main()
