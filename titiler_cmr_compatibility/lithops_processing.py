"""
Lithops-based distributed processing for CMR collections.

This module provides fault-tolerant distributed processing using Lithops
and S3-based state tracking. Collections are processed independently,
with results written to S3 directories for tracking.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import logging
from typing import Dict, Any, Optional, List
from pathlib import Path

import boto3
import earthaccess
from lithops import FunctionExecutor
import pandas as pd

from .api import fetch_cmr_collections, fetch_granule_by_id
from .metadata import extract_random_granule_info
from .tiling import GranuleTilingInfo, IncompatibilityReason

ssm = boto3.client('ssm', region_name='us-west-2')
logger = logging.getLogger(__name__)

import os
os.environ['AWS_MAX_POOL_CONNECTIONS'] = '100'

def write_collection_id_to_s3(
    page_info: Dict[str, Any],
    bucket: str,
    prefix: str = "collections"
) -> List[str]:
    """
    Fetch a page of collections and write their concept IDs to unprocessed directory.

    This function is designed to be called by Lithops in parallel across pages.

    Args:
        page_info: Dict with 'page_num' and 'page_size' keys
        bucket: S3 bucket name
        prefix: S3 prefix for collection directories

    Returns:
        List of collection concept IDs created
    """
    page_num = page_info['page_num']
    page_size = page_info['page_size']

    logger.info(f"Fetching page {page_num} with page_size {page_size}")

    try:
        collections, _ = fetch_cmr_collections(page_size=page_size, page_num=page_num)
    except Exception as e:
        logger.error(f"Error fetching page {page_num}: {e}")
        return []

    s3_client = boto3.client('s3')
    created_ids = []

    for collection in collections:
        concept_id = collection.get("meta", {}).get("concept-id", "")
        if not concept_id:
            continue

        # Write to unprocessed directory
        key = f"{prefix}/unprocessed/{concept_id}/.marker"

        try:
            s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=b'',
                Metadata={'status': 'pending'}
            )
            created_ids.append(concept_id)
            logger.info(f"Created unprocessed marker for collection {concept_id}")
        except Exception as e:
            logger.error(f"Error creating marker for {concept_id}: {e}")

    return created_ids


def process_collection_to_s3(
    concept_id: str,
    bucket: str,
    prefix: str = "collections",
    access_type: str = "direct"
) -> Dict[str, Any]:
    """
    Process a single collection, write results to processed/, and remove from unprocessed/.

    This function is designed to be called by Lithops for each collection.

    Args:
        concept_id: Collection concept ID
        bucket: S3 bucket name
        prefix: S3 prefix for collection directories
        access_type: Access type for granules ("direct" or "external")

    Returns:
        Dict with processing status and collection_concept_id
    """
    logger.info(f"Processing collection {concept_id}")
    s3_client = boto3.client('s3')

    # Authenticate with earthaccess
    try:
        auth = earthaccess.login(strategy="environment")
    except Exception as e:
        logger.error(f"Failed to authenticate: {e}")
        auth = None

    # Fetch the collection
    try:
        collections, _ = fetch_cmr_collections(page_size=1, concept_id=concept_id)
        if not collections:
            error_msg = f"Collection {concept_id} not found"
            logger.error(error_msg)
            return {
                'collection_concept_id': concept_id,
                'status': 'failed',
                'error': error_msg
            }
        collection = collections[0]
    except Exception as e:
        error_msg = f"Error fetching collection {concept_id}: {e}"
        logger.error(error_msg)
        return {
            'collection_concept_id': concept_id,
            'status': 'failed',
            'error': error_msg
        }

    # Extract granule info
    try:
        ginfo = extract_random_granule_info(collection=collection, access_type=access_type)
    except Exception as e:
        logger.error(f"Error extracting granule info for {concept_id}: {e}")
        ginfo = GranuleTilingInfo(
            collection_concept_id=concept_id,
            error_message=str(e),
            incompatible_reason=IncompatibilityReason.FAILED_TO_EXTRACT_URL
        )

    if ginfo is None:
        ginfo = GranuleTilingInfo(
            collection_concept_id=concept_id,
            error_message="No granule info returned",
            incompatible_reason=IncompatibilityReason.NO_GRANULE_FOUND
        )

    # Test tiling if we have a tiles URL
    if ginfo.tiles_url:
        ginfo.test_tiling(auth)

    # Write results to S3 in processed directory with status and reason in the path
    result_dict = ginfo.to_report_dict()

    # Encode tiling status and incompatibility reason in the S3 key
    tiling_status = "true" if ginfo.tiling_compatible else "false"
    incompatibility_reason = ginfo.incompatible_reason.value if ginfo.incompatible_reason else "none"

    key = f"{prefix}/processed/{concept_id}/status={tiling_status}/reason={incompatibility_reason}/result.json"

    try:
        # Write result to processed directory
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(result_dict, indent=2),
            ContentType='application/json'
        )
        logger.info(f"Wrote results for collection {concept_id} to processed/")

        # Delete from unprocessed directory
        unprocessed_key = f"{prefix}/unprocessed/{concept_id}/.marker"
        try:
            s3_client.delete_object(Bucket=bucket, Key=unprocessed_key)
            logger.info(f"Removed {concept_id} from unprocessed/")
        except Exception as delete_error:
            logger.warning(f"Failed to delete unprocessed marker for {concept_id}: {delete_error}")

        return {
            'collection_concept_id': concept_id,
            'status': 'completed',
            's3_key': key
        }
    except Exception as e:
        error_msg = f"Error writing results to S3 for {concept_id}: {e}"
        logger.error(error_msg)
        return {
            'collection_concept_id': concept_id,
            'status': 'failed',
            'error': error_msg
        }


def create_collection_directories(
    bucket: str,
    prefix: str = "collections",
    total_collections: Optional[int] = None,
    page_size: int = 100,
    lithops_config: Optional[Dict[str, Any]] = None
) -> List[str]:
    """
    Create S3 directories for all collections using Lithops.

    Args:
        bucket: S3 bucket name
        prefix: S3 prefix for collection directories
        total_collections: Total number of collections to process (None for all)
        page_size: Number of collections per page
        lithops_config: Optional Lithops configuration dict

    Returns:
        List of all created collection concept IDs
    """
    # Get total collection count
    _, total_hits = fetch_cmr_collections(page_size=1)
    total_to_process = min(total_collections, total_hits) if total_collections else total_hits

    logger.info(f"Creating directories for {total_to_process} collections")

    # Calculate number of pages
    num_pages = (total_to_process + page_size - 1) // page_size

    # Create page info for each page
    page_infos = [
        {'page_info': {'page_num': page_num, 'page_size': page_size}}
        for page_num in range(1, num_pages + 1)
    ]

    # Use Lithops to process pages in parallel
    with FunctionExecutor(config=lithops_config) as fexec:
        futures = fexec.map(
            lambda page_info: write_collection_id_to_s3(page_info=page_info, bucket=bucket, prefix=prefix),
            page_infos
        )
        results = fexec.get_result(futures)

    # Flatten results
    all_concept_ids = [concept_id for page_results in results for concept_id in page_results]
    logger.info(f"Created {len(all_concept_ids)} collection directories")

    return all_concept_ids


def process_all_collections(
    bucket: str,
    prefix: str = "collections",
    access_type: str = "direct",
    lithops_config: Optional[Dict[str, Any]] = None
) -> List[Dict[str, Any]]:
    """
    Process all unprocessed collections using Lithops and move them to processed/.

    Args:
        bucket: S3 bucket name
        prefix: S3 prefix for collection directories
        access_type: Access type for granules ("direct" or "external")
        lithops_config: Optional Lithops configuration dict

    Returns:
        List of processing status dicts for each collection
    """
    # Get collection IDs from unprocessed/ directory
    s3_client = boto3.client('s3')
    logger.info(f"Listing unprocessed collections from s3://{bucket}/{prefix}/unprocessed/")

    paginator = s3_client.get_paginator('list_objects_v2')
    collection_ids = []

    for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/unprocessed/", Delimiter='/'):
        for common_prefix in page.get('CommonPrefixes', []):
            # Extract collection ID from prefix (e.g., "collections/unprocessed/C123/" -> "C123")
            dir_name = common_prefix['Prefix'].rstrip('/').split('/')[-1]
            collection_ids.append(dir_name)

    logger.info(f"Found {len(collection_ids)} unprocessed collections to process")

    if not collection_ids:
        logger.info("No unprocessed collections found")
        return []

    username = ssm.get_parameter(
        Name='/earthdata-aimeeb/username',
        WithDecryption=True
    )['Parameter']['Value']

    password = ssm.get_parameter(
        Name='/earthdata-aimeeb/password',
        WithDecryption=True
    )['Parameter']['Value']

    # Use Lithops to process collections in parallel
    with FunctionExecutor(
        config=lithops_config,
        extra_env={
            'EARTHDATA_USERNAME': username,
            'EARTHDATA_PASSWORD': password
        }
    ) as fexec:
        futures = fexec.map(
            lambda concept_id: process_collection_to_s3(
                concept_id=concept_id,
                bucket=bucket,
                prefix=prefix,
                access_type=access_type
            ),
            collection_ids
        )
        results = fexec.get_result(futures, throw_except=False)

    return results


def count_unprocessed_collections(bucket: str, prefix: str = "collections") -> int:
    """
    Count the number of unprocessed collections.

    Args:
        bucket: S3 bucket name
        prefix: S3 prefix for collection directories

    Returns:
        Number of unprocessed collections
    """
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    count = 0

    for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/unprocessed/", Delimiter='/'):
        count += len(page.get('CommonPrefixes', []))

    return count


def count_processed_collections(bucket: str, prefix: str = "collections") -> int:
    """
    Count the number of processed collections.

    Args:
        bucket: S3 bucket name
        prefix: S3 prefix for collection directories

    Returns:
        Number of processed collections
    """
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    count = 0

    for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/processed/", Delimiter='/'):
        count += len(page.get('CommonPrefixes', []))

    return count


def get_collections_by_status(
    bucket: str,
    prefix: str = "collections",
    tiling_compatible: Optional[bool] = None,
    incompatibility_reason: Optional[str] = None
) -> List[str]:
    """
    Get list of collection IDs filtered by tiling status and/or incompatibility reason.

    Uses S3 prefix filtering to efficiently query without reading file contents.

    Args:
        bucket: S3 bucket name
        prefix: S3 prefix for collection directories
        tiling_compatible: Filter by tiling compatibility (True/False/None for all)
        incompatibility_reason: Filter by specific incompatibility reason
                               (e.g., "unsupported_format", "tile_generation_failed")

    Returns:
        List of collection concept IDs matching the filter criteria

    Examples:
        # Get all failed collections
        get_collections_by_status(bucket, tiling_compatible=False)

        # Get all collections that failed due to unsupported format
        get_collections_by_status(bucket, tiling_compatible=False,
                                 incompatibility_reason="unsupported_format")

        # Get all successful collections
        get_collections_by_status(bucket, tiling_compatible=True)
    """
    s3_client = boto3.client('s3')
    matching_collections = []

    # Build the S3 prefix based on filters
    if tiling_compatible is not None:
        status_str = "true" if tiling_compatible else "false"

        if incompatibility_reason is not None:
            # Filter by both status and reason
            search_prefix = f"{prefix}/processed/"
            paginator = s3_client.get_paginator('list_objects_v2')

            for page in paginator.paginate(Bucket=bucket, Prefix=search_prefix):
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    # Match pattern: prefix/processed/CONCEPT_ID/status=STATUS/reason=REASON/result.json
                    if f"/status={status_str}/reason={incompatibility_reason}/result.json" in key:
                        # Extract concept_id from the key
                        parts = key.split('/')
                        if len(parts) >= 4:
                            concept_id = parts[3]  # prefix/processed/CONCEPT_ID/status=/...
                            matching_collections.append(concept_id)
        else:
            # Filter by status only
            search_prefix = f"{prefix}/processed/"
            paginator = s3_client.get_paginator('list_objects_v2')

            for page in paginator.paginate(Bucket=bucket, Prefix=search_prefix):
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    # Match pattern: prefix/processed/CONCEPT_ID/status=STATUS/reason=*/result.json
                    if f"/status={status_str}/" in key and key.endswith('/result.json'):
                        parts = key.split('/')
                        if len(parts) >= 4:
                            concept_id = parts[3]
                            if concept_id not in matching_collections:
                                matching_collections.append(concept_id)
    else:
        # No status filter, just incompatibility reason
        if incompatibility_reason is not None:
            search_prefix = f"{prefix}/processed/"
            paginator = s3_client.get_paginator('list_objects_v2')

            for page in paginator.paginate(Bucket=bucket, Prefix=search_prefix):
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    if f"/reason={incompatibility_reason}/result.json" in key:
                        parts = key.split('/')
                        if len(parts) >= 4:
                            concept_id = parts[3]
                            matching_collections.append(concept_id)
        else:
            # No filters - return all processed collections
            search_prefix = f"{prefix}/processed/"
            paginator = s3_client.get_paginator('list_objects_v2')

            for page in paginator.paginate(Bucket=bucket, Prefix=search_prefix):
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    if key.endswith('/result.json'):
                        parts = key.split('/')
                        if len(parts) >= 4:
                            concept_id = parts[3]
                            if concept_id not in matching_collections:
                                matching_collections.append(concept_id)

    logger.info(f"Found {len(matching_collections)} collections matching filter criteria")
    return matching_collections


def reprocess_collections_by_reason(
    bucket: str,
    prefix: str = "collections",
    incompatibility_reason: str = None,
    access_type: str = "direct",
    lithops_config: Optional[Dict[str, Any]] = None
) -> List[Dict[str, Any]]:
    """
    Reprocess collections that failed with a specific incompatibility reason.

    This function:
    1. Discovers collections that failed with the given reason using S3 directory structure
    2. Reprocesses them using Lithops
    3. Updates result.json in the correct new location
    4. Removes obsolete directory information (old status/reason paths)

    Args:
        bucket: S3 bucket name
        prefix: S3 prefix for collection directories
        incompatibility_reason: The incompatibility reason to reprocess
        access_type: Access type for granules ("direct" or "external")
        lithops_config: Optional Lithops configuration dict

    Returns:
        List of processing status dicts for each reprocessed collection
    """
    logger.info(f"Finding collections with incompatibility reason: {incompatibility_reason}")

    # Get collections that failed with the specified reason
    collection_ids = get_collections_by_status(
        bucket=bucket,
        prefix=prefix,
        tiling_compatible=False,
        incompatibility_reason=incompatibility_reason
    )

    if not collection_ids:
        logger.info(f"No collections found with reason: {incompatibility_reason}")
        return []

    logger.info(f"Found {len(collection_ids)} collections to reprocess")

    # Get SSM parameters for authentication
    username = ssm.get_parameter(
        Name='/earthdata-aimeeb/username',
        WithDecryption=True
    )['Parameter']['Value']

    password = ssm.get_parameter(
        Name='/earthdata-aimeeb/password',
        WithDecryption=True
    )['Parameter']['Value']

    # Helper function to reprocess a single collection and clean up old paths
    def reprocess_and_cleanup(concept_id: str) -> Dict[str, Any]:
        """Reprocess a collection and clean up old result paths."""
        s3_client = boto3.client('s3')

        # First, find and delete the old result.json
        old_key_prefix = f"{prefix}/processed/{concept_id}/"
        try:
            paginator = s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=bucket, Prefix=old_key_prefix):
                for obj in page.get('Contents', []):
                    if obj['Key'].endswith('result.json'):
                        # Delete the old result.json
                        logger.info(f"Deleting old result: {obj['Key']}")
                        s3_client.delete_object(Bucket=bucket, Key=obj['Key'])

                        # Also try to clean up the parent directories if empty
                        # Extract the directory path (everything before result.json)
                        old_dir_parts = obj['Key'].rsplit('/', 1)[0]
                        # Try to delete status= and reason= directories (S3 will fail if not empty, which is fine)
                        for part in ['', '/..', '/../..']:
                            try:
                                cleanup_key = old_dir_parts + part + '/'
                                s3_client.delete_object(Bucket=bucket, Key=cleanup_key)
                            except:
                                pass  # Ignore errors, directories might not be empty or might not exist
        except Exception as e:
            logger.warning(f"Error cleaning up old results for {concept_id}: {e}")

        # Now reprocess the collection
        result = process_collection_to_s3(
            concept_id=concept_id,
            bucket=bucket,
            prefix=prefix,
            access_type=access_type
        )

        return result

    # Use Lithops to reprocess collections in parallel
    with FunctionExecutor(
        config=lithops_config,
        extra_env={
            'EARTHDATA_USERNAME': username,
            'EARTHDATA_PASSWORD': password
        }
    ) as fexec:
        futures = fexec.map(reprocess_and_cleanup, collection_ids)
        results = fexec.get_result(futures)#, throw_except=False)

    return results


def download_results_from_s3(
    bucket: str,
    prefix: str = "collections",
    output_file: str = "tiling_results.json"
) -> None:
    """
    Download all processed results from S3 and compile into a single file.

    Args:
        bucket: S3 bucket name
        prefix: S3 prefix for collection directories
        output_file: Path to output JSON file
    """
    s3_client = boto3.client('s3')
    all_results = []

    def _download_and_parse(bucket, key):
        """Download and parse a single result file"""
        try:
            response = s3_client.get_object(Bucket=bucket, Key=key)
            return json.loads(response['Body'].read())
        except Exception as e:
            logger.error(f"Error downloading {key}: {e}")
            return None

    # List all result.json files in processed directory
    keys_to_download = []
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/processed/"):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('result.json'):
                keys_to_download.append(obj['Key'])

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(_download_and_parse, bucket, key): key 
                for key in keys_to_download}
        
        for future in as_completed(futures):
            result = future.result()
            if result is not None:
                all_results.append(result)

    # Write combined results
    df = pd.DataFrame(all_results)

    # Write the DataFrame to a Parquet file
    df.to_parquet(output_file, index=False) # index=False to avoid writing the DataFrame index

    print(f"Data successfully written to {output_file}")
