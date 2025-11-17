"""
Lithops-based distributed processing for CMR collections.

This module provides fault-tolerant distributed processing using Lithops
and S3-based state tracking. Collections are processed independently,
with results written to S3 directories for tracking.
"""

import json
import logging
from typing import Dict, Any, Optional, List
from pathlib import Path

import boto3
import earthaccess
from lithops import FunctionExecutor

from .api import fetch_cmr_collections, fetch_granule_by_id
from .metadata import extract_random_granule_info
from .tiling import GranuleTilingInfo, IncompatibilityReason

ssm = boto3.client('ssm', region_name='us-west-2')
logger = logging.getLogger(__name__)


def write_collection_id_to_s3(
    page_info: Dict[str, Any],
    bucket: str,
    prefix: str = "collections"
) -> List[str]:
    """
    Fetch a page of collections and write their concept IDs as S3 directories.

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

        # Create a "directory" in S3 by writing a marker object
        key = f"{prefix}/{concept_id}/.marker"

        try:
            s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=b'',
                Metadata={'status': 'pending'}
            )
            created_ids.append(concept_id)
            logger.info(f"Created S3 directory for collection {concept_id}")
        except Exception as e:
            logger.error(f"Error creating S3 directory for {concept_id}: {e}")

    return created_ids


def process_collection_to_s3(
    collection_info: Dict[str, Any],
    bucket: str,
    prefix: str = "collections",
    access_type: str = "direct"
) -> Dict[str, Any]:
    """
    Process a single collection and write results to S3.

    This function is designed to be called by Lithops for each collection.

    Args:
        collection_info: Dict with 'concept_id' key
        bucket: S3 bucket name
        prefix: S3 prefix for collection directories
        access_type: Access type for granules ("direct" or "external")

    Returns:
        Dict with processing status and collection_concept_id
    """
    concept_id = collection_info['concept_id']
    logger.info(f"Processing collection {concept_id}")

    # Authenticate with earthaccess
    try:
        auth = earthaccess.login()
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
            incompatible_reason=IncompatibilityReason.FAILED_TO_EXTRACT
        )

    if ginfo is None:
        ginfo = GranuleTilingInfo(
            collection_concept_id=concept_id,
            error_message="No granule info returned",
            incompatible_reason=IncompatibilityReason.NO_GRANULE_FOUND
        )

    # Test tiling if we have a tiles URL
    if ginfo.tiles_url:
        try:
            ginfo.test_tiling(auth)
        except Exception as e:
            logger.error(f"Error testing tile generation for {concept_id}: {e}")
            ginfo.error_message = str(e)
            ginfo.incompatible_reason = IncompatibilityReason.TILE_GENERATION_FAILED

    # Write results to S3 as JSON
    result_dict = ginfo.to_report_dict()
    s3_client = boto3.client('s3')
    key = f"{prefix}/{concept_id}/result.json"

    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(result_dict, indent=2),
            ContentType='application/json'
        )
        logger.info(f"Wrote results for collection {concept_id} to S3")
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
        {'page_num': page_num, 'page_size': page_size}
        for page_num in range(1, num_pages + 1)
    ]

    # Use Lithops to process pages in parallel
    with FunctionExecutor(config=lithops_config) as fexec:
        futures = fexec.map(
            lambda page_info: write_collection_id_to_s3(page_info, bucket, prefix),
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
    lithops_config: Optional[Dict[str, Any]] = None,
    collection_ids: Optional[List[str]] = None
) -> List[Dict[str, Any]]:
    """
    Process all collections using Lithops and write results to S3.

    Args:
        bucket: S3 bucket name
        prefix: S3 prefix for collection directories
        access_type: Access type for granules ("direct" or "external")
        lithops_config: Optional Lithops configuration dict
        collection_ids: Optional list of specific collection IDs to process.
                       If None, will fetch all collection IDs from S3.

    Returns:
        List of processing status dicts for each collection
    """
    # Get collection IDs from S3 if not provided
    if collection_ids is None:
        s3_client = boto3.client('s3')
        logger.info(f"Listing collection directories from s3://{bucket}/{prefix}/")

        paginator = s3_client.get_paginator('list_objects_v2')
        collection_ids = []

        for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/", Delimiter='/'):
            for common_prefix in page.get('CommonPrefixes', []):
                # Extract collection ID from prefix (e.g., "collections/C123/" -> "C123")
                dir_name = common_prefix['Prefix'].rstrip('/').split('/')[-1]
                collection_ids.append(dir_name)

        logger.info(f"Found {len(collection_ids)} collections to process")

    # Create collection info dicts
    collection_infos = [{'concept_id': concept_id} for concept_id in collection_ids]

    username = ssm.get_parameter(
        Name='/earthdata/username',
        WithDecryption=True
    )['Parameter']['Value']

    password = ssm.get_parameter(
        Name='/earthdata/password', 
        WithDecryption=True
    )['Parameter']['Value']

    # Use Lithops to process collections in parallel
    with FunctionExecutor(
        config=lithops_config,
        runtime_env_vars={
            'EARTHDATA_USERNAME': username,
            'EARTHDATA_PASSWORD': password
        }
    ) as fexec:
        futures = fexec.map(
            lambda info: process_collection_to_s3(info, bucket, prefix, access_type),
            collection_infos
        )
        results = fexec.get_result(futures)

    return results


def get_unprocessed_collections(bucket: str, prefix: str = "collections") -> List[str]:
    """
    Get list of collection IDs that haven't been processed yet (no result.json).

    Args:
        bucket: S3 bucket name
        prefix: S3 prefix for collection directories

    Returns:
        List of unprocessed collection concept IDs
    """
    s3_client = boto3.client('s3')
    unprocessed = []

    # List all collection directories
    paginator = s3_client.get_paginator('list_objects_v2')

    for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/", Delimiter='/'):
        for common_prefix in page.get('CommonPrefixes', []):
            concept_id = common_prefix['Prefix'].rstrip('/').split('/')[-1]

            # Check if result.json exists
            result_key = f"{prefix}/{concept_id}/result.json"
            try:
                s3_client.head_object(Bucket=bucket, Key=result_key)
            except s3_client.exceptions.ClientError:
                # result.json doesn't exist
                unprocessed.append(concept_id)

    return unprocessed


def download_results_from_s3(
    bucket: str,
    prefix: str = "collections",
    output_file: str = "tiling_results.json"
) -> None:
    """
    Download all results from S3 and compile into a single file.

    Args:
        bucket: S3 bucket name
        prefix: S3 prefix for collection directories
        output_file: Path to output JSON file
    """
    s3_client = boto3.client('s3')
    all_results = []

    # List all result.json files
    paginator = s3_client.get_paginator('list_objects_v2')

    for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/"):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('result.json'):
                # Download and parse result
                try:
                    response = s3_client.get_object(Bucket=bucket, Key=obj['Key'])
                    result = json.loads(response['Body'].read())
                    all_results.append(result)
                except Exception as e:
                    logger.error(f"Error downloading {obj['Key']}: {e}")

    # Write combined results
    with open(output_file, 'w') as f:
        json.dump(all_results, f, indent=2)

    logger.info(f"Downloaded {len(all_results)} results to {output_file}")
