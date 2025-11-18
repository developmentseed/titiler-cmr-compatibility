# Lithops Distributed Processing Workflow

This document describes how to use the Lithops-based distributed processing workflow for processing CMR collections. This approach is more fault-tolerant than the multiprocessing approach and allows for easy resumption of failed jobs.

## Overview

The Lithops workflow consists of four phases:

1. **Setup**: Create S3 directories for all collections to track processing state
2. **Process**: Process all collections in parallel using Lithops
3. **Reprocess**: Reprocess only the collections that failed or were not completed
4. **Download**: Download all results from S3 and compile into a single file

## Prerequisites

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure AWS credentials

If you haven't already, configure your AWS credentials:

```bash
aws configure
```

You'll need to provide:
- AWS Access Key ID
- AWS Secret Access Key
- Default region (e.g., `us-west-2`)
- Default output format (can leave as `json`)

### 3. Store Earthdata credentials in AWS Systems Manager Parameter Store

The Lambda functions need access to your Earthdata credentials to authenticate with NASA APIs. Store them securely in AWS Systems Manager Parameter Store:

```bash
# Store your Earthdata username
aws ssm put-parameter \
  --name "/earthdata/username" \
  --value YOUR_EARTHDATA_USERNAME \
  --type "SecureString"

# Store your Earthdata password
aws ssm put-parameter \
  --name "/earthdata/password" \
  --value 'YOUR_EARTHDATA_PASSWORD' \
  --type "SecureString"
```

Replace `YOUR_EARTHDATA_USERNAME` and `YOUR_EARTHDATA_PASSWORD` with your actual Earthdata credentials.

### 4. Configure Lithops

Create a Lithops configuration file at `~/.lithops/config`:

```yaml
lithops:
    backend: aws_lambda
    storage: aws_s3

aws:
    region: us-west-2  # Change to your preferred region

aws_lambda:
    runtime: tcr-runtime:latest
    runtime_memory: 10240
    runtime_timeout: 900
    # Lithops will create the execution role automatically if not specified
    # Or you can specify an existing role:
    # execution_role: arn:aws:iam::YOUR_ACCOUNT_ID:role/lithops-execution-role

aws_s3:
    # Lithops will use the region specified in the aws section above
    # You can optionally specify a storage bucket:
    # storage_bucket: your-bucket-name
```

**That's it!** Lithops will automatically create the necessary Lambda execution role when you run it for the first time.

#### Optional: Manually create the Lambda execution role

If you want to create the IAM role manually (or if Lithops has permission issues creating it automatically):

```bash
# Create the role
aws iam create-role \
  --role-name lithops-execution-role \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": {"Service": "lambda.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }]
  }'

# Attach necessary policies
aws iam attach-role-policy \
  --role-name lithops-execution-role \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

aws iam attach-role-policy \
  --role-name lithops-execution-role \
  --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess

# Create and attach policy for SSM Parameter Store access
aws iam put-role-policy \
  --role-name lithops-execution-role \
  --policy-name SSMParameterAccess \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Action": [
        "ssm:GetParameter",
        "ssm:GetParameters"
      ],
      "Resource": "arn:aws:ssm:*:*:parameter/earthdata-aimeeb/*"
    }]
  }'
```

Then uncomment and update the `execution_role` line in your `~/.lithops/config` file with the role ARN.

**Note:** The SSM parameter access policy is required for Lambda to read the Earthdata credentials from Parameter Store.

### 4. Build and push runtime

```bash
export DOCKER_BUILDKIT=0
lithops runtime build -b aws_lambda -f Dockerfile -c lithops.yaml tcr-runtime -- --platform linux/amd64
```

### 4. Test your Lithops setup

Verify that Lithops can connect to AWS and create Lambda functions:

```bash
lithops test
```

This will run a simple "Hello World" function on AWS Lambda to verify your setup.

### 5. Create an S3 bucket for tracking collection processing

```bash
aws s3 mb s3://veda-odd-scratch
```

Choose a unique bucket name. This bucket will store the collection directories and processing results.

## Usage

### Phase 1: Setup - Create Collection Directories

This phase fetches all collection metadata from CMR and creates S3 directories for each collection. Each directory serves as a tracking mechanism - if a directory contains a `result.json` file, the collection has been processed.

```bash
python -m titiler_cmr_compatibility.cli \
  --lithops \
  --lithops-setup \
  --s3-bucket veda-odd-scratch \
  --s3-prefix titiler-cmr-compatibility/collections \
  --total-collections 10800 \
  --batch-size 100
```

Options:
- `--s3-bucket`: S3 bucket name (required)
- `--s3-prefix`: S3 prefix for collection directories (default: `collections`)
- `--total-collections`: Total number of collections to process (optional, defaults to all)
- `--batch-size`: Number of collections to fetch per page (default: 100)

This will create directories like:
```
s3://your-bucket-name/collections/C1234567890-PROVIDER/.marker
s3://your-bucket-name/collections/C0987654321-PROVIDER/.marker
...
```

### Phase 2: Process Collections

This phase processes all collections using Lithops. Each collection is processed independently, and results are written to S3 as JSON files.

```bash
python -m titiler_cmr_compatibility.cli \
  --lithops \
  --lithops-process \
  --s3-bucket veda-odd-scratch \
  --s3-prefix titiler-cmr-compatibility/collections \
  --access-type direct
```

Options:
- `--access-type`: Access type for granules (`direct` for S3 links, `external` for HTTPS) (default: `direct`)

For each collection, this will create:
```
s3://your-bucket-name/collections/C1234567890-PROVIDER/result.json
```

The `result.json` file contains the `GranuleTilingInfo.to_report_dict()` output for that collection.

### Phase 3: Reprocess Failed Collections

If some collections failed or timed out, you can reprocess only the unprocessed collections:

```bash
python -m titiler_cmr_compatibility.cli \
  --lithops \
  --lithops-reprocess \
  --s3-bucket veda-odd-scratch \
  --s3-prefix titiler-cmr-compatibility/collections \
  --access-type direct
```

This command:
1. Scans S3 to find all collection directories without `result.json` files
2. Reprocesses only those collections
3. Writes results to S3

You can run this command multiple times until all collections are processed.

### Phase 4: Download Results

Once all collections are processed, download the results and compile them into a single file:

```bash
python -m titiler_cmr_compatibility.cli \
  --lithops \
  --lithops-download \
  --s3-bucket veda-odd-scratch \
  --s3-prefix titiler-cmr-compatibility/collections
```

This will create a single JSON file containing all collection results.

## Monitoring Progress

You can monitor progress by checking the S3 bucket:

```bash
# Count total collections
aws s3 ls s3://your-bucket-name/collections/ | wc -l

# Count processed collections (those with result.json)
aws s3 ls s3://veda-odd-scratch/titiler-cmr-compatibility/collections/ --recursive | grep result.json | wc -l
```

Remove all results:

```
aws s3 rm s3://veda-odd-scratch/titiler-cmr-compatibility/collections/ --recursive --exclude "*" --include "*/result.json" --dryrun
aws s3 rm s3://veda-odd-scratch/titiler-cmr-compatibility/collections/ --recursive --exclude "*" --include "*/result.json"
```

You can also check Lithops logs:
```bash
# View Lithops logs
lithops logs
```

## Advantages Over Multiprocessing

1. **Fault Tolerance**: If one collection fails, only that one job fails - not the entire batch
2. **Resumable**: You can easily reprocess failed collections without redoing successful ones
3. **Scalability**: Lithops can scale to thousands of parallel executions using AWS Lambda
4. **State Tracking**: S3 directories provide clear visibility into processing state
5. **No CPU Contention**: Each collection runs in its own isolated environment
6. **No Timeouts**: Collections that take a long time to process won't affect others

## Troubleshooting

### Collection Processing Fails

Check the specific collection directory in S3. If there's no `result.json`, the collection failed. You can:

1. Run `--lithops-reprocess` to retry all failed collections
2. Process a specific collection manually using:
   ```bash
   python -m titiler_cmr_compatibility.cli \
     --collection-id C1234567890-PROVIDER
   ```

### Lithops Configuration Issues

Check your Lithops configuration:
```bash
lithops test
```

### AWS Permissions Issues

Ensure your AWS credentials have permissions for:
- S3: `s3:GetObject`, `s3:PutObject`, `s3:ListBucket`
- Lambda: `lambda:CreateFunction`, `lambda:InvokeFunction`, `lambda:GetFunction`

## Cost Considerations

- **AWS Lambda**: Charges per execution time (GB-seconds)
- **S3**: Charges for storage and requests
- **Example**: Processing 10,000 collections with 1GB Lambda for 30s each ≈ 83 Lambda GB-hours ≈ $1.40

To reduce costs:
- Use smaller Lambda memory if possible
- Clean up S3 results after downloading
- Use S3 lifecycle policies to archive old results

## Example Complete Workflow

```bash
# 1. Setup: Create collection directories
python -m titiler_cmr_compatibility.cli \
  --lithops --lithops-setup \
  --s3-bucket my-bucket \
  --total-collections 1000

# 2. Process: Process all collections
python -m titiler_cmr_compatibility.cli \
  --lithops --lithops-process \
  --s3-bucket my-bucket

# 3. Reprocess: Retry any failures (can run multiple times)
python -m titiler_cmr_compatibility.cli \
  --lithops --lithops-reprocess \
  --s3-bucket my-bucket

# 4. Download: Get all results
python -m titiler_cmr_compatibility.cli \
  --lithops --lithops-download \
  --s3-bucket my-bucket \
  --output-file results.json
```
