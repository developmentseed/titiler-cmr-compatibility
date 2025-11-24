# Lithops Distributed Processing Workflow

This document describes how to use the Lithops-based distributed processing workflow for processing CMR collections. This approach is more fault-tolerant than the multiprocessing approach and allows for easy resumption of failed jobs.

## Overview

The Lithops workflow consists of three simple phases:

1. **Setup**: Fetch all collection IDs from CMR and store them in `unprocessed/` directory
2. **Process**: Process all collections in `unprocessed/`, move them to `processed/` when done. Run repeatedly until all are processed.
3. **Download**: Download all results from `processed/` and compile into a single file

The workflow uses two S3 directories to track state:
- `unprocessed/`: Collections waiting to be processed
- `processed/`: Collections that have been successfully processed

This eliminates expensive S3 listing operations and makes it trivial to resume processing.

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

### Phase 1: Setup - Create Unprocessed Collection Markers

This phase fetches all collection IDs from CMR and creates markers in the `unprocessed/` directory. Collections remain in `unprocessed/` until they are successfully processed.

```bash
python -m titiler_cmr_compatibility.cli \
  --lithops \
  --lithops-setup \
  --s3-bucket veda-odd-scratch \
  --s3-prefix titiler-cmr-compatibility/collections-11-23 \
  --total-collections 10800 \
  --batch-size 100
```

Options:
- `--s3-bucket`: S3 bucket name (required)
- `--s3-prefix`: S3 prefix for collection directories (default: `collections`)
- `--total-collections`: Total number of collections to process (optional, defaults to all)
- `--batch-size`: Number of collections to fetch per page (default: 100)

This will create markers like:

```
s3://your-bucket-name/collections/unprocessed/C1234567890-PROVIDER/.marker
s3://your-bucket-name/collections/unprocessed/C0987654321-PROVIDER/.marker
...
```

### Phase 2: Process Collections

This phase processes all collections in `unprocessed/`. For each successful processing:
1. Result is written to `processed/CONCEPT_ID/status=.../reason=.../result.json`
2. Collection is removed from `unprocessed/`

Simply run this command repeatedly until all collections are processed:

```bash
python -m titiler_cmr_compatibility.cli \
  --lithops \
  --lithops-process \
  --s3-bucket veda-odd-scratch \
  --s3-prefix titiler-cmr-compatibility/collections-11-23 \
  --access-type direct
```

Options:
- `--access-type`: Access type for granules (`direct` for S3 links, `external` for HTTPS) (default: `direct`)

Each run processes whatever is left in `unprocessed/`. If a Lambda times out or fails, that collection stays in `unprocessed/` and will be retried next time.

Results are stored like:
```
s3://your-bucket-name/collections/processed/C1234567890-PROVIDER/status=true/reason=none/result.json
s3://your-bucket-name/collections/processed/C0987654321-PROVIDER/status=false/reason=unsupported_format/result.json
```

### Check Processing Status

To see how many collections have been processed:

```bash
python -m titiler_cmr_compatibility.cli \
  --lithops \
  --lithops-status \
  --s3-bucket veda-odd-scratch \
  --s3-prefix titiler-cmr-compatibility/collections-11-23
```

This shows:
- Total collections
- Processed count and percentage
- Unprocessed count and percentage


Or check S3 directly:

```bash
# Count unprocessed collections
aws s3 ls s3://veda-odd-scratch/titiler-cmr-compatibility/collections/unprocessed/ | wc -l

# Count processed collections
aws s3 ls s3://veda-odd-scratch/titiler-cmr-compatibility/collections/processed/ | wc -l
```

### Phase 3: Download Results

Once all collections are processed, download the results and compile them into a single file:

```bash
python -m titiler_cmr_compatibility.cli \
  --lithops \
  --lithops-download \
  --s3-bucket veda-odd-scratch \
  --s3-prefix titiler-cmr-compatibility/collections-11-23
```

This will create a single JSON file containing all collection results.

## Troubleshooting

### Check Lithops logs:

```bash
# View Lithops logs
lithops logs
```

### Finding Problematic Collections

If processing keeps failing, find which collections are causing issues:

```bash
# List first 20 unprocessed collections (ordered alphabetically)
aws s3 ls s3://veda-odd-scratch/titiler-cmr-compatibility/collections/unprocessed/ | head -20
```

These are likely the ones causing OOM or other errors. You can:

1. Process a specific collection manually to investigate:

```bash
python -m titiler_cmr_compatibility.cli \
  --collection-id C1234567890-PROVIDER
```

2. Skip problematic collections by manually moving them:

```bash
# Move to a "skipped" directory for later investigation
aws s3 mv s3://veda-odd-scratch/titiler-cmr-compatibility/collections/unprocessed/C1234567890-PROVIDER/ \
        s3://veda-odd-scratch/titiler-cmr-compatibility/collections/skipped/C1234567890-PROVIDER/ --recursive
```

## Cost Considerations

- **AWS Lambda**: Charges per execution time (GB-seconds)
- **S3**: Charges for storage and requests
- **Example**: Processing 10,000 collections with 1GB Lambda for 30s each ≈ 83 Lambda GB-hours ≈ $1.40

