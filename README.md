# Resource Metrics & Analytics API

A serverless API application that retrieves CloudWatch metrics for AWS resources (S3, DynamoDB, and Lambda) using AWS API Gateway and Lambda functions.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        API Gateway                              │
│  /api/metrics/s3          /api/metrics/dynamodb                 │
│  /api/metrics/lambda      /api/metrics/report                   │
└─────────────────────────────────────────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
        ▼                     ▼                     ▼
┌───────────────┐   ┌───────────────┐   ┌───────────────┐
│ S3 Metrics    │   │ DynamoDB      │   │ Lambda        │
│ Lambda        │   │ Metrics Lambda│   │ Metrics Lambda│
└───────────────┘   └───────────────┘   └───────────────┘
        │                     │                     │
        └─────────────────────┼─────────────────────┘
                              │
                              ▼
                    ┌───────────────┐
                    │  CloudWatch   │
                    │   Metrics     │
                    └───────────────┘
```

## API Endpoints

### S3 Metrics
- `GET /api/metrics/s3` - Get summary metrics for all S3 buckets
- `GET /api/metrics/s3/{bucket_name}` - Get detailed metrics for a specific bucket

Query Parameters:
- `hours` (optional): Number of hours to look back (1-720, default: 24)
- `include_request_metrics` (optional): Include request metrics if enabled (true/false)

### DynamoDB Metrics
- `GET /api/metrics/dynamodb` - Get summary metrics for all DynamoDB tables
- `GET /api/metrics/dynamodb/{table_name}` - Get detailed metrics for a specific table

Query Parameters:
- `hours` (optional): Number of hours to look back (1-720, default: 24)
- `include_operations` (optional): Include per-operation metrics (true/false)

### Lambda Metrics
- `GET /api/metrics/lambda` - Get summary metrics for all Lambda functions
- `GET /api/metrics/lambda/{function_name}` - Get detailed metrics for a specific function

Query Parameters:
- `hours` (optional): Number of hours to look back (1-720, default: 24)

### Aggregated Report
- `GET /api/metrics/report` - Generate aggregated metrics report
- `POST /api/metrics/report` - Generate report with custom parameters

Query/Body Parameters:
- `hours` (optional): Number of hours to look back (1-720, default: 24)
- `services` (optional): Comma-separated list of services (s3,dynamodb,lambda)

## Prerequisites

- [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-install.html)
- [Python 3.11+](https://www.python.org/downloads/)
- [Docker](https://www.docker.com/products/docker-desktop) (for local testing)
- AWS credentials configured

## Deployment

### Build the application
```bash
sam build
```

### Deploy to AWS
```bash
# Guided deployment (first time)
sam deploy --guided

# Deploy to specific environment
sam deploy --config-env dev
sam deploy --config-env staging
sam deploy --config-env prod
```

### Validate template
```bash
sam validate --lint
```

## Local Development

### Start local API
```bash
sam local start-api
```

### Invoke a single function
```bash
sam local invoke S3MetricsFunction --event events/s3_event.json
```

### Run tests
```bash
python -m pytest tests/ -v
```

## Example API Responses

### S3 Summary Response
```json
{
  "status": "success",
  "message": "S3 metrics summary for all buckets",
  "data": {
    "total_buckets": 5,
    "buckets": [
      {
        "name": "my-bucket",
        "size_bytes": 1073741824,
        "number_of_objects": 1000
      }
    ],
    "aggregated": {
      "total_size_bytes": 5368709120,
      "total_objects": 5000
    },
    "time_range_hours": 24
  },
  "timestamp": "2026-01-28T10:30:00.000000"
}
```

### Aggregated Report Response
```json
{
  "status": "success",
  "message": "Resource metrics report generated successfully",
  "data": {
    "report_generated_at": "2026-01-28T10:30:00.000000",
    "time_range": {
      "start": "2026-01-27T10:30:00.000000",
      "end": "2026-01-28T10:30:00.000000",
      "hours": 24
    },
    "services": {
      "s3": {
        "bucket_count": 5,
        "total_size_gb": 5.0,
        "status": "healthy"
      },
      "dynamodb": {
        "table_count": 3,
        "total_throttle_events": 0,
        "status": "healthy"
      },
      "lambda": {
        "function_count": 10,
        "total_invocations": 50000,
        "error_rate_percent": 0.5,
        "status": "healthy"
      }
    },
    "overall_status": "healthy",
    "recommendations": [
      "All services are operating within normal parameters."
    ]
  },
  "timestamp": "2026-01-28T10:30:00.000000"
}
```

## Project Structure

```
resource-utlization-reporting-app/
├── template.yaml           # SAM template
├── README.md               # This file
├── src/
│   ├── requirements.txt    # Python dependencies
│   ├── handlers/
│   │   ├── s3_metrics.py       # S3 metrics handler
│   │   ├── dynamodb_metrics.py # DynamoDB metrics handler
│   │   ├── lambda_metrics.py   # Lambda metrics handler
│   │   └── metrics_report.py   # Aggregated report handler
│   └── utils/
│       ├── cloudwatch_helper.py  # CloudWatch API utilities
│       └── response_helper.py    # API response utilities
```

## IAM Permissions

The Lambda functions require the following IAM permissions:
- `cloudwatch:GetMetricData`
- `cloudwatch:GetMetricStatistics`
- `cloudwatch:ListMetrics`
- `s3:ListAllMyBuckets`
- `s3:GetBucketLocation`
- `dynamodb:ListTables`
- `dynamodb:DescribeTable`
- `lambda:ListFunctions`
- `lambda:GetFunction`

