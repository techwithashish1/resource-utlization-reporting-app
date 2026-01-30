"""
S3 Metrics Lambda Handler
Retrieves CloudWatch metrics for S3 buckets
"""

import boto3
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

from utils.cloudwatch_helper import CloudWatchHelper, parse_time_range, calculate_period
from utils.response_helper import success_response, error_response, not_found_response

# Configure logging
logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))

# S3 metrics to retrieve
S3_METRICS = [
    'BucketSizeBytes',
    'NumberOfObjects'
]

# S3 Request metrics (requires request metrics to be enabled on bucket)
S3_REQUEST_METRICS = [
    'AllRequests',
    'GetRequests',
    'PutRequests',
    'DeleteRequests',
    '4xxErrors',
    '5xxErrors',
    'FirstByteLatency',
    'TotalRequestLatency'
]


def get_all_buckets() -> List[str]:
    """Get list of all S3 buckets"""
    s3_client = boto3.client('s3')
    response = s3_client.list_buckets()
    return [bucket['Name'] for bucket in response.get('Buckets', [])]


def get_bucket_metrics(
    bucket_name: str,
    hours: int = 24,
    include_request_metrics: bool = False
) -> Dict[str, Any]:
    """
    Get CloudWatch metrics for a specific S3 bucket
    
    Args:
        bucket_name: Name of the S3 bucket
        hours: Number of hours to look back
        include_request_metrics: Whether to include request metrics
    
    Returns:
        Dictionary containing bucket metrics
    """
    cw_helper = CloudWatchHelper()
    start_time, end_time = parse_time_range(hours)
    period = calculate_period(hours)
    
    metrics_data = {
        'bucket_name': bucket_name,
        'metrics': {},
        'time_range': {
            'start': start_time.isoformat(),
            'end': end_time.isoformat(),
            'hours': hours
        }
    }
    
    # Get storage metrics (daily metrics for S3)
    for metric_name in S3_METRICS:
        try:
            # S3 storage metrics require specific storage type dimension
            for storage_type in ['StandardStorage', 'StandardIAStorage', 'GlacierStorage']:
                dimensions = [
                    {'Name': 'BucketName', 'Value': bucket_name},
                    {'Name': 'StorageType', 'Value': storage_type}
                ]
                
                result = cw_helper.get_metric_statistics(
                    namespace='AWS/S3',
                    metric_name=metric_name,
                    dimensions=dimensions,
                    start_time=start_time,
                    end_time=end_time,
                    period=86400,  # S3 storage metrics are daily
                    statistics=['Average', 'Maximum']
                )
                
                if result['datapoints']:
                    key = f"{metric_name}_{storage_type}"
                    metrics_data['metrics'][key] = result
        except Exception as e:
            logger.warning(f"Could not get {metric_name} for {bucket_name}: {str(e)}")
    
    # Get request metrics if enabled
    if include_request_metrics:
        for metric_name in S3_REQUEST_METRICS:
            try:
                dimensions = [
                    {'Name': 'BucketName', 'Value': bucket_name},
                    {'Name': 'FilterId', 'Value': 'EntireBucket'}
                ]
                
                result = cw_helper.get_metric_statistics(
                    namespace='AWS/S3',
                    metric_name=metric_name,
                    dimensions=dimensions,
                    start_time=start_time,
                    end_time=end_time,
                    period=period,
                    statistics=['Sum', 'Average', 'Maximum']
                )
                
                if result['datapoints']:
                    metrics_data['metrics'][metric_name] = result
            except Exception as e:
                logger.warning(f"Could not get request metric {metric_name}: {str(e)}")
    
    return metrics_data


def get_all_buckets_summary(hours: int = 24) -> Dict[str, Any]:
    """
    Get summary metrics for all S3 buckets
    
    Args:
        hours: Number of hours to look back
    
    Returns:
        Dictionary containing summary for all buckets
    """
    buckets = get_all_buckets()
    
    summary = {
        'total_buckets': len(buckets),
        'buckets': [],
        'aggregated': {
            'total_size_bytes': 0,
            'total_objects': 0
        },
        'time_range_hours': hours
    }
    
    for bucket_name in buckets:
        try:
            bucket_info = {
                'name': bucket_name,
                'size_bytes': 0,
                'number_of_objects': 0
            }
            
            metrics = get_bucket_metrics(bucket_name, hours)
            
            # Extract latest values
            for key, data in metrics.get('metrics', {}).items():
                if 'BucketSizeBytes' in key and data.get('datapoints'):
                    latest = data['datapoints'][-1] if data['datapoints'] else {}
                    size = latest.get('Average', 0)
                    bucket_info['size_bytes'] += size
                    summary['aggregated']['total_size_bytes'] += size
                elif 'NumberOfObjects' in key and data.get('datapoints'):
                    latest = data['datapoints'][-1] if data['datapoints'] else {}
                    count = latest.get('Average', 0)
                    bucket_info['number_of_objects'] += count
                    summary['aggregated']['total_objects'] += count
            
            summary['buckets'].append(bucket_info)
        except Exception as e:
            logger.warning(f"Could not get metrics for bucket {bucket_name}: {str(e)}")
            summary['buckets'].append({
                'name': bucket_name,
                'error': str(e)
            })
    
    return summary


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for S3 metrics
    
    Args:
        event: API Gateway event
        context: Lambda context
    
    Returns:
        API Gateway response
    """
    logger.info(f"Received event: {event}")
    
    try:
        # Parse path parameters
        path_params = event.get('pathParameters') or {}
        bucket_name = path_params.get('bucket_name')
        
        # Parse query parameters
        query_params = event.get('queryStringParameters') or {}
        hours = int(query_params.get('hours', 24))
        include_request_metrics = query_params.get('include_request_metrics', 'false').lower() == 'true'
        
        # Validate hours parameter
        if hours < 1 or hours > 720:  # Max 30 days
            return error_response(
                message="Hours parameter must be between 1 and 720",
                status_code=400
            )
        
        if bucket_name:
            # Get metrics for specific bucket
            logger.info(f"Getting metrics for bucket: {bucket_name}")
            
            # Verify bucket exists
            buckets = get_all_buckets()
            if bucket_name not in buckets:
                return not_found_response(f"Bucket '{bucket_name}'")
            
            metrics = get_bucket_metrics(
                bucket_name=bucket_name,
                hours=hours,
                include_request_metrics=include_request_metrics
            )
            
            return success_response(
                data=metrics,
                message=f"S3 metrics for bucket '{bucket_name}'"
            )
        else:
            # Get summary for all buckets
            logger.info("Getting metrics summary for all S3 buckets")
            summary = get_all_buckets_summary(hours)
            
            return success_response(
                data=summary,
                message="S3 metrics summary for all buckets"
            )
    
    except Exception as e:
        logger.error(f"Error processing request: {str(e)}", exc_info=True)
        return error_response(
            message=f"Error retrieving S3 metrics: {str(e)}",
            status_code=500
        )
