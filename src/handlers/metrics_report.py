"""
Metrics Report Lambda Handler
Generates aggregated metrics reports across S3, DynamoDB, and Lambda
"""

import boto3
import logging
import os
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

from utils.cloudwatch_helper import CloudWatchHelper, parse_time_range, calculate_period
from utils.response_helper import success_response, error_response

# Configure logging
logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))


def get_s3_summary(cw_helper: CloudWatchHelper, start_time: datetime, end_time: datetime) -> Dict[str, Any]:
    """Get S3 metrics summary"""
    s3_client = boto3.client('s3')
    
    try:
        buckets = s3_client.list_buckets().get('Buckets', [])
        bucket_count = len(buckets)
        
        total_size = 0
        total_objects = 0
        
        for bucket in buckets[:10]:  # Limit to first 10 for performance
            bucket_name = bucket['Name']
            try:
                for storage_type in ['StandardStorage']:
                    result = cw_helper.get_metric_statistics(
                        namespace='AWS/S3',
                        metric_name='BucketSizeBytes',
                        dimensions=[
                            {'Name': 'BucketName', 'Value': bucket_name},
                            {'Name': 'StorageType', 'Value': storage_type}
                        ],
                        start_time=start_time,
                        end_time=end_time,
                        period=86400,
                        statistics=['Average']
                    )
                    if result.get('datapoints'):
                        total_size += result['datapoints'][-1].get('Average', 0)
                    
                    result = cw_helper.get_metric_statistics(
                        namespace='AWS/S3',
                        metric_name='NumberOfObjects',
                        dimensions=[
                            {'Name': 'BucketName', 'Value': bucket_name},
                            {'Name': 'StorageType', 'Value': 'AllStorageTypes'}
                        ],
                        start_time=start_time,
                        end_time=end_time,
                        period=86400,
                        statistics=['Average']
                    )
                    if result.get('datapoints'):
                        total_objects += result['datapoints'][-1].get('Average', 0)
            except Exception as e:
                logger.warning(f"Error getting S3 metrics for {bucket_name}: {e}")
        
        return {
            'bucket_count': bucket_count,
            'total_size_bytes': total_size,
            'total_size_gb': round(total_size / (1024 ** 3), 2),
            'total_objects': int(total_objects),
            'status': 'healthy'
        }
    except Exception as e:
        logger.error(f"Error getting S3 summary: {e}")
        return {'error': str(e), 'status': 'error'}


def get_dynamodb_summary(cw_helper: CloudWatchHelper, start_time: datetime, end_time: datetime, period: int) -> Dict[str, Any]:
    """Get DynamoDB metrics summary"""
    dynamodb = boto3.client('dynamodb')
    
    try:
        tables = []
        paginator = dynamodb.get_paginator('list_tables')
        for page in paginator.paginate():
            tables.extend(page.get('TableNames', []))
        
        table_count = len(tables)
        total_read_consumed = 0
        total_write_consumed = 0
        total_throttles = 0
        
        for table_name in tables[:20]:  # Limit for performance
            try:
                # Read capacity
                result = cw_helper.get_metric_statistics(
                    namespace='AWS/DynamoDB',
                    metric_name='ConsumedReadCapacityUnits',
                    dimensions=[{'Name': 'TableName', 'Value': table_name}],
                    start_time=start_time,
                    end_time=end_time,
                    period=period,
                    statistics=['Sum']
                )
                total_read_consumed += sum(dp.get('Sum', 0) for dp in result.get('datapoints', []))
                
                # Write capacity
                result = cw_helper.get_metric_statistics(
                    namespace='AWS/DynamoDB',
                    metric_name='ConsumedWriteCapacityUnits',
                    dimensions=[{'Name': 'TableName', 'Value': table_name}],
                    start_time=start_time,
                    end_time=end_time,
                    period=period,
                    statistics=['Sum']
                )
                total_write_consumed += sum(dp.get('Sum', 0) for dp in result.get('datapoints', []))
                
                # Throttles
                for metric in ['ReadThrottleEvents', 'WriteThrottleEvents']:
                    result = cw_helper.get_metric_statistics(
                        namespace='AWS/DynamoDB',
                        metric_name=metric,
                        dimensions=[{'Name': 'TableName', 'Value': table_name}],
                        start_time=start_time,
                        end_time=end_time,
                        period=period,
                        statistics=['Sum']
                    )
                    total_throttles += sum(dp.get('Sum', 0) for dp in result.get('datapoints', []))
            except Exception as e:
                logger.warning(f"Error getting DynamoDB metrics for {table_name}: {e}")
        
        return {
            'table_count': table_count,
            'total_read_capacity_consumed': round(total_read_consumed, 2),
            'total_write_capacity_consumed': round(total_write_consumed, 2),
            'total_throttle_events': int(total_throttles),
            'status': 'healthy' if total_throttles == 0 else 'warning'
        }
    except Exception as e:
        logger.error(f"Error getting DynamoDB summary: {e}")
        return {'error': str(e), 'status': 'error'}


def get_lambda_summary(cw_helper: CloudWatchHelper, start_time: datetime, end_time: datetime, period: int) -> Dict[str, Any]:
    """Get Lambda metrics summary"""
    lambda_client = boto3.client('lambda')
    
    try:
        functions = []
        paginator = lambda_client.get_paginator('list_functions')
        for page in paginator.paginate():
            functions.extend(page.get('Functions', []))
        
        function_count = len(functions)
        total_invocations = 0
        total_errors = 0
        total_throttles = 0
        total_duration = 0
        duration_count = 0
        
        for func in functions[:30]:  # Limit for performance
            function_name = func['FunctionName']
            try:
                # Invocations
                result = cw_helper.get_metric_statistics(
                    namespace='AWS/Lambda',
                    metric_name='Invocations',
                    dimensions=[{'Name': 'FunctionName', 'Value': function_name}],
                    start_time=start_time,
                    end_time=end_time,
                    period=period,
                    statistics=['Sum']
                )
                total_invocations += sum(dp.get('Sum', 0) for dp in result.get('datapoints', []))
                
                # Errors
                result = cw_helper.get_metric_statistics(
                    namespace='AWS/Lambda',
                    metric_name='Errors',
                    dimensions=[{'Name': 'FunctionName', 'Value': function_name}],
                    start_time=start_time,
                    end_time=end_time,
                    period=period,
                    statistics=['Sum']
                )
                total_errors += sum(dp.get('Sum', 0) for dp in result.get('datapoints', []))
                
                # Throttles
                result = cw_helper.get_metric_statistics(
                    namespace='AWS/Lambda',
                    metric_name='Throttles',
                    dimensions=[{'Name': 'FunctionName', 'Value': function_name}],
                    start_time=start_time,
                    end_time=end_time,
                    period=period,
                    statistics=['Sum']
                )
                total_throttles += sum(dp.get('Sum', 0) for dp in result.get('datapoints', []))
                
                # Duration
                result = cw_helper.get_metric_statistics(
                    namespace='AWS/Lambda',
                    metric_name='Duration',
                    dimensions=[{'Name': 'FunctionName', 'Value': function_name}],
                    start_time=start_time,
                    end_time=end_time,
                    period=period,
                    statistics=['Average']
                )
                for dp in result.get('datapoints', []):
                    total_duration += dp.get('Average', 0)
                    duration_count += 1
            except Exception as e:
                logger.warning(f"Error getting Lambda metrics for {function_name}: {e}")
        
        error_rate = (total_errors / total_invocations * 100) if total_invocations > 0 else 0
        avg_duration = (total_duration / duration_count) if duration_count > 0 else 0
        
        return {
            'function_count': function_count,
            'total_invocations': int(total_invocations),
            'total_errors': int(total_errors),
            'total_throttles': int(total_throttles),
            'error_rate_percent': round(error_rate, 2),
            'avg_duration_ms': round(avg_duration, 2),
            'status': 'healthy' if error_rate < 1 and total_throttles == 0 else 'warning'
        }
    except Exception as e:
        logger.error(f"Error getting Lambda summary: {e}")
        return {'error': str(e), 'status': 'error'}


def generate_report(hours: int = 24, services: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Generate aggregated metrics report
    
    Args:
        hours: Number of hours to look back
        services: List of services to include (s3, dynamodb, lambda). Defaults to all.
    
    Returns:
        Dictionary containing aggregated report
    """
    if services is None:
        services = ['s3', 'dynamodb', 'lambda']
    
    cw_helper = CloudWatchHelper()
    start_time, end_time = parse_time_range(hours)
    period = calculate_period(hours)
    
    report = {
        'report_generated_at': datetime.utcnow().isoformat(),
        'time_range': {
            'start': start_time.isoformat(),
            'end': end_time.isoformat(),
            'hours': hours
        },
        'services': {},
        'overall_status': 'healthy'
    }
    
    statuses = []
    
    if 's3' in services:
        logger.info("Getting S3 summary...")
        report['services']['s3'] = get_s3_summary(cw_helper, start_time, end_time)
        statuses.append(report['services']['s3'].get('status', 'unknown'))
    
    if 'dynamodb' in services:
        logger.info("Getting DynamoDB summary...")
        report['services']['dynamodb'] = get_dynamodb_summary(cw_helper, start_time, end_time, period)
        statuses.append(report['services']['dynamodb'].get('status', 'unknown'))
    
    if 'lambda' in services:
        logger.info("Getting Lambda summary...")
        report['services']['lambda'] = get_lambda_summary(cw_helper, start_time, end_time, period)
        statuses.append(report['services']['lambda'].get('status', 'unknown'))
    
    # Determine overall status
    if 'error' in statuses:
        report['overall_status'] = 'error'
    elif 'warning' in statuses:
        report['overall_status'] = 'warning'
    else:
        report['overall_status'] = 'healthy'
    
    # Add recommendations based on findings
    report['recommendations'] = generate_recommendations(report['services'])
    
    return report


def generate_recommendations(services: Dict[str, Any]) -> List[str]:
    """Generate recommendations based on metrics"""
    recommendations = []
    
    # S3 recommendations
    if 's3' in services and 'error' not in services['s3']:
        s3 = services['s3']
        if s3.get('total_size_gb', 0) > 100:
            recommendations.append(
                "Consider implementing S3 Lifecycle policies to manage storage costs for large buckets."
            )
    
    # DynamoDB recommendations
    if 'dynamodb' in services and 'error' not in services['dynamodb']:
        ddb = services['dynamodb']
        if ddb.get('total_throttle_events', 0) > 0:
            recommendations.append(
                f"DynamoDB throttling detected ({ddb['total_throttle_events']} events). "
                "Consider increasing provisioned capacity or switching to on-demand mode."
            )
    
    # Lambda recommendations
    if 'lambda' in services and 'error' not in services['lambda']:
        lam = services['lambda']
        if lam.get('error_rate_percent', 0) > 1:
            recommendations.append(
                f"Lambda error rate is {lam['error_rate_percent']}%. "
                "Review CloudWatch logs to identify and fix recurring errors."
            )
        if lam.get('total_throttles', 0) > 0:
            recommendations.append(
                f"Lambda throttling detected ({lam['total_throttles']} events). "
                "Consider requesting a concurrency limit increase."
            )
        if lam.get('avg_duration_ms', 0) > 10000:
            recommendations.append(
                f"Average Lambda duration is high ({lam['avg_duration_ms']}ms). "
                "Consider optimizing function code or increasing memory allocation."
            )
    
    if not recommendations:
        recommendations.append("All services are operating within normal parameters.")
    
    return recommendations


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for generating metrics report
    
    Args:
        event: API Gateway event
        context: Lambda context
    
    Returns:
        API Gateway response
    """
    logger.info(f"Received event: {event}")
    
    try:
        http_method = event.get('httpMethod', 'GET')
        
        # Parse parameters
        if http_method == 'POST':
            body = json.loads(event.get('body', '{}')) if event.get('body') else {}
            hours = body.get('hours', 24)
            services = body.get('services')
        else:
            query_params = event.get('queryStringParameters') or {}
            hours = int(query_params.get('hours', 24))
            services_param = query_params.get('services')
            services = services_param.split(',') if services_param else None
        
        # Validate hours parameter
        if hours < 1 or hours > 720:
            return error_response(
                message="Hours parameter must be between 1 and 720",
                status_code=400
            )
        
        # Validate services parameter
        valid_services = {'s3', 'dynamodb', 'lambda'}
        if services:
            invalid_services = set(services) - valid_services
            if invalid_services:
                return error_response(
                    message=f"Invalid services: {invalid_services}. Valid options: {valid_services}",
                    status_code=400
                )
        
        logger.info(f"Generating report for hours={hours}, services={services}")
        report = generate_report(hours=hours, services=services)
        
        return success_response(
            data=report,
            message="Resource metrics report generated successfully"
        )
    
    except json.JSONDecodeError as e:
        return error_response(
            message=f"Invalid JSON in request body: {str(e)}",
            status_code=400
        )
    except Exception as e:
        logger.error(f"Error processing request: {str(e)}", exc_info=True)
        return error_response(
            message=f"Error generating metrics report: {str(e)}",
            status_code=500
        )
