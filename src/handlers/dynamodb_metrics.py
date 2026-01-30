"""
DynamoDB Metrics Lambda Handler
Retrieves CloudWatch metrics for DynamoDB tables
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

# DynamoDB metrics to retrieve
DYNAMODB_METRICS = [
    'ConsumedReadCapacityUnits',
    'ConsumedWriteCapacityUnits',
    'ProvisionedReadCapacityUnits',
    'ProvisionedWriteCapacityUnits',
    'ReadThrottleEvents',
    'WriteThrottleEvents',
    'ThrottledRequests',
    'ConditionalCheckFailedRequests',
    'SuccessfulRequestLatency',
    'SystemErrors',
    'UserErrors',
    'ReturnedItemCount',
    'ReturnedBytes'
]

# Operation-specific metrics
OPERATION_METRICS = [
    'GetItem',
    'PutItem',
    'Query',
    'Scan',
    'DeleteItem',
    'UpdateItem'
]


def get_all_tables() -> List[Dict[str, Any]]:
    """Get list of all DynamoDB tables with basic info"""
    dynamodb = boto3.client('dynamodb')
    tables = []
    
    paginator = dynamodb.get_paginator('list_tables')
    for page in paginator.paginate():
        for table_name in page.get('TableNames', []):
            try:
                table_info = dynamodb.describe_table(TableName=table_name)['Table']
                tables.append({
                    'name': table_name,
                    'status': table_info.get('TableStatus'),
                    'item_count': table_info.get('ItemCount', 0),
                    'size_bytes': table_info.get('TableSizeBytes', 0),
                    'billing_mode': table_info.get('BillingModeSummary', {}).get('BillingMode', 'PROVISIONED')
                })
            except Exception as e:
                logger.warning(f"Could not describe table {table_name}: {str(e)}")
                tables.append({'name': table_name, 'error': str(e)})
    
    return tables


def get_table_metrics(
    table_name: str,
    hours: int = 24,
    include_operations: bool = False
) -> Dict[str, Any]:
    """
    Get CloudWatch metrics for a specific DynamoDB table
    
    Args:
        table_name: Name of the DynamoDB table
        hours: Number of hours to look back
        include_operations: Whether to include per-operation metrics
    
    Returns:
        Dictionary containing table metrics
    """
    cw_helper = CloudWatchHelper()
    start_time, end_time = parse_time_range(hours)
    period = calculate_period(hours)
    
    metrics_data = {
        'table_name': table_name,
        'metrics': {},
        'time_range': {
            'start': start_time.isoformat(),
            'end': end_time.isoformat(),
            'hours': hours
        }
    }
    
    # Get table-level metrics
    for metric_name in DYNAMODB_METRICS:
        try:
            dimensions = [
                {'Name': 'TableName', 'Value': table_name}
            ]
            
            result = cw_helper.get_metric_statistics(
                namespace='AWS/DynamoDB',
                metric_name=metric_name,
                dimensions=dimensions,
                start_time=start_time,
                end_time=end_time,
                period=period,
                statistics=['Sum', 'Average', 'Maximum', 'Minimum']
            )
            
            if result['datapoints']:
                metrics_data['metrics'][metric_name] = result
        except Exception as e:
            logger.warning(f"Could not get {metric_name} for {table_name}: {str(e)}")
    
    # Get operation-specific metrics if requested
    if include_operations:
        for operation in OPERATION_METRICS:
            try:
                dimensions = [
                    {'Name': 'TableName', 'Value': table_name},
                    {'Name': 'Operation', 'Value': operation}
                ]
                
                result = cw_helper.get_metric_statistics(
                    namespace='AWS/DynamoDB',
                    metric_name='SuccessfulRequestLatency',
                    dimensions=dimensions,
                    start_time=start_time,
                    end_time=end_time,
                    period=period,
                    statistics=['Average', 'Maximum', 'Minimum', 'SampleCount']
                )
                
                if result['datapoints']:
                    metrics_data['metrics'][f'Latency_{operation}'] = result
            except Exception as e:
                logger.warning(f"Could not get operation metric for {operation}: {str(e)}")
    
    return metrics_data


def get_all_tables_summary(hours: int = 24) -> Dict[str, Any]:
    """
    Get summary metrics for all DynamoDB tables
    
    Args:
        hours: Number of hours to look back
    
    Returns:
        Dictionary containing summary for all tables
    """
    tables = get_all_tables()
    cw_helper = CloudWatchHelper()
    start_time, end_time = parse_time_range(hours)
    period = calculate_period(hours)
    
    summary = {
        'total_tables': len(tables),
        'tables': [],
        'aggregated': {
            'total_read_capacity_consumed': 0,
            'total_write_capacity_consumed': 0,
            'total_throttle_events': 0,
            'total_size_bytes': 0,
            'total_items': 0
        },
        'time_range_hours': hours
    }
    
    for table in tables:
        if 'error' in table:
            summary['tables'].append(table)
            continue
        
        table_name = table['name']
        table_summary = {
            'name': table_name,
            'status': table.get('status'),
            'billing_mode': table.get('billing_mode'),
            'item_count': table.get('item_count', 0),
            'size_bytes': table.get('size_bytes', 0),
            'read_capacity_consumed': 0,
            'write_capacity_consumed': 0,
            'throttle_events': 0
        }
        
        summary['aggregated']['total_size_bytes'] += table.get('size_bytes', 0)
        summary['aggregated']['total_items'] += table.get('item_count', 0)
        
        try:
            # Get consumed capacity metrics
            for metric_name in ['ConsumedReadCapacityUnits', 'ConsumedWriteCapacityUnits']:
                dimensions = [{'Name': 'TableName', 'Value': table_name}]
                result = cw_helper.get_metric_statistics(
                    namespace='AWS/DynamoDB',
                    metric_name=metric_name,
                    dimensions=dimensions,
                    start_time=start_time,
                    end_time=end_time,
                    period=period,
                    statistics=['Sum']
                )
                
                total = sum(dp.get('Sum', 0) for dp in result.get('datapoints', []))
                if 'Read' in metric_name:
                    table_summary['read_capacity_consumed'] = total
                    summary['aggregated']['total_read_capacity_consumed'] += total
                else:
                    table_summary['write_capacity_consumed'] = total
                    summary['aggregated']['total_write_capacity_consumed'] += total
            
            # Get throttle events
            for metric_name in ['ReadThrottleEvents', 'WriteThrottleEvents']:
                dimensions = [{'Name': 'TableName', 'Value': table_name}]
                result = cw_helper.get_metric_statistics(
                    namespace='AWS/DynamoDB',
                    metric_name=metric_name,
                    dimensions=dimensions,
                    start_time=start_time,
                    end_time=end_time,
                    period=period,
                    statistics=['Sum']
                )
                
                total = sum(dp.get('Sum', 0) for dp in result.get('datapoints', []))
                table_summary['throttle_events'] += total
                summary['aggregated']['total_throttle_events'] += total
        
        except Exception as e:
            logger.warning(f"Could not get metrics for table {table_name}: {str(e)}")
            table_summary['metrics_error'] = str(e)
        
        summary['tables'].append(table_summary)
    
    return summary


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for DynamoDB metrics
    
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
        table_name = path_params.get('table_name')
        
        # Parse query parameters
        query_params = event.get('queryStringParameters') or {}
        hours = int(query_params.get('hours', 24))
        include_operations = query_params.get('include_operations', 'false').lower() == 'true'
        
        # Validate hours parameter
        if hours < 1 or hours > 720:  # Max 30 days
            return error_response(
                message="Hours parameter must be between 1 and 720",
                status_code=400
            )
        
        if table_name:
            # Get metrics for specific table
            logger.info(f"Getting metrics for table: {table_name}")
            
            # Verify table exists
            tables = get_all_tables()
            table_names = [t['name'] for t in tables]
            if table_name not in table_names:
                return not_found_response(f"Table '{table_name}'")
            
            metrics = get_table_metrics(
                table_name=table_name,
                hours=hours,
                include_operations=include_operations
            )
            
            return success_response(
                data=metrics,
                message=f"DynamoDB metrics for table '{table_name}'"
            )
        else:
            # Get summary for all tables
            logger.info("Getting metrics summary for all DynamoDB tables")
            summary = get_all_tables_summary(hours)
            
            return success_response(
                data=summary,
                message="DynamoDB metrics summary for all tables"
            )
    
    except Exception as e:
        logger.error(f"Error processing request: {str(e)}", exc_info=True)
        return error_response(
            message=f"Error retrieving DynamoDB metrics: {str(e)}",
            status_code=500
        )
