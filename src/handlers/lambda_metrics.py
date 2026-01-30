"""
Lambda Metrics Lambda Handler
Retrieves CloudWatch metrics for Lambda functions
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

# Lambda metrics to retrieve
LAMBDA_METRICS = [
    'Invocations',
    'Errors',
    'Throttles',
    'Duration',
    'ConcurrentExecutions',
    'IteratorAge',
    'DeadLetterErrors',
    'DestinationDeliveryFailures',
    'ProvisionedConcurrencyInvocations',
    'ProvisionedConcurrencySpilloverInvocations'
]


def get_all_functions() -> List[Dict[str, Any]]:
    """Get list of all Lambda functions with basic info"""
    lambda_client = boto3.client('lambda')
    functions = []
    
    paginator = lambda_client.get_paginator('list_functions')
    for page in paginator.paginate():
        for func in page.get('Functions', []):
            functions.append({
                'name': func['FunctionName'],
                'runtime': func.get('Runtime', 'N/A'),
                'memory_size': func.get('MemorySize', 0),
                'timeout': func.get('Timeout', 0),
                'code_size': func.get('CodeSize', 0),
                'last_modified': func.get('LastModified', ''),
                'description': func.get('Description', '')
            })
    
    return functions


def get_function_metrics(
    function_name: str,
    hours: int = 24
) -> Dict[str, Any]:
    """
    Get CloudWatch metrics for a specific Lambda function
    
    Args:
        function_name: Name of the Lambda function
        hours: Number of hours to look back
    
    Returns:
        Dictionary containing function metrics
    """
    cw_helper = CloudWatchHelper()
    start_time, end_time = parse_time_range(hours)
    period = calculate_period(hours)
    
    metrics_data = {
        'function_name': function_name,
        'metrics': {},
        'time_range': {
            'start': start_time.isoformat(),
            'end': end_time.isoformat(),
            'hours': hours
        }
    }
    
    # Get function-level metrics
    for metric_name in LAMBDA_METRICS:
        try:
            dimensions = [
                {'Name': 'FunctionName', 'Value': function_name}
            ]
            
            # Use appropriate statistics based on metric type
            if metric_name == 'Duration':
                statistics = ['Average', 'Maximum', 'Minimum', 'SampleCount']
            elif metric_name == 'ConcurrentExecutions':
                statistics = ['Maximum', 'Average']
            else:
                statistics = ['Sum', 'Average', 'Maximum']
            
            result = cw_helper.get_metric_statistics(
                namespace='AWS/Lambda',
                metric_name=metric_name,
                dimensions=dimensions,
                start_time=start_time,
                end_time=end_time,
                period=period,
                statistics=statistics
            )
            
            if result['datapoints']:
                metrics_data['metrics'][metric_name] = result
        except Exception as e:
            logger.warning(f"Could not get {metric_name} for {function_name}: {str(e)}")
    
    # Calculate derived metrics
    if 'Invocations' in metrics_data['metrics'] and 'Errors' in metrics_data['metrics']:
        invocations_data = metrics_data['metrics']['Invocations']
        errors_data = metrics_data['metrics']['Errors']
        
        total_invocations = sum(dp.get('Sum', 0) for dp in invocations_data.get('datapoints', []))
        total_errors = sum(dp.get('Sum', 0) for dp in errors_data.get('datapoints', []))
        
        metrics_data['summary'] = {
            'total_invocations': total_invocations,
            'total_errors': total_errors,
            'error_rate': (total_errors / total_invocations * 100) if total_invocations > 0 else 0
        }
        
        if 'Duration' in metrics_data['metrics']:
            duration_data = metrics_data['metrics']['Duration']
            avg_duration = sum(dp.get('Average', 0) for dp in duration_data.get('datapoints', [])) / max(len(duration_data.get('datapoints', [])), 1)
            max_duration = max((dp.get('Maximum', 0) for dp in duration_data.get('datapoints', [])), default=0)
            
            metrics_data['summary']['avg_duration_ms'] = avg_duration
            metrics_data['summary']['max_duration_ms'] = max_duration
    
    return metrics_data


def get_all_functions_summary(hours: int = 24) -> Dict[str, Any]:
    """
    Get summary metrics for all Lambda functions
    
    Args:
        hours: Number of hours to look back
    
    Returns:
        Dictionary containing summary for all functions
    """
    functions = get_all_functions()
    cw_helper = CloudWatchHelper()
    start_time, end_time = parse_time_range(hours)
    period = calculate_period(hours)
    
    summary = {
        'total_functions': len(functions),
        'functions': [],
        'aggregated': {
            'total_invocations': 0,
            'total_errors': 0,
            'total_throttles': 0,
            'total_code_size_bytes': 0,
            'avg_error_rate': 0
        },
        'time_range_hours': hours
    }
    
    error_rates = []
    
    for func in functions:
        function_name = func['name']
        function_summary = {
            'name': function_name,
            'runtime': func.get('runtime'),
            'memory_size': func.get('memory_size'),
            'timeout': func.get('timeout'),
            'code_size': func.get('code_size', 0),
            'invocations': 0,
            'errors': 0,
            'throttles': 0,
            'avg_duration_ms': 0,
            'error_rate': 0
        }
        
        summary['aggregated']['total_code_size_bytes'] += func.get('code_size', 0)
        
        try:
            # Get invocations
            result = cw_helper.get_metric_statistics(
                namespace='AWS/Lambda',
                metric_name='Invocations',
                dimensions=[{'Name': 'FunctionName', 'Value': function_name}],
                start_time=start_time,
                end_time=end_time,
                period=period,
                statistics=['Sum']
            )
            invocations = sum(dp.get('Sum', 0) for dp in result.get('datapoints', []))
            function_summary['invocations'] = invocations
            summary['aggregated']['total_invocations'] += invocations
            
            # Get errors
            result = cw_helper.get_metric_statistics(
                namespace='AWS/Lambda',
                metric_name='Errors',
                dimensions=[{'Name': 'FunctionName', 'Value': function_name}],
                start_time=start_time,
                end_time=end_time,
                period=period,
                statistics=['Sum']
            )
            errors = sum(dp.get('Sum', 0) for dp in result.get('datapoints', []))
            function_summary['errors'] = errors
            summary['aggregated']['total_errors'] += errors
            
            # Calculate error rate
            if invocations > 0:
                error_rate = (errors / invocations) * 100
                function_summary['error_rate'] = round(error_rate, 2)
                error_rates.append(error_rate)
            
            # Get throttles
            result = cw_helper.get_metric_statistics(
                namespace='AWS/Lambda',
                metric_name='Throttles',
                dimensions=[{'Name': 'FunctionName', 'Value': function_name}],
                start_time=start_time,
                end_time=end_time,
                period=period,
                statistics=['Sum']
            )
            throttles = sum(dp.get('Sum', 0) for dp in result.get('datapoints', []))
            function_summary['throttles'] = throttles
            summary['aggregated']['total_throttles'] += throttles
            
            # Get average duration
            result = cw_helper.get_metric_statistics(
                namespace='AWS/Lambda',
                metric_name='Duration',
                dimensions=[{'Name': 'FunctionName', 'Value': function_name}],
                start_time=start_time,
                end_time=end_time,
                period=period,
                statistics=['Average']
            )
            if result.get('datapoints'):
                avg_duration = sum(dp.get('Average', 0) for dp in result['datapoints']) / len(result['datapoints'])
                function_summary['avg_duration_ms'] = round(avg_duration, 2)
        
        except Exception as e:
            logger.warning(f"Could not get metrics for function {function_name}: {str(e)}")
            function_summary['metrics_error'] = str(e)
        
        summary['functions'].append(function_summary)
    
    # Calculate average error rate
    if error_rates:
        summary['aggregated']['avg_error_rate'] = round(sum(error_rates) / len(error_rates), 2)
    
    # Sort functions by invocations (most active first)
    summary['functions'] = sorted(
        summary['functions'],
        key=lambda x: x.get('invocations', 0),
        reverse=True
    )
    
    return summary


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for Lambda function metrics
    
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
        function_name = path_params.get('function_name')
        
        # Parse query parameters
        query_params = event.get('queryStringParameters') or {}
        hours = int(query_params.get('hours', 24))
        
        # Validate hours parameter
        if hours < 1 or hours > 720:  # Max 30 days
            return error_response(
                message="Hours parameter must be between 1 and 720",
                status_code=400
            )
        
        if function_name:
            # Get metrics for specific function
            logger.info(f"Getting metrics for function: {function_name}")
            
            # Verify function exists
            functions = get_all_functions()
            function_names = [f['name'] for f in functions]
            if function_name not in function_names:
                return not_found_response(f"Function '{function_name}'")
            
            metrics = get_function_metrics(
                function_name=function_name,
                hours=hours
            )
            
            return success_response(
                data=metrics,
                message=f"Lambda metrics for function '{function_name}'"
            )
        else:
            # Get summary for all functions
            logger.info("Getting metrics summary for all Lambda functions")
            summary = get_all_functions_summary(hours)
            
            return success_response(
                data=summary,
                message="Lambda metrics summary for all functions"
            )
    
    except Exception as e:
        logger.error(f"Error processing request: {str(e)}", exc_info=True)
        return error_response(
            message=f"Error retrieving Lambda metrics: {str(e)}",
            status_code=500
        )
