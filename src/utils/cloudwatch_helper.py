"""
CloudWatch Helper Module
Common utilities for fetching CloudWatch metrics
"""

import boto3
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger(__name__)


class CloudWatchHelper:
    """Helper class for CloudWatch metric operations"""
    
    def __init__(self, region: Optional[str] = None):
        self.cloudwatch = boto3.client('cloudwatch', region_name=region)
    
    def get_metric_statistics(
        self,
        namespace: str,
        metric_name: str,
        dimensions: List[Dict[str, str]],
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        period: int = 3600,
        statistics: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Get metric statistics from CloudWatch
        
        Args:
            namespace: CloudWatch namespace (e.g., 'AWS/S3', 'AWS/DynamoDB', 'AWS/Lambda')
            metric_name: Name of the metric
            dimensions: List of dimension dictionaries with 'Name' and 'Value'
            start_time: Start time for metrics (default: 24 hours ago)
            end_time: End time for metrics (default: now)
            period: Period in seconds (default: 3600 = 1 hour)
            statistics: List of statistics to retrieve (default: ['Average', 'Sum', 'Maximum', 'Minimum'])
        
        Returns:
            Dictionary containing metric data points
        """
        if end_time is None:
            end_time = datetime.utcnow()
        if start_time is None:
            start_time = end_time - timedelta(hours=24)
        if statistics is None:
            statistics = ['Average', 'Sum', 'Maximum', 'Minimum']
        
        try:
            response = self.cloudwatch.get_metric_statistics(
                Namespace=namespace,
                MetricName=metric_name,
                Dimensions=dimensions,
                StartTime=start_time,
                EndTime=end_time,
                Period=period,
                Statistics=statistics
            )
            
            # Sort datapoints by timestamp
            datapoints = sorted(
                response.get('Datapoints', []),
                key=lambda x: x['Timestamp']
            )
            
            # Convert timestamps to ISO format strings
            for dp in datapoints:
                dp['Timestamp'] = dp['Timestamp'].isoformat()
            
            return {
                'metric_name': metric_name,
                'namespace': namespace,
                'dimensions': dimensions,
                'datapoints': datapoints,
                'period': period,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat()
            }
        except Exception as e:
            logger.error(f"Error fetching metric {metric_name}: {str(e)}")
            raise
    
    def get_metric_data(
        self,
        metric_queries: List[Dict[str, Any]],
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Get metric data using GetMetricData API for multiple metrics
        
        Args:
            metric_queries: List of metric data queries
            start_time: Start time for metrics
            end_time: End time for metrics
        
        Returns:
            Dictionary containing metric results
        """
        if end_time is None:
            end_time = datetime.utcnow()
        if start_time is None:
            start_time = end_time - timedelta(hours=24)
        
        try:
            response = self.cloudwatch.get_metric_data(
                MetricDataQueries=metric_queries,
                StartTime=start_time,
                EndTime=end_time
            )
            
            results = []
            for result in response.get('MetricDataResults', []):
                # Convert timestamps to ISO format
                timestamps = [ts.isoformat() for ts in result.get('Timestamps', [])]
                results.append({
                    'id': result['Id'],
                    'label': result.get('Label', ''),
                    'timestamps': timestamps,
                    'values': result.get('Values', []),
                    'status_code': result.get('StatusCode', '')
                })
            
            return {
                'results': results,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat()
            }
        except Exception as e:
            logger.error(f"Error fetching metric data: {str(e)}")
            raise
    
    def list_metrics(
        self,
        namespace: str,
        metric_name: Optional[str] = None,
        dimensions: Optional[List[Dict[str, str]]] = None
    ) -> List[Dict[str, Any]]:
        """
        List available metrics in CloudWatch
        
        Args:
            namespace: CloudWatch namespace
            metric_name: Optional metric name filter
            dimensions: Optional dimension filters
        
        Returns:
            List of available metrics
        """
        params = {'Namespace': namespace}
        
        if metric_name:
            params['MetricName'] = metric_name
        if dimensions:
            params['Dimensions'] = dimensions
        
        try:
            paginator = self.cloudwatch.get_paginator('list_metrics')
            metrics = []
            
            for page in paginator.paginate(**params):
                for metric in page.get('Metrics', []):
                    metrics.append({
                        'namespace': metric['Namespace'],
                        'metric_name': metric['MetricName'],
                        'dimensions': metric.get('Dimensions', [])
                    })
            
            return metrics
        except Exception as e:
            logger.error(f"Error listing metrics: {str(e)}")
            raise


def parse_time_range(hours: int = 24) -> tuple:
    """
    Parse time range for metrics query
    
    Args:
        hours: Number of hours to look back
    
    Returns:
        Tuple of (start_time, end_time)
    """
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=hours)
    return start_time, end_time


def calculate_period(hours: int) -> int:
    """
    Calculate appropriate period based on time range
    
    Args:
        hours: Number of hours in the time range
    
    Returns:
        Period in seconds
    """
    if hours <= 3:
        return 60  # 1 minute
    elif hours <= 24:
        return 300  # 5 minutes
    elif hours <= 168:  # 7 days
        return 3600  # 1 hour
    else:
        return 86400  # 1 day
