"""
Response Helper Module
Utilities for formatting Lambda API Gateway responses
"""

import json
from typing import Any, Dict, Optional
from datetime import datetime


def create_response(
    status_code: int,
    body: Any,
    headers: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    """
    Create a properly formatted API Gateway response
    
    Args:
        status_code: HTTP status code
        body: Response body (will be JSON serialized)
        headers: Optional additional headers
    
    Returns:
        API Gateway response dictionary
    """
    default_headers = {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key',
        'Access-Control-Allow-Methods': 'GET,POST,OPTIONS'
    }
    
    if headers:
        default_headers.update(headers)
    
    return {
        'statusCode': status_code,
        'headers': default_headers,
        'body': json.dumps(body, default=json_serializer)
    }


def success_response(data: Any, message: str = "Success") -> Dict[str, Any]:
    """
    Create a success response
    
    Args:
        data: Response data
        message: Success message
    
    Returns:
        API Gateway response dictionary
    """
    return create_response(
        status_code=200,
        body={
            'status': 'success',
            'message': message,
            'data': data,
            'timestamp': datetime.utcnow().isoformat()
        }
    )


def error_response(
    message: str,
    status_code: int = 500,
    error_code: Optional[str] = None
) -> Dict[str, Any]:
    """
    Create an error response
    
    Args:
        message: Error message
        status_code: HTTP status code
        error_code: Optional error code for programmatic handling
    
    Returns:
        API Gateway response dictionary
    """
    body = {
        'status': 'error',
        'message': message,
        'timestamp': datetime.utcnow().isoformat()
    }
    
    if error_code:
        body['error_code'] = error_code
    
    return create_response(status_code=status_code, body=body)


def validation_error(message: str, field: Optional[str] = None) -> Dict[str, Any]:
    """
    Create a validation error response
    
    Args:
        message: Validation error message
        field: Optional field name that failed validation
    
    Returns:
        API Gateway response dictionary
    """
    body = {
        'status': 'error',
        'message': message,
        'error_code': 'VALIDATION_ERROR',
        'timestamp': datetime.utcnow().isoformat()
    }
    
    if field:
        body['field'] = field
    
    return create_response(status_code=400, body=body)


def not_found_response(resource: str) -> Dict[str, Any]:
    """
    Create a not found response
    
    Args:
        resource: Resource that was not found
    
    Returns:
        API Gateway response dictionary
    """
    return create_response(
        status_code=404,
        body={
            'status': 'error',
            'message': f'{resource} not found',
            'error_code': 'NOT_FOUND',
            'timestamp': datetime.utcnow().isoformat()
        }
    )


def json_serializer(obj: Any) -> str:
    """
    Custom JSON serializer for objects not serializable by default json code
    
    Args:
        obj: Object to serialize
    
    Returns:
        String representation
    """
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
