"""
Part 3: Plotting Lambda function
Plots bucket size change over the last 10 seconds and maximum size ever reached.
Exposes a REST API endpoint.
"""

import boto3
import json
import os
import time
from datetime import datetime, timedelta

# Initialize AWS clients
dynamodb = boto3.client('dynamodb')
s3 = boto3.client('s3')

TABLE_NAME = os.environ["TABLE_NAME"]
BUCKET_NAME = os.environ["BUCKET_NAME"]
PLOT_KEY = os.environ.get("PLOT_KEY", "plot")
PLOT_SECONDS = int(os.environ.get("PLOT_SECONDS", "300"))
GLOBAL_MAX_BUCKET = '__GLOBAL_MAX__'

def query_bucket_size_history(bucket_name, start_timestamp):
    """
    Query DynamoDB for bucket size history from start_timestamp onwards.
    Returns list of (timestamp, total_size) tuples.
    """
    try:
        # Query items for the specific bucket with timestamp >= start_timestamp
        response = dynamodb.query(
            TableName=TABLE_NAME,
            KeyConditionExpression='bucket_name = :bucket AND #ts >= :start_time',
            ExpressionAttributeNames={
                '#ts': 'timestamp'
            },
            ExpressionAttributeValues={
                ':bucket': {'S': bucket_name},
                ':start_time': {'N': str(start_timestamp)}
            },
            ScanIndexForward=True  # Sort by timestamp ascending
        )
        
        history = []
        for item in response.get('Items', []):
            timestamp = int(item['timestamp']['N'])
            total_size = int(item['total_size']['N'])
            history.append((timestamp, total_size))
        
        return history
    except Exception as e:
        print(f"Error querying DynamoDB: {e}")
        raise

def get_max_size_ever():
    """
    Query DynamoDB to find the maximum size any bucket has ever reached.
    Uses query-only approach with a synthetic partition key.
    """
    try:
        response = dynamodb.query(
            TableName=TABLE_NAME,
            KeyConditionExpression='bucket_name = :bucket',
            ExpressionAttributeValues={
                ':bucket': {'S': GLOBAL_MAX_BUCKET}
            },
            ScanIndexForward=False,  # latest timestamp first
            Limit=1
        )

        items = response.get('Items', [])
        if not items:
            return 0
        return int(items[0].get('total_size', {}).get('N', '0'))
    except Exception as e:
        print(f"Error querying global max size: {e}")
        return 0

def _escape_xml(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


def create_plot_svg(bucket_name, history_data, max_size_ever) -> str | None:
    """
    Create a lightweight SVG plot (no third-party deps).
    Returns: SVG string, or None if no data.
    """
    if not history_data:
        print("No data to plot")
        return None

    width, height = 900, 450
    pad_left, pad_right, pad_top, pad_bottom = 70, 20, 40, 60
    plot_w = width - pad_left - pad_right
    plot_h = height - pad_top - pad_bottom

    timestamps = [t for (t, _) in history_data]
    sizes = [s for (_, s) in history_data]
    min_ts, max_ts = min(timestamps), max(timestamps)
    min_sz = 0
    max_sz = max(max(sizes), int(max_size_ever or 0), 1)

    def x_for(ts: int) -> float:
        if max_ts == min_ts:
            return pad_left + plot_w / 2
        return pad_left + (ts - min_ts) * plot_w / (max_ts - min_ts)

    def y_for(sz: int) -> float:
        # SVG y increases downward
        return pad_top + plot_h - (sz - min_sz) * plot_h / (max_sz - min_sz)

    # Polyline for history
    points = " ".join(f"{x_for(ts):.2f},{y_for(sz):.2f}" for ts, sz in history_data)

    # Axis ticks (simple: start/end time, min/max size)
    start_label = datetime.fromtimestamp(min_ts).strftime("%H:%M:%S")
    end_label = datetime.fromtimestamp(max_ts).strftime("%H:%M:%S")

    # Max line
    max_line = ""
    if max_size_ever and int(max_size_ever) > 0:
        y_max = y_for(int(max_size_ever))
        max_line = (
            f'<line x1="{pad_left}" y1="{y_max:.2f}" x2="{pad_left + plot_w}" y2="{y_max:.2f}" '
            f'stroke="#d33" stroke-width="2" stroke-dasharray="6,4" />'
            f'<text x="{pad_left + 6}" y="{y_max - 6:.2f}" font-size="12" fill="#d33">'
            f'Max ever: {int(max_size_ever)} bytes</text>'
        )

    title = f"S3 Bucket Size Change - {bucket_name}"

    svg = f"""<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">
  <rect x="0" y="0" width="{width}" height="{height}" fill="#ffffff"/>
  <text x="{width/2:.0f}" y="24" font-size="18" text-anchor="middle" fill="#111">{_escape_xml(title)}</text>

  <!-- Axes -->
  <line x1="{pad_left}" y1="{pad_top}" x2="{pad_left}" y2="{pad_top + plot_h}" stroke="#333" stroke-width="2" />
  <line x1="{pad_left}" y1="{pad_top + plot_h}" x2="{pad_left + plot_w}" y2="{pad_top + plot_h}" stroke="#333" stroke-width="2" />

  <!-- Y labels -->
  <text x="{pad_left - 10}" y="{pad_top + 4}" font-size="12" text-anchor="end" fill="#333">{max_sz}B</text>
  <text x="{pad_left - 10}" y="{pad_top + plot_h}" font-size="12" text-anchor="end" fill="#333">0B</text>

  <!-- X labels -->
  <text x="{pad_left}" y="{pad_top + plot_h + 26}" font-size="12" text-anchor="start" fill="#333">{_escape_xml(start_label)}</text>
  <text x="{pad_left + plot_w}" y="{pad_top + plot_h + 26}" font-size="12" text-anchor="end" fill="#333">{_escape_xml(end_label)}</text>

  <!-- Grid (light) -->
  <line x1="{pad_left}" y1="{pad_top + plot_h/2:.2f}" x2="{pad_left + plot_w}" y2="{pad_top + plot_h/2:.2f}" stroke="#ddd" stroke-width="1" />

  {max_line}

  <!-- Data -->
  <polyline points="{points}" fill="none" stroke="#1f77b4" stroke-width="3" />
  {"".join(f'<circle cx="{x_for(ts):.2f}" cy="{y_for(sz):.2f}" r="4" fill="#1f77b4" />' for ts, sz in history_data)}

  <!-- Legend -->
  <rect x="{pad_left + 10}" y="{pad_top + 10}" width="12" height="12" fill="#1f77b4"/>
  <text x="{pad_left + 28}" y="{pad_top + 20}" font-size="12" fill="#333">{_escape_xml(bucket_name)} size</text>
</svg>
"""
    return svg

def upload_plot_to_s3(bucket_name, plot_svg: str, key: str):
    """
    Upload the plot (SVG) to S3.
    """
    try:
        s3.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=plot_svg.encode("utf-8"),
            ContentType='image/svg+xml'
        )
        print(f"✓ Plot uploaded to s3://{bucket_name}/{key}")
    except Exception as e:
        print(f"Error uploading plot to S3: {e}")
        raise

def lambda_handler(event, context):
    """
    Lambda handler function that can be called via REST API (API Gateway).
    Handles both direct Lambda invocation and API Gateway events.
    """
    print(f"Received event: {json.dumps(event)}")
    
    try:
        # Calculate timestamp window for plot
        current_timestamp = int(time.time())
        start_timestamp = current_timestamp - PLOT_SECONDS
        
        # Query bucket size history for the configured window
        history_data = query_bucket_size_history(BUCKET_NAME, start_timestamp)
        print(f"Found {len(history_data)} data points in the last {PLOT_SECONDS} seconds")
        
        # Get maximum size ever reached by any bucket
        max_size_ever = get_max_size_ever()
        print(f"Maximum size ever reached: {max_size_ever} bytes")
        
        # Create the plot (SVG)
        plot_svg = create_plot_svg(BUCKET_NAME, history_data, max_size_ever)

        if plot_svg:
            # Upload plot to S3
            upload_plot_to_s3(BUCKET_NAME, plot_svg, PLOT_KEY)
            
            response_body = {
                'message': 'Plot created successfully',
                'bucket': BUCKET_NAME,
                'plot_key': PLOT_KEY,
                'data_points': len(history_data),
                'max_size_ever': max_size_ever
            }
            
            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'  # For CORS if needed
                },
                'body': json.dumps(response_body)
            }
        else:
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'error': 'No data available to plot'
                })
            }
            
    except Exception as e:
        print(f"Error in plotting lambda: {e}")
        import traceback
        traceback.print_exc()
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'error': str(e)
            })
        }
