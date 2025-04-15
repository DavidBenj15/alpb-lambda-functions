import json
import psycopg2
import os
from typing import Optional
from enum import Enum
from pydantic import BaseModel, Field, validator, ValidationError

# Database settings
rds_host = os.environ['DB_HOST']
name = os.environ['DB_USERNAME']
password = os.environ['DB_PASSWORD']
db_name = os.environ['DB_NAME']
db_port = os.environ.get('DB_PORT', '5432')  # Default PostgreSQL port is 5432

class OrderDirection(str, Enum):
    ASC = "ASC"
    DESC = "DESC"

# Define the request model
class BallparkQueryParams(BaseModel):
    ballpark_name: Optional[str] = Field(None, max_length=100)
    city: Optional[str] = Field(None, max_length=100)
    state: Optional[str] = Field(None, max_length=2)  # Assuming state is a 2-letter code
    limit: Optional[int] = Field(None, ge=1, le=1000)
    page: Optional[int] = Field(None, ge=1)
    order: OrderDirection = OrderDirection.ASC

    class Config:
        # Allow extra fields (API Gateway might include other params)
        extra = "ignore"

# Establish a connection to the RDS PostgreSQL database
def connect_to_rds():
    try:
        conn = psycopg2.connect(
            host=rds_host,
            database=db_name,
            user=name,
            password=password,
            port=db_port
        )
        return conn
    except psycopg2.DatabaseError as e:
        print(f"ERROR: Could not connect to PostgreSQL instance. {e}")
        raise e

# Lambda handler
def lambda_handler(event, context):
    parameters = event.get('queryStringParameters', {})
    if parameters is None:
        parameters = {}

    # Validate parameters using Pydantic
    try:
        params = BallparkQueryParams(**parameters)
    except ValidationError as e:
        # Return validation errors
        return {
            'statusCode': 400,
            'body': json.dumps({
                'success': False,
                'message': 'Invalid request parameters',
                'errors': str(e)
            }),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': "*",
            }
        }
    
    # Connect to the database
    try:
        conn = connect_to_rds()
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'success': False,
                'message': 'Error connecting to database',
            }),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': "*",
            }
        }
    
    # Base SQL query
    sql = """
    SELECT ballpark_id, ballpark_name, dimensions, city, state
    FROM ballpark
    """
    
    # Dynamically add filters based on parameters
    filters = []
    args = []
    
    if params.ballpark_name is not None:
        filters.append("ballpark_name LIKE %s")
        args.append(f"%{params.ballpark_name}%")
    
    if params.city is not None:
        filters.append("city LIKE %s")
        args.append(f"%{params.city}%")
    
    if params.state is not None:
        filters.append("state LIKE %s")
        args.append(f"%{params.state}%")
    
    # Append filters to the SQL query if any
    if filters:
        sql += " WHERE " + " AND ".join(filters)

    sql += f" ORDER BY ballpark_name {params.order.value}"

    if params.limit is not None:
        sql += " LIMIT %s"
        args.append(params.limit)
    
    try:
        with conn.cursor() as cur:
            cur.execute(sql, args)
            rows = cur.fetchall()
            
            # Prepare the result
            result = [{
                'ballpark_id': str(row[0]),
                'ballpark_name': row[1],
                'dimensions': row[2],
                'city': row[3],
                'state': row[4]
            } for row in rows]
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'success': True,
                'message': 'Ballparks retrieved successfully',
                'data': result,
            }),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': "*",
            }
        }
        
    except psycopg2.DatabaseError as e:
        print(f"ERROR: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'success': False,
                'message': 'Error fetching ballparks',
            }),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': "*",
            }
        }
        
    finally:
        conn.close()