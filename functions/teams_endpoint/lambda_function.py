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

class League(str, Enum):
    North = "north"
    South = "south"

# Define the request model
class TeamQueryParams(BaseModel):
    home_ballpark_name: Optional[str] = Field(None, max_length=100)
    league: Optional[League] = None
    team_name: Optional[str] = Field(None, max_length=100)
    limit: Optional[int] = Field(None, ge=1, le=1000)
    page: Optional[int] = Field(None, ge=1)
    order: OrderDirection = OrderDirection.DESC

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

def lambda_handler(event, context):
    parameters = event.get('queryStringParameters', {})
    if parameters is None:
        parameters = {}

    # Validate parameters using Pydantic
    try:
        params = TeamQueryParams(**parameters)
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
    query = """
    SELECT 
        team.team_id,
        team.team_name,
        team.league,
        ballpark.ballpark_id,
        ballpark.ballpark_name,
        ballpark.city,
        ballpark.state,
        ballpark.dimensions
    FROM team
    JOIN ballpark ON team.home_ballpark_id = ballpark.ballpark_id
    """
    
    # Dynamically add filters based on parameters
    filters = []
    args = []
    
    if 'home_ballpark_name' in parameters:
        filters.append("ballpark.ballpark_name LIKE %s")
        args.append(parameters['home_ballpark_name'])
    
    if 'team_name' in parameters:
        filters.append("team_name LIKE %s")
        args.append(f"%{parameters['team_name']}%")
        
    if 'league' in parameters:
        filters.append("league LIKE %s")
        args.append(f"%{parameters['league'].lower()}%")
        
    # Append filters to the SQL query if any
    if filters:
        query += " WHERE " + " AND ".join(filters)

    query += f" ORDER BY team_name {params.order.value}"

    if params.limit is not None:
        query += " LIMIT %s"
        args.append(params.limit)
    
    try:
        with conn.cursor() as cur:
            cur.execute(query, args)
            rows = cur.fetchall()

            # Prepare the result
            result = [{
                'team_id': str(row[0]),
                'team_name': row[1],
                'league': row[2],
                'ballpark': {
                    'ballpark_id': str(row[3]),
                    'ballpark_name': row[4],
                    'city': row[5],
                    'state': row[6],
                    'dimensions': row[7]
                }
            } for row in rows]
    
        return {
            'statusCode': 200,
            'body': json.dumps({
                'success': True,
                'message': 'Teams retrieved successfully',
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
                'message': 'Error fetching teams',
            }),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': "*",
            }
        }
        
    finally:
        conn.close()
