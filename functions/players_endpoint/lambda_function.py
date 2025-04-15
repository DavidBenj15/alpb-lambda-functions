import json
import psycopg2
import os
import math
from typing import Optional, List
from enum import Enum
from pydantic import BaseModel, Field, validator, ValidationError, UUID4

# Database settings
rds_host = os.environ['DB_HOST']
name = os.environ['DB_USERNAME']
password = os.environ['DB_PASSWORD']
db_name = os.environ['DB_NAME']
db_port = os.environ.get('DB_PORT', '5432')  # Default PostgreSQL port is 5432

# Enums for specific fields
class OrderDirection(str, Enum):
    ASC = "ASC"
    DESC = "DESC"

class PitchingHandedness(str, Enum):
    LEFT = "Left"
    RIGHT = "Right"
    SWITCH = "Switch"
    UNKNOWN = "Unknown"
    NONE = None

class BattingHandedness(str, Enum):
    LEFT = "Left"
    RIGHT = "Right"
    SWITCH = "Switch"
    UNKNOWN = "Unknown"
    NONE = None

# Player Query Parameters Model
class PlayerQueryParams(BaseModel):
    team_id: Optional[UUID4] = None
    team_name: Optional[str] = None
    player_name: Optional[str] = None
    player_id: Optional[UUID4] = None
    player_pitching_handedness: Optional[PitchingHandedness] = None
    player_batting_handedness: Optional[BattingHandedness] = None
    page: int = Field(1, ge=1)
    limit: int = Field(20, ge=1, le=1000)
    order: OrderDirection = OrderDirection.ASC

def replace_nan_with_none(data):
    if isinstance(data, dict):
        return {k: replace_nan_with_none(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [replace_nan_with_none(v) for v in data]
    elif isinstance(data, float) and math.isnan(data):
        return None
    else:
        return data

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
    # Get query parameters
    parameters = event.get('queryStringParameters', {})
    if parameters is None:
        parameters = {}

    # Validate parameters using Pydantic
    try:
        params = PlayerQueryParams(**parameters)
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

    # Calculate offset for pagination
    offset = (params.page - 1) * params.limit
    
    # Base SQL query
    query = """
        SELECT player.player_id, player.player_name,
            player.player_pitching_handedness, player.player_batting_handedness, 
            team.team_name
        FROM player
        LEFT JOIN team ON player.team_id = team.team_id
    """

    # Dynamically add filters based on parameters
    filters = []
    args = []

    if params.player_id is not None:
        filters.append("player.player_id = %s")
        args.append(str(params.player_id))
    
    if params.team_id is not None:
        filters.append("player.team_id = %s")
        args.append(params.team_id)
    
    if params.team_name is not None:
        filters.append("team.team_name LIKE %s")
        args.append(f"%{params.team_name}%")
    
    if params.player_name is not None:
        filters.append("player.player_name LIKE %s")
        args.append(f"%{params.player_name}%")
    
    if params.player_pitching_handedness is not None:
        filters.append("player.player_pitching_handedness = %s")
        args.append(params.player_pitching_handedness.value)
    
    if params.player_batting_handedness is not None:
        filters.append("player.player_batting_handedness = %s")
        args.append(params.player_batting_handedness.value)
    
    # Append filters to the SQL query if any
    if filters:
        query += " WHERE " + " AND ".join(filters)

    # Add order by, limit and offset
    query += f" ORDER BY player_name {params.order.value}"
    query += f" LIMIT %s OFFSET %s"
    args.extend([params.limit, offset])

    try:
        with conn.cursor() as cur:
            # Execute the main query
            cur.execute(query, args)
            rows = cur.fetchall()
            
            # Get the column names from the cursor
            column_names = ['player_id', 'player_name', 'player_pitching_handedness', 
                            'player_batting_handedness', 'team_name']

            # Prepare the result as a list of dictionaries
            result = [dict(zip(column_names, row)) for row in rows]
            cleaned_result = replace_nan_with_none(result)
            
            # Count total records (without pagination)
            count_query = """
                SELECT COUNT(*)
                FROM player
                LEFT JOIN team ON player.team_id = team.team_id
            """
            if filters:
                count_query += " WHERE " + " AND ".join(filters)
            
            cur.execute(count_query, args[:-2] if args else [])
            total_count = cur.fetchone()[0]

            return {
                'statusCode': 200,
                'body': json.dumps({
                    'success': True,
                    'message': 'Players retrieved successfully',
                    'data': cleaned_result,
                    'meta': {
                        'page': params.page,
                        'limit': params.limit,
                        'total': total_count,
                        'pages': (total_count + params.limit - 1) // params.limit
                    }
                }, default=str),
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
                'message': 'Error fetching players',
            }),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': "*",
            }
        }

    finally:
        conn.close()