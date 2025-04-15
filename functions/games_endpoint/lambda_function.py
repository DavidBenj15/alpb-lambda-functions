import json
import psycopg2
import os
from typing import Optional, List
from enum import Enum
from pydantic import BaseModel, Field, validator, ValidationError

# Database settings
rds_host = os.environ['DB_HOST']
name = os.environ['DB_USERNAME']
password = os.environ['DB_PASSWORD']
db_name = os.environ['DB_NAME']
db_port = os.environ.get('DB_PORT', '5432')  # Default PostgreSQL port is 5432

# Dynamically create enum for team names
class TeamNameEnum(str, Enum):
    YorkRevolution = "York Revolution"
    StatenIslandFerryHawks = "Staten Island FerryHawks"
    SouthernMarylandBlueCrabs = "Southern Maryland Blue Crabs"
    LongIslandDucks = "Long Island Ducks"
    LexingtonLegends = "Lexington Legends"
    LancasterStormers = "Lancaster Stormers"
    HighPointRockers = "High Point Rockers"
    HagerstownFlyingBoxcars = "Hagerstown Flying Boxcars"
    GastoniaBaesballClub = "Gastonia Baseball Club"
    CharlestonDirtyBirds = "Charleston Dirty Birds"

class OrderDirection(str, Enum):
    ASC = "ASC"
    DESC = "DESC"

# Define the request model
class GameQueryParams(BaseModel):
    home_team_name: Optional[TeamNameEnum] = Field(None)
    visiting_team_name: Optional[TeamNameEnum] = Field(None)
    ballpark_name: Optional[str] = Field(None, max_length=100)
    date: Optional[str] = Field(None, max_length=50)  # Adjust max length as needed
    page: Optional[int] = Field(1, ge=1)  # Default to 1, minimum 1
    limit: Optional[int] = Field(20, ge=1, le=1000)  # Default to 20, between 1 and 1000
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
    # Get query parameters
    parameters = event.get('queryStringParameters', {})
    if parameters is None:
        parameters = {}

    # Validate parameters using Pydantic
    try:
        params = GameQueryParams(**parameters)
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
    SELECT game.game_id, home_team.team_name AS home_team_name, visiting_team.team_name AS visiting_team_name, ballpark.ballpark_name, game.date
    FROM game
    JOIN team AS home_team ON game.home_team_id = home_team.team_id
    JOIN team AS visiting_team ON game.visiting_team_id = visiting_team.team_id
    JOIN ballpark ON game.ballpark_id = ballpark.ballpark_id
    """
    
    # Dynamically add filters based on parameters
    filters = []
    args = []
    
    if params.home_team_name is not None:
        filters.append("home_team.team_name = %s")
        args.append(params.home_team_name.value)
        
    if params.visiting_team_name is not None:
        filters.append("visiting_team.team_name = %s")
        args.append(params.visiting_team_name.value)
        
    if params.ballpark_name is not None:
        filters.append("ballpark.ballpark_name LIKE %s")
        args.append(f"%{params.ballpark_name}%")
        
    if params.date is not None:
        filters.append("game.date LIKE %s")
        args.append(f"%{params.date}%")
        
    # Append filters to the SQL query if any
    if filters:
        query += " WHERE " + " AND ".join(filters)

    # Add ordering, limit, and offset
    query += f" ORDER BY date {params.order.value}"
    query += f" LIMIT %s OFFSET %s"
    args.extend([params.limit, offset])
    
    try:
        with conn.cursor() as cur:
            cur.execute(query, args)
            rows = cur.fetchall()

            # Prepare the result
            result = [{
                'game_id': row[0],
                'home_team_name': row[1],
                'visiting_team_name': row[2],
                'ballpark_name': row[3],
                'date': row[4]
            } for row in rows]

        return {
            'statusCode': 200,
            'body': json.dumps({
                'success': True,
                'message': 'Games retrieved successfully',
                'data': result,
                'meta': {
                    'page': params.page,
                    'limit': params.limit,
                    'total': len(result)
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
                'message': "Error fetching games",
            }),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': "*",
            }
        }

    finally:
        conn.close()