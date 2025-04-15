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
class TopOrBottom(str, Enum):
    TOP = "Top"
    BOTTOM = "Bottom"

class AutoPitchType(str, Enum):
    CUTTER = "Cutter"
    NULL = None
    SPLITTER = "Splitter"
    CHANGEUP = "Changeup"
    SLIDER = "Slider"
    CURVEBALL = "Curveball"
    FOUR_SEAM = "Four-Seam"
    SINKER = "Sinker"
    # Add other pitch types as needed

class PitchCall(str, Enum):
    BALL_INTENTIONAL = "BallIntentional"
    IN_PLAY = "InPlay"
    BALL_IN_DIRT = "BallInDirt"
    HIT_BY_PITCH = "HitByPitch"
    FOUL_BALL = "FoulBall"
    STRIKE_SWINGING = "StrikeSwinging"
    FOUL_BALL_NOT_FIELDABLE = "FoulBallNotFieldable"
    UNDEFINED = "Undefined"
    STRIKE_CALLED = "StrikeCalled"
    BALL_CALLED = "BallCalled"
    FOUL_BALL_FIELDABLE = "FoulBallFieldable"
    # Add other pitch calls as needed

class PlayResult(str, Enum):
    SINGLE = "Single"
    DOUBLE = "Double"
    TRIPLE = "Triple"
    ERROR = "Error"
    FIELDERS_CHOICE = "FieldersChoice"
    HOME_RUN = "HomeRun"
    OUT = "Out"
    SACRIFICE = "Sacrifice"
    UNDEFINED = "Undefined"
    NONE = None
    # Add other play results as needed

class OrderDirection(str, Enum):
    ASC = "ASC"
    DESC = "DESC"

# Pitch Query Parameters Model
class PitchQueryParams(BaseModel):
    game_id: Optional[UUID4] = None
    pitcher_id: Optional[UUID4] = None
    batter_id: Optional[UUID4] = None
    catcher_id: Optional[UUID4] = None
    inning: Optional[int] = Field(None, ge=1)
    top_or_bottom: Optional[TopOrBottom] = None
    outs: Optional[int] = Field(None, ge=0, le=3)
    strikes: Optional[int] = Field(None, ge=0, le=3)
    balls: Optional[int] = Field(None, ge=0, le=4)
    auto_pitch_type: Optional[AutoPitchType] = None
    play_result: Optional[PlayResult] = None
    date: Optional[str] = None
    date_range_start: Optional[str] = None
    date_range_end: Optional[str] = None
    pitch_call: Optional[PitchCall] = None
    page: Optional[int] = Field(1, ge=1)
    limit: Optional[int] = Field(20, ge=1, le=1000)
    order: OrderDirection = OrderDirection.DESC

    @validator('date_range_start', 'date_range_end', 'date', pre=True, always=False)
    def validate_date(cls, v):
        # Add basic date format validation if needed
        # This is a simple check, you might want to use more robust date validation
        if v and not (isinstance(v, str) and len(v) > 0):
            raise ValueError('Invalid date format')
        return v

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
        params = PitchQueryParams(**parameters)
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
    SELECT * 
    FROM pitch
    """

    # Dynamically add filters based on parameters
    filters = []
    args = []
    
    if params.game_id is not None:
        filters.append("game_id = %s")
        args.append(str(params.game_id))
    
    if params.pitcher_id is not None:
        filters.append("pitcher_id = %s")
        args.append(str(params.pitcher_id))
    
    if params.batter_id is not None:
        filters.append("batter_id = %s")
        args.append(str(params.batter_id))
    
    if params.catcher_id is not None:
        filters.append("catcher_id = %s")
        args.append(str(params.catcher_id))
    
    if params.inning is not None:
        filters.append("inning = %s")
        args.append(params.inning)
    
    if params.top_or_bottom is not None:
        filters.append("top_or_bottom = %s")
        args.append(params.top_or_bottom.value)
    
    if params.outs is not None:
        filters.append("outs = %s")
        args.append(params.outs)
    
    if params.strikes is not None:
        filters.append("strikes = %s")
        args.append(params.strikes)
    
    if params.balls is not None:
        filters.append("balls = %s")
        args.append(params.balls)
    
    if params.auto_pitch_type is not None:
        filters.append("auto_pitch_type = %s")
        args.append(params.auto_pitch_type.value)
    
    if params.play_result is not None:
        filters.append("play_result = %s")
        args.append(params.play_result.value)
    
    if params.date is not None:
        filters.append("date = %s")
        args.append(params.date)
    
    if params.date_range_start is not None and params.date_range_end is not None:
        filters.append("date BETWEEN %s AND %s")
        args.append(params.date_range_start)
        args.append(params.date_range_end)
    
    if params.pitch_call is not None:
        filters.append("pitch_call = %s")
        args.append(params.pitch_call.value)

    # Append filters to the SQL query if any
    if filters:
        query += " WHERE " + " AND ".join(filters)

    args.extend([params.limit, offset])

    query += f" ORDER BY date, time {params.order.value}"
    query += f" LIMIT %s OFFSET %s"

    try:
        with conn.cursor() as cur:
            cur.execute(query, args)
            rows = cur.fetchall()
            
            # Get the column names from the cursor
            column_names = [desc[0] for desc in cur.description]

            # Prepare the result as a list of dictionaries
            result = [dict(zip(column_names, row)) for row in rows]

            cleaned_result = replace_nan_with_none(result)
        return {
            'statusCode': 200,
            'body': json.dumps({
                'success': True,
                'message': 'Pitches retrieved successfully',
                'data': cleaned_result,
                'meta': {
                    'page': params.page,
                    'limit': params.limit,
                    'total': len(cleaned_result)
                }
            }, default=str),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': "*"
            }
        }

    except psycopg2.DatabaseError as e:
        print(f"ERROR: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'success': False,
                'message': 'Error fetching pitches',
            }),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': "*",
            }
        }

    finally:
        conn.close()