import json
import psycopg2
import os

# Database settings
rds_host = os.environ['DB_HOST']
name = os.environ['DB_USERNAME']
password = os.environ['DB_PASSWORD']
db_name = os.environ['DB_NAME']
db_port = os.environ.get('DB_PORT', '5432')  # Default PostgreSQL port is 5432

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
    try:
        conn = connect_to_rds()
        cursor = conn.cursor()
        cursor.execute("""
            WITH weekly_metrics AS (
                SELECT widget_id,
                    COUNT(*) AS total_launches,
                    COUNT(DISTINCT user_id) AS unique_views
                FROM widget_interactions
                WHERE timestamp >= NOW() - INTERVAL '7 days'
                GROUP BY widget_id
            )
            INSERT INTO widget_metrics (widget_id, timeframe_type, timeframe_start, total_launches, unique_views)
            SELECT widget_id, 
                'weekly', 
                NOW() - INTERVAL '7 days', 
                total_launches, 
                unique_views
            FROM weekly_metrics
            ON CONFLICT (widget_id, timeframe_type)
            DO UPDATE SET total_launches = EXCLUDED.total_launches, unique_views = EXCLUDED.unique_views; 
        """)

        cursor.execute("""
            WITH monthly_metrics AS (
                SELECT widget_id,
                    COUNT(*) AS total_launches,
                    COUNT(DISTINCT user_id) AS unique_launches
                FROM widget_interactions
                WHERE timestamp >= NOW() - INTERVAL '30 days'
                GROUP BY widget_id
            )
            INSERT INTO widget_metrics (widget_id, timeframe_type, timeframe_start, total_launches, unique_launches)
            SELECT widget_id, 
                'monthly', 
                NOW() - INTERVAL '30 days', 
                total_launches, 
                unique_launches
            FROM monthly_metrics
            ON CONFLICT (widget_id, timeframe_type)
            DO UPDATE SET total_launches = EXCLUDED.total_launches, unique_launches = EXCLUDED.unique_launches;        
        """)

        cursor.execute("""
            WITH yearly_metrics AS (
                SELECT widget_id,
                    COUNT(*) AS total_launches,
                    COUNT(DISTINCT user_id) AS unique_launches
                FROM widget_interactions
                WHERE timestamp >= NOW() - INTERVAL '365 days'
                GROUP BY widget_id
            )
            INSERT INTO widget_metrics (widget_id, timeframe_type, timeframe_start, total_launches, unique_launches)
            SELECT widget_id, 
                'yearly', 
                NOW() - INTERVAL '365 days', 
                total_launches, 
                unique_launches
            FROM yearly_metrics
            ON CONFLICT (widget_id, timeframe_type)
            DO UPDATE SET total_launches = EXCLUDED.total_launches, unique_launches = EXCLUDED.unique_launches;                        
        """)

        cursor.execute("""
            WITH all_time_metrics AS (
                SELECT widget_id,
                    COUNT(*) AS total_launches,
                    COUNT(DISTINCT user_id) AS unique_launches
                FROM widget_interactions
                GROUP BY widget_id
            )
            INSERT INTO widget_metrics (widget_id, timeframe_type, timeframe_start, total_launches, unique_launches)
            SELECT widget_id, 
                'all_time', 
                '1970-01-01'::timestamp,  -- or the earliest date you want to track from
                total_launches, 
                unique_launches
            FROM all_time_metrics
            ON CONFLICT (widget_id, timeframe_type)
            DO UPDATE SET total_launches = EXCLUDED.total_launches, unique_launches = EXCLUDED.unique_launches;      
        """)

        # Commit changes
        conn.commit()
        cursor.close()
        conn.close()
        
        conn.commit()

        return {"statusCode": 200, 
            "body": {
                "success": True,
                "message": "Widget metrics updated successfully"
            }
        }
        
    except psycopg2.DatabaseError as e:
        print(f"ERROR: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)}),
            'headers': {
                'Content-Type': 'application/json'
            }
        }
        
    finally:
        cursor.close()
        conn.close()
