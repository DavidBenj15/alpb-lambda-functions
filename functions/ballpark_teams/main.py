import boto3
import sys
import os
import json
# Adjust Python path to enable absolute imports:
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from functions.process_trackman.image.src.main import connect_to_db 


def insert_teams(file, conn):
    cursor = conn.cursor()
    for ballpark in file['ballparks']:
        cursor.execute(
            """
            SELECT * FROM team WHERE team_code = %s
            """,
            (
                ballpark['teamCode'],
            )
        )
        res = cursor.fetchone()
        if not res:
            cursor.execute(
                """
                INSERT INTO team (team_code, team_name, league)
                VALUES (%s, %s, %s)
                """,
                (
                    ballpark['teamCode'],
                    ballpark['teamName'],
                    ballpark['league'],
                )
            )
            conn.commit()


def insert_ballparks(file, conn):
    cursor = conn.cursor()
    for ballpark in file['ballparks']:
        cursor.execute(
            """
            SELECT * FROM ballpark WHERE ballpark_name = %s
            """,
            (
                ballpark['name'],
            )
        )
        res = cursor.fetchone()
        if not res:
            cursor.execute(
                """
                INSERT INTO ballpark (ballpark_name, city, state)
                VALUES (%s, %s, %s)
                """,
                (
                    ballpark['name'],
                    ballpark['city'],
                    ballpark['state']
                )
            )
            conn.commit()


def link_teams_and_ballparks(file, conn):
    cursor = conn.cursor()
    for ballpark in file['ballparks']:
        # 1. get team_id from team by team name
        cursor.execute(
            """
            SELECT team_id FROM team
            WHERE team_code = %s
            """,
            (
                ballpark['teamCode'],
            )
        )
        team_id = cursor.fetchone()[0]

        # 2. set FKs in ballpark
        cursor.execute(
            """
            UPDATE ballpark
            SET home_team_id = %s
            WHERE ballpark_name = %s
            RETURNING ballpark_id
            """,
            (
                team_id,
                ballpark['name'],
            )
        )
        ballpark_id = cursor.fetchone()[0]

        # 3. set FKs in team
        cursor.execute(
            """
            UPDATE team
            SET home_ballpark_id = %s
            WHERE team_code = %s
            """,
            (
                ballpark_id,
                ballpark['teamCode']
            )
        )
        conn.commit()


if __name__ == '__main__':
    conn = connect_to_db()
    s3 = boto3.client('s3')
    res = s3.get_object(Bucket='alpb-json',
                        Key='ballparks/ballpark_data.json')
    string = res['Body'].read().decode('utf-8')
    json_file = json.loads(string)
    insert_teams(json_file, conn)
    insert_ballparks(json_file, conn)
    link_teams_and_ballparks(json_file, conn)
    conn.close()
