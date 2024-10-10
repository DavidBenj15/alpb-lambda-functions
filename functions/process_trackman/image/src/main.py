# TODO: when inserting data, the program will throw an exception with the message "integer out of range,"
# but only (as far as I've noticed) in rows with lots of missing data. I'm not sure if this is worth debugging,
# as the data is missing in the first place (and thus the row likely will not be useful if inserted).

import os
import boto3
import psycopg2
import pandas as pd
from dotenv import load_dotenv
from io import StringIO
from datetime import datetime, timedelta

def handler(event, context):
    """Entry point for Lambda."""
    s3 = boto3.client('s3') # init. S3 client
    csv, file_name = get_csv(event, s3)
    conn = connect_to_db()
    process_csv(csv, file_name, conn, s3)
    conn.close()


def get_csv(event, s3):
    """Use event object's JSON to return a CSV from the S3 bucket."""
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key'] # path to CSV file in S3 bucket
    res = s3.get_object(Bucket=bucket, Key=key)


    string = res['Body'].read().decode('utf-8')
    csv = StringIO(string)
    file_name = key.split('/')[-1]
    print("Got csv:", file_name)

    return csv, file_name


def connect_to_db():
    """Use environment variables to return a connection object to the PostgreSQL database."""
    # get database details from environment
    load_dotenv()
    db_name = os.environ['DB_NAME']
    db_username = os.environ['DB_USERNAME']
    db_password = os.environ['DB_PASSWORD']
    db_host = os.environ['DB_HOST']
    db_port = os.environ['DB_PORT']
    # create connect object to database
    conn = psycopg2.connect(
        dbname = db_name,
        user=db_username,
        password=db_password,
        host=db_host,
        port=db_port
    )
    return conn


def process_csv(file, file_name, conn, s3):
    """ Read CSV, operate on the data, and insert the data into the database."""
    print("Processing csv...")
    df = pd.read_csv(file)
    df = df.where(pd.notnull(df), None) # cast empty values to None (instead of Float, for ex.)
    game = get_game_info(file_name, df, conn, s3)
    game_id = determine_game_id(file_name, conn, df, game, s3)
    if not game_id:
        print("Not inserting game.")
        return # "game_id == None" tells us that we should not insert the given data.
    
    # check if game exists already
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT 1
        FROM pitch
        WHERE game_id = %s;
        """,
        (game_id,)
    )
    game_exists = True if cursor.fetchone() else False
    
    if game['file_type'] == 'pitch data':
        handle_pitch_data(conn, df, game_id, game_exists)
    elif game['file_type'] == 'player positioning':
        handle_playerpos_data(conn, df, game_id, game_exists)
    else:
        print(f'Error: invalid file type. {file_name} was not inserted.')


def handle_pitch_data(conn, df, game_id, game_exists):
    # create PITCH table linked to game_id; insert data into PITCH table.
    cursor = conn.cursor()

    columns = (
        'hit_trajectory_zc2', 'pitcher_id', 'batter_id', 'game_id', 'date', 'time', 'pa_of_inning', 'pitch_of_pa', 'hit_trajectory_zc7', 
        'hit_trajectory_zc8', 'throw_speed', 'pop_time', 'exchange_time', 'time_to_base', 'catch_position_x', 'catch_position_y', 
        'catch_position_z', 'throw_position_x', 'throw_position_y', 'throw_position_z', 'base_position_x', 'base_position_y', 
        'base_position_z', 'throw_trajectory_xc0', 'throw_trajectory_xc1', 'throw_trajectory_xc2', 'throw_trajectory_yc0', 
        'throw_trajectory_yc1', 'throw_trajectory_yc2', 'throw_trajectory_zc0', 'throw_trajectory_zc1', 'throw_trajectory_zc2', 
        'inning', 'outs', 'balls', 'strikes', 'outs_on_play', 'runs_scored', 'tilt', 'y0', 'local_date_time', 'catcher_id', 
        'pitch_number', 'rel_speed', 'vert_rel_angle', 'horz_rel_angle', 'spin_rate', 
        'spin_axis', 'rel_height', 'rel_side', 'extension', 'vert_break', 'induced_vert_break', 'horz_break', 'plate_loc_height', 
        'plate_loc_side', 'zone_speed', 'vert_appr_angle', 'horz_appr_angle', 'zone_time', 'exit_speed', 'angle', 'direction', 
        'hit_spin_rate', 'position_at_110_x', 'position_at_110_y', 'position_at_110_z', 'distance', 'last_tracked_distance', 
        'bearing', 'hang_time', 'pfxx', 'pfxz', 'x0', 'z0', 'vx0', 'vy0', 'vz0', 'ax0', 'ay0', 'az0', 'effective_velo', 
        'max_height', 'measured_duration', 'speed_drop', 'pitch_last_measured_x', 'pitch_last_measured_y', 'pitch_last_measured_z', 
        'contact_position_x', 'contact_position_y', 'contact_position_z', 'pitch_trajectory_xc0', 'pitch_trajectory_xc1', 
        'pitch_trajectory_xc2', 'pitch_trajectory_yc0', 'pitch_trajectory_yc1', 'pitch_trajectory_yc2', 'pitch_trajectory_zc0', 
        'pitch_trajectory_zc1', 'pitch_trajectory_zc2', 'hit_spin_axis', 'hit_trajectory_xc0', 'hit_trajectory_xc1', 
        'hit_trajectory_xc2', 'hit_trajectory_xc3', 'hit_trajectory_xc4', 'hit_trajectory_xc5', 'hit_trajectory_xc6', 
        'hit_trajectory_xc7', 'hit_trajectory_xc8', 'hit_trajectory_yc0', 'hit_trajectory_yc1', 'hit_trajectory_yc2', 
        'hit_trajectory_yc3', 'hit_trajectory_yc4', 'hit_trajectory_yc5', 'hit_trajectory_yc6', 'hit_trajectory_yc7', 
        'hit_trajectory_yc8', 'hit_trajectory_zc0', 'hit_trajectory_zc1', 'hit_trajectory_zc3', 'hit_trajectory_zc4', 
        'hit_trajectory_zc5', 'hit_trajectory_zc6', 'pitcher_throws', 'pitcher_team_code', 'batter_side', 'batter_team_code', 'pitcher_set', 
        'catcher_throws', 'top_or_bottom', 'hit_launch_confidence', 'hit_landing_confidence', 'tagged_pitch_type', 
        'auto_pitch_type', 'pitch_call', 'k_or_bb', 'tagged_hit_type', 'play_result', 'catcher_throw_catch_confidence', 
        'catcher_throw_release_confidence', 'notes', 'catcher_throw_location_confidence', 'pitch_release_confidence', 
        'pitch_location_confidence', 'auto_hit_type', 'pitch_movement_confidence'
        )
    
    placeholders_str = ', '.join(['%s'] * len(columns))
    # iterate over each row in the DataFrame to insert pitch data
    for index, row in df.iterrows():
        # Get or insert player data for pitcher, batter, and catcher
        pitcher_id = get_or_insert_player(row['Pitcher'], row['PitcherThrows'], row['PitcherTeam'], "pitcher", conn)
        batter_id = get_or_insert_player(row['Batter'], row['BatterSide'], row['BatterTeam'], "batter", conn)
        catcher_id = get_or_insert_player(row['Catcher'], row['CatcherThrows'], row['CatcherTeam'], "catcher", conn)
        pitcher_set = handle_pitcher_set(row['PitcherSet'])

        values = ( 
            row['HitTrajectoryZc2'], pitcher_id, batter_id, game_id, row['Date'], row['Time'], row['PAofInning'], 
            row['PitchofPA'], row['HitTrajectoryZc7'], row['HitTrajectoryZc8'], row['ThrowSpeed'], row['PopTime'], row['ExchangeTime'], row['TimeToBase'], 
            row['CatchPositionX'], row['CatchPositionY'], row['CatchPositionZ'], row['ThrowPositionX'], row['ThrowPositionY'], 
            row['ThrowPositionZ'], row['BasePositionX'], row['BasePositionY'], row['BasePositionZ'], row['ThrowTrajectoryXc0'], 
            row['ThrowTrajectoryXc1'], row['ThrowTrajectoryXc2'], row['ThrowTrajectoryYc0'], row['ThrowTrajectoryYc1'], 
            row['ThrowTrajectoryYc2'], row['ThrowTrajectoryZc0'], row['ThrowTrajectoryZc1'], row['ThrowTrajectoryZc2'], row['Inning'], 
            row['Outs'], row['Balls'], row['Strikes'], row['OutsOnPlay'], row['RunsScored'], row['Tilt'], row['y0'], 
            row['LocalDateTime'], 
            catcher_id, row['PitchNo'], row['RelSpeed'], row['VertRelAngle'], row['HorzRelAngle'], 
            row['SpinRate'], row['SpinAxis'], row['RelHeight'], row['RelSide'], row['Extension'], row['VertBreak'], row['InducedVertBreak'], 
            row['HorzBreak'], row['PlateLocHeight'], row['PlateLocSide'], row['ZoneSpeed'], row['VertApprAngle'], row['HorzApprAngle'], 
            row['ZoneTime'], row['ExitSpeed'], row['Angle'], row['Direction'], row['HitSpinRate'], row['PositionAt110X'], 
            row['PositionAt110Y'], row['PositionAt110Z'], row['Distance'], row['LastTrackedDistance'], row['Bearing'], row['HangTime'], 
            row['pfxx'], row['pfxz'], row['x0'], row['z0'], row['vx0'], row['vy0'], row['vz0'], row['ax0'], row['ay0'], row['az0'], 
            row['EffectiveVelo'], row['MaxHeight'], row['MeasuredDuration'], row['SpeedDrop'], row['PitchLastMeasuredX'],
            row['PitchLastMeasuredY'], row['PitchLastMeasuredZ'], row['ContactPositionX'], row['ContactPositionY'], 
            row['ContactPositionZ'], row['PitchTrajectoryXc0'], row['PitchTrajectoryXc1'], row['PitchTrajectoryXc2'], row['PitchTrajectoryYc0'], 
            row['PitchTrajectoryYc1'], row['PitchTrajectoryYc2'], row['PitchTrajectoryZc0'], row['PitchTrajectoryZc1'], row['PitchTrajectoryZc2'], 
            row['HitSpinAxis'], row['HitTrajectoryXc0'], row['HitTrajectoryXc1'], row['HitTrajectoryXc2'], row['HitTrajectoryXc3'], 
            row['HitTrajectoryXc4'], row['HitTrajectoryXc5'], row['HitTrajectoryXc6'], row['HitTrajectoryXc7'], row['HitTrajectoryXc8'], 
            row['HitTrajectoryYc0'], row['HitTrajectoryYc1'], row['HitTrajectoryYc2'], row['HitTrajectoryYc3'], row['HitTrajectoryYc4'], 
            row['HitTrajectoryYc5'], row['HitTrajectoryYc6'], row['HitTrajectoryYc7'], row['HitTrajectoryYc8'], row['HitTrajectoryZc0'], 
            row['HitTrajectoryZc1'], row['HitTrajectoryZc3'], row['HitTrajectoryZc4'], row['HitTrajectoryZc5'], 
            row['HitTrajectoryZc6'], row['PitcherThrows'], row['PitcherTeam'], row['BatterSide'], row['BatterTeam'], pitcher_set, 
            row['CatcherThrows'], row['Top/Bottom'], row['HitLaunchConfidence'], row['HitLandingConfidence'], 
            row['TaggedPitchType'], row['AutoPitchType'], row['PitchCall'], row['KorBB'], row['TaggedHitType'], row['PlayResult'], 
            row['CatcherThrowCatchConfidence'], row['CatcherThrowReleaseConfidence'], row['Notes'], row['CatcherThrowLocationConfidence'], 
            row['PitchReleaseConfidence'], row['PitchLocationConfidence'], row['AutoHitType'], row['PitchMovementConfidence']
            )
        
        if game_exists:
            insert_data_game_exists(columns, values, game_id, row['PitchNo'], conn)
        else:
            insert_data_game_dne(columns, values, placeholders_str, conn)


def handle_pitcher_set(val):
    if val == "Undefined":
        return None
    return val

def handle_playerpos_data(conn, df, game_id, game_exists):
    cursor = conn.cursor()
    columns = (
        'pitch_number', 'date', 'time', 'pitch_call', 'play_result', 'detected_shift', 'first_b_position_at_release_x', 'first_b_position_at_release_z',
        'second_b_position_at_release_x', 'second_b_position_at_release_z', 'third_b_position_at_release_x', 'third_b_position_at_release_z',
        'ss_position_at_release_x', 'ss_position_at_release_z', 'lf_position_at_release_x', 'lf_position_at_release_z', 'cf_position_at_release_x',
        'cf_position_at_release_z', 'rf_position_at_release_x', 'rf_position_at_release_z', 'first_b_player_id', 'second_b_player_id',
        'third_b_player_id', 'ss_player_id', 'lf_player_id', 'cf_player_id', 'rf_player_id'
        )
    placeholders_str = ', '.join(['%s'] * len(columns))
    for index, row in df.iterrows():
        # could optimize these queries to only run if trackman-generated player ids in the current row
        # are different than those in the previous row, but speed does not seem to be a high priority
        # for our application.
        first_base_player_id = get_or_insert_player(row['1B_Name'], None, row['PitcherTeam'], "defense", conn)
        second_base_player_id = get_or_insert_player(row['2B_Name'], None, row['PitcherTeam'], "defense", conn)
        third_base_player_id = get_or_insert_player(row['3B_Name'], None, row['PitcherTeam'], "defense", conn)
        ss_player_id = get_or_insert_player(row['SS_Name'], None, row['PitcherTeam'], "defense", conn)
        lf_player_id = get_or_insert_player(row['LF_Name'], None, row['PitcherTeam'], "defense", conn)
        cf_player_id = get_or_insert_player(row['CF_Name'], None, row['PitcherTeam'], "defense", conn)
        rf_player_id = get_or_insert_player(row['RF_Name'], None, row['PitcherTeam'], "defense", conn)

        values = (
            row['PitchNo'], row['Date'], row['Time'], row['PitchCall'], row['PlayResult'], row['DetectedShift'], row['1B_PositionAtReleaseX'], 
            row['1B_PositionAtReleaseZ'], row['2B_PositionAtReleaseX'], row['2B_PositionAtReleaseZ'], row['3B_PositionAtReleaseX'],
            row['3B_PositionAtReleaseZ'], row['SS_PositionAtReleaseX'], row['SS_PositionAtReleaseZ'], row['LF_PositionAtReleaseX'],
            row['LF_PositionAtReleaseZ'], row['CF_PositionAtReleaseX'], row['CF_PositionAtReleaseZ'], row['RF_PositionAtReleaseX'],
            row['RF_PositionAtReleaseZ'], first_base_player_id, second_base_player_id, third_base_player_id, ss_player_id, lf_player_id,
            cf_player_id, rf_player_id
            )
        
        if game_exists:
            insert_data_game_exists(columns, values, game_id, row['PitchNo'], conn)
        else:
            insert_data_game_dne(columns, values, placeholders_str, conn)


def insert_data_game_exists(columns, values, game_id, pitch_number, conn):
    cursor = conn.cursor()
    try:
        set_clause = construct_set_clause(columns)
        # update data
        cursor.execute(
            f"""
            UPDATE pitch
            SET {set_clause}
            WHERE game_id = %s
            AND pitch_number = %s;
            """,
            values + (game_id,) + (pitch_number,)
            )
        # commit the transaction
        conn.commit()
        print('updated row')
    except psycopg2.DataError as e:
        conn.rollback()
        print(f"DataError inserting data: {e}")
        print(f"Problematic values: {values}")
    except Exception as e:
        # rollback the transaction in case of error
        conn.rollback()
        print(f"Error inserting data when game exists in DB: {e}")
    finally:
        cursor.close()

def insert_data_game_dne(columns, values, placeholders_str, conn):
    columns_str = ', '.join(columns)
    cursor = conn.cursor()
    # insert data
    try:
        cursor.execute(
            f"""
            INSERT INTO pitch ({columns_str})
            VALUES ({placeholders_str});
            """,
            values
        )
        conn.commit()
        print('inserted row')
    except Exception as e:
        # rollback the transaction in case of error
        conn.rollback()
        print(f"Error inserting data when game previously DNE in DB: {e}")
    finally:
        cursor.close()


def validate_type(data):
    return data if isinstance(data, str) else None

def construct_set_clause(columns):
    set_clause = ''
    for i in range(len(columns)):
        set_clause += (f'{columns[i]} = %s, ')
    set_clause = set_clause[:-2] # remove final ', '
    return set_clause  


def get_or_insert_player(player_name, handedness, team_code, player_type, conn):
    """ Get the player ID from the player name, handedness, and team. Insert the player if they do not exist. """

    # Edge case: player_name is null (not usefull to us)
    if not player_name or (isinstance(player_name, str) and player_name.lower() == "nan"):
        return None

    # Edge case: plyaer_type is not a string
    if not isinstance(player_type, str):
        raise Exception("paramter player_type is not of type string")
    
    # Edge case: handedness is string "Undefined"
    if handedness == "Undefined":
        handedness = None
    
    team_id = get_or_insert_team_id(team_code, conn)
    player_type = player_type.lower()
    
    try:
        cursor = conn.cursor()
        # check if the player already exists
        cursor.execute(
            """
            SELECT player_id, player_pitching_handedness, player_batting_handedness
            FROM player 
            WHERE player_name = %s AND team_id = %s;
            """,
            (player_name, team_id)
        )
        result = cursor.fetchone()
        if result:
            player_id = result[0]
            existing_pitch_hand = result[1]
            existing_bat_hand = result[2]
            # player might be a switch hitter or have empty an empty batting/pitching field.
            # update accordingly:
            if player_type == "batter":
                handle_update_batting_handedness(player_id, handedness, existing_bat_hand, conn)
            elif player_type == "pitcher":
                handle_update_pitching_handedness(player_id, handedness, existing_pitch_hand, conn)
            return player_id
        else:
            # insert the player if they do not exist
            if player_type == "batter":
                cursor.execute(
                    """
                    INSERT INTO player (player_name, player_batting_handedness, team_id) 
                    VALUES (%s, %s, %s) RETURNING player_id;
                    """,
                    (player_name, handedness, team_id)
                )
            elif player_type == "pitcher":
                cursor.execute(
                    """
                    INSERT INTO player (player_name, player_pitching_handedness, team_id) 
                    VALUES (%s, %s, %s) RETURNING player_id;
                    """,
                    (player_name, handedness, team_id)
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO player (player_name, team_id)
                    VALUES (%s, %s) RETURNING player_id;
                    """,
                    (player_name, team_id)
                )
            conn.commit()
            result = cursor.fetchone()
            return result[0]
    except Exception as e:
        print(f'Error getting or inserting player id: {e}')
        return None


def handle_update_batting_handedness(id, hand, existing_hand, conn):
    try:
        cursor = conn.cursor()
        if isinstance(existing_hand, str) and existing_hand != hand:
            hand = "Switch"
        if not existing_hand or (isinstance(existing_hand, str) and (existing_hand.lower() == "nan" or hand == 'Switch')):
            # batter_handedness DNE; insert it.
            cursor.execute(
                """
                UPDATE player
                SET player_batting_handedness = %s
                WHERE player_id = %s;
                """,
                (hand, id)
            )
            conn.commit()
    except Exception as e:
        print(f'Error handling updating batter handedness: {e}')


def handle_update_pitching_handedness(id, hand, existing_hand, conn):
    try:
        cursor = conn.cursor()
        if not existing_hand or (isinstance(existing_hand, str) and (existing_hand.lower() == "nan")):
            cursor.execute(
                """
                UPDATE player
                SET player_pitching_handedness = %s
                WHERE player_id = %s;
                """,
                (hand, id)
            )
            conn.commit()
    except Exception as e:
        print(f'Error handling updating batter handedness: {e}')


def get_or_insert_team_id(team_code, conn):
    """
    Get the team ID from the team name. Insert the team if it does not exist in the DB.
    Will not fill in "league" (North or South) or "home_ballpark_id" fields.
    """
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT team_id FROM team
        WHERE team_code = %s;
        """,
        (team_code,)
    )
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        # insert team if it does not exist
        cursor.execute(
            """
            INSERT INTO team (team_code)
            VALUES (%s) RETURNING team_id;
            """,
            (team_code,)
        )
        conn.commit()
        result = cursor.fetchone()
        return result[0]
    

def determine_game_id(file_name, conn, df, game, s3):
    """ Determine the appropriate game ID for the file.
    If the game does not already have an associated ID, 
    this function will create a new row in 'game'.

    Parameters:
        file_name (str): The name of the file to analyze.
        conn (connection): PostgreSQL connection object.
        df (dataframe): Dataframe containing the CSV's data.
        game (dict): Contains crucial information about the game.

    Returns:
        int: The game ID the new game is associated with; 
            None if the game should not be inserted to the DB.
    """
    if not game:
        return None
    game_id = None
    try:
        cursor = conn.cursor()
        # get home_team and away_team based on ids.
        team_id_query = """
            SELECT team_id FROM team
            WHERE team_code = %s;
        """
        cursor.execute(team_id_query, (game['home_team'],))
        home_team_id = cursor.fetchone()[0]

        cursor.execute(team_id_query, (game['away_team'],))
        visiting_team_id = cursor.fetchone()[0]
        # query the databse to check if this game already exists.
        cursor.execute(
            """
            SELECT verified, game_id FROM GAME 
            WHERE home_team_id = %s
                AND visiting_team_id = %s
                AND date = %s
                AND daily_game_number = %s;
            """,
            (home_team_id, visiting_team_id, game['date'], game['daily_game_number'])
        )
        res = cursor.fetchone()
        if res:
            existing_is_verified, existing_game_id = res
            if (game['verified'] and not existing_is_verified):
                # If there already exists pitch data for this game, we only want to replace it if
                # the old data is unverified and the new data is verified.
                game_id = existing_game_id
                cursor.execute(
                    """
                    UPDATE game
                    SET verified = true
                    WHERE game_id = %s;
                    """,
                    (game_id,)

                )
                conn.commit()
            elif game['file_type'] == 'player positioning':
                # We assume that all player positioning data is unverified, so we can insert it regardless
                # of whether the existing game is verified or not.
                game_id = existing_game_id
        else:
            cursor.execute(
                """
                INSERT INTO GAME (home_team_id, visiting_team_id, ballpark_id, verified, date, daily_game_number)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING game_id;
                """,
                (home_team_id, visiting_team_id, game['ballpark_id'], game['verified'], game['date'], game['daily_game_number'])
            )
            game_id = cursor.fetchone()[0]
            conn.commit()
    except psycopg2.Error as db_error:
        print(f'Database error: {db_error}')
    except KeyError as key_error:
        print(f'Key error: {key_error}')
    except TypeError as type_error:
        print(f'Type error: {type_error}')
    except AttributeError as attr_error:
        print(f'Attribute error: {attr_error}')
    except IndexError as index_error:
        print(f'Index error: {index_error}')
    except Exception as e:
        print(f'Error determining game ID: {e}')
    return game_id


def get_date_from_df(df):
    """Some values in the Date column are empty for some CSVs.
    This function loops over each row until it finds a non-null date.
    """
    for date in df['Date']:
        if date is not None and (isinstance(date, str) and date.lower() != "nan"):
            return date
    raise ValueError('All values in Date column are null.')


def get_game_info(file_name, df, conn, s3):
    """ Return the game's details based on the CSV data, its file name, and existing data in the DB.

    Parameters:
        file_name (str): The name of the file to analyze.
        conn (connection): PostgreSQL connection object.
        df (dataframe): Dataframe containing the CSV's data.

    Returns:
        dict: Dictionary containing data about the game.
    """
    cursor = conn.cursor()
    game = {}

    # get game info from file name
    file_name_details = file_name.split('-') # ex: ['20240628', 'HagerstownBallpark', '1_unverified']
    game['ballpark'] = file_name_details[1]
    game['daily_game_number'] = int(file_name_details[2][0])
    if len(file_name_details[2]) > 1 and file_name_details[2][2:].startswith('unverified'):
        game['verified'] = False
    else:
        game['verified'] = True
    if len(file_name_details[2]) > 1 and file_name_details[2].endswith('playerpositioning_FHC.csv'):
        game['file_type'] = 'player positioning'
        home_and_away = get_player_positioning_teams(file_name, s3)
        if not home_and_away:
            # Could not find corresponding pitch data in S3 for given player positioning data. Abort insertion.
            return None
        game['home_team'], game['away_team'] = home_and_away
    else:
        game['file_type'] = 'pitch data'
        # only pitch data CSVs contain fields about home team and away team (for whatever reason)
        game['home_team'] = df['HomeTeam'][0]
        game['away_team'] = df['AwayTeam'][0]

    game['date'] = get_date_from_df(df)

    # query database for ids based on names.
    ballpark_id_query = """
        SELECT ballpark_id FROM ballpark
        WHERE ballpark_name = %s;
    """
    cursor.execute(ballpark_id_query, (game['ballpark'],))
    game['ballpark_id'] = cursor.fetchone()[0]

    team_id_query = """
        SELECT team_id FROM TEAM
        WHERE team_code = %s;
    """
    cursor.execute(team_id_query, (game['home_team'],))
    game['home_team_id'] = cursor.fetchone()[0]
    cursor.execute(team_id_query, (game['away_team'],))
    game['away_team_id'] = cursor.fetchone()[0]

    return game


def get_player_positioning_teams(file_name, s3):
    """
    Get the home team and away team for player positioning files by looking at the
    corresponding pitch data CSVs in S3.

    Returns:
        2-tuple: (HomeTeam, AwayTeam); Strings.
    """
    split = file_name.split('_')
    verified_pitch_file_name = split[0] + '.csv'
    unverified_pitch_file_name = '_'.join(split[:2]) + '.csv'

    # We need to check in the S3 folders at the date of the file's date and the date 
    # of the day before the file's date. This is because some files will be placed in
    # the folder of the day after they were recorded, ex: "20240629..." being placed
    # in ".../06/30/2024".

    year, month, day = file_name[:4], file_name[4:6], file_name[6:8] # yyyy, mm, dd

    day_after_year, day_after_month, day_after_day = get_day_after(year, month, day)

    bucket = 'trackman-data'
    key_prefixes = [None] * 2
    key_prefixes[0] = '/'.join([year, month, day, 'CSV'])
    key_prefixes[1] = '/'.join([day_after_year, day_after_month, day_after_day, 'CSV'])
    file = None
    exception_message = None

    for key_prefix in key_prefixes:
        try:
            # First, try to retrieve verified pitching data file.
            verified_file_path = '/'.join([key_prefix, verified_pitch_file_name])
            file = s3.get_object(Bucket=bucket, Key=verified_file_path)
            break
        except Exception:
            try:
                # Then, if unsuccessful, try to retrieve unverified pitching data file.
                unverified_file_path = '/'.join([key_prefix, unverified_pitch_file_name])
                file = s3.get_object(Bucket=bucket, Key=unverified_file_path)
                break
            except Exception as e:
                exception_message = e
    
    if not file:
        print(exception_message)
        return None
    
    file_content = file['Body'].read().decode('utf-8')
    csv = StringIO(file_content) # Convert file from str. to csv
    df = pd.read_csv(csv)

    return (df['HomeTeam'][0], df['AwayTeam'][0])


def get_day_after(year, month, day):
    """
    Parameters:
        year (str): yyyy
        month (str): mm
        day (str): dd

    Returns:
        3-tuple: (yyyy, mm, dd) strings representing the day after the date
            passed to the function.
    """
    date_str = f"{year}, {month}, {day}"
    date_obj = datetime.strptime(date_str, '%Y, %m, %d')
    day_after_obj = date_obj + timedelta(days=1)

    return day_after_obj.strftime('%Y, %m, %d').split(', ')