# To run test from terminal: py -m pytest the/test/location.py -s
from functions.process_trackman.image.src.main import connect_to_db, get_csv, get_game_info, handler, determine_game_id, get_or_insert_player  
import sys
import os
import pytest
import json
import pandas as pd
import boto3
from io import StringIO
# Adjust Python path to enable absolute imports:
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))
      

# Only allow the script to be run on the test database.
# if os.getenv('DB_NAME') != 'test1':
#     print('ERROR: This script should only be run on the test1 database. Exiting script.')
#     sys.exit(1)


s3 = boto3.client('s3')
test_dir = os.path.dirname(os.path.abspath(__file__))

class TestConnectToDB:
    expected_params = {
        'dbname': 'postgres',
        'user': 'postgres',
        'host': 'alpb-1-instance-1.cx866cecsebt.us-east-2.rds.amazonaws.com',
        'port': '5432',
    }

    def test_connection_established_with_correct_params(self):
        conn = connect_to_db()
        actual_params = conn.get_dsn_parameters()
        for param, value in self.expected_params.items():
            assert value == actual_params[param]


class TestGetCSV:
    event = open(os.path.join(test_dir, './test_events/unverified_pitching_test.json'))
    data = json.load(event)
    print("Data:", data)
    
    def test_get_csv_returns_file_and_filename(self):
        file, filename = get_csv(self.data, s3)
        assert filename == self.data['Records'][0]['s3']['object']['key'].split('/')[-1]

class TestDetermineGameIDAndInsertData:
    """These two tests are grouped together because they require similar helper methods."""
    conn = connect_to_db()

    def get_team_id_by_code(self, cursor, team_code):
        cursor.execute(
            """
            SELECT team_id FROM team
            WHERE team_code = %s;
            """,
            (team_code,)
        )
        return cursor.fetchone()[0]
    

    def get_ballpark_id_by_name(self, cursor, ballpark_name):
        cursor.execute(
            """
            SELECT ballpark_id FROM ballpark
            WHERE ballpark_name = %s;
            """,
            (ballpark_name,)
        )
        return cursor.fetchone()[0]


    def get_game_ids(self, cursor, home_team_code, visiting_team_code, ballpark_name, verified, date, daily_number):
        home_team_id = self.get_team_id_by_code(cursor, home_team_code)
        visiting_team_id = self.get_team_id_by_code(cursor, visiting_team_code)
        ballpark_id = self.get_ballpark_id_by_name(cursor, ballpark_name)
        cursor.execute(
            """
            SELECT game_id FROM game
            WHERE home_team_id = %s
            AND visiting_team_id = %s
            AND ballpark_id = %s
            AND verified = %s
            AND date = %s
            AND daily_game_number = %s;
            """,
            (home_team_id, visiting_team_id, ballpark_id, verified, date, daily_number)
        )
        game_ids = cursor.fetchall()

        return game_ids


    def delete_data_by_game_id(self, cursor, game_ids):
        if not game_ids:
            return
        for game_id in game_ids:
            cursor.execute(
                """
                DELETE FROM pitch
                WHERE game_id = %s;
                DELETE FROM game
                WHERE game_id = %s;
                """,
                (game_id, game_id)
            )
        self.conn.commit()


    def call_determine_game_id(self, file_path):
        event = open(os.path.join(test_dir, file_path))
        data = json.load(event)
        file, file_name = get_csv(data, s3)
        df = pd.read_csv(file)
        game = get_game_info(file_name, df, self.conn, s3)
        return determine_game_id(file_name, self.conn, df, game, s3)
        

    def test_determine_playerpos_gameid_unverified_pitching_exists(self):
        cursor = self.conn.cursor()
        self.call_determine_game_id('test_events/unverified_pitching_test.json')
        pitching_game_ids = None
        playerpos_game_ids = None
        try:
            pitching_game_ids = self.get_game_ids(cursor, 'LAN', 'LI', 'ClipperMagazine', False, '2024-06-29', 1)
            assert len(pitching_game_ids) == 1
            self.call_determine_game_id('test_events/unverified_player_positioning_test.json')
            playerpos_game_ids = self.get_game_ids(cursor, 'LAN', 'LI', 'ClipperMagazine', False, '2024-06-29', 1)
            assert pitching_game_ids == playerpos_game_ids
        finally:
            self.delete_data_by_game_id(cursor, pitching_game_ids)
            self.delete_data_by_game_id(cursor, playerpos_game_ids)


    def test_determine_playerpos_gameid_verified_pitching_exists(self):
        cursor = self.conn.cursor()
        self.call_determine_game_id('test_events/verified_pitching_test.json')
        pitching_game_ids = None
        playerpos_game_ids = None
        try:
            pitching_game_ids = self.get_game_ids(cursor, 'LAN', 'LI', 'ClipperMagazine', True, '2024-06-29', 1)
            assert len(pitching_game_ids) == 1
            self.call_determine_game_id('test_events/unverified_player_positioning_test.json')
            playerpos_game_ids = self.get_game_ids(cursor, 'LAN', 'LI', 'ClipperMagazine', True, '2024-06-29', 1)
            assert pitching_game_ids == playerpos_game_ids
        finally:
            self.delete_data_by_game_id(cursor, pitching_game_ids)
            self.delete_data_by_game_id(cursor, playerpos_game_ids)


    def test_determine_playerpos_gameid_pitching_dne(self):
        cursor = self.conn.cursor()
        game_ids = None
        try:
            self.call_determine_game_id('test_events/unverified_player_positioning_test.json')
            game_ids = self.get_game_ids(cursor, 'LAN', 'LI', 'ClipperMagazine', False, '2024-06-29', 1)
            assert len(game_ids) == 1
        finally:
            self.delete_data_by_game_id(cursor, game_ids)


    def test_determine_verified_gameid_unverified_exists(self):
        cursor = self.conn.cursor()
        self.call_determine_game_id('test_events/unverified_pitching_test.json')
        verified_game_ids = None
        unverified_game_ids = None
        try:
            unverified_game_ids = self.get_game_ids(cursor, 'LAN', 'LI', 'ClipperMagazine', False, '2024-06-29', 1)
            assert len(unverified_game_ids) == 1
            self.call_determine_game_id('test_events/verified_pitching_test.json')
            verified_game_ids = self.get_game_ids(cursor, 'LAN', 'LI', 'ClipperMagazine', True, '2024-06-29', 1)
            assert unverified_game_ids == verified_game_ids
        finally:
            self.delete_data_by_game_id(cursor, unverified_game_ids)
            self.delete_data_by_game_id(cursor, verified_game_ids)


    def test_determine_unverified_gameid_verified_exists(self):
        self.call_determine_game_id('test_events/verified_pitching_test.json')
        cursor = self.conn.cursor()
        verified_game_ids = None
        unverified_game_ids = None
        try:
            self.call_determine_game_id('test_events/unverified_pitching_test.json')
            verified_game_ids = self.get_game_ids(cursor, 'LAN', 'LI', 'ClipperMagazine', True, '2024-06-29', 1)
            assert len(verified_game_ids) == 1
            unverified_game_ids = self.get_game_ids(cursor, 'LAN', 'LI', 'ClipperMagazine', False, '2024-06-29', 1)
            assert len(unverified_game_ids) == 0
        finally:
            self.delete_data_by_game_id(cursor, verified_game_ids)
            self.delete_data_by_game_id(cursor, unverified_game_ids)
        

    def test_determine_unverified_gameid_verified_dne(self):
        self.call_determine_game_id('test_events/unverified_pitching_test.json')
        cursor = self.conn.cursor()
        try:
            game_ids = self.get_game_ids(cursor, 'LAN', 'LI', 'ClipperMagazine', False, '2024-06-29', 1)
            assert len(game_ids) == 1
        finally:
            self.delete_data_by_game_id(cursor, game_ids)

    
    def test_determine_game_id_double_header(self):
        self.call_determine_game_id('test_events/unverified_pitch_double_header1_test.json')
        cursor = self.conn.cursor()
        game1_ids = None
        game2_ids = None
        try:
            game1_ids = self.get_game_ids(cursor, 'SMD', 'LAN', 'RegencyFurnitureStadium', False, '2024-06-18', 1)
            assert len(game1_ids) == 1
            self.call_determine_game_id('test_events/unverified_pitch_double_header2_test.json')
            game2_ids = self.get_game_ids(cursor, 'SMD', 'LAN', 'RegencyFurnitureStadium', False, '2024-06-18', 2)
            assert game1_ids != game2_ids
        finally:
            self.delete_data_by_game_id(cursor, game1_ids)
            self.delete_data_by_game_id(cursor, game2_ids)

    # TEST INSERTION:

    def test_insert_unverified_pitch(self):
        event = json.load(open(os.path.join(test_dir,'test_events/unverified_pitching_test.json')))
        cursor = self.conn.cursor()
        try:
            handler(event, None)
        finally:
            game_id = self.get_game_ids(cursor, 'LAN', 'LI', 'ClipperMagazine', False, '2024-06-29', 1)
            self.delete_data_by_game_id(cursor, game_id)


    # def test_insert_verified_pitch_unverified_exists(self):
    #     unverified_event = json.load(open(os.path.join(test_dir,'test_events/unverified_pitching_test.json')))
    #     verified_event = json.load(open(os.path.join(test_dir, 'test_events/verified_pitching_test.json')))
    #     cursor = self.conn.cursor()
    #     try:
    #         handler(unverified_event, None)
    #         handler(verified_event, None)
    #     finally:
    #         unverified_game_id = self.get_game_ids(cursor, 'LAN', 'LI', 'ClipperMagazine', False, '2024-06-29', 1)
    #         verified_game_id = self.get_game_ids(cursor, 'LAN', 'LI', 'ClipperMagazine', True, '2024-06-29', 1)
    #         self.delete_data_by_game_id(cursor, unverified_game_id)
    #         self.delete_data_by_game_id(cursor, verified_game_id)


    # def test_insert_unverified_playerpos(self):
    #     event = json.load(open(os.path.join(test_dir, 'test_events/unverified_player_positioning_test.json')))
    #     cursor = self.conn.cursor()
    #     try:
    #         handler(event, None)
    #     finally:
    #         game_id = self.get_game_ids(cursor, 'LAN', 'LI', 'ClipperMagazine', False, '2024-06-29', 1)
    #         self.delete_data_by_game_id(cursor, game_id)


    # def test_insert_unverified_playerpos_pitch_exists(self):
    #     pitching_event = json.load(open(os.path.join(test_dir, 'test_events/verified_pitching_test.json')))
    #     playerpos_event = json.load(open(os.path.join(test_dir, 'test_events/unverified_player_positioning_test.json')))
    #     cursor = self.conn.cursor()
    #     try:
    #         handler(pitching_event, None)
    #         handler(playerpos_event, None)
    #     finally:
    #         game_id = self.get_game_ids(cursor, 'LAN', 'LI', 'ClipperMagazine', True, '2024-06-29', 1)
    #         self.delete_data_by_game_id(cursor, game_id)


    # def test_insert_double_headers(self):
    #     game1_event = json.load(open(os.path.join(test_dir, 'test_events/unverified_pitch_double_header1_test.json')))
    #     game2_event = json.load(open(os.path.join(test_dir, 'test_events/unverified_pitch_double_header2_test.json')))
    #     cursor = self.conn.cursor()
    #     try:
    #         handler(game1_event, None)
    #         handler(game2_event, None)
    #     finally:
    #         game1_id = self.get_game_ids(cursor, 'SMD', 'LAN', 'RegencyFurnitureStadium', False, '2024-06-18', 1)
    #         game2_id = self.get_game_ids(cursor, 'SMD', 'LAN', 'RegencyFurnitureStadium', False, '2024-06-18', 2)
    #         # self.delete_data_by_game_id(cursor, game1_id)
    #         # self.delete_data_by_game_id(cursor, game2_id)


class TestGetGameInfo:
    conn = connect_to_db()
    
    def call_get_info(self, file_path):
        event = open(os.path.join(test_dir, file_path))
        data = json.load(event)
        file, file_name = get_csv(data, s3)
        df = pd.read_csv(file)
        return get_game_info(file_name, df, self.conn, s3)

    def test_unverified_pitching_game_info(self):
        expected_info = {
            'home_team': 'LAN',
            'away_team': 'LI',
            'date': '2024-06-29',
            'ballpark': 'ClipperMagazine',
            'daily_game_number': 1,
            'verified': False,
            'file_type': 'pitch data'
        }
        actual_info = self.call_get_info('test_events/unverified_pitching_test.json')
        for key, expected_value in expected_info.items():
            assert expected_value == actual_info[key]

    def test_verified_pitching_game_info(self):
        expected_info = {
            'home_team': 'LAN',
            'away_team': 'LI',
            'date': '2024-06-29',
            'ballpark': 'ClipperMagazine',
            'daily_game_number': 1,
            'verified': True,
            'file_type': 'pitch data'
        }
        actual_info = self.call_get_info('test_events/verified_pitching_test.json')
        for key, expected_value in expected_info.items():
            assert expected_value == actual_info[key]

    def test_unverified_player_positioning_data(self):
        expected_info = {
            'home_team': 'LAN',
            'away_team': 'LI',
            'date': '2024-06-29',
            'ballpark': 'ClipperMagazine',
            'daily_game_number': 1,
            'verified': False,
            'file_type': 'player positioning'
        }
        actual_info = self.call_get_info('test_events/unverified_player_positioning_test.json')
        for key, expected_value in expected_info.items():
            assert expected_value == actual_info[key]

class TestGetOrInsertPlayer:
    conn = connect_to_db()

    def delete_player_by_id(self, cursor, id):
        if not id:
            return

        cursor.execute(
            """
            DELETE FROM player
            WHERE player_id = %s;
            """,
            (id,)
        )
        self.conn.commit()
    
    def test_insert_batter(self):
        cursor = self.conn.cursor()
        player_id = None
        try:
            player_id = get_or_insert_player(
                "Test Batter",
                "Right",
                "LAN",
                "batter",
                self.conn,
            )
            cursor.execute(
                """
                SELECT player_name, player_batting_handedness FROM player
                WHERE player_id = %s
                """,
                (player_id,)
            )
            name, bat_hand = cursor.fetchone()
            print(name, bat_hand)
            assert (name == "Test Batter" and bat_hand == "Right")
        finally:
            self.delete_player_by_id(cursor, player_id)


    def test_insert_pitcher(self):
        cursor = self.conn.cursor()
        player_id = None
        try:
            player_id = get_or_insert_player(
                "Test Pitcher",
                "Left",
                "LAN",
                "pitcher",
                self.conn,
            )
            cursor.execute(
                """
                SELECT player_name, player_pitching_handedness FROM player
                WHERE player_id = %s
                """,
                (player_id,)
            )
            name, pitch_hand = cursor.fetchone()
            print(name, pitch_hand)
            assert (name == "Test Pitcher" and pitch_hand == "Left")
        finally:
            self.delete_player_by_id(cursor, player_id)


    def test_pitch_and_bat_hands_exist(self):
        cursor = self.conn.cursor()
        player_id1 = None
        player_id2 = None
        try:
            player_id1 = get_or_insert_player(
                "Test pitch and bat hands exist",
                "Left",
                "LAN",
                "pitcher",
                self.conn,
            )
            player_id2 = get_or_insert_player(
                "Test pitch and bat hands exist",
                "Right",
                "LAN",
                "batter",
                self.conn,
            )
            assert(player_id1 == player_id2)
            cursor.execute(
                """
                SELECT player_name, player_pitching_handedness, player_batting_handedness FROM player
                WHERE player_id = %s
                """,
                (player_id1,)
            )
            name, pitch_hand, bat_hand = cursor.fetchone()
            print(name, pitch_hand)
            assert (name == "Test pitch and bat hands exist" and pitch_hand == "Left" and bat_hand == "Right")
        finally:
            self.delete_player_by_id(cursor, player_id1)
            self.delete_player_by_id(cursor, player_id2)


    def test_update_to_switch_hitter(self):
        cursor = self.conn.cursor()
        player_id1 = None
        player_id2 = None
        try:
            player_id1 = get_or_insert_player(
                "Test update to switch hitter",
                "Left",
                "LAN",
                "batter",
                self.conn,
            )
            player_id2 = get_or_insert_player(
                "Test update to switch hitter",
                "Right",
                "LAN",
                "batter",
                self.conn,
            )
            assert(player_id1 == player_id2)
            cursor.execute(
                """
                SELECT player_name, player_batting_handedness FROM player
                WHERE player_id = %s
                """,
                (player_id1,)
            )
            name, bat_hand = cursor.fetchone()
            print(name, bat_hand)
            assert (name == "Test update to switch hitter" and bat_hand == "Switch")
        finally:
            self.delete_player_by_id(cursor, player_id1)
            self.delete_player_by_id(cursor, player_id2)


    def test_update_switch_hitter_pitch_hand(self):
        cursor = self.conn.cursor()
        player_id1 = None
        player_id2 = None
        try:
            player_id1 = get_or_insert_player(
                "Test update switch hitter hand",
                "Left",
                "LAN",
                "batter",
                self.conn,
            )
            player_id2 = get_or_insert_player(
                "Test update switch hitter hand",
                "Right",
                "LAN",
                "batter",
                self.conn,
            )
            player_id3 = get_or_insert_player(
                "Test update switch hitter hand",
                "Right",
                "LAN",
                "Pitcher",
                self.conn,
            )
            assert(player_id1 == player_id2 == player_id3)
            cursor.execute(
                """
                SELECT player_name, player_batting_handedness, player_pitching_handedness FROM player
                WHERE player_id = %s
                """,
                (player_id1,)
            )
            name, bat_hand, pitch_hand = cursor.fetchone()
            print(name, bat_hand)
            assert (name == "Test update switch hitter hand" and bat_hand == "Switch" and pitch_hand == "Right")
        finally:
            self.delete_player_by_id(cursor, player_id1)
            self.delete_player_by_id(cursor, player_id2)
