import sqlite3
import pandas as pd
import time
import requests
from nba_api.stats.static import teams
from nba_api.stats.endpoints import cumestatsteamgames, cumestatsteam
import numpy as np
import json
import difflib

DATABASE_NAME = "NBA_DATA.db"

def generate_database(seasons: list[int], seasonType: str):
    # Generate a database

    def retry(func, retries=3):
        def retry_wrapper(*args, **kwargs):
            attempts = 0
            while attempts < retries:
                try:
                    return func(*args, **kwargs)
                except requests.exceptions.RequestException as e:
                    print(e)
                    time.sleep(30)
                    attempts += 1

        return retry_wrapper

    def getSeasonScheduleFrame(season, seasonType, teamLookup):
        def getGameDate(matchup):
            return matchup.partition(' at')[0][:10]

        def getHomeTeam(matchup):
            return matchup.partition(' at')[2]

        def getAwayTeam(matchup):
            return matchup.partition(' at')[0][10:]

        def getTeamIDFromNickname(nickname):
            return teamLookup.loc[teamLookup['TEAM_NICKNAME'] == difflib.get_close_matches(nickname, teamLookup['TEAM_NICKNAME'], 1)[0]].values[0][0]

        @retry
        def getRegularSeasonSchedule(season, teamID, seasonType):
            season = str(season) + "-" + str(season + 1)[-2:]
            teamGames = cumestatsteamgames.CumeStatsTeamGames(league_id='00', season=season,
                                                            season_type_all_star=seasonType,
                                                            team_id=teamID).get_normalized_json()

            teamGames = pd.DataFrame(json.loads(teamGames)['CumeStatsTeamGames'])
            teamGames['SEASON'] = season
            return teamGames

        scheduleFrame = pd.DataFrame()
    
        for id in teamLookup['TEAM_ID']:
            time.sleep(10)
            teamGames = getRegularSeasonSchedule(season, id, seasonType)
            scheduleFrame = pd.concat([scheduleFrame, teamGames], ignore_index=True)

        scheduleFrame['GAME_DATE'] = pd.to_datetime(scheduleFrame['MATCHUP'].map(getGameDate))
        scheduleFrame['HOME_TEAM_NICKNAME'] = scheduleFrame['MATCHUP'].map(getHomeTeam)
        scheduleFrame['HOME_TEAM_ID'] = scheduleFrame['HOME_TEAM_NICKNAME'].map(getTeamIDFromNickname)
        scheduleFrame['AWAY_TEAM_NICKNAME'] = scheduleFrame['MATCHUP'].map(getAwayTeam)
        scheduleFrame['AWAY_TEAM_ID'] = scheduleFrame['AWAY_TEAM_NICKNAME'].map(getTeamIDFromNickname)
        scheduleFrame = scheduleFrame.drop_duplicates()  # There's a row for both teams, only need 1
        scheduleFrame = scheduleFrame.reset_index(drop=True)

        return scheduleFrame
    
    def getSingleGameMetrics(gameID,homeTeamID,awayTeamID,awayTeamNickname,seasonYear,gameDate):

        @retry
        def getGameStats(teamID,gameID,seasonYear):
            #season = str(seasonYear) + "-" + str(seasonYear+1)[-2:]
            gameStats = cumestatsteam.CumeStatsTeam(game_ids=gameID,league_id ="00",
                                                season=seasonYear,season_type_all_star="Regular Season",
                                                team_id = teamID).get_normalized_json()

            gameStats = pd.DataFrame(json.loads(gameStats)['TotalTeamStats'])

            return gameStats

        data = getGameStats(homeTeamID,gameID,seasonYear)
        data.at[1,'NICKNAME'] = awayTeamNickname.strip()
        data.at[1,'TEAM_ID'] = awayTeamID
        data.at[1,'OFFENSIVE_EFFICIENCY'] = (data.at[1,'FG'] + data.at[1,'AST'])/(data.at[1,'FGA'] - data.at[1,'OFF_REB'] + data.at[1,'AST'] + data.at[1,'TOTAL_TURNOVERS'])
        data.at[1,'SCORING_MARGIN'] = data.at[1,'PTS'] - data.at[0,'PTS']

        data.at[0,'OFFENSIVE_EFFICIENCY'] = (data.at[0,'FG'] + data.at[0,'AST'])/(data.at[0,'FGA'] - data.at[0,'OFF_REB'] + data.at[0,'AST'] + data.at[0,'TOTAL_TURNOVERS'])
        data.at[0,'SCORING_MARGIN'] = data.at[0,'PTS'] - data.at[1,'PTS']

        data['SEASON'] = seasonYear
        data['GAME_DATE'] = gameDate
        data['GAME_ID'] = gameID

        return data
    
    def getGameLogs(gameLogs,scheduleFrame):
        
        # Functions to prepare additional columns after gameLogs table loads
        def getHomeAwayFlag(gameDF):
            gameDF['HOME_FLAG'] = gameDF['CITY'].apply(lambda x: 0 if x == 'OPPONENTS' else 1)
            gameDF['AWAY_FLAG'] = gameDF['CITY'].apply(lambda x: 1 if x == 'OPPONENTS' else 0)

            # Rellenar las columnas W_HOME, L_HOME, W_ROAD, L_ROAD
            gameDF['W_HOME'] = gameDF.apply(lambda row: row['W'] if row['HOME_FLAG'] == 1 else 0, axis=1)
            gameDF['L_HOME'] = gameDF.apply(lambda row: row['L'] if row['HOME_FLAG'] == 1 else 0, axis=1)
            gameDF['W_ROAD'] = gameDF.apply(lambda row: row['W'] if row['AWAY_FLAG'] == 1 else 0, axis=1)
            gameDF['L_ROAD'] = gameDF.apply(lambda row: row['L'] if row['AWAY_FLAG'] == 1 else 0, axis=1)
            #return gameDF 

        def getTotalWinPctg(gameDF):
            gameDF['TOTAL_GAMES_PLAYED'] = gameDF.groupby(['TEAM_ID','SEASON'])['GAME_DATE'].rank(ascending=True)
            gameDF['TOTAL_WINS'] = gameDF.sort_values(by='GAME_DATE').groupby(['TEAM_ID','SEASON'])['W'].cumsum()
            gameDF['TOTAL_WIN_PCTG'] = gameDF['TOTAL_WINS']/gameDF['TOTAL_GAMES_PLAYED']
            return gameDF.drop(['TOTAL_GAMES_PLAYED','TOTAL_WINS'],axis=1)

        def getHomeWinPctg(gameDF):
            gameDF['HOME_GAMES_PLAYED'] = gameDF.sort_values(by='GAME_DATE').groupby(['TEAM_ID','SEASON'])['HOME_FLAG'].cumsum()
            gameDF['HOME_WINS'] = gameDF.sort_values(by='GAME_DATE').groupby(['TEAM_ID','SEASON'])['W_HOME'].cumsum()
            gameDF['HOME_WIN_PCTG'] = gameDF['HOME_WINS']/gameDF['HOME_GAMES_PLAYED']
            return gameDF.drop(['HOME_GAMES_PLAYED','HOME_WINS'],axis=1)

        def getAwayWinPctg(gameDF):
            gameDF['AWAY_GAMES_PLAYED'] = gameDF.sort_values(by='GAME_DATE').groupby(['TEAM_ID','SEASON'])['AWAY_FLAG'].cumsum()
            gameDF['AWAY_WINS'] = gameDF.sort_values(by='GAME_DATE').groupby(['TEAM_ID','SEASON'])['W_ROAD'].cumsum()
            gameDF['AWAY_WIN_PCTG'] = gameDF['AWAY_WINS']/gameDF['AWAY_GAMES_PLAYED']
            return gameDF.drop(['AWAY_GAMES_PLAYED','AWAY_WINS'],axis=1)

        def getRollingOE(gameDF):
            gameDF['ROLLING_OE'] = gameDF.sort_values(by='GAME_DATE').groupby(['TEAM_ID','SEASON'])['OFFENSIVE_EFFICIENCY'].transform(lambda x: x.rolling(3, 1).mean())

        def getRollingScoringMargin(gameDF):
            gameDF['ROLLING_SCORING_MARGIN'] = gameDF.sort_values(by='GAME_DATE').groupby(['TEAM_ID','SEASON'])['SCORING_MARGIN'].transform(lambda x: x.rolling(3, 1).mean())

        def getRestDays(gameDF):
            gameDF['GAME_DATE'] = pd.to_datetime(gameDF['GAME_DATE'])
            gameDF['LAST_GAME_DATE'] = gameDF.sort_values(by='GAME_DATE').groupby(['TEAM_ID','SEASON'])['GAME_DATE'].shift(1)
            gameDF['NUM_REST_DAYS'] = (gameDF['GAME_DATE'] - gameDF['LAST_GAME_DATE'])/np.timedelta64(1,'D') 
            return gameDF.drop('LAST_GAME_DATE',axis=1)
        
        def getmetrics(gameLogs):
            getHomeAwayFlag(gameLogs)
            gameLogs = getHomeWinPctg(gameLogs)
            gameLogs = getAwayWinPctg(gameLogs)
            gameLogs = getTotalWinPctg(gameLogs)
            getRollingScoringMargin(gameLogs)
            getRollingOE(gameLogs)
            gameLogs = getRestDays(gameLogs)

            return gameLogs
        
        start = time.perf_counter_ns()

        i = 0 #Can use a previously completed gameLog dataset

        while i<len(scheduleFrame):


            time.sleep(10)
            gameLogs = pd.concat([gameLogs, getSingleGameMetrics(scheduleFrame.at[i,'GAME_ID'],scheduleFrame.at[i,'H_TEAM_ID'],
                            scheduleFrame.at[i,'A_TEAM_ID'],scheduleFrame.at[i,'A_TEAM_NICKNAME'],
                            scheduleFrame.at[i,'SEASON'],scheduleFrame.at[i,'GAME_DATE'])])
            
            gameLogs = gameLogs.reset_index(drop=True)

            end = time.perf_counter_ns()

            if i%100 == 0:
                mins = ((end-start)/1e9)/60
                print(f"{i} games procesed in: {int(mins)} minutes")

            i += 1

        return getmetrics(gameLogs).reset_index(drop=True).drop(columns=['GAME_DATE', 'SEASON'])
    
    conexion = sqlite3.connect(DATABASE_NAME)

    cursor = conexion.cursor()

    # Crear la tabla TEAMS
    cursor.execute("""
    create table If Not Exists TEAMS (
    TEAM_ID int primary key,
    TEAM_NICKNAME text not null,
    ABBREVIATION text not null,
    CITY text not null,
    STATE text not null,
    FULL_NAME text not null
    )
    """)

    # Crear la tabla GAMES
    cursor.execute("""
    create table If Not Exists GAMES (
    GAME_ID text primary key,
    GAME_DATE date not null,
    H_TEAM_NICKNAME text not null,
    A_TEAM_NICKNAME text not null,
    H_TEAM_ID int references teams (TEAM_ID),
    A_TEAM_ID int references teams (TEAM_ID),
    SEASON int not null
    )
    """)

    # Crear la tabla TEAM_INFO_COMMON
    cursor.execute("""
    create table If Not Exists GAME_STATS (
    GAME_ID text references games (GAME_ID),
    HOME_FLAG boolean not null,
    AWAY_FLAG boolean not null,
    CITY text,
    NICKNAME text,
    TEAM_ID int,
    W int,
    L int,
    W_HOME int,
    L_HOME int,
    W_ROAD int,
    L_ROAD int,
    TEAM_TURNOVERS int,
    TEAM_REBOUNDS int,
    GP int,
    GS int,
    ACTUAL_MINUTES int,
    ACTUAL_SECONDS int,
    FG int,
    FGA int,
    FG_PCT double precision,
    FG3 int,
    FG3A int,
    FG3_PCT double precision,
    FT int,
    FTA int,
    FT_PCT double precision,
    OFF_REB int,
    DEF_REB int,
    TOT_REB int,
    AST int,
    PF int,
    STL int,
    TOTAL_TURNOVERS int,
    BLK int,
    PTS int,
    AVG_REB double precision,
    AVG_PTS double precision,
    DQ int,
    OFFENSIVE_EFFICIENCY double precision,
    SCORING_MARGIN double precision,
    HOME_WIN_PCTG double precision,
    AWAY_WIN_PCTG double precision,
    TOTAL_WIN_PCTG double precision,
    ROLLING_SCORING_MARGIN int,
    ROLLING_OE double precision,
    NUM_REST_DAYS int,
    primary key (GAME_ID, HOME_FLAG)
    )
    """)

    teamLookup = pd.DataFrame(teams.get_teams())
    teamLookup = teamLookup.rename(columns={"id": "TEAM_ID", "nickname": "TEAM_NICKNAME", "full_name": "FULL_NAME", "abbreviation": "ABBREVIATION", "city": "CITY", "state": "STATE"}).drop(columns='year_founded')

    teamLookup.to_sql("TEAMS", conexion, if_exists="replace", index=False)

    TOTAL_GAMELOGS = []
    for season in seasons:
        print(f"Generation season schedule and game logs for season {season}")
        start = time.perf_counter_ns()
        # Get all the avaliable games for the season
        scheduleFrame = getSeasonScheduleFrame(season, seasonType, teamLookup)

        # Get the games already in the database
        cursor.execute("SELECT GAME_ID FROM GAMES")
        existing_game_ids = set(row[0] for row in cursor.fetchall())

        # Get and save just the new games
        new_games = scheduleFrame[~scheduleFrame['GAME_ID'].isin(existing_game_ids)].rename(columns={"HOME_TEAM_NICKNAME": "H_TEAM_NICKNAME", "HOME_TEAM_ID": "H_TEAM_ID",
                                                   "AWAY_TEAM_NICKNAME": "A_TEAM_NICKNAME", "AWAY_TEAM_ID": "A_TEAM_ID"}).drop(columns='MATCHUP')
        new_games.to_sql("GAMES", conexion, if_exists="append", index=False)
        end = time.perf_counter_ns()

        secs = (end-start)/1e9
        mins = secs/60
        print(f"scheduleFrame takes: {int(mins)}:{int(secs)%60}")

        start = time.perf_counter_ns()
        gameLogs = pd.DataFrame()

        season_str = f'{season}-{season+1-2000}'
        query = f"""
        SELECT * 
        FROM GAMES 
        WHERE SEASON = '{season_str}' AND GAME_ID NOT IN (SELECT GAME_ID FROM GAME_STATS)
        """
        games_without_stats = pd.read_sql_query(query, conexion)
        print(len(games_without_stats))
        if len(games_without_stats) > 0:
            gameLogs = getGameLogs(gameLogs,games_without_stats)
        #gameLogs.to_csv('gameLogs.csv')
        try:
            gameLogs.to_sql("GAME_STATS", conexion, if_exists="append", index=False)
        except:
            print("Error saving gameLogs in year", season)

        end = time.perf_counter_ns()

        secs = (end-start)/1e9
        mins = secs/60
        print(f"GameLogs takes: {int(mins)}:{int(secs)%60}")
        TOTAL_GAMELOGS.append(gameLogs)

    return TOTAL_GAMELOGS