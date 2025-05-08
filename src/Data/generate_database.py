import sqlite3
import pandas as pd
import time
import requests
from nba_api.stats.static import teams
from nba_api.stats.endpoints import cumestatsteamgames, cumestatsteam
import numpy as np
import json
import difflib
from bs4 import BeautifulSoup
import time
import requests
import pandas as pd
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from datetime import timedelta, datetime
from src.Data.maper import EQUIPOS_ODDS



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
    
    def getSingleGameMetrics(gameID,homeTeamID,awayTeamID,awayTeamNickname,seasonYear,gameDate,seasonType:str):

        @retry
        def getGameStats(teamID,gameID,seasonYear,seasonType):
            #season = str(seasonYear) + "-" + str(seasonYear+1)[-2:]
            gameStats = cumestatsteam.CumeStatsTeam(game_ids=gameID,league_id ="00",
                                                season=seasonYear,season_type_all_star=seasonType,
                                                team_id = teamID).get_normalized_json()

            gameStats = pd.DataFrame(json.loads(gameStats)['TotalTeamStats'])

            return gameStats

        data = getGameStats(homeTeamID,gameID,seasonYear,seasonType)
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
    
    def getGameLogs(gameLogs,scheduleFrame,seasonType:str):
        
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

        i = 0

        while i<len(scheduleFrame):


            time.sleep(10)
            gameLogs = pd.concat([gameLogs, getSingleGameMetrics(scheduleFrame.at[i,'GAME_ID'],scheduleFrame.at[i,'H_TEAM_ID'],
                            scheduleFrame.at[i,'A_TEAM_ID'],scheduleFrame.at[i,'A_TEAM_NICKNAME'],
                            scheduleFrame.at[i,'SEASON'],scheduleFrame.at[i,'GAME_DATE'],seasonType)])
            
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
            gameLogs = getGameLogs(gameLogs,games_without_stats,seasonType)
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

def generate_ods_table(url: str, pages: int, dataset_ods: pd.DataFrame = None):
    def scroll_to_bottom(driver, pause_time=2):
        """Desplazarse poco a poco hasta el final de la página."""
        last_height = driver.execute_script("return document.body.scrollHeight*0.9")
        
        while True:
            # Desplazarse hacia abajo
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight*0.9);")
            
            # Esperar a que se cargue el contenido
            time.sleep(pause_time)
            
            # Calcular la nueva altura de la página y comparar con la última altura
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

    def get_urls(url, pages):
        partidos = []
        home_team = []
        away_team = []
        fechas = []
        i = 0

        
        print(url)
        temp_urls = [url+f'/#/page/{j}/' for j in range(1,pages+1)]

        options = Options()
        #options.add_experimental_option("detach", True)
        options.add_argument("--disable-search-engine-choice-screen")

        j = 0
        while j <= pages-1:
            print(temp_urls[j])
            driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()),options=options)
            driver.get(temp_urls[j])

            driver.maximize_window()
            time.sleep(5)
            scroll_to_bottom(driver, pause_time=3)
            time.sleep(5)
            html = driver.page_source
            
            soup = BeautifulSoup(html, 'html.parser')
            
            divs = soup.find_all('div', class_='eventRow flex w-full flex-col text-xs')

            for div in divs:
                
                fecha_div = div.find('div', class_='text-black-main font-main w-full truncate text-xs font-normal leading-5')
                
                if fecha_div is not None:
                    fecha_div = fecha_div.text
                    fecha = fecha_div.split('-')[0].rstrip()
                    if 'Today' in fecha:
                        fecha = datetime.now().strftime("%d %b %Y")
                    elif 'Yesterday' in fecha:
                        fecha = (datetime.now() - timedelta(days=1)).strftime("%d %b %Y")
                divs2 = div.find_all('a', class_='next-m:flex next-m:!mt-0 ml-2 mt-2 min-h-[32px] w-full hover:cursor-pointer')

                for div2 in divs2:
                    
                    partidos.append('https://www.oddsportal.com' + div2['href'])
                    equipos = div2.find_all('p', class_='participant-name truncate')
                    home_team.append(equipos[0].text)
                    away_team.append(equipos[1].text)
                    fechas.append(fecha)
            j += 1        
            driver.quit()    

        temp_data_df = pd.DataFrame()       
        temp_data_df['URL'] = partidos
        temp_data_df['H_TEAM_NICKNAME'] = home_team
        temp_data_df['A_TEAM_NICKNAME'] = away_team
        temp_data_df['GAME_DATE'] = fechas

        return temp_data_df
    
    def get_game_id(dataset_urls: pd.DataFrame):
        dataset_urls['H_TEAM_ID'] = dataset_urls['H_TEAM_NICKNAME'].map(EQUIPOS_ODDS).replace({float('nan'): None})
        dataset_urls['A_TEAM_ID'] = dataset_urls['A_TEAM_NICKNAME'].map(EQUIPOS_ODDS).replace({float('nan'): None})
        dataset_urls['GAME_DATE'] = pd.to_datetime(dataset_urls['GAME_DATE'])

        conexion = sqlite3.connect("NBA_DATA.db")

        # Crear una lista para almacenar los GAME_IDs
        game_ids = []
        # Realizar una consulta SQL para cada fila en dataset_urls
        for index, row in dataset_urls.iterrows():
            if row['H_TEAM_ID'] is None or row['A_TEAM_ID'] is None:
                game_ids.append(None)
                continue
            home_team_id = row['H_TEAM_ID']
            away_team_id = row['A_TEAM_ID']
            game_date = row['GAME_DATE']
            start_date = (game_date - pd.Timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
            game_date = game_date.strftime('%Y-%m-%d %H:%M:%S')


            query = """
            SELECT GAME_ID, GAME_DATE
            FROM GAMES 
            WHERE H_TEAM_ID = ? 
            AND A_TEAM_ID = ? 
            AND GAME_DATE BETWEEN ? AND ?
            """
            
            result = pd.read_sql_query(query, conexion, params=[home_team_id, away_team_id, start_date, game_date])
            
            if not result.empty:
                result.sort_values(by='GAME_DATE', ascending=False, inplace=True)
                if len(result) > 1:
                    game_ids.append(result.loc[result["GAME_DATE"] == game_date]['GAME_ID'])
                else:
                    game_ids.append(result.iloc[0]['GAME_ID'])
            else:
                game_ids.append(None)
        dataset_urls['GAME_ID'] = game_ids

        return dataset_urls[['URL', 'H_TEAM_NICKNAME', 'A_TEAM_NICKNAME', 'GAME_DATE', 'GAME_ID']]

    def get_season(url: str):
        conexion = sqlite3.connect("NBA_DATA.db")
        season = url.split('/')[5].split('-')[1:]
        season = season[0] + '-'+season[1][2:]

        query = """
            SELECT GAME_ID
            FROM GAMES
            WHERE SEASON = ?
            """
        
        return pd.read_sql_query(query, conexion, params=[season])

    def get_odds(dataset_urls: pd.DataFrame):

        keys = ['URL', 'GAME_ID', 'H_TEAM_NICKNAME', 'A_TEAM_NICKNAME', 'GAME_DATE', 'Average_1','Average_X','Average_2','Highest_1','Highest_X','Highest_2', 'Average_H', 'Average_A', 'Highest_H', 'Highest_A', 'Average_1X','Average_12','Average_X2','Highest_1X','Highest_12','Highest_X2']
        extensiones = ['#1X2;3', '#home-away;1', '#double;3']
        columnas = [['Average_1','Average_X','Average_2','Highest_1','Highest_X','Highest_2'],['Average_H', 'Average_A', 'Highest_H', 'Highest_A'],
                ['Average_1X','Average_12','Average_X2','Highest_1X','Highest_12','Highest_X2']]

        dataset_urls = get_game_id(dataset_urls)
        ids = list(get_season(dataset_urls.iloc[-1]['URL']))
        filtered_dataset_urls = dataset_urls[~dataset_urls['GAME_ID'].isin(ids)]
        filtered_dataset_urls = dataset_urls[(~dataset_urls['GAME_ID'].isin(ids)) & (dataset_urls['GAME_ID'].notna())]
        
        options = Options()
        #options.add_experimental_option("detach", True)
        options.add_argument("--disable-search-engine-choice-screen")
        options.add_argument("--headless")
        
        dataset_ods = pd.DataFrame(columns=keys)

        for index, row in filtered_dataset_urls.iterrows():

            print(index)
            diccionario_estadisticas = {key: None for key in keys}
            diccionario_estadisticas['URL'] = row['URL']
            diccionario_estadisticas['H_TEAM_NICKNAME'] = row['H_TEAM_NICKNAME']
            diccionario_estadisticas['A_TEAM_NICKNAME'] = row['A_TEAM_NICKNAME']
            diccionario_estadisticas['GAME_DATE'] = row['GAME_DATE']
            diccionario_estadisticas['GAME_ID'] = row['GAME_ID']
            i = -1
            for extension in extensiones:
                
                try:
                    i += 1
                    url = row['URL'] + extension

                    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()),options=options)

                    driver.get(url)
                    driver.maximize_window()
                    time.sleep(3)
                    last_height = driver.execute_script("return document.body.scrollHeight")
                    while True:
                        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                        time.sleep(3)  # Espera un poco para que se cargue el contenido dinámico
                        new_height = driver.execute_script("return document.body.scrollHeight")
                        if new_height == last_height:
                            break
                        last_height = new_height
                        
                    html = driver.page_source

                    soup = BeautifulSoup(html, 'html.parser')

                    divs = soup.find_all('div', class_='border-black-borders flex h-9 border-b border-l border-r text-xs')

                    for div in divs:
                        ps = div.find_all('p', class_='height-content')
                        calculator_list = [[] for _ in range(len(ps)-1)]
                        for i in range(len(ps)-1):
                            calculator_list[i].append(float(ps[i+1].text))
                    from statistics import mean
                    medias = [mean(lista) for lista in calculator_list]

                    # Aplicar la transformación según la condición
                    for j in range(len(medias)):
                        diccionario_estadisticas[columnas[i][j]] = medias[j]
                except Exception as e:
                    # Si ocurre un error, se captura la excepción y se imprime la variable
                    print(f"Ocurrió un error: {e} en el partido {index}")

            data_df = pd.DataFrame([diccionario_estadisticas])
            #data_df[['GAME_ID', 'Average_1', 'Average_X', 'Average_2', 'Highest_1', 'Highest_X', 'Highest_2', 'Average_H', 'Average_A', 'Highest_H', 'Highest_A', 'Average_1X', 'Average_12', 'Average_X2', 'Highest_1X', 'Highest_12', 'Highest_X2']].to_sql('GAME_ODS', conexion, if_exists='append', index=False)
            # Aqui debo incluir el formateo para que todo funcione correctamente
            dataset_ods = pd.concat([dataset_ods, data_df], ignore_index=True)
        return dataset_ods
    
    def process_odds(dataset_ods: pd.DataFrame):
        conexion = sqlite3.connect("NBA_DATA.db")

        query = """
        SELECT GAME_ID
        FROM GAME_ODS
        """
        result = pd.read_sql_query(query, conexion)
        dataset_ods = dataset_ods[~dataset_ods['GAME_ID'].isin(list(result['GAME_ID']))]

        # Cerrar la conexión
        conexion.close()
        return dataset_ods[['GAME_ID', 'Average_1', 'Average_X', 'Average_2', 'Highest_1', 'Highest_X', 'Highest_2', 'Average_H', 'Average_A', 'Highest_H', 'Highest_A', 'Average_1X', 'Average_12', 'Average_X2', 'Highest_1X', 'Highest_12', 'Highest_X2']]
    
    conexion = sqlite3.connect("NBA_DATA.db") 

    cursor = conexion.cursor()
    cursor.execute("""
    create table If Not Exists GAME_ODS (
        GAME_ID text,
        Average_1 double precision,
        Average_X double precision, 
        Average_2 double precision, 
        Highest_1 double precision, 
        Highest_X double precision, 
        Highest_2 double precision,
        Average_H double precision, 
        Average_A double precision, 
        Highest_H double precision, 
        Highest_A double precision, 
        Average_1X double precision,
        Average_12 double precision, 
        Average_X2 double precision, 
        Highest_1X double precision, 
        Highest_12 double precision, 
        Highest_X2 double precision,
        primary key (GAME_ID)
        foreign key (GAME_ID) REFERENCES GAMES(GAME_ID)
    )
    """)
    print("Generando tabla de urls")
    dataset_urls = get_urls(url, pages)
    print("Generando tabla de odds")
    dataset_ods = get_odds(dataset_urls)
    dataset_guardar = process_odds(dataset_ods)

    dataset_guardar.to_sql('GAME_ODS', conexion, if_exists='append', index=False)
    print("Tabla de odds guardada")
    return dataset_ods

def database_inicialization():

    seasons = [2017, 2018, 2019, 2020, 2021, 2022, 2023]
    seasonType = ['Regular Season', 'Pre Season', 'Playoffs', 'All Star']
    for type in seasonType:
        if type == 'All Star' or type == 'Pre Season':
            continue
        generate_database(seasons, type)

    print("Generando tabla de odds")
    urls = ['https://www.oddsportal.com/basketball/usa/nba-2023-2024/results/',
            'https://www.oddsportal.com/basketball/usa/nba-2022-2023/results/',
            'https://www.oddsportal.com/basketball/usa/nba-2021-2022/results/',
            'https://www.oddsportal.com/basketball/usa/nba-2020-2021/results/',
            'https://www.oddsportal.com/basketball/usa/nba-2019-2020/results/',
            'https://www.oddsportal.com/basketball/usa/nba-2018-2019/results/',
            'https://www.oddsportal.com/basketball/usa/nba-2017-2018/results/',]
    pages = [28,
             28,
             28,
             28,
             28,
             28,
             28,]

    columnas = ['URL', 'H_TEAM_NICKNAME', 'A_TEAM_NICKNAME', 'id', 'GAME_DATE', 'Averge_1','Average_X','Average_2','Highest_1','Highest_X','Highest_2', 'Average_H', 'Average_A', 'Highest_H', 'Highest_A', 'Average_1X','Average_12','Average_X2','Highest_1X','Highest_12','Highest_X2']
    dataset_ods = pd.DataFrame(columns=columnas)
    for i in range(len(urls)):
        print(f"Generando tabla de odds para la temporada {urls[i].split('/')[-2]}")
        dataset_ods = generate_ods_table(urls[i], pages[i],dataset_ods)

def database_actualization():
    seasons = [2024]
    seasonType = ['Regular Season', 'Pre Season', 'Playoffs', 'All Star']
    for type in seasonType:
        if type == 'All Star' or type == 'Pre Season':
            continue
        generate_database(seasons, type)

    print("Generando tabla de odds")
    urls = ['https://www.oddsportal.com/basketball/usa/nba-2023-2024/results/']
    pages = [3]

    columnas = ['URL', 'H_TEAM_NICKNAME', 'A_TEAM_NICKNAME', 'id', 'GAME_DATE', 'Averge_1','Average_X','Average_2','Highest_1','Highest_X','Highest_2', 'Average_H', 'Average_A', 'Highest_H', 'Highest_A', 'Average_1X','Average_12','Average_X2','Highest_1X','Highest_12','Highest_X2']
    dataset_ods = pd.DataFrame(columns=columnas)
    for i in range(len(urls)):
        print(f"Generando tabla de odds para la temporada {urls[i].split('/')[-2]}")
        generate_ods_table(urls[i], pages[i], dataset_ods)
    
