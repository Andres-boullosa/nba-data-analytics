import pandas as pd
import numpy as np

def getGameLogFeatureSet(gameDF):

    def shiftGameLogRecords(gameDF):
        gameDF['LAST_GAME_OE'] = gameDF.sort_values('GAME_DATE').groupby(['TEAM_ID','SEASON'])['OFFENSIVE_EFFICIENCY'].shift(1)
        gameDF['LAST_GAME_HOME_WIN_PCTG'] = gameDF.sort_values('GAME_DATE').groupby(['TEAM_ID','SEASON'])['HOME_WIN_PCTG'].shift(1)
        gameDF['LAST_GAME_AWAY_WIN_PCTG'] = gameDF.sort_values('GAME_DATE').groupby(['TEAM_ID','SEASON'])['AWAY_WIN_PCTG'].shift(1)
        gameDF['LAST_GAME_TOTAL_WIN_PCTG'] = gameDF.sort_values('GAME_DATE').groupby(['TEAM_ID','SEASON'])['TOTAL_WIN_PCTG'].shift(1)
        gameDF['LAST_GAME_ROLLING_SCORING_MARGIN'] = gameDF.sort_values('GAME_DATE').groupby(['TEAM_ID','SEASON'])['ROLLING_SCORING_MARGIN'].shift(1)
        gameDF['LAST_GAME_ROLLING_OE'] = gameDF.sort_values('GAME_DATE').groupby(['TEAM_ID','SEASON'])['ROLLING_OE'].shift(1)
    
    
    def getHomeTeamFrame(gameDF):
        homeTeamFrame = gameDF[gameDF['CITY'] != 'OPPONENTS']
        homeTeamFrame = homeTeamFrame[['LAST_GAME_OE','LAST_GAME_HOME_WIN_PCTG','NUM_REST_DAYS','LAST_GAME_AWAY_WIN_PCTG','LAST_GAME_TOTAL_WIN_PCTG','LAST_GAME_ROLLING_SCORING_MARGIN','LAST_GAME_ROLLING_OE','W','TEAM_ID','GAME_ID','SEASON']]

        colRenameDict = {}
        for col in homeTeamFrame.columns:
            if (col != 'GAME_ID') & (col != 'SEASON') :
                colRenameDict[col] = 'HOME_' + col 

        homeTeamFrame.rename(columns=colRenameDict,inplace=True)

        return homeTeamFrame

    def getAwayTeamFrame(gameDF):
        awayTeamFrame = gameDF[gameDF['CITY'] == 'OPPONENTS']
        awayTeamFrame = awayTeamFrame[['LAST_GAME_OE','LAST_GAME_HOME_WIN_PCTG','NUM_REST_DAYS','LAST_GAME_AWAY_WIN_PCTG','LAST_GAME_TOTAL_WIN_PCTG','LAST_GAME_ROLLING_SCORING_MARGIN','LAST_GAME_ROLLING_OE','TEAM_ID','GAME_ID','SEASON']]

        colRenameDict = {}
        for col in awayTeamFrame.columns:
            if (col != 'GAME_ID') & (col != 'SEASON'):
                colRenameDict[col] = 'AWAY_' + col 

        awayTeamFrame.rename(columns=colRenameDict,inplace=True)

        return awayTeamFrame
    
    shiftGameLogRecords(gameDF)
    awayTeamFrame = getAwayTeamFrame(gameDF)
    homeTeamFrame = getHomeTeamFrame(gameDF)
    
    return pd.merge(homeTeamFrame, awayTeamFrame, how="inner", on=[ "GAME_ID","SEASON"]).drop(['GAME_ID','AWAY_TEAM_ID','HOME_TEAM_ID'],axis=1)

