import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd


def calcular_estadisticas(df):
    total_bets = len(df)
    df['profit'] = np.where(df['result'] == 1, df['odds'] * df['stake'] - df['stake'], -df['stake'])
    wins = df[df['result'] == 1]
    loses = df[df['result'] == 0]
    roi = df['profit'].sum() / df['stake'].sum()
    yield_ = df['profit'].sum() / total_bets
    winrate = len(wins) / total_bets

    return {
        "Total Bets": total_bets,
        "ROI": round(roi, 4),
        "Yield": round(yield_, 2),
        "Winrate": round(winrate * 100, 2)
    }

def return_grafic(df):
    df = df.copy()
    df['profit'] = np.where(df['result'] == 1, df['odds'] * df['stake'] - df['stake'], -df['stake'])
    df['accumulated'] = df['profit'].cumsum()

    # Asegurar tipo datetime
    df['date'] = pd.to_datetime(df['date'])

    # Ordenar por fecha
    df = df.sort_values('date')

    # Graficar
    plt.figure(figsize=(12, 6))
    plt.plot(df['date'], df['accumulated'], marker='o', linestyle='-')

    # Formateo de fechas
    plt.gca().xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.xticks(rotation=45, ha='right')

    plt.title("Cumulative gain over time")
    plt.xlabel("Date")
    plt.ylabel("Cumulative gain")
    plt.grid(True)
    plt.tight_layout()
    plt.show()