# CELDA 1 ▸ pip install yfinance mibian plotly tqdm
import sys, subprocess
subprocess.run([sys.executable, "-m", "pip", "install", "yfinance", "mibian", "plotly", "tqdm"], check=True)

# CELDA 2 ▸ Imports, parámetros
import time
import numpy as np
import pandas as pd
import yfinance as yf
import plotly.graph_objects as go
import mibian
from tqdm import tqdm

RANGE_PTS = 300
LOOPS = 12
INTERVAL_SEC = 300
R = 0.05

SNAPSHOTS = pd.DataFrame(columns=["timestamp", "strike", "notionalGamma"])
SPOT_SERIES = pd.DataFrame(columns=["timestamp", "spot"])

# CELDA 3 ▸ get_spx_spot()
def get_spx_spot(retries=3, delay=5):
    """Obtiene el precio spot del SPX con reintentos."""
    for _ in range(retries):
        try:
            spot = yf.Ticker("^SPX").history(period="1d", interval="1m")["Close"].iloc[-1]
            return float(spot)
        except Exception:
            time.sleep(delay)
    raise ConnectionError("Fallo al obtener el spot del SPX")

# CELDA 4 ▸ get_option_chain_spx(expiry)
def get_option_chain_spx(expiry, retries=3, delay=5):
    """Descarga la cadena de opciones calls y puts para un vencimiento."""
    for _ in range(retries):
        try:
            ticker = yf.Ticker("^SPX")
            chain = ticker.option_chain(expiry)
            calls = chain.calls.copy()
            calls["optionType"] = "call"
            puts = chain.puts.copy()
            puts["optionType"] = "put"
            df = pd.concat([calls, puts], ignore_index=True)
            now = pd.Timestamp.now(tz="US/Eastern")
            expiry_dt = pd.Timestamp(expiry + " 16:00", tz="US/Eastern")
            days = max((expiry_dt - now).total_seconds() / 86400, 0.0001)
            df["t"] = days / 365
            return df
        except Exception:
            time.sleep(delay)
    raise ConnectionError("Fallo al obtener la cadena de opciones")

# CELDA 5 ▸ bs_gamma() + calc_gex()
def bs_gamma(row, spot, r, t):
    """Calcula Gamma usando Black-Scholes con mibian."""
    bs = mibian.BS([spot, row["strike"], r * 100, t * 365], volatility=row["impliedVolatility"] * 100)
    return bs.gamma

def calc_gex(chain_df, spot):
    """Calcula la exposición gamma neta por strike."""
    df = chain_df.copy()
    df = df[(df["openInterest"] > 0) & (~df["impliedVolatility"].isna())]
    df = df[df["strike"].between(spot - RANGE_PTS, spot + RANGE_PTS)]
    t = df["t"].iloc[0] if not df.empty else 1 / 365
    df["gamma"] = df.apply(lambda row: bs_gamma(row, spot, R, t), axis=1)
    df["notionalGamma"] = df["gamma"] * df["openInterest"] * 100 * spot ** 2
    df.loc[df["optionType"] == "put", "notionalGamma"] *= -1
    res = df.groupby("strike", as_index=False)["notionalGamma"].sum()
    return res
# CELDA 6 ▸ plot_interval_snapshot()
def plot_interval_snapshot(gex_df, spot, timestamp):
    """Actualiza el Interval Map con un nuevo snapshot."""
    global SNAPSHOTS, SPOT_SERIES
    temp = gex_df.copy()
    temp["timestamp"] = timestamp
    SNAPSHOTS = pd.concat([SNAPSHOTS, temp], ignore_index=True)
    SPOT_SERIES = pd.concat([SPOT_SERIES, pd.DataFrame({"timestamp": [timestamp], "spot": [spot]})], ignore_index=True)
    max_abs = SNAPSHOTS["notionalGamma"].abs().max()
    scale = max_abs / 40 if max_abs > 0 else 1
    colors = SNAPSHOTS["notionalGamma"].apply(lambda x: "green" if x > 0 else "red")
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=SNAPSHOTS["timestamp"], y=SNAPSHOTS["strike"],
                             mode="markers", marker=dict(size=SNAPSHOTS["notionalGamma"].abs() / scale,
                             color=colors), name="GEX"))
    fig.add_trace(go.Scatter(x=SPOT_SERIES["timestamp"], y=SPOT_SERIES["spot"],
                             mode="lines", line=dict(color="blue"), name="Spot"))
    fig.update_layout(xaxis_title="timestamp", yaxis_title="strike")
    fig.show()

# CELDA 7 ▸ live_interval_map()
def live_interval_map(loops, interval):
    """Ejecuta el mapa de intervalos en vivo."""
    for _ in tqdm(range(loops)):
        ts = pd.Timestamp.now(tz="US/Eastern")
        spot = get_spx_spot()
        chain_df = get_option_chain_spx(EXPIRY)
        gex_df = calc_gex(chain_df, spot)
        plot_interval_snapshot(gex_df, spot, ts)
        if _ < loops - 1:
            time.sleep(interval)

# CELDA 8 ▸ Bloque main
if __name__ == "__main__":
    ticker = yf.Ticker("^SPX")
    expiry_list = ticker.options
    today = pd.Timestamp.now(tz="US/Eastern").strftime("%Y-%m-%d")
    EXPIRY = today if today in expiry_list else expiry_list[0]
    live_interval_map(LOOPS, INTERVAL_SEC)
    SNAPSHOTS.to_csv("gex_intradia.csv", index=False)
