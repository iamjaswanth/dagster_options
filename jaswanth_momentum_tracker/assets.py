# jaswanth_momentum_tracker/assets.py

from dagster import asset
from datetime import datetime
import pandas as pd
import datetime as dt
import numpy as np
import pyotp
from SmartApi import SmartConnect
from logzero import logger
import json
import time

# Constants & Credentials (consider moving to config)
API_KEY = 'fYXsEaUG'
USERNAME = 'V111503'
PWD = '1995'
TOKEN = "FPNYSK4M3YFZM6GF6VCIVYDFNI"
TARGET_ALLOCATION = 15000

# Helper functions that can be used by multiple assets
def heikinashi(df: pd.DataFrame) -> pd.DataFrame:
    df_HA = df.copy()
    df_HA['close'] = (df_HA['open'] + df_HA['high'] + df_HA['low'] + df_HA['close']) / 4
    df_HA.iloc[0, df_HA.columns.get_loc('open')] = (df_HA.iloc[0]['open'] + df_HA.iloc[0]['close']) / 2
    for i in range(1, len(df_HA)):
        df_HA.iloc[i, df_HA.columns.get_loc('open')] = (df_HA.iloc[i - 1]['open'] + df_HA.iloc[i - 1]['close']) / 2
    df_HA['high'] = df_HA[['open', 'close', 'high']].max(axis=1)
    df_HA['low'] = df_HA[['open', 'close', 'low']].min(axis=1)
    return df_HA

def get_smart_api_connection():
    try:
        totp = pyotp.TOTP(TOKEN).now()
        obj = SmartConnect(api_key=API_KEY)
        data = obj.generateSession(USERNAME, PWD, totp)
        feed_token = obj.getfeedToken()
        logger.info("Session generated successfully.")
        return obj
    except Exception as e:
        logger.error(f"Error generating session: {e}")
        raise e

@asset
def API_CONNECTION():
    # --- Authentication ---
    try:
        obj = get_smart_api_connection()
    except Exception as e:
        raise e

    # --- Fetch Holdings or Nifty Data (Stub Example) ---
    try:
        holdings = obj.holding()
        logger.info("Holdings fetched successfully.")
        print(holdings)  # You can log this if needed
    except Exception as e:
        logger.error(f"Error fetching holdings: {e}")
        raise e

    return {"status": "success", "timestamp": datetime.now().isoformat()}

@asset
def instrument_list(API_CONNECTION):
    import urllib.request
    instrument_url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
    try:
        response = urllib.request.urlopen(instrument_url)
        instruments = json.loads(response.read())
        logger.info("Instrument list fetched successfully.")
        return instruments
    except Exception as e:
        logger.error(f"Failed to fetch instrument list: {e}")
        raise e

def option_contracts(instrument_list, ticker, option_type="CE"):
    return pd.DataFrame([
        instrument for instrument in instrument_list
        if instrument["name"] == ticker and instrument["instrumenttype"] in ["OPTSTK", "OPTIDX"]
        and instrument["symbol"][-2:] == option_type
    ])

def option_chain_bear_spread(instrument_list, ticker, underlying_price, duration=0):
    df = option_contracts(instrument_list, ticker, option_type="CE")
    df["time_to_expiry"] = (pd.to_datetime(df["expiry"]) + dt.timedelta(hours=16) - dt.datetime.now()).dt.total_seconds() / 86400
    df["strike"] = pd.to_numeric(df["strike"]) / 100
    min_day = np.sort(df["time_to_expiry"].unique())[duration]
    temp = df[df["time_to_expiry"] == min_day].sort_values(by="strike").reset_index(drop=True)
    atm_idx = abs(temp["strike"] - underlying_price).argmin()
    return temp.iloc[[atm_idx + 8, atm_idx + 10]]

def option_chain_bull_spread(instrument_list, ticker, underlying_price, duration=0):
    df = option_contracts(instrument_list, ticker, option_type="PE")
    df["time_to_expiry"] = (pd.to_datetime(df["expiry"]) + dt.timedelta(hours=16) - dt.datetime.now()).dt.total_seconds() / 86400
    df["strike"] = pd.to_numeric(df["strike"]) / 100
    min_day = np.sort(df["time_to_expiry"].unique())[duration]
    temp = df[df["time_to_expiry"] == min_day].sort_values(by="strike").reset_index(drop=True)
    atm_idx = abs(temp["strike"] - underlying_price).argmin()
    return temp.iloc[[atm_idx - 6, atm_idx - 8]]

@asset
def option_spreads(instrument_list):
    obj = get_smart_api_connection()

    # Fetch underlying price for NIFTY
    try:
        underlying_price = obj.ltpData("NSE", "NIFTY-EQ", "26000")["data"]["ltp"]
    except Exception as e:
        logger.error("Error fetching LTP for NIFTY")
        raise e

    # Build spreads
    try:
        bear = option_chain_bear_spread(instrument_list, "NIFTY", underlying_price)
        bull = option_chain_bull_spread(instrument_list, "NIFTY", underlying_price)
        logger.info("Bear & Bull spreads calculated.")
        return {
            "underlying_price": underlying_price,
            "bear_spread": bear.to_dict(orient="records"),
            "bull_spread": bull.to_dict(orient="records")
        }
    except Exception as e:
        logger.error(f"Error calculating spreads: {e}")
        raise e

@asset
def check_signal(option_spreads, instrument_list):
    obj = get_smart_api_connection()
    
    # Set date range (you may want to parameterize these)
    end_date = datetime.now().strftime("%Y-%m-%d %H:%M")
    start_date = (datetime.now() - dt.timedelta(days=7)).strftime("%Y-%m-%d %H:%M")
    
    # Get historical data
    try:
        NIFTY_data = obj.getCandleData({
            "exchange": "NSE",
            "symboltoken": "99926000",
            "interval": "THIRTY_MINUTE",
            "fromdate": start_date,
            "todate": end_date
        })
        df = pd.DataFrame(NIFTY_data["data"], columns=["date", "open", "high", "low", "close", "volume"])
        df.set_index("date", inplace=True)
    except Exception as e:
        logger.error(f"Error fetching historical data: {e}")
        raise e

    # Calculate Heikin-Ashi
    zap_ashi = heikinashi(df)
    zap_ashi['Signal'] = None
    zap_ashi.loc[zap_ashi['open'] == zap_ashi['high'], 'Signal'] = 'sell'
    zap_ashi.loc[zap_ashi['open'] == zap_ashi['low'], 'Signal'] = 'buy'

    logger.info(zap_ashi[['open', 'high', 'low', 'close', 'Signal']].dropna())

    last_row = zap_ashi.dropna(subset=['Signal']).iloc[-1]
    signal = last_row['Signal'].lower()
    last_high = last_row['high']
    strike_level = int(round(last_high, -2))

    logger.info(f"📉 Signal: {signal.upper()} | Strike Level: {strike_level}")
    
    return {
        "signal": signal,
        "strike_level": strike_level,
        "timestamp": datetime.now().isoformat()
    }