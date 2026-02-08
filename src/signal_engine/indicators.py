
import pandas as pd
import numpy as np
from typing import List, Dict

class Indicators:
    """
    Technical Indicator calculations.
    """
    
    @staticmethod
    def calculate_ema(prices: List[float], period: int) -> float:
        if len(prices) < period:
            return 0.0
        return pd.Series(prices).ewm(span=period, adjust=False).mean().iloc[-1]

    @staticmethod
    def calculate_rsi(prices: List[float], period: int = 14) -> float:
        if len(prices) < period + 1:
            return 50.0
        
        delta = pd.Series(prices).diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        
        # Use EWM with alpha=1/period for Wilder's Smoothing (RMA)
        avg_gain = gain.ewm(alpha=1/period, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1/period, adjust=False).mean()
        
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs)).iloc[-1]

    @staticmethod
    def calculate_atr(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> float:
        if len(closes) < period + 1:
            return 0.0
            
        high = pd.Series(highs)
        low = pd.Series(lows)
        close = pd.Series(closes)
        
        tr1 = high - low
        tr2 = (high - close.shift()).abs()
        tr3 = (low - close.shift()).abs()
        
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        # Use EWM with alpha=1/period for Wilder's Smoothing (RMA)
        return tr.ewm(alpha=1/period, adjust=False).mean().iloc[-1]

    @staticmethod
    def calculate_vwap(df: pd.DataFrame) -> float:
        """
        Calculates VWAP for the given session dataframe.
        Expected columns: 'price', 'volume'
        """
        if df.empty:
            return 0.0
        
        v = df['volume'].values
        p = df['price'].values
        return (p * v).sum() / v.sum()

    @staticmethod
    def calculate_cvd_delta(trades: List[Dict]) -> float:
        """
        Calculates the delta (Buy Vol - Sell Vol) for a list of trades.
        Trades must have 'q' (qty) and 'm' (is_buyer_maker).
        """
        buy_vol = 0.0
        sell_vol = 0.0
        
        for t in trades:
            qty = float(t['q'])
            is_buyer_maker = t['m']
            if is_buyer_maker:
                sell_vol += qty
            else:
                buy_vol += qty
                
        return buy_vol - sell_vol
