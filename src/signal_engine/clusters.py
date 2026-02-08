
import pandas as pd
import numpy as np
from typing import List, Dict, Tuple

class LiquidationClusters:
    """
    Detects Liquidation Clusters from a stream of liquidation events.
    Uses a sliding window to aggregate volume per price bin.
    """
    def __init__(self, bin_size_pct: float = 0.0025, window_minutes: int = 30):
        self.bin_size_pct = bin_size_pct
        self.window_minutes = window_minutes
        self.liquidations: List[Dict] = [] # List of {price, qty, timestamp}

    def update(self, liquidations: List[Dict]):
        """
        Updates the internal state with a fresh list of liquidations.
        Expected format: {'p': price, 'q': qty, 'S': side, 'T': timestamp}
        """
        self.liquidations = []
        for liq in liquidations:
            self.liquidations.append({
                'price': float(liq.get('p', 0)),
                'qty': float(liq.get('q', 0)),
                'timestamp': liq.get('T', 0)
            })

    def _cleanup(self, current_ts: int):
        """Removes events older than window."""
        cutoff = current_ts - (self.window_minutes * 60 * 1000)
        self.liquidations = [e for e in self.liquidations if e['timestamp'] > cutoff]

    def get_clusters(self) -> List[Tuple[float, float]]:
        """
        Returns a list of (price_level, volume) for significant clusters.
        Significant = Top 3 or > Mean + 2*StdDev
        """
        if not self.liquidations:
            return []

        df = pd.DataFrame(self.liquidations)
        
        # Determine current price reference (approximate from liquidations or pass it in)
        # Using mean price of recent liquidations as anchor isn't great, 
        # but for binning we just need absolute price buckets.
        
        # Binning: Round price to nearest bin_size
        # But bin size is percentage... 0.25%.
        # So we really need log prices or just fixed width if price doesn't move much.
        # User said "Bin liquidation prices (0.25% bins)".
        # This implies dynamic bins based on price? 
        # Or fixed bins relative to a baseline? 
        # Simpler: Round(price / bin_step) * bin_step? 
        # If Price=2000, 0.25% = 5. So bins of size 5.
        
        # Use fixed bins based on price range to avoid jitter
        # For ETH (~2500), 0.25% is ~6.25. Let's use flexible rounding.
        # We'll use the price itself to determine bin.
        
        # Vectorized binning
        df['bin'] = (df['price'] / df['price'] * self.bin_size_pct).round() # This is wrong logic
        
        # Correct approach:
        # We need a reference price. Let's use the first price in the window or just round to nearest X
        # For crypto, rounding to nearest 0.25% log level is best, but linear is fine locally.
        # Let's use a fixed step based on recent average price.
        avg_price = df['price'].mean()
        bin_step = max(avg_price * self.bin_size_pct, 1.0) # Ensure at least 1.0
        
        df['bin'] = (df['price'] / bin_step).round() * bin_step
        
        gb = df.groupby('bin')['qty'].sum().reset_index()
        
        if gb.empty:
            return []
            
        mean_vol = gb['qty'].mean()
        std_vol = gb['qty'].std(ddof=0)
        if pd.isna(std_vol): std_vol = 0
        
        threshold = mean_vol + (2 * std_vol)
        
        # Filter
        # 1. Statistical Significance 
        significant = gb[gb['qty'] > threshold]
        
        # 2. Minimum Volume Filter (e.g. 50k USD) to avoid noise in quiet periods
        min_vol = 50000 
        # Note: 'qty' in liquidations is usually in Base Asset (ETH). 
        # So 50k USD / 2500 ~ 20 ETH.
        # Better to check if we can estimate USD value. 
        # For now, let's assume we want at least 10 units of base asset (approx 25k).
        significant = significant[significant['qty'] > 10]
        
        # Also always include Top 3
        top3 = gb.sort_values('qty', ascending=False).head(3)
        
        # Combine
        clusters = pd.concat([significant, top3]).drop_duplicates()
        
        return list(zip(clusters['bin'], clusters['qty']))
