
import math
import logging

logger = logging.getLogger(__name__)

class PositionSizer:
    """
    Calculates trade size based on risk parameters.
    Default Risk: 1% of Capital.
    Formula: Risk_Amount / (Entry - StopLoss)
    """
    def __init__(self, capital_usdt: float = 20.0, risk_pct: float = 0.01):
        self.capital = capital_usdt
        self.risk_pct = risk_pct
        self.risk_amount = self.capital * self.risk_pct

    def calculate_size(self, entry_price: float, stop_loss: float, leverage: int = 1) -> float:
        """
        Calculates position size in base asset (e.g. ETH).
        """
        if entry_price <= 0 or stop_loss <= 0:
            return 0.0
            
        risk_per_unit = abs(entry_price - stop_loss)
        
        # If stop loss is too tight (0), unexpected error
        if risk_per_unit == 0:
            logger.error("Stop loss equals entry price!")
            return 0.0
            
        position_size = self.risk_amount / risk_per_unit
        
        # Apply leverage constraint check? No, leverage just determines margin required.
        # Max Position Value = Capital * Leverage
        max_notional = self.capital * leverage
        notional_value = position_size * entry_price
        
        if notional_value > max_notional:
            logger.warning(f"Position size {notional_value} exceeds max leverage {max_notional}. Capping.")
            position_size = max_notional / entry_price
            
        # Min Notional Check (Binance Futures is usually 5 USDT)
        if position_size * entry_price < 5.0:
            logger.warning(f"Position value {position_size * entry_price} < 5.0 USDT. Skipping.")
            return 0.0
            
        return position_size

    def round_step_size(self, quantity: float, step_size: float) -> float:
        """
        Rounds quantity to exchange step size.
        """
        precision = int(round(-math.log(step_size, 10), 0))
        return round(quantity, precision)
