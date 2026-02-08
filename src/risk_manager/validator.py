
import logging

logger = logging.getLogger(__name__)

class RiskValidator:
    """
    Validates trade parameters against exchange rules and internal safety limits.
    """
    def __init__(self, min_qty: float = 0.001, min_notional: float = 5.0):
        self.min_qty = min_qty
        self.min_notional = min_notional
        self.daily_loss = 0.0
        self.max_daily_loss = 10.0 # USDT
        
    def validate_signal(self, size: float, price: float) -> bool:
        """
        Returns True if signal is safe and actionable.
        """
        # 1. Min Qty
        if size < self.min_qty:
            logger.warning(f"Signal rejected: Size {size} < Min Qty {self.min_qty}")
            return False
            
        # 2. Min Notional
        notional = size * price
        if notional < self.min_notional:
            logger.warning(f"Signal rejected: Notional {notional} < Min {self.min_notional}")
            return False
            
        # 3. Circuit Breaker
        if self.daily_loss > self.max_daily_loss:
            logger.warning("Signal rejected: Circuit breaker active.")
            return False
            
        return True
