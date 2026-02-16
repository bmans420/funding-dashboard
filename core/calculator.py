"""APR calculation from normalized funding rates."""


def calculate_apr(rate_sum: float, days: float) -> float:
    """Annualize funding rate sum.
    
    APR = rate_sum * (365 / days) * 100
    rate_sum is in decimal (e.g., 0.0003 = 0.03%)
    Returns percentage (e.g., 10.95)
    """
    if days <= 0:
        return 0.0
    return rate_sum * (365.0 / days) * 100.0


def rate_sum_to_percent(rate_sum: float) -> float:
    """Convert decimal rate sum to percentage."""
    return rate_sum * 100.0
