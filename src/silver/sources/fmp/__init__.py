"""Financial Modeling Prep source adapters and clients."""

from silver.sources.fmp.client import (
    FMPClient,
    FMPClientError,
    FMPConfigurationError,
    FMPHTTPError,
    FMPRawResponse,
    FMPTransport,
    FMPTransportError,
    FMPTransportResponse,
)
from silver.sources.fmp.daily_prices import (
    FmpDailyPriceParseError,
    parse_historical_daily_prices,
)

__all__ = [
    "FMPClient",
    "FMPClientError",
    "FMPConfigurationError",
    "FMPHTTPError",
    "FMPRawResponse",
    "FMPTransport",
    "FMPTransportError",
    "FMPTransportResponse",
    "FmpDailyPriceParseError",
    "parse_historical_daily_prices",
]
