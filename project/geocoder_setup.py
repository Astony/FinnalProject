from geopy.geocoders import Here
from geopy.extra.rate_limiter import RateLimiter

GEOLOCATION_API_KEY = "dYrajXLId5Rnp_WKZHaonsR-SMy2Z0xOlPRM4uELmfY"


def geocoder_setup(limit=True) -> Here:
    """Function that return geocoder object with required setups"""
    geocoder = Here(
        apikey=GEOLOCATION_API_KEY, user_agent="weather.script.py", timeout=3
    )
    if limit:
        geocoder = RateLimiter(geocoder.reverse, min_delay_seconds=0.1)
    return geocoder