import xarray as xr
import rasterio
import earthaccess
from urllib.parse import urlparse
import signal
import logging
from functools import wraps

logger = logging.getLogger(__name__)


class TimeoutError(Exception):
    """Exception raised when an operation times out."""
    pass


def timeout_handler(signum, frame):
    """Signal handler for timeout."""
    raise TimeoutError("Operation timed out")


def with_timeout(seconds=60):
    """
    Decorator to add timeout to a function using signal.alarm (Unix only).

    Args:
        seconds: Number of seconds before timing out
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Set the signal handler and alarm
            old_handler = signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(seconds)
            try:
                result = func(*args, **kwargs)
            finally:
                # Disable the alarm and restore old handler
                signal.alarm(0)
                signal.signal(signal.SIGALRM, old_handler)
            return result
        return wrapper
    return decorator

@with_timeout(seconds=120)
def open_xarray_dataset(url, data_center_name):
    """
    Open a NetCDF URL that may be HTTPS or S3 and return the dataset.

    Args:
        url: URL to the data file
        data_center_name: Data center identifier for authentication

    Returns:
        xarray.Dataset

    Raises:
        TimeoutError: If opening the dataset takes longer than 120 seconds
        ValueError: If URL scheme is not supported
    """
    logger.info(f"Opening xarray dataset from {url}")
    scheme = urlparse(url).scheme.lower()

    try:
        if scheme in ("http", "https"):
            fs = earthaccess.get_fsspec_https_session()
            return xr.open_dataset(fs.open(url), decode_times=False)
        elif scheme == "s3":
            s3 = earthaccess.get_s3fs_session(daac=data_center_name)
            return xr.open_dataset(s3.open(url, "rb"), decode_times=False)
        else:
            raise ValueError(f"Unsupported URL scheme: {scheme}")
    except TimeoutError:
        logger.error(f"Timeout opening xarray dataset from {url}")
        raise
    except Exception as e:
        logger.error(f"Error opening xarray dataset from {url}: {e}")
        raise

@with_timeout(seconds=120)
def open_rasterio_dataset(url, data_center_name):
    """
    Open a rasterio dataset from a URL that may be HTTPS or S3.

    Args:
        url: URL to the data file
        data_center_name: Data center identifier for authentication

    Returns:
        rasterio.DatasetReader

    Raises:
        TimeoutError: If opening the dataset takes longer than 120 seconds
        ValueError: If URL scheme is not supported
    """
    logger.info(f"Opening rasterio dataset from {url}")
    scheme = urlparse(url).scheme.lower()

    try:
        if scheme in ("http", "https"):
            fs = earthaccess.get_fsspec_https_session()
            return rasterio.open(fs.open(url))
        elif scheme == "s3":
            s3 = earthaccess.get_s3fs_session(daac=data_center_name)
            return rasterio.open(s3.open(url, "rb"))
        else:
            raise ValueError(f"Unsupported URL scheme: {scheme}")
    except TimeoutError:
        logger.error(f"Timeout opening rasterio dataset from {url}")
        raise
    except Exception as e:
        logger.error(f"Error opening rasterio dataset from {url}: {e}")
        raise
