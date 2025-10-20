import xarray as xr
import rasterio
import earthaccess
from urllib.parse import urlparse

daac_map = {
    "ORNL_DAAC": "ORNLDAAC",
    "NASA NSIDC DAAC": "NSIDC",
    "NASA/MSFC/GHRC": "GHRCDAAC",
    "NASA/JPL/PODAAC": "PODAAC",
    "ASF": "ASF",
    "LP DAAC": "LPDAAC",
    "NASA/GSFC/SED/ESD/TISL/GESDISC": "GES_DISC",
    "NASA/GSFC/SED/ESD/GCDC/OB.DAAC": "OBDAAC",
    "SEDAC": "SEDAC",
    "NASA/GSFC/SED/ESD/HBSL/BISB/LAADS": "LAADS",
    "NASA/LARC/SD/ASDC": "ASDC"
}

def open_xarray_dataset(url, data_center_name):
    """Open a NetCDF URL that may be HTTPS or S3 and return (ds, scheme)."""
    scheme = urlparse(url).scheme.lower()
    if scheme in ("http", "https"):
        fs = earthaccess.get_fsspec_https_session()
        ds = xr.open_dataset(
            fs.open(url), engine="h5netcdf", decode_times=False
        )
        return ds
    elif scheme == "s3":
        s3 = earthaccess.get_s3fs_session(daac=daac_map[data_center_name])
        ds = xr.open_dataset(
            s3.open(url, "rb"), engine="h5netcdf", decode_times=False
        )
    else:
        raise ValueError(f"Unsupported URL scheme: {scheme}")

def open_rasterio_dataset(url):
    """Open a rasterio dataset from a URL that may be HTTPS or S3."""
    scheme = urlparse(url).scheme.lower()
    if scheme in ("http", "https"):
        fs = earthaccess.get_fsspec_https_session()
        return rasterio.open(fs.open(url))
    elif scheme == "s3":
        s3 = earthaccess.get_s3fs_session()
        return rasterio.open(s3.open(url, "rb"))
    else:
        raise ValueError(f"Unsupported URL scheme: {scheme}")
