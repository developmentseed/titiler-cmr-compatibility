import os
os.environ["RIO_TILER_MAX_THREADS"] = "1"

import earthaccess
import logging

from rio_tiler.io import Reader
from titiler.cmr.backend import CMRBackend
from titiler.cmr.reader import MultiFilesBandsReader
logging.getLogger('earthaccess').setLevel(logging.DEBUG)

auth = earthaccess.login() # using ~/.netrc
s3_credentials = auth.get_s3_credentials(provider="LPCLOUD")

cmr_query = {
    "concept_id": "C2021957295-LPCLOUD",
}
tile_args = {
    "tile_x": 0,
    "tile_y": 0,
    "tile_z": 0,
    "cmr_query": cmr_query,
    "bands_regex": "B[0-9][0-9]",
    "bands": ["B02"],
    "s3_credentials": s3_credentials
}

with CMRBackend(
    reader=MultiFilesBandsReader,
    auth=auth,
) as src_dst:
    image, _ = src_dst.tile(**tile_args)

# aws_session = rasterio.session.AWSSession(
#     aws_access_key_id=s3_credentials["accessKeyId"],
#     aws_secret_access_key=s3_credentials["secretAccessKey"],
#     aws_session_token=s3_credentials["sessionToken"]
# )
# url = 's3://lp-prod-protected/HLSS30.020/HLS.S30.T55JCM.2015332T001732.v2.0/HLS.S30.T55JCM.2015332T001732.v2.0.B06.tif'

# with rasterio.Env(aws_session):
#     # works
#     # src = Reader(url)
#     # works
#     # ds = rasterio.open(url)
#     print(src)





