import earthaccess
from titiler.cmr.backend import CMRBackend
from titiler.cmr.reader import MultiFilesBandsReader

cmr_query = {
    "concept_id": "C2021957295-LPCLOUD",
}
auth = earthaccess.login()

tile_args = {
    "tile_x": 0,
    "tile_y": 0,
    "tile_z": 0,
    "cmr_query": cmr_query,
    "bands_regex": "B[0-9][0-9]",
    "bands": ["B02"]
}

with CMRBackend(
    reader=MultiFilesBandsReader,
    auth=auth
) as src_dst:
    image, _ = src_dst.tile(**tile_args)
    print("successful tile operation")






