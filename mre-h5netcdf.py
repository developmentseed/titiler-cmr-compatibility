import earthaccess
from titiler.cmr.backend import CMRBackend
from titiler.cmr.reader import xarray_open_dataset
from titiler.xarray.io import Reader as XarrayReader
from rio_tiler.models import ImageData
import numpy as np

cmr_query = {
    "concept_id": "C1615905582-GES_DISC",
}
auth = earthaccess.login()

x, y, z = 0, 0, 0
tile_args = {
    "tile_x": x,
    "tile_y": y,
    "tile_z": z,
    "cmr_query": cmr_query
}


with CMRBackend(
    reader=XarrayReader,
    auth=auth,
    reader_options={
        "variable": "blah",
        "opener": xarray_open_dataset
    }
) as src_dst:
    image, _ = src_dst.tile(**tile_args)
    print("successful tile operation")


def save_to_png(image_data: ImageData):
    # Rescale to 0-255 range (for 8-bit PNG)
    # Get min/max from your data
    data = image_data.data
    data_min = np.nanmin(data)
    data_max = np.nanmax(data)
    
    # Rescale to 0-255
    rescaled = ((data - data_min) / (data_max - data_min) * 255).astype(np.uint8)
    
    # Create new ImageData with rescaled values
    rescaled_image = ImageData(rescaled)
    png_bytes = rescaled_image.render(img_format="PNG")
    
    with open("output.png", "wb") as f:
        f.write(png_bytes)





