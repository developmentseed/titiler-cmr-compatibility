
```bash
git clone https://github.com/developmentseed/titiler-cmr-compatibility.git
cd titiler-cmr-compatibility/
git submodule update --init --recursive
pip install -e ./titiler_cmr
pip install -r requirements.txt
```

Running the tests:

```bash
python run_test.py --parallel --num-workers 8 > output.log 2>&1
```

TODO:

* test L2 collections
* determine why so many more NetCDF-4 datasets were deemed compatible [here](https://github.com/developmentseed/datacube-guide/blob/main/docs/visualization/titiler/titiler-cmr/test-netcdf4-datasets.ipynb)
  * many collections are L2 and not actually tilable
  * statistics returned 0 values but still evaluated to compatible
  * tiling is not actually tested
* do a fuzzy match on variable selection or otherwise improve variable / band selection
* Address missing band information for rasterio datasets
* Sometimes getting no tile for 0-0-0
* documentation

