
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