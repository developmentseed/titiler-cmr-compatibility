# TiTiler-CMR Compatibility

This repository contains code for testing collections in CMR with TiTiler-CMR

## How to use it

Instead of calling the TiTiler-CMR API directly, we install it as a git submodule. This gets around any networking limitations and slow downs of using the actual API and also allows to make small fixes to the titiler-cmr codebase (which will be addressed in near-future TiTiler-CMR releases).

```bash
git clone https://github.com/developmentseed/titiler-cmr-compatibility.git
cd titiler-cmr-compatibility/
git submodule update --init --recursive
pip install -e ./titiler_cmr
pip install -r requirements.txt
```

Running tests:

```bash
python -m titiler_cmr_compatibility.cli
```

But you probably don't want to do that since it will process >10k collections sequentially.

You can use the help argument to see the CLI options, but I recommend heading to [`METHODOLOGY.md`](./METHODOLOGY.md).

```bash
python -m titiler_cmr_compatibility.cli --help
```

## What else is here?

* [METHODOLOGY.md](./METHODOLOGY.md): The methodology documentation explains the testing steps.
* [LITHOPS_WORKFLOW.md](./LITHOPS_WORKFLOW.md): The lithops workflow documentation how to setup lithops and use lithops parallel processing.
* [QUERY_REPROCESSING.md](./QUERY_REPROCESSING.md): The query reprocessing documentation explains how to use the CLI to reprocess any failed collections.




