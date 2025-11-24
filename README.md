# TiTiler-CMR Compatibility

This repository contains code for testing tile generation for NASA Earth data collections using [TiTiler-CMR](https://github.com/developmentseed/titiler-cmr).

## What's here?

* [METHODOLOGY.md](./METHODOLOGY.md): The methodology documentation explains the testing steps.
* [LITHOPS_WORKFLOW.md](./LITHOPS_WORKFLOW.md): The lithops workflow documentation how to setup lithops and use lithops parallel processing.
* [QUERY_REPROCESSING.md](./QUERY_REPROCESSING.md): The query reprocessing documentation explains how to use the CLI to reprocess any failed collections.
* [titiler_cmr/](./titiler_cmr/): TiTiler-CMR installed as a [git submodule](https://git-scm.com/book/en/v2/Git-Tools-Submodules).
* [titiler_cmr_compatibility/](./titiler_cmr_compatibility/): Various modules used to query CMR and test tile generation.
* [read_results.ipynb](./read_results.ipynb): Basic notebook for inspecting tiling results. The full report on results is deferred to documentation in [TiTiler-CMR's docs](https://developmentseed.org/titiler-cmr/dev/).

## How to use it

First clone the repo:

```bash
git clone https://github.com/developmentseed/titiler-cmr-compatibility.git
cd titiler-cmr-compatibility/
```

Instead of calling the TiTiler-CMR API directly, the library is installed as a git submodule. This gets around any networking limitations and slow downs of using the actual API and also allows to make small fixes to the titiler-cmr codebase (which will be addressed in near-future TiTiler-CMR releases).

```bash
git submodule update --init --recursive
pip install -e ./titiler_cmr
pip install -r requirements.txt
```

You can kick the tires of the CLI with:

```bash
python -m titiler_cmr_compatibility.cli
```

But you probably don't want to do that since it will process >10k collections sequentially.

You can use the help argument to see the CLI options.

```bash
python -m titiler_cmr_compatibility.cli --help
```

Next, head to [`METHODOLOGY.md`](./METHODOLOGY.md) learn more about the approach or [`LITHOPS_WORKFLOW.md`](./LITHOPS_WORKFLOW.md) to see how to setup parallel processing using [lithops](https://lithops-cloud.github.io/).



