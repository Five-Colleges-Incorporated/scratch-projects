# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.17.2
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %%
# %conda env update -n base --file environment.yaml

# %%
is_notebook = False
try:
    get_ipython()
    is_notebook = True
    from rich import print
except:
    pass
print("In a notebook: ", is_notebook)

# %%
from pathlib import Path

import orjson


def to_ndjson(json_f: Path):
    ndjson_f = Path("./mod_proxy_urls.ndjson")
    prev_line = None
    first = True
    with json_f.open("r") as j, ndjson_f.open("w") as ndj:
        for jl in j.readlines():
            if first:
                # skipping the opening array [
                first = False
                continue
            if prev_line is not None and prev_line != "null,":
                # skipping the closing array ] by having a lag of 1 line
                # somehow there's also nulls in the list, idk?
                ndj.write(prev_line)
            # I manually checked for any nested lists of objects
            # Adjust this to taste if it doesn't work anymore,
            #   maybe by counting opening/closing brackets
            prev_line = jl.strip().replace("},", "}\n")

    with ndjson_f.open("r") as ndj:
        for ndjl in ndj.readlines()[:10]:
            # sanity checking the json viability
            orjson.loads(ndjl)


to_ndjson(Path("./mod_proxy_urls.json"))
print("ndjson conversion done...")

# %%
import os
from contextlib import contextmanager

from dotenv import load_dotenv
from pyfolioclient import FolioBaseClient


@contextmanager
def get_client(env: str = "DEV"):
    load_dotenv(override=True)
    fep = os.getenv(f"FOLIO_ENDPOINT_{env}")
    fte = os.getenv(f"FOLIO_TENANT_{env}")
    fun = os.getenv(f"FOLIO_USER_{env}")
    fpw = os.getenv(f"FOLIO_PASSWORD_{env}")
    with FolioBaseClient(fep, fte, fun, fpw) as folio:
        yield folio


if is_notebook:
    with get_client():
        print("ok connection!")

# %%
from pathlib import Path

import orjson
from pyfolioclient import BadRequestError, UnprocessableContentError


def do_bulk_update(folio, holdings: list[str]):
    try:
        hc = len(holdings)
        folio.post_data(
            "/holdings-storage/batch/synchronous",
            params={"upsert": "true"},
            # pyfolioclient requires a dict and doesn't take the raw json string : /
            payload=orjson.loads('{ "holdingsRecords": [' + ", ".join(holdings) + "]}"),
        )
    except (
        BadRequestError,
        UnprocessableContentError,
        RuntimeError,
        TimeoutError,
    ) as e:
        if hc == 1:
            h = orjson.loads(holdings[0])
            yield (
                h.get("id"),
                None if "id" in h else holdings[0],
                str(e.__cause__ if hasattr(e, "__cause__") else e),
            )
            return
        yield from do_bulk_update(folio, holdings[: hc // 2])
        yield from do_bulk_update(folio, holdings[hc // 2 :])
    except orjson.JSONDecodeError as e:
        if hc == 1:
            yield (None, holdings[0], str(e))
            return
        yield from do_bulk_update(folio, holdings[: hc // 2])
        yield from do_bulk_update(folio, holdings[hc // 2 :])
    except Exception as e:
        yield from ((orjson.loads(h)["id"], None, str(e)) for h in holdings)


if is_notebook:
    ndjson_f = Path("./mod_proxy_urls.ndjson")
    with get_client() as folio, ndjson_f.open("r") as ndj:
        holdings = []
        for ndjl in ndj.readlines()[10:20]:
            holdings.append(ndjl)
        print([e for e in do_bulk_update(folio, holdings)])

# %%
from datetime import datetime
from itertools import chain, islice
from pathlib import Path

import orjson
import polars as pl

chunk_size = 500


def import_ndjson(ndjson_f: Path, output_f: Path):
    schema = {"id": pl.Utf8, "body": pl.Utf8, "error": pl.Utf8}
    errors = pl.DataFrame([], schema)
    with get_client() as folio, ndjson_f.open("r") as ndj:
        holdings = ndj.readlines()

        def chunks():
            iterator = iter(holdings)
            for first in iterator:
                yield chain([first], islice(iterator, chunk_size - 1))

        for c in chunks():
            errors.vstack(
                pl.DataFrame(list(do_bulk_update(folio, list(c))), schema=schema),
                in_place=True,
            )
            print(".", end="")

    errors.sink_csv(output_f)


output = Path(datetime.now().strftime("%m%d%H%M%S") + ".csv")
import_ndjson(Path("./mod_proxy_urls.ndjson"), output)
print(f"{output} done!")

# %%
