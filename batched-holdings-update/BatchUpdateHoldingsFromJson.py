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
from dotenv import load_dotenv

load_dotenv(override=True)

is_notebook = False
try:
    get_ipython()
    is_notebook = True
    from rich import print
except:
    pass
print("In a notebook: ", is_notebook)

# %%
import json
from pathlib import Path


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
            json.loads(ndjl)


to_ndjson(Path("./mod_proxy_urls.json"))
print("ndjson conversion done...")

# %%
