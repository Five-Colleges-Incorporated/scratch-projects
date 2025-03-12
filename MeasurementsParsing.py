# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.16.7
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %%
# %conda env update -n base --file environment.yml

# %%
from dotenv import load_dotenv

load_dotenv()

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
import os

import oracledb

mimsy = oracledb.connect(
    dsn=f"{os.environ["MIMSY_HOST"]}:{os.environ["MIMSY_PORT"]}/{os.environ["MIMSY_SERVICE"]}",
    user=os.environ["MIMSY_USERNAME"],
    password=os.environ["MIMSY_PASSWORD"],
    tcp_connect_timeout=5.0,
)
mimsy.is_healthy()

# %%
import polars as pl

measurements = pl.read_database(
    connection=mimsy,
    query=f"SELECT M_ID, MEASUREMENTS FROM CATALOGUE WHERE MEASUREMENTS IS NOT NULL FETCH NEXT 10 ROWS ONLY",
)
if is_notebook:
    ms = measurements.to_dicts()
    for m in ms:
        print(f"\t# M_ID: {m["M_ID"]}")
        print(f"\td = {m["MEASUREMENTS"]}\n")

# %%
import pyparsing as pp

dim = pp.Group(
    (
        pp.Group(
            pp.Word(pp.nums)("whole")
            + pp.Combine(
                pp.Optional("." + pp.Word(pp.nums))
                + pp.Optional(pp.Word(pp.nums) + "/" + pp.Word(pp.nums))
            )("fraction")
        )("value")
    )
    + (pp.oneOf(["in", "cm"])("unit"))
)

if is_notebook:
    for d in ["5 3/4 in", "12 in", "14.6 cm", "11 cm"]:
        print(d)
        print(dim("dim").parse_string(d).as_dict())

vol = pp.Group(
    (dim("width"))
    + pp.Suppress("x")
    + (dim("height"))
    + pp.Optional(pp.Suppress("x") + dim("depth"))
)


dimensions = pp.Group(
    pp.Group(pp.Word(pp.alphas) + pp.Optional(pp.Combine("(" + pp.Word(pp.alphas) + ")")))("type")
    + ":"
    + pp.OneOrMore(vol("measurements*") + pp.Suppress(pp.Optional(";")))
)

if is_notebook:
    for d in [
        "Overall: 5 3/4 in x 12 1/4 in x 9 1/8 in; 14.6 cm x 31.1 cm x 23.2 cm",
        "Overall: 5 1/8 in x 2 7/8 in; 13 cm x 7.3 cm",
    ]:
        print(d)
        print(dimensions("dimensions").parse_string(d).as_dict())

mimsy_string = pp.OneOrMore(dimensions("dimensions*"))

if is_notebook:
    mimsy_string.create_diagram("parser.html", show_results_names=True)

    # M_ID: 3
    m = "Overall: 5 3/4 in x 12 1/4 in x 9 1/8 in; 14.6 cm x 31.1 cm x 23.2 cm"
    print(m)
    print(mimsy_string.parse_string(m).as_dict())

    # M_ID: 12
    m = "Overall: 5 1/8 in x 2 7/8 in; 13 cm x 7.3 cm"
    print(m)
    print(mimsy_string.parse_string(m).as_dict())

    # M_ID: 13
    m = "Sheet: 12 in x 16 in; 30.5 cm x 40.6 cm"
    print(m)
    print(mimsy_string.parse_string(m).as_dict())

    # M_ID: 14
    m = "Overall (a): 4 in x 2 7/8 in; 10.2 cm x 7.3 cm; Overall (b): 3 5/8 in x 4 1/8 in; 9.2 cm x 10.5 cm"
    print(m)
    print(mimsy_string.parse_string(m).as_dict())

    # M_ID: 15
    m = "Overall: 8 3/8 in x 4 1/16 in; 21.3 cm x 10.3 cm"
    print(m)
    print(mimsy_string.parse_string(m).as_dict())

    # M_ID: 16
    m = "Overall: 7 9/16 in x 4 5/16 in; 19.2 cm x 11 cm"
    print(m)
    print(mimsy_string.parse_string(m).as_dict())

    # M_ID: 17
    m = "Sheet: 9 15/16 in x 13 15/16 in; 25.2 cm x 35.4 cm"
    print(m)
    print(mimsy_string.parse_string(m).as_dict())

    # M_ID: 18
    m = "mat: 24 3/8 in x 30 5/8 in; 61.9 cm x 77.8 cm; sheet: 18 in x 23 7/8 in; 45.7 cm x 60.6 cm"
    print(m)
    print(mimsy_string.parse_string(m).as_dict())

    # M_ID: 19
    m = "mat: 19 13/16 in x 25 5/8 in; 50.3 cm x 65.1 cm; sheet: 15 1/4 in x 20 in; 38.7 cm x 50.8 cm"
    print(m)
    print(mimsy_string.parse_string(m).as_dict())

    # M_ID: 20
    m = "Sheet: 16 in x 20 in; 40.6 cm x 50.8 cm"
    print(m)
    print(mimsy_string.parse_string(m).as_dict())

# %%
