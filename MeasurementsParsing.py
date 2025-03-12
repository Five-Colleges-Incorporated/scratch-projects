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
# query = "SELECT M_ID, MEASUREMENTS FROM CATALOGUE WHERE MEASUREMENTS IS NOT NULL FETCH NEXT 10 ROWS ONLY"
query = "SELECT M_ID, MEASUREMENTS FROM CATALOGUE WHERE MEASUREMENTS IS NOT NULL ORDER BY M_ID ASC"

# %%
import polars as pl

def measurements():
    return pl.read_database(
    connection=mimsy,
    query=query,
    iter_batches=True,
    batch_size=500
    )


# %%
if is_notebook:
    ms = pl.concat(list(measurements())).to_dicts()
    tests = """
    (ok, res) = mimsy_string.run_tests('''"""
    for m in ms:
        tests += f"\n\n\t# M_ID: {m["M_ID"]}"
        tests += f"\n\t{m["MEASUREMENTS"]}"
    tests += """''',
        print_results=False,
        full_dump=False,
    )
    print("Success!" if ok else res) 
    """
    print(tests)

# %%
import pyparsing as pp

dim =  pp.Group(
        pp.Word(pp.nums + "." + "/" + " ").set_parse_action(pp.token_map(str.strip))("value")
    + pp.Optional(pp.oneOf(["in", "ft", "cm", "m"])("unit") + pp.Optional(".")))

if is_notebook:
    (ok, res) = dim.run_tests(
        [
            "15/16 in", 
            "5 3/4 in",
            "12 in",
            "14.6 cm",
            "11 cm",
            "3",
            "31 7/8",
            "125.7",
            "81",
            "17 ft",
            "5.2 m",
            "47 13/16 in.",
        ],
        print_results=False,
        full_dump=False,
    )
    print("Success!" if ok else res)

vol = pp.Group(
    (dim("width"))
    + pp.Optional(pp.Suppress("x") + dim("height"))
    + pp.Optional(pp.Suppress("x") + dim("depth"))
)


dimensions = pp.Group(
    pp.Optional(pp.Group(
        pp.Word(pp.alphanums + "/") + pp.Optional(pp.Combine("(" + pp.Word(pp.alphas + "-") + ")"))
    )("type")
    + pp.Suppress(":"))
    + pp.OneOrMore(vol("measurements*") + pp.Suppress(pp.Optional(";")))
)

if is_notebook:
    (ok, res) = dimensions.run_tests(
        [
            "Overall: 5 3/4 in x 12 1/4 in x 9 1/8 in; 14.6 cm x 31.1 cm x 23.2 cm",
            "Overall (a): 4 in x 2 7/8 in; 10.2 cm x 7.3 cm",
            "5Sheet: 17 3/8 in x 13 in; 44.1 cm x 33 cm",
            "Overall: 15/16 in x 3 1/16 in x 2 in; 2.4 cm x 7.8 cm x 5.1 cm",
            "24 x 24 in; 60.96 x 60.96 cm",
            "Sheet/Image: 5 x 7 1/4 in; 12.7 x 18.4 cm",
            "Overall: 9 in; 22.9 cm",
            "canvas (semi-circular): 31 1/8 x 59 1/8 in.; 79.0575 x 150.1775 cm",
            "panel: 83 7/8 x 47 13/16 in.; 213.0425 x 121.4438 cm",
        ],
        print_results=False,
        full_dump=False,
    )
    print("Success!" if ok else res)

mimsy_string = pp.OneOrMore(dimensions("dimensions*"))

if is_notebook:
    mimsy_string.create_diagram("parser.html", show_results_names=True)

    print(mimsy_string.parse_string("canvas (semi-circular): 31 1/8 x 59 1/8 in.; 79.0575 x 150.1775 cm"))
    
    (ok, res) = mimsy_string.run_tests(
        """

        # M_ID: 3
        Overall: 5 3/4 in x 12 1/4 in x 9 1/8 in; 14.6 cm x 31.1 cm x 23.2 cm

        # M_ID: 12
        Overall: 5 1/8 in x 2 7/8 in; 13 cm x 7.3 cm

        # M_ID: 13
        Sheet: 12 in x 16 in; 30.5 cm x 40.6 cm

        # M_ID: 14
        Overall (a): 4 in x 2 7/8 in; 10.2 cm x 7.3 cm; Overall (b): 3 5/8 in x 4 1/8 in; 9.2 cm x 10.5 cm

        # M_ID: 15
        Overall: 8 3/8 in x 4 1/16 in; 21.3 cm x 10.3 cm

        # M_ID: 16
        Overall: 7 9/16 in x 4 5/16 in; 19.2 cm x 11 cm

        # M_ID: 17
        Sheet: 9 15/16 in x 13 15/16 in; 25.2 cm x 35.4 cm

        # M_ID: 18
        mat: 24 3/8 in x 30 5/8 in; 61.9 cm x 77.8 cm; sheet: 18 in x 23 7/8 in; 45.7 cm x 60.6 cm

        # M_ID: 19
        mat: 19 13/16 in x 25 5/8 in; 50.3 cm x 65.1 cm; sheet: 15 1/4 in x 20 in; 38.7 cm x 50.8 cm

        # M_ID: 20
        Sheet: 16 in x 20 in; 40.6 cm x 50.8 cm""",
        print_results=False,
        full_dump=False,
    )
    print("Success!" if ok else res)

# %%
for batch in measurements():
    for m in batch.to_dicts():
        try:
            mimsy_string.parse_string(m["MEASUREMENTS"])
        except:
            print(m)
            raise

# %%
