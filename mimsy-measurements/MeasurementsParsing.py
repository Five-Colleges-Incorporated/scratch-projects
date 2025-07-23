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
# %conda env update -n base --file environment.yml

# %%
from dotenv import load_dotenv

load_dotenv(override=True)

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
if not mimsy.is_healthy():
    raise Exception("Please check mimsy connection")

# %%
query = """
SELECT M_ID, MEASUREMENTS FROM CATALOGUE
WHERE 
    M_ID > {0}
    AND MEASUREMENTS IS NOT NULL 
    AND mkey not in (select mkey from measurements)
    AND mkey not in (select mkey from diams_weights)
    AND mkey not in (select mkey from new_linear_measurements)
ORDER BY M_ID ASC
"""
# query = "SELECT M_ID, MEASUREMENTS FROM CATALOGUE WHERE M_ID > {0} AND MEASUREMENTS IS NOT NULL ORDER BY M_ID ASC"

# %%
import polars as pl


def measurements(last=0):
    return pl.read_database(
        connection=mimsy, query=query.format(last), iter_batches=True, batch_size=1000
    )


# %%
import pyparsing as pp


def check(parser, cases, out=None):
    if not is_notebook:
        return
    ok, res = parser.run_tests(cases, print_results=False, full_dump=False)
    if out != False and (out == True or not ok):
        print()
        for r in res:
            print(r)


ws = pp.White()

dim = pp.Combine(
    pp.Optional(".")
    + pp.Word(pp.nums)
    + pp.Optional("." + pp.Word(pp.nums))
    + pp.Optional("/" + pp.Word(pp.nums))
    + pp.Optional(ws + pp.Word(pp.nums) + "/" + pp.Word(pp.nums))
)
check(dim, ["1", "23", "2.8", ".88", "33.77", "1 1/4", "13/17"])

inches_unit = pp.Optional(
    pp.Suppress(pp.Optional(ws))
    + pp.one_of(["in", "in.", "inches", '"'], caseless=True)
)
feet_unit = pp.Optional(
    pp.Suppress(pp.Optional(ws)) + pp.one_of(["ft", "ft.", "feet", "'"], caseless=True)
)
imperial_unit = pp.Or([inches_unit, feet_unit])
single_imperial = pp.OneOrMore(
    pp.Combine(
        dim("dims*") + imperial_unit("units*") + pp.Suppress(pp.Optional(ws + "x" + ws))
    )
)
check(
    single_imperial,
    [
        "1 in",
        "3.5in.",
        "2 x 3 x 5 inches",
        "35 1/2  x 18 in.",
        "28 5/8 x 40 1/2  in.",
        '1 3/4" x 1/2 in',
        "4'",
        "5 ft.",
    ],
)

double_imperial = pp.OneOrMore(
    pp.Combine(
        pp.Optional(dim("dimsft*") + feet_unit("units*") + ws)
        + dim("dimsin*")
        + imperial_unit("units*")
        + pp.Suppress(pp.Optional(" x "))
    )
)
check(
    double_imperial,
    [
        "4' 3\"",
        "2' x 3' 2\" x 5\"",
    ],
)

metric_unit = pp.Optional(
    pp.Suppress(pp.Optional(ws))
    + pp.one_of(["cm", "cm.", "mm", "mm.", "m", "m."], caseless=True)
)
metric = pp.OneOrMore(
    pp.Combine(
        dim("dims*") + metric_unit("units*") + pp.Suppress(pp.Optional(ws + "x" + ws))
    )
)
check(metric, ["5cm", "65mm.", "10.4 m. x 15 cm"])

measurement = pp.OneOrMore(
    pp.Group(pp.Or([metric, double_imperial, single_imperial]))("measurements*")
    + pp.Suppress(pp.Optional(";"))
)
check(
    measurement,
    [
        "1 in",
        "3.5in.",
        "2 x 3 inches",
        '1 3/4" x 1/2 in',
        "5cm",
        "65mm.",
        "10.4 m. x 15 cm",
        "20 1/2 x 15 in.; 52.07 x 38.1 cm",
        "1' 3/4\" x 2 ft. 1/2 in",
    ],
)

types = pp.one_of(
    [
        "base",
        "block",
        "board",
        "canvas",
        "frame",
        "housing",
        "image",
        "mat",
        "mount",
        "overall",
        "panel",
        "plate",
        "sheet",
        "sight",
        "stone",
        "stretcher",
    ],
    caseless=True,
)
facet = pp.Group(pp.Optional(types("type*") + pp.Suppress(":")) + measurement)(
    "facets*"
)
check(
    facet,
    [
        ".875 x .5 in.",
        "sheet: 13 x 17 1/2 in",
    ],
)

easy_parser = pp.OneOrMore(facet + pp.Suppress(pp.Optional(";")))
check(
    easy_parser,
    [
        ".875 x .5 in.",
        "sheet: 13 x 17 1/2 in",
        "sheet: 13 x 17 1/2 in.; stone: 10 x 12 1/4 in.",
    ],
)

if is_notebook:
    easy_parser.create_diagram("easy_parser.html", show_results_names=True)

# %%
from copy import deepcopy
from datetime import datetime
from pathlib import Path

last = 0
output = Path("output", datetime.now().strftime("%m%d%H%M%S"))
output.mkdir(parents=True, exist_ok=False)
for batch_no, batch in enumerate(measurements(last - 1)):
    if is_notebook:
        print(f"{batch_no}...")

    results = []
    for item in batch.to_dicts():
        base_res = {"M_ID": item["M_ID"], "MEASUREMENTS": item["MEASUREMENTS"]}

        (ok, err) = easy_parser.run_tests(
            item["MEASUREMENTS"], print_results=False, full_dump=False
        )
        if not ok:
            base_res["Test Error"] = str(err)

        try:
            parsed = easy_parser.parse_string(item["MEASUREMENTS"])
        except pp.ParseException as e:
            base_res["Parse Error"] = str(e)
            results.append(base_res)
            continue

        for f in parsed["facets"]:
            f_res = deepcopy(base_res)
            f_res["Type"] = f["type"][0] if "type" in f else "overall"

            if not "measurements" in f:
                results.append(f_res)

            for m in f["measurements"]:
                m_res = deepcopy(f_res)
                units = set()
                if "dimsft" in m:
                    m_res["Feet and Inches"] = True
                    results.append(m_res)
                    continue

                if (not "dims" in m) or not "units" in m:
                    results.append(m_res)
                    continue

                for i, d in enumerate(m["dims"]):
                    if i >= 3:
                        break
                    m_res[f"Dimension{i+1}"] = d
                    units.update(m["units"][min(len(m["units"]) - 1, i)])

                m_res[f"Units"] = ", ".join(units)
                results.append(m_res)

    pl.DataFrame(
        results,
        schema=[
            ("M_ID", pl.Int64),
            ("MEASUREMENTS", pl.String),
            ("Test Error", pl.String),
            ("Parse Error", pl.String),
            ("Feet and Inches", pl.Boolean),
            ("Type", pl.String),
            ("Units", pl.String),
            ("Dimension1", pl.String),
            ("Dimension2", pl.String),
            ("Dimension3", pl.String),
        ],
        # ).write_csv(output / f"{batch_no:03d}.csv")
    ).write_parquet(output / f"{batch_no:03d}.parquet")

results = pl.read_parquet(output / "*.parquet")
results.filter(
    pl.col("Test Error").is_not_null(),
    pl.col("Parse Error").is_null(),
).write_csv(output / "maybe_issues.csv")
results.filter(
    pl.Expr.or_(
        pl.col("Parse Error").is_not_null(),
        pl.col("Feet and Inches"),
    )
).write_csv(output / "issues.csv")
results.filter(
    pl.col("Test Error").is_null(),
    pl.col("Parse Error").is_null(),
).write_csv(output / "newly_parsed.csv")

print(f"{output} done!")

# %%
