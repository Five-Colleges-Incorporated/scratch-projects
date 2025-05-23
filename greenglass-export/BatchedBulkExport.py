# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.17.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %%
# %conda env update -n base --file environment.yaml

# %%
import io
import shutil
import time
import uuid
from pathlib import Path

import httpx
import polars as pl
from pyfolioclient import FolioBaseClient

# %%
import os

from dotenv import load_dotenv

load_dotenv()
# env = "DEV"
env = "PROD"
fep = os.getenv(f"FOLIO_ENDPOINT_{env}")
fte = os.getenv(f"FOLIO_TENANT_{env}")
fun = os.getenv(f"FOLIO_USER_{env}")
fpw = os.getenv(f"FOLIO_PASSWORD_{env}")

# %%
pl.read_csv("./missing_bibs.csv").select(pl.col("item_hrid")).filter(
    pl.Expr.and_(
        pl.col("item_hrid").is_not_null(),
        pl.col("item_hrid").str.len_chars().ge(10),
    )
).unique().sort("item_hrid").write_csv("./unique_itemids.csv")

# %%
items = (
    pl.scan_csv("./unique_itemids.csv")
    .select(pl.lit('items.hrid="') + pl.col("item_hrid") + pl.lit('"'))
    .with_row_index()
)

instance_ids = pl.DataFrame([], schema={"instance_id": pl.Utf8})

start = time.time()
batch_size = 100
rows_batched = batch_size
batch_num = 0
with FolioBaseClient(fep, fte, fun, fpw) as folio:

    while rows_batched == batch_size:
        if batch_num % 100 == 0:
            print(f"{batch_num}, ", end="")
        batch = items.filter(
            pl.Expr.and_(
                pl.col("index").ge(pl.lit(batch_size * batch_num)),
                pl.col("index").lt(pl.lit(batch_size * (batch_num + 1))),
            )
        )
        batch_num += 1
        rows_batched = int(batch.select(pl.len()).collect().item())
        cql = " or ".join([r["literal"] for r in batch.collect().to_dicts()])

        instance_ids.vstack(
            pl.DataFrame(
                [
                    i["id"]
                    for i in folio.get_data(
                        "/search/instances",
                        key="instances",
                        cql_query=f'({cql} and staffSuppress=="false")',
                        limit=batch_size,
                    )
                ],
                schema={"instance_id": pl.Utf8},
            ),
            in_place=True,
        )

instance_ids.rechunk().unique("instance_id").sort("instance_id").write_csv(
    f"unique_instanceids_{env}.csv"
)
print()
print((time.time() - start) / 60)

# %%
pl.read_csv(f"unique_instanceids_{env}.csv").with_row_index().filter(
    pl.col("index").ge(pl.lit(1500)),
    pl.col("index").lt(pl.lit(2000)),
).sort("instance_id").drop("index").write_csv("has_four_failures.csv")

# %%
start = time.time()
run = uuid.uuid4()

source = f"unique_instanceids_{env}.csv"
# source = f"has_four_failures.csv"

job_ids = []
file_ids = []
sample_size = 2000000
batch_size = 100
rows_batched = batch_size
batch_num = 0
with FolioBaseClient(fep, fte, fun, fpw) as folio:
    instances = pl.scan_csv(source).with_row_index().head(sample_size)
    while rows_batched == batch_size:
        if batch_num % 10 == 0:
            print(f"{batch_num}, ", end="")
        batch = instances.filter(
            pl.Expr.and_(
                pl.col("index").ge(pl.lit(batch_size * batch_num)),
                pl.col("index").lt(pl.lit(batch_size * (batch_num + 1))),
            )
        )
        batch_name = f"_{batch_size * batch_num}-{batch_size * (batch_num + 1)}_"
        batch_num += 1
        rows_batched = int(batch.select(pl.len()).collect().item())

        csv = io.BytesIO()
        batch.select("instance_id").collect().write_csv(csv, include_header=False)

        csv.seek(0)
        req = {
            "size": int(csv.getbuffer().nbytes / 2**10),
            "fileName": str(run) + batch_name + ".csv",
            "uploadFormat": "csv",
        }
        csv.seek(0)

        res = folio.post_data("/data-export/file-definitions", payload=req)
        file_id = res["id"]
        file_ids.append(file_id)
        job_ids.append(res["jobExecutionId"])

        res = folio.post_data(
            f"/data-export/file-definitions/{file_id}/upload", content=csv
        )

print()
print((time.time() - start) / 60)

# %%
start = time.time()

concurrency_limit = 4

with FolioBaseClient(fep, fte, fun, fpw) as folio:
    print(f"{len(file_ids)}, ", end="")
    while len(file_ids) > 0:
        print(f"{len(file_ids)}, ", end="")

        res = folio.get_data(
            "/data-export/job-executions",
            key="jobExecutions",
            cql_query=f'status="IN_PROGRESS"',
        )
        if len(res) <= concurrency_limit:
            file_id = file_ids.pop()
            req = {
                "fileDefinitionId": file_id,
                "jobProfileId": "524d3f1b-f008-4c6b-815f-de76620ccd90",
                "idType": "instance",
            }
            res = folio.post_data("/data-export/export", payload=req)
        else:
            time.sleep(5)
print()
print((time.time() - start) / 60)

# %%
start = time.time()
with FolioBaseClient(fep, fte, fun, fpw) as folio:
    waiting = 1
    while waiting > 0:
        res = folio.get_data(
            "/data-export/job-executions",
            key="jobExecutions",
            cql_query=f'status="IN_PROGRESS"',
        )
        waiting = len(res)
        print(f"{waiting}, ", end="")
        time.sleep(5)


print()
print((time.time() - start) / 60)

# %%
with FolioBaseClient(fep, fte, fun, fpw) as folio:
    res = folio.get_data(
        "/data-export/job-executions",
        key="jobExecutions",
        cql_query=f'status="COMPLETED_WITH_ERRORS" sortBy completedDate/sort.descending',
        limit=10,
    )
    import pprint

    pprint.pprint(res)

# %%
run = uuid.UUID("d7975a00-0a2b-45f3-b525-f5dd94292f46")
with FolioBaseClient(fep, fte, fun, fpw) as folio:
    res = folio.get_data(
        "/data-export/job-executions",
        key="jobExecutions",
        cql_query=f"status=(COMPLETED_WITH_ERRORS)",
        limit=5,
    )
    job_ids = [r["id"] for r in res]

# %%
start = time.time()

jids = job_ids.copy()
instance_errors = None
job_errors = None
with FolioBaseClient(fep, fte, fun, fpw) as folio:
    output = Path(str(run))
    shutil.rmtree(output)
    output.mkdir()
    print(f"{len(jids)}, ", end="")
    while len(jids) > 0:
        print(f"{len(jids)}, ", end="")
        id = jids.pop()
        res = folio.get_data(
            "/data-export/job-executions",
            key="jobExecutions",
            cql_query=f'id="{id}"',
        )

        job = res[0]
        if job["status"] in ["COMPLETED", "COMPLETED_WITH_ERRORS"]:
            marc_id = job["exportedFiles"][0]["fileId"]
            marc_name = job["exportedFiles"][0]["fileName"]
            res = folio.get_data(f"/data-export/job-executions/{id}/download/{marc_id}")
            res = httpx.get(res["link"])
            with (output / marc_name).open("wb") as f:
                f.write(res.content)

        if job["status"] == "COMPLETED_WITH_ERRORS":
            res = folio.get_data(
                f"/data-export/logs",
                key="errorLogs",
                limit=10000,
                cql_query=f'jobExecutionId=="{job["id"]}"',
            )
            for err in res:
                if "affectedRecord" in err:
                    if instance_errors is None:
                        instance_errors = pl.DataFrame([err])
                    else:
                        instance_errors.vstack(pl.DataFrame([err]), in_place=True)
                else:
                    if job_errors is None:
                        job_errors = pl.DataFrame([err])
                    else:
                        job_errors.vstack(pl.DataFrame([err]), in_place=True)

if instance_errors is not None:
    instance_errors.glimpse()
    instance_errors = (
        instance_errors.rechunk()
        .with_columns(pl.col("affectedRecord").struct.unnest())
        .select(
            pl.col("id").alias("instanceId"),
            pl.col("errorMessageCode").alias("errorCode"),
            pl.col("errorMessageValues")
            .list.join(separator="\n")
            .alias("errorMessage"),
            "inventoryRecordLink",
        )
    )
    instance_errors.glimpse()

if job_errors is not None:
    job_errors.glimpse()
    job_errors = job_errors.rechunk()
    opaque_errs = job_errors.filter(
        pl.col("errorMessageCode").eq("error.someRecordsFailed")
    ).select(
        "jobExecutionId", pl.col("errorMessageValues").list.first().alias("errors")
    )
    opaque_errs.glimpse()
    job_errors = job_errors.filter(
        pl.col("errorMessageCode").ne("error.someRecordsFailed")
    ).select(
        pl.col("errorMessageValues").list.first().alias("instanceId"),
        pl.col("errorMessageCode").alias("errorCode"),
        pl.col("errorMessageValues")
        .list.slice(1)
        .list.join(separator="\n")
        .alias("errorMessage"),
        pl.lit("").alias("inventoryRecordLink"),
    )
    job_errors.glimpse()

if instance_errors and job_errors:
    instance_errors.concat(job_errors)
else if job_errors:
    instance_errors = job_errors

if instance_errors:
    instance_errors.write_csv(output / "errors.csv")

if opaque_errs:
    opaque_errs.write_csv(output / "opaque_errors.csv")

print()
print((time.time() - start) / 60)
print(run)

# %%
job_errors.glimpse()
instance_errors.glimpse()

# %%
