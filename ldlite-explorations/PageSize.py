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
import os
from contextlib import contextmanager

import httpx
from dotenv import load_dotenv


class RefreshTokenAuth(httpx.Auth):
    requires_response_body = True

    def __init__(self, base_url, env: str):
        self.fe = base_url
        self.fu = os.getenv(f"FOLIO_USER_{env}")
        self.fp = os.getenv(f"FOLIO_PASSWORD_{env}")

        self.hdr = {"x-okapi-tenant": os.getenv(f"FOLIO_TENANT_{env}")}
        self._do_auth()

    def auth_flow(self, request):
        request.headers.update(self.hdr)
        response = yield request

        if response.status_code == 401:
            self._do_auth()
            request.headers.update(self.hdr)
            yield request

    def _do_auth(self):
        # del self.hdr["x-okapi-token"]
        res = httpx.post(
            self.fe + "/authn/login-with-expiry",
            headers=self.hdr,
            json={
                "username": self.fu,
                "password": self.fp,
            },
        )
        res.raise_for_status()
        self.hdr["x-okapi-token"] = res.cookies["folioAccessToken"]


@contextmanager
def get_client():
    load_dotenv(override=True)
    # env = "DEV"
    env = "PROD"
    base_url = os.getenv(f"FOLIO_ENDPOINT_{env}")

    with httpx.Client(
        base_url=base_url,
        auth=RefreshTokenAuth(base_url, env),
        transport=httpx.HTTPTransport(retries=1),
        timeout=600.0,
    ) as client:
        yield client


# %%
import random
import statistics
import time
from uuid import uuid4

import orjson


def gen_id():
    return str(random.randint(0, 4)) + str(uuid4())[1:]


def run_test(target, limit, endpoint):
    with get_client() as client:
        total = 0
        call_times = []
        start = time.time()
        while total < target:
            if total % 100000 == 0:
                print(".", end="")

            call_start = time.time()
            res = client.get(
                endpoint,
                params={
                    "query": f'id>="{gen_id()}"',
                    "limit": limit,
                },
            )
            res.raise_for_status()
            orjson.loads(res.text)
            call_times.append(time.time() - call_start)

            total += limit

        return (
            time.time() - start,
            min(call_times),
            statistics.mean(call_times),
            statistics.median(call_times),
            max(call_times),
        )


# %%
import polars as pl

rs = {
    "endpoint": pl.Utf8,
    "limit": pl.Int32,
    "total": pl.Float32,
    "per_100k_avg": pl.Float32,
    "per_100k_med": pl.Float32,
}
endpoint = "/inventory/instances"
results = pl.DataFrame([], schema=rs)
for l in range(0, 50001, 10000):
    if l == 0:
        l = 1000
    for i in range(0, 5):
        print(l, " ", end="")
        res = run_test(500000, l, endpoint)
        results.vstack(
            pl.DataFrame(
                [
                    [
                        endpoint,
                        l,
                        res[0] / 60,
                        (100000 / l) * (res[2] / 60),
                        (100000 / l) * (res[3] / 60),
                    ]
                ],
                orient="row",
                schema=rs,
            ),
            in_place=True,
        )
        print()
        
results.glimpse()
results.write_csv("results.csv")

# %%
