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
import statistics
import random
import time
from uuid import uuid4

import orjson

target = 500000
limit = 1000
with get_client() as client:

    def gen_id():
        id = str(uuid4())
        id[0] = random.rand_int(0, 4)
        return id

    total = 0
    start = time.time()
    call_times = []
    while total < target:
        if total % 100000 == 0:
            print(".", end="")
            
        call_start = time.time()
        res = client.get(
            "/inventory/instances",
            params={
                "query": f'id>="{gen_id()}"',
                "limit": limit,
            },
        )
        res.raise_for_status()
        orjson.loads(res.text)
        call_times.append(time.time() - call_start)
        
        total += limit

print()
print("total", time.time() - start)
print("average", statistics.mean(call_times))
print("median", statistics.median(call_times))

# %%
