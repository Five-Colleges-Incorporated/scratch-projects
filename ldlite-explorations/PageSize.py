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

from dotenv import load_dotenv

load_dotenv(override=True)
# env = "DEV"
env = "PROD"
fep = os.getenv(f"FOLIO_ENDPOINT_{env}")
fte = os.getenv(f"FOLIO_TENANT_{env}")
fun = os.getenv(f"FOLIO_USER_{env}")
fpw = os.getenv(f"FOLIO_PASSWORD_{env}")
