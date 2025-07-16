# Mimsy Measurements

A small utility for migrating measurement data to Qi

## Using

First install python and miniconda. Then create and activate a new environment by running
```sh
conda env create -f environment.yml

conda activate mimsy-measurements
```
If you're using windows you will have to use `conda.exe` instead of `conda`.

Then create a copy of the existing `.env.template` file named `.env`.
Fill out the information in this file with the correct Mimsy Connection information.

If all has gone well you can run
```sh
python MeasurementsParsing.py
```
If you're using windows you will have to use `python.exe` instead of `python`.

Once the script has finished running you can find the results in the output folder.
There will be a subfolder with the curent date and time the script was run.
Inside that folder are two csv files indicating Mimsy records that require further investigation.
