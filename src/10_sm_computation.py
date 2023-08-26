import pandas as pd
import numpy as np
from glob import glob
from pathlib import Path

path_csv = glob("/home/satyukt/Desktop/RAHUL/csv/*.csv")
out_path = "/home/satyukt/Desktop/RAHUL/csv/"

for file in path_csv:
    name = Path(file).stem.split("_")[2].split(".")[0].lower()
    df = pd.read_csv(file)
   
    df["Date"] = df["Date"].apply(lambda x: str(x))
    df["Date"] = pd.to_datetime(df["Date"])
    max = df["vv_value"].max()
    min = df["vv_value"].min()
    df["Soil_Moisture"] = df["vv_value"].apply(lambda x: ((x - min)/(max - min)))
    df.to_csv(f"{out_path}{name}_sm.csv", index=False)


