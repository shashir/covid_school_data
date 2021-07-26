import pandas as pd
import statistics

from progressbar.progressbar import ProgressBar
from typing import List, Text

def read_lea_lookup_csv(lookup_files: List[Text]) -> pd.DataFrame:
  lookups = dict()
  drops = set()
  for f in lookup_files:
    df = pd.read_csv(f, dtype={"leaid": pd.StringDtype()})
    if "is_match" in df:
      f_lookup = {k: v for k, v in df[["DistrictName", "leaid"]].dropna().values}
      lookups.update(f_lookup)
      drop_df = df[df["is_match"] == "drop"]
      drop_set = set([x[0] for x in drop_df[["DistrictName"]].values])
      drops.update(drop_set)
    elif "drop" in df:
      keep_df = df[df["drop"] == "keep"]
      drop_df = df[df["drop"] == "drop"]
      f_lookup = {k: v for k, v in keep_df[["DistrictName", "leaid"]].dropna().values}
      lookups.update(f_lookup)
      drop_set = set([x[0] for x in drop_df[["DistrictName"]].values])
      drops.update(drop_set)
  return lookups, drops

def read_nces_lookup_csv(lookup_files: List[Text]) -> pd.DataFrame:
  lookups = dict()
  drops = set()
  for f in lookup_files:
    df = pd.read_csv(f, dtype={"ncessch": pd.StringDtype()})
    if "is_match" in df:
      f_lookup = {k: v for k, v in df[["SchoolName", "ncessch"]].dropna().values}
      lookups.update(f_lookup)
      drop_df = df[df["is_match"] == "drop"]
      drop_set = set([x[0] for x in drop_df[["SchoolName"]].values])
      drops.update(drop_set)
    elif "drop" in df:
      keep_df = df[df["drop"] == "keep"]
      drop_df = df[df["drop"] == "drop"]
      f_lookup = {k: v for k, v in keep_df[["SchoolName", "ncessch"]].dropna().values}
      lookups.update(f_lookup)
      drop_set = set([x[0] for x in drop_df[["SchoolName"]].values])
      drops.update(drop_set)
  return lookups, drops

def process_state(state_case_df, lookups, drops, process_districts=False):
  if process_districts:
    state_case_df["NCESDistrictID"] = state_case_df["DistrictName"].map(lookups)
    state_case_df = state_case_df[state_case_df["DistrictName"].map(lambda x: x not in drops)].reset_index()
  else:
    state_case_df["NCESSchoolID"] = state_case_df["SchoolName"].map(lookups)
    state_case_df = state_case_df[state_case_df["SchoolName"].map(lambda x: x not in drops)].reset_index()
  return state_case_df
