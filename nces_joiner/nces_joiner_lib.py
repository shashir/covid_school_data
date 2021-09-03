import pandas as pd

from typing import List, Text

def process_fixed_length_codes(codes: Text, length: int) -> Text:
  return ",".join([code.strip().zfill(length) for code in codes.split(",")])

def read_lea_lookup_csv(lookup_files: List[Text]) -> pd.DataFrame:
  lookups = dict()
  drops = set()
  for f in lookup_files:
    df = pd.read_csv(f, dtype={
        "leaid": pd.StringDtype(),
        "is_match": pd.StringDtype(),
        "drop": pd.StringDtype(),
    })
    if "is_match" in df:
      f_lookup = {
          k: process_fixed_length_codes(str(v), 7)
          for k, v in df[["DistrictName", "leaid"]].dropna().values
      }
      lookups.update(f_lookup)
      drop_df = df[df["is_match"].str.strip() == "drop"]
      drop_set = set([x[0] for x in drop_df[["DistrictName"]].values])
      drops.update(drop_set)
    elif "drop" in df:
      keep_df = df[df["drop"].str.strip() == "keep"]
      drop_df = df[df["drop"].str.strip() == "drop"]
      f_lookup = {
          k: process_fixed_length_codes(str(v), 7)
          for k, v in keep_df[["DistrictName", "leaid"]].dropna().values
      }
      lookups.update(f_lookup)
      drop_set = set([x[0] for x in drop_df[["DistrictName"]].values])
      drops.update(drop_set)
  return lookups, drops

def convert_school_ids_to_district_ids(school_ids: Text):
  if str(school_ids) == "<NA>":
    return None
  school_ids = school_ids.split(",")
  district_ids = []
  for school_id in school_ids:
    district_ids.append(school_id[:-5])
  if len(set(district_ids)) == 1:
    return district_ids[0]
  return ",".join(district_ids)

def read_nces_lookup_csv(lookup_files: List[Text]) -> pd.DataFrame:
  lookups = dict()
  drops = set()
  for f in lookup_files:
    df = pd.read_csv(f, dtype={
        "ncessch": pd.StringDtype(),
        "is_match": pd.StringDtype(),
        "drop": pd.StringDtype(),
    })
    if "is_match" in df:
      f_lookup = {k: process_fixed_length_codes(str(v), 12)
                  for k, v in df[["SchoolName", "ncessch"]].dropna().values}
      lookups.update(f_lookup)
      drop_df = df[df["is_match"].str.strip() == "drop"]
      drop_set = set([x[0] for x in drop_df[["SchoolName"]].values])
      drops.update(drop_set)
    elif "drop" in df:
      keep_df = df[df["drop"].str.strip() == "keep"]
      drop_df = df[df["drop"].str.strip() == "drop"]
      f_lookup = {
          k: process_fixed_length_codes(str(v), 12)
          for k, v in keep_df[["SchoolName", "ncessch"]].dropna().values
      }
      lookups.update(f_lookup)
      drop_set = set([x[0] for x in drop_df[["SchoolName"]].values])
      drops.update(drop_set)
  return lookups, drops

def process_state(state_case_df, lookups, drops, process_districts=False):
  if process_districts:
    state_case_df["NCESDistrictID"] = state_case_df["DistrictName"].map(lookups)
    state_case_df = state_case_df[state_case_df["DistrictName"].map(lambda x: x not in drops)]
  else:
    state_case_df["NCESSchoolID"] = state_case_df["SchoolName"].map(lookups)
    # Infer district id from school id.
    state_case_df["NCESDistrictID"] = state_case_df["NCESSchoolID"].astype(
        pd.StringDtype()).map(convert_school_ids_to_district_ids)
    state_case_df = state_case_df[state_case_df["SchoolName"].map(lambda x: x not in drops)]
  return state_case_df
