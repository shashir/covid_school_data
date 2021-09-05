import pandas as pd
import re

from typing import List, Text

def process_fixed_length_codes(codes: Text, length: int) -> Text:
  return ",".join([str(int(float(code.strip()))).zfill(length)
                   for code in codes.split(",")])

def normalize(s):
  if not isinstance(s, str):
    print(s)
    return ""
  tokens = list()
  for token in re.split('[^a-zA-Z]', s.lower()):
    if token:
      tokens.append(token)
  return " ".join(tokens)

def convert_school_ids_to_district_ids(school_ids: Text):
  if pd.isna(school_ids):
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
  for f in lookup_files:
    df = pd.read_csv(f, dtype={
        "school_name": pd.StringDtype(),
        "lea_name": pd.StringDtype(),
        "ncessch": pd.StringDtype(),
    })
    f_lookup = {
        (normalize(s), normalize(l)) : process_fixed_length_codes(str(n), 12)
        for s, l, n in df[
          ["school_name", "lea_name", "ncessch"]].dropna().values
    }
    lookups.update(f_lookup)
  return lookups

def process_state(state_case_df, lookups):
  state_case_df["NCESSchoolID"] = state_case_df.apply(
      lambda row: lookups.get(
          (normalize(row["SchoolName"]), normalize(row["DistrictName"]))),
      axis=1).astype(pd.StringDtype()).combine_first(
          state_case_df["NCESSchoolID"])
  # Infer district id from school id.
  state_case_df["NCESDistrictID"] = state_case_df["NCESSchoolID"].astype(
      pd.StringDtype()).map(convert_school_ids_to_district_ids)
  return state_case_df
