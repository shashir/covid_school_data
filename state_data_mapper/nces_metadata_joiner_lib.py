import pandas as pd

from typing import Dict, List, Text

def comma_separated_multilookup(map: Dict[Text, Text], key: Text):
  if pd.isna(key):
    return None
  keys: List[Text] = key.split(",")
  values: List[Text] = []
  for k in keys:
    if k in map:
      values.append(map[k])
  if len(set(values)) == 1:
    return values[0]
  return ",".join(values)

def join_nces_metadata_for_schools(
    case_data_df,
    nces_school_metadata_df,
    nces_district_metadata_df):
  district_type_lookup = {
      k: v
      for k, v in nces_district_metadata_df[
        ["NCESDistrictID", "DistrictType"]].dropna().values
  }
  case_data_df["DistrictType"] = case_data_df["NCESDistrictID"].map(
      lambda key: comma_separated_multilookup(district_type_lookup, key))

  charter_lookup = {
      k: v
      for k, v in nces_school_metadata_df[
        ["NCESSchoolID", "Charter"]].dropna().values
  }
  case_data_df["Charter"] = case_data_df["NCESSchoolID"].map(
      lambda key: comma_separated_multilookup(charter_lookup, key))

  school_type_lookup = {
      k: v
      for k, v in nces_school_metadata_df[
        ["NCESSchoolID", "SchoolType"]].dropna().values
  }
  case_data_df["SchoolType"] = case_data_df["NCESSchoolID"].map(
      lambda key: comma_separated_multilookup(school_type_lookup, key))

  return case_data_df

def join_nces_metadata_for_districts(case_data_df, nces_district_metadata_df):
  district_type_lookup = {
      k: v
      for k, v in nces_district_metadata_df[
        ["NCESDistrictID", "DistrictType"]].dropna().values
  }
  case_data_df["DistrictType"] = case_data_df["NCESDistrictID"].map(
      lambda key: comma_separated_multilookup(district_type_lookup, key))

  charter_lookup = {
      k: v
      for k, v in nces_district_metadata_df[
        ["NCESDistrictID", "Charter"]].dropna().values
  }
  case_data_df["Charter"] = case_data_df["NCESDistrictID"].map(
      lambda key: comma_separated_multilookup(charter_lookup, key))

  return case_data_df
