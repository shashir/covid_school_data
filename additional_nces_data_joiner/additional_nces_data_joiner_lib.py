import pandas as pd

from typing import Dict, List, Text

def format_id(id: Text, length: int) -> Text:
  return str(int(float(id))).strip().zfill(length)

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

def join_nces_school_data(
    state_case_df,
    nces_school_demographics_df,
    nces_district_demographics_df):
  # state_leaid_lookup = {
  #     format_id(k, 12): v
  #     for k, v in nces_school_demographics_df[
  #       ["ncessch_num", "state_leaid"]].dropna().values
  # }
  # state_case_df["StateAssignedDistrictID"] = state_case_df["NCESSchoolID"].map(
  #     lambda key: comma_separated_multilookup(state_leaid_lookup, key))

  # Unfortunately, we can only look up district type using
  # nces_district_demographics_df
  agency_type_lookup = {
      format_id(k, 7): v
      for k, v in nces_district_demographics_df[
        ["leaid", "agency_type"]].dropna().values
  }
  state_case_df["DistrictType"] = state_case_df["NCESDistrictID"].map(
      lambda key: comma_separated_multilookup(agency_type_lookup, key))

  # We can only look up DistrictName using nces_district_demographics_df
  lea_name_lookup = {
      format_id(k, 7): v
      for k, v in nces_district_demographics_df[
        ["leaid", "lea_name"]].dropna().values
  }
  # Do not override DistrictName if it already exists.
  state_case_df["DistrictName"] = state_case_df["DistrictName"].combine_first(
      state_case_df["NCESDistrictID"].map(
      lambda key: comma_separated_multilookup(lea_name_lookup, key)))

  charter_lookup = {
      format_id(k, 12): v
      for k, v in nces_school_demographics_df[
        ["ncessch_num", "charter"]].dropna().values
  }
  state_case_df["Charter"] = state_case_df["NCESSchoolID"].map(
      lambda key: comma_separated_multilookup(charter_lookup, key))

  school_type_lookup = {
      format_id(k, 12): v
      for k, v in nces_school_demographics_df[
        ["ncessch_num", "school_type"]].dropna().values
  }
  state_case_df["SchoolType"] = state_case_df["NCESSchoolID"].map(
      lambda key: comma_separated_multilookup(school_type_lookup, key))

  # seasch_type_lookup = {
  #     format_id(k, 12): v
  #     for k, v in nces_school_demographics_df[
  #       ["ncessch_num", "seasch"]].dropna().values
  # }
  # state_case_df["StateAssignedSchoolID"] = state_case_df["NCESSchoolID"].map(
  #     lambda key: comma_separated_multilookup(seasch_type_lookup, key))
  return state_case_df

def join_nces_district_data(state_case_df, nces_df):
  # state_leaid_lookup = {
  #     format_id(k, 7): v
  #     for k, v in nces_df[["leaid", "state_leaid"]].dropna().values
  # }
  # state_case_df["StateAssignedDistrictID"] = state_case_df[
  #   "NCESDistrictID"
  # ].map(lambda key: comma_separated_multilookup(state_leaid_lookup, key))

  agency_type_lookup = {
      format_id(k, 7): v
      for k, v in nces_df[["leaid", "agency_type"]].dropna().values
  }
  state_case_df["DistrictType"] = state_case_df["NCESDistrictID"].map(
      lambda key: comma_separated_multilookup(agency_type_lookup, key))

  charter_lookup = {
      format_id(k, 7): v
      for k, v in nces_df[["leaid", "charter"]].dropna().values
  }
  state_case_df["Charter"] = state_case_df["NCESDistrictID"].map(
      lambda key: comma_separated_multilookup(charter_lookup, key))

  return state_case_df
