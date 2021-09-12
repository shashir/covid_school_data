import numpy as np
import pandas as pd
import re

from recordclass import RecordClass
from typing import Any, Dict, List, NamedTuple, Optional, Set, Text


class ColumnMapping(NamedTuple):
  """Defines how to process columns in state data source file."""
  # Processed output file column name.
  target_column: Text
  # Source file column name.
  source_column: Optional[Text] = None
  # Pandas data type (for casting and validation).
  dtype: Any = None
  # Converter function/lambda.
  converter: Any = None
  # Converter dictionary. If provided, will create a converter that maps values
  # according to this dictionary (if the key is present, otherwise keep the
  # original value).
  converter_dict: Dict[Any, Any] = None
  # Constant value for target column (if provided, source_column is ignored).
  constant: Any = None
  # Expected null values in source column.
  na_values: Optional[List] = None
  # Is temporary column. Will be use for intermediate manipulation, but will not
  # be in the final data frame.
  is_temporary: bool = False
  # Calculate this column from other columns.
  calculation: Any = None
  # List of values for rows that need to be filtered out.
  filter_values: List[Any] = None
  # Drop rows from the data frame if this column value is NA.
  dropna: bool = False


class StateConfig(NamedTuple):
  """Contains configuration for a state data source file."""
  # State name, e.g. "Colorado".
  state: Text
  # State abbreviation, e.g. "CO".
  state_abbreviation: Text
  # State data source XLSX filepath. Expects sheet named "Data for {state}".
  source_filepath: Text
  # Processed output CSV filepath.
  target_filepath: Text
  # List of column mapping from source file to target file.
  column_mappings: List[ColumnMapping] = []
  # Source sheet name containing data. If not specified, then read from
  # "Data for {state}".
  source_sheet_name: Optional[Text] = None
  # If more than one, source sheet name(s) containing data.
  # If not specified, then read from "Data for {state}".
  source_sheet_names_list: List[Text] = []
  # If True, then output rows are deduped.
  dedupe_rows: bool = True
  # Filter values file path. XLSX file whose rows with matching values will be
  # filtered out of the case data file.
  filter_values_file: Text = None
  # Filter value file columns to match on.
  filter_values_file_match_on: List[Text] = []
  # Filter value file columns to match on with fuzzy text matching.
  filter_values_file_fuzzy_match_on: List[Text] = []
  nces_id_lookup_file: Text = None
  nces_id_lookup_file_match_on: List[Text] = []
  nces_id_lookup_file_fuzzy_match_on: List[Text] = []

class ColumnReadReport(RecordClass):
  """Report summarizing the processed output column."""
  state: Text               # State name.
  column: Text              # Column name in the processed output.
  dtype: Any                # Pandas data type.
  count: int = None         # Number of non-null values.
  null_count: float = None  # Number of null values.
  min: Any = None           # Minimum.
  max: Any = None           # Maximum.
  mean: Any = None          # Mean (only defined for numeric columns).
  mode: Any = None          # Mode.


def converter_dict_getter(converter_dict: Dict[Any, Any]):
  def get(x: Any):
    return converter_dict.get(x, x)
  return get


def normalize(s: Text) -> Text:
  """Normalize a string for easier matching."""
  if not isinstance(s, str):
    return s
  tokens = list()
  for token in re.split('[^a-zA-Z0-9]', s.lower()):
    if token:
      tokens.append(token.strip())
  return " ".join(tokens)


def left_join(
    left_df: pd.DataFrame,
    right_df: pd.DataFrame,
    on: List[Text],
    on_fuzzy_match_columns: List[Text]):
  """Left join data frames on given columns. Handles string fuzzy match.

  Args:
    left_df: left data frame
    right_df: right data frame.
    on: list of columns to match on.
    on_fuzzy_match_columns: additional string columns to fuzzy match on.

  Returns:
    Left joined data frame.
  """
  assert set(on).issubset(left_df.columns)
  assert set(on).issubset(right_df.columns)
  assert not set(on_fuzzy_match_columns).issubset(on)
  left_df = left_df.copy(deep=True)
  right_df = right_df.copy(deep=True)
  normalized_columns = []
  # Create temporary string columns with normalized versions of the strings for
  # fuzzy matching.
  for column in on_fuzzy_match_columns:
    assert column in left_df.columns
    assert column in right_df.columns
    column_normalized = "_normalized_" + column
    normalized_columns.append(column_normalized)
    left_df[column_normalized] = left_df[column].transform(normalize)
    right_df[column_normalized] = right_df[column].transform(normalize)
    # Drop the original column from the right data frame (we don't need it
    # anymore now that the normalized version of it available.
    right_df = right_df.drop(column, axis=1)

  merged_df = left_df.merge(right_df, on=on + normalized_columns, how="left")
  # Drop the normalized columns from the merged dataframe.
  for column in normalized_columns:
    merged_df.drop(column, axis=1, inplace=True)
  return merged_df


def filter_matching_rows(
    left_df: pd.DataFrame,
    right_df: pd.DataFrame,
    on: List[Text],
    on_fuzzy_match_columns: List[Text],
    filter_out=True):
  """Filter matching rows from left data frame.

  Args:
    left_df: left data frame
    right_df: right data frame.
    on: list of columns to match on.
    on_fuzzy_match_columns: additional string columns to fuzzy match on.
    filter_out: If True, matching rows are filtered out. If False,
                non-matching rows are filtered out.

  Returns:
    Left data frame without matching rows from right data frame.
  """
  right_df = right_df[on + on_fuzzy_match_columns].copy(deep=True)
  right_df["_drop"] = True
  merged_df = left_join(left_df, right_df, on, on_fuzzy_match_columns)
  if filter_out:
    merged_df = merged_df[merged_df["_drop"].isna() |
                          (merged_df["_drop"] == False)]
  else:
    merged_df = merged_df[merged_df["_drop"] == True]
  merged_df.drop("_drop", axis=1, inplace=True)
  return merged_df


def process_fixed_length_codes(codes: Text, length: int) -> Text:
  return ",".join([str(int(float(code.strip()))).zfill(length)
                   for code in codes.split(",")])


def process_state_data(
    state_config: StateConfig,
    required_columns: List[Text]=[]
):
  """Read and process data for a single state based on config.

  Args:
    state_config: configuration for state data source.
    required_columns: List of mandatory output columns. If no data is available,
                      they will be empty in the output.

  Returns:
    (df, report_df) where df is the processed DataFrame and report_df is the
    DataFrame report summarizing the data.
  """

  # Process column mappings configuration.
  source_dtype_map = dict()
  target_dtype_map = dict()
  usecols = list()
  converters = dict()
  constant_columns = dict()
  column_rename_map = dict()
  na_values = dict()
  temporary_columns = list()
  for mapping in state_config.column_mappings:
    if mapping.is_temporary:
      temporary_columns.append(mapping.target_column)

    if mapping.constant:
      constant_columns[mapping.target_column] = mapping.constant
      assert not mapping.source_column, (
          f"For column {mapping.target_column} provide "
          "either constant or source column.")
      if mapping.dtype:
        target_dtype_map[mapping.target_column] = mapping.dtype
      continue
    if not mapping.source_column:
      continue

    # If this is a calculated column, then no need to read it from XLSX.
    if mapping.calculation:
      continue

    # Pandas XLSX read parameters
    usecols.append(mapping.source_column)
    column_rename_map[mapping.source_column] = mapping.target_column
    if mapping.converter_dict:
      converters[mapping.source_column] = converter_dict_getter(
          mapping.converter_dict)
    elif mapping.converter:
      converters[mapping.source_column] = mapping.converter
    elif mapping.dtype:
      source_dtype_map[mapping.source_column] = mapping.dtype
    if mapping.na_values:
      na_values[mapping.source_column] = mapping.na_values

  if state_config.source_sheet_names_list:
    df_list = []
    for sheet_name in state_config.source_sheet_names_list:
      # Read XLSX file.
      partial_df = pd.read_excel(
          state_config.source_filepath,
          # Sheet with this name must exist.
          sheet_name=sheet_name,
          usecols=usecols,  # Drop unmapped columns.
          dtype=source_dtype_map,  # Cast columns
          converters=converters,  # Drop unmapped columns.
          na_values=na_values  # Null values in source file.
      )
      df_list.append(partial_df)
    df = pd.concat(df_list)
  else:
    sheet_name = state_config.source_sheet_name
    if not sheet_name:
      sheet_name = f"Data for {state_config.state}"

    # Read XLSX file.
    df = pd.read_excel(
        state_config.source_filepath,
        # Sheet with this name must exist.
        sheet_name=sheet_name,
        usecols=usecols,  # Drop unmapped columns.
        dtype=source_dtype_map,  # Cast columns
        converters=converters,  # Drop unmapped columns.
        na_values=na_values  # Null values in source file.
    )

  # Rename columns.
  df.rename(columns=column_rename_map, errors="raise", inplace=True)

  # Constant columns.
  for column, value in constant_columns.items():
    df[column] = value
    if column in target_dtype_map:
      df[column] = df[column].astype(target_dtype_map[column])

  # Calculated columns.
  for mapping in state_config.column_mappings:
    if mapping.calculation:
      df[mapping.target_column] = df.apply(mapping.calculation, axis=1).astype(
          mapping.dtype)

  # Reorder columns in order of the config.
  df = df[[mapping.target_column for mapping in state_config.column_mappings]]

  # Validate dtypes.
  for mapping in state_config.column_mappings:
    if mapping.dtype:
      assert df[mapping.target_column].equals(
          df[mapping.target_column].astype(mapping.dtype)), (
          f"Invalid dtype '{mapping.dtype}' for "
          f"column '{mapping.target_column}' which has type "
          f"{df[mapping.target_column].dtype}.")

  # Remove rows that need to be filtered out from filter values in the config.
  for mapping in state_config.column_mappings:
    if mapping.filter_values:
      df = df[~df[mapping.target_column].isin(set(mapping.filter_values))]
    if mapping.dropna:
      df.dropna(subset=[mapping.target_column])

  # Remove rows that need to be filtered out based on the filter XLSX file.
  if state_config.filter_values_file:
    filter_values_df = pd.read_excel(state_config.filter_values_file)
    filter_columns = state_config.filter_values_file_match_on +\
                     state_config.filter_values_file_fuzzy_match_on
    assert filter_columns
    assert set(filter_columns).issubset(df.columns)
    assert set(filter_columns).issubset(filter_values_df.columns)
    df = filter_matching_rows(
        df,
        filter_values_df,
        state_config.filter_values_file_match_on,
        state_config.filter_values_file_fuzzy_match_on)

  # Dates should be mm/dd/YY.
  for mapping in state_config.column_mappings:
    if str(mapping.dtype).startswith("datetime64"):
      df[mapping.target_column] = df[mapping.target_column].dt.strftime(
          "%m/%d/%y")

  # Drop temporary columns.
  df.drop(temporary_columns, axis=1, inplace=True)

  # Dedupe rows.
  if state_config.dedupe_rows:
    df.drop_duplicates(inplace=True)

  # Join NCES IDs
  if state_config.nces_id_lookup_file:
    nces_id_lookup_df = pd.read_excel(
        state_config.nces_id_lookup_file,
        dtype={
            "NCESSchoolID": pd.StringDtype(),
            "NCESDistrictID": pd.StringDtype(),
        })
    # Ensure that the NCES codes are strings.
    if "NCESDistrictID" in nces_id_lookup_df:
      nces_id_lookup_df["NCESDistrictID"] =\
        nces_id_lookup_df["NCESDistrictID"].map(
          lambda value: process_fixed_length_codes(value, 7)
          if pd.notna((value)) else value
        )
    if "NCESSchoolID" in nces_id_lookup_df:
      nces_id_lookup_df["NCESSchoolID"] = nces_id_lookup_df["NCESSchoolID"].map(
          lambda value: process_fixed_length_codes(value, 12)
          if pd.notna((value)) else value
      )
    filter_columns = state_config.nces_id_lookup_file_match_on + \
                     state_config.nces_id_lookup_file_fuzzy_match_on
    assert filter_columns
    assert set(filter_columns).issubset(df.columns)
    assert set(filter_columns).issubset(nces_id_lookup_df.columns)
    df = left_join(
        df,
        nces_id_lookup_df,
        state_config.nces_id_lookup_file_match_on,
        state_config.nces_id_lookup_file_fuzzy_match_on)

  # Create report.
  report_rows = list()
  for column in df:
    column_report = ColumnReadReport(
        state_config.state,
        column,
        dtype=df[column].dtype,
        count=df[column].count(),
        null_count=df[column].isna().sum())
    column_report.column = column
    if df[column].dtype == object:
      column_report.min = df[column].astype("string").min()
      column_report.max = df[column].astype("string").max()
    else:
      column_report.min = df[column].min()
      column_report.max = df[column].max()
    mode = list(df[column].mode())
    column_report.mode = mode[0] if mode else np.NaN
    if pd.api.types.is_numeric_dtype(df[column]):
      column_report.mean = df[column].mean()
    report_rows.append(column_report)

  # Mandatory columns.
  for column in required_columns:
    if column not in df:
      df[column] = pd.Series(dtype="object")
      column_report = ColumnReadReport(
          state_config.state,
          column,
          dtype="object",
          count=df[column].count(),
          null_count=df[column].isna().sum())
      report_rows.append(column_report)

  # Reorder columns in the global column order.
  unexpected_columns = []
  if required_columns:
    for column in required_columns:
      assert(column in df.columns), column
    for column in df.columns:
      if column not in required_columns:
        print("Following column was unexpected:", column)
        unexpected_columns.append(column)
    df = df[required_columns + unexpected_columns]

  report_df = pd.DataFrame.from_records(
      report_rows,
      columns=["state", "column", "dtype", "count",
               "null_count", "min", "max", "mean", "mode"])

  # Write file.
  if state_config.target_filepath:
    df.to_csv(state_config.target_filepath, index=False)

  return df, report_df


def process_all_states(
    config: List[StateConfig],
    report_filepath: Text=None,
    required_columns: List[Text]=[],
    states_to_process: List[Text]=[]
):
  """Process all states in list of configs.

  Args:
    config: List of StateConfigs defining how to process each file.
    report_filepath: If provided, then write the processed data summary
                     report to this path.
    required_columns: List of mandatory output columns. If no data is available,
                      they will be empty in the output.
    states_to_process: List of states to process from the config. If not
                       provided, then all states from the config will be
                       processed.

  Returns:
    (state_dfs, state_report_dfs): state_dfs is the list of processed state
    DataFrames and state_report_dfs is the list corresponding reports.
  """
  states_to_process = [state.lower() for state in states_to_process]
  state_dfs = list()
  state_report_dfs = list()
  for state_config in config:
    if states_to_process:
      if state_config.state.lower() not in states_to_process and \
          state_config.state_abbreviation.lower() not in states_to_process:
        continue
    print(f"Reading {state_config.state} from {state_config.source_filepath}.")
    df, report_df = process_state_data(state_config, required_columns)
    state_dfs.append(df)
    state_report_dfs.append(report_df)
    pd.set_option("display.max_columns", None, "display.width", 300)
  if report_filepath:
    print(f"Writing processed read reports to {report_filepath}...")
    pd.concat(state_report_dfs).to_csv(report_filepath, index=False)
  return state_dfs, state_report_dfs


def read_config_file(config_filepath: Text):
  """Read config file."""
  with open(config_filepath, "r") as f:
    contents = f.read()
    return eval(contents)
