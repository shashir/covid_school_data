import numpy as np
import pandas as pd

from recordclass import RecordClass
from typing import Any, List, NamedTuple, Optional, Text


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
  # Constant value for target column (if provided, source_column is ignored).
  constant: Any = None
  # Expected null values in source column.
  na_values: Optional[List] = None


class NCESMergeConfig(NamedTuple):
  """Defines how to merge NCES data."""
  # Left column to join NCES data with.
  nces_join_left_on: Optional[Text] = None
  # Whether to transform left on.
  nces_join_left_on_transformation: Any = lambda x: x
  # Right column to join NCES data with.
  nces_join_right_on: Optional[Text] = None
  # Whether to transform right on.
  nces_join_right_on_transformation: Any = lambda x: x


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
  # NCES merge configuration for school names.
  nces_schools_merge_config: Optional[NCESMergeConfig] = None
  # NCES merge configuration for district names.
  nces_districts_merge_config: Optional[NCESMergeConfig] = None
  # Source sheet name containing data. If not specified, then read from "Data for {state}".
  source_sheet_name: Optional[Text] = None
  # If more than one, source sheet name(s) containing data.
  # If not specified, then read from "Data for {state}".
  source_sheet_names_list: List[Text] = []

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


def merge_nces_schools(
    state_data_df: pd.DataFrame,
    filtered_schools_df: pd.DataFrame,
    nces_schools_merge_config: NCESMergeConfig):
  state_data_df["left_join_key"] = state_data_df[nces_schools_merge_config.nces_join_left_on]
  state_data_df["left_join_key"] = state_data_df["left_join_key"].apply(
      nces_schools_merge_config.nces_join_left_on_transformation)
  filtered_schools_df["right_join_key"] = filtered_schools_df[
    nces_schools_merge_config.nces_join_right_on]
  filtered_schools_df["right_join_key"] = \
    filtered_schools_df["right_join_key"].apply(
        nces_schools_merge_config.nces_join_right_on_transformation)
  state_data_df = state_data_df.merge(filtered_schools_df,
                                      how="left",
                                      left_on="left_join_key",
                                      right_on="right_join_key")
  state_data_df.drop(["left_join_key", "right_join_key"], axis=1, inplace=True)
  return state_data_df


def merge_nces_districts(
    state_data_df: pd.DataFrame,
    filtered_districts_df: pd.DataFrame,
    nces_districts_merge_config: NCESMergeConfig):
  state_data_df["left_join_key"] = state_data_df[nces_districts_merge_config.nces_join_left_on]
  state_data_df["left_join_key"] = state_data_df["left_join_key"].apply(
      nces_districts_merge_config.nces_join_left_on_transformation)
  filtered_districts_df["right_join_key"] = filtered_districts_df[
    nces_districts_merge_config.nces_join_right_on]
  filtered_districts_df["right_join_key"] = \
    filtered_districts_df["right_join_key"].apply(
        nces_districts_merge_config.nces_join_right_on_transformation)
  state_data_df = state_data_df.merge(filtered_districts_df,
                how="left",
                left_on="left_join_key",
                right_on="right_join_key")
  state_data_df.drop(["left_join_key", "right_join_key"], axis=1, inplace=True)
  return state_data_df


def process_state_data(
    state_config: StateConfig,
    nces_schools_df: Optional[pd.DataFrame]=None,
    nces_districts_df: Optional[pd.DataFrame]=None):
  """Read and process data for a single state based on config.

  Args:
    state_config: configuration for state data source.
    nces_schools_df: Optional DataFrame containing NCES school data.
    nces_districts_df: Optional DataFrame containing NCES district data.

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
  for mapping in state_config.column_mappings:
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
    usecols.append(mapping.source_column)
    column_rename_map[mapping.source_column] = mapping.target_column
    if mapping.converter:
      converters[mapping.source_column] = mapping.converter
    elif mapping.dtype:
      source_dtype_map[mapping.source_column] = mapping.dtype
    if mapping.na_values:
      na_values[mapping.source_column] = mapping.na_values

  df = None
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

  # Constant columns
  for column, value in constant_columns.items():
    df[column] = value
    if column in target_dtype_map:
      df[column] = df[column].astype(target_dtype_map[column])

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

  # Join NCES district data if available.
  if state_config.nces_districts_merge_config and nces_districts_df is not None:
    filtered_districts_df = nces_districts_df[
      nces_districts_df["state"] == state_config.state_abbreviation]
    filtered_districts_df = filtered_districts_df[[
        "district_name", "state_leaid", "leaid"]]
    df = merge_nces_districts(df, filtered_districts_df,
                              state_config.nces_districts_merge_config)
  if state_config.nces_schools_merge_config and nces_schools_df is not None:
    filtered_schools_df = nces_schools_df[
      nces_schools_df["state"] == state_config.state_abbreviation]
    filtered_schools_df = filtered_schools_df[[
        "sch_name", "ncessch", "state_schid"]]
    df = merge_nces_schools(df, filtered_schools_df,
                            state_config.nces_schools_merge_config)

  # Write file.
  if state_config.target_filepath:
    df.to_csv(state_config.target_filepath, index=False)

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
    column_report.min = df[column].min()
    column_report.max = df[column].max()
    column_report.mode = df[column].mode()[0]
    if pd.api.types.is_numeric_dtype(df[column]):
      column_report.mean = df[column].mean()
    report_rows.append(column_report)
  report_df = pd.DataFrame.from_records(
      report_rows,
      columns=["state", "column", "dtype", "count",
               "null_count", "min", "max", "mean", "mode"])

  return df, report_df


def process_all_states(
    config: List[StateConfig],
    report_filepath: Text=None,
    nces_schools_df: Optional[pd.DataFrame]=None,
    nces_districts_df: Optional[pd.DataFrame]=None):
  """Process all states in list of configs.

  Args:
    config: List of StateConfigs defining how to process each file.
    report_filepath: If provided, then write the processed data summary
                     report to this path.
    nces_schools_df: Optional DataFrame containing NCES school data.
    nces_districts_df: Optional DataFrame containing NCES district data.

  Returns:
    (state_dfs, state_report_dfs): state_dfs is the list of processed state
    DataFrames and state_report_dfs is the list corresponding reports.
  """
  state_dfs = list()
  state_report_dfs = list()
  for state_config in config:
    print(f"Reading {state_config.state} from {state_config.source_filepath}.")
    df, report_df = process_state_data(
        state_config, nces_schools_df, nces_districts_df)
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
