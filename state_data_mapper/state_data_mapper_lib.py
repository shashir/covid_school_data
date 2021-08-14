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


def process_state_data(
    state_config: StateConfig,
    required_columns: List[Text]=[]):
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

  # Constant columns.
  for column, value in constant_columns.items():
    df[column] = value
    if column in target_dtype_map:
      df[column] = df[column].astype(target_dtype_map[column])

  # Mandatory columns.
  for column in required_columns:
    if column not in df:
      df[column] = pd.Series(dtype="object")

  # Reorder columns in order of the config.
  df = df[(
    [mapping.target_column for mapping in state_config.column_mappings] +
    required_columns)]

  # Validate dtypes.
  for mapping in state_config.column_mappings:
    if mapping.dtype:
      assert df[mapping.target_column].equals(
          df[mapping.target_column].astype(mapping.dtype)), (
          f"Invalid dtype '{mapping.dtype}' for "
          f"column '{mapping.target_column}' which has type "
          f"{df[mapping.target_column].dtype}.")

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
    mode = list(df[column].mode())
    column_report.mode = mode[0] if mode else np.NaN
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
    required_columns: List[Text]=[]):
  """Process all states in list of configs.

  Args:
    config: List of StateConfigs defining how to process each file.
    report_filepath: If provided, then write the processed data summary
                     report to this path.
    required_columns: List of mandatory output columns. If no data is available,
                      they will be empty in the output.

  Returns:
    (state_dfs, state_report_dfs): state_dfs is the list of processed state
    DataFrames and state_report_dfs is the list corresponding reports.
  """
  state_dfs = list()
  state_report_dfs = list()
  for state_config in config:
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
