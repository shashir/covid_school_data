import pandas as pd
import statistics

from progressbar.progressbar import ProgressBar
from school_name_matcher import data_frame_text_search_lib
from typing import Text

STATE_ABBREV_MAPPING = {
    "CO": "Colorado",
    "IA": "Iowa",
    "IL": "Illinois",
    "KS": "Kansas",
    "MD": "Maryland",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "NC": "North Carolina",
    "OR": "Oregon",
    "VA": "Virginia",
    "WV": "West Virginia",
    "CT": "Connecticut",
    "ID": "Idaho",
    "IN": "Indiana",
    "KY": "Kentucky",
    "ME": "Maine",
    "MO": "Missouri",
    "MT": "Montana",
    "NH": "New Hampshire",
    "RI": "Rhode Island",
    "VT": "Vermont",
    "NY": "New York",
    "TX": "Texas",
    "FL": "Florida",
    "LA": "Louisiana",
    "AR": "Arkansas",
    "DE": "Delaware",
    "IA": "Iowa",
    "KY": "Kentucky",
    "MA": "Massachusetts",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "UT": "Utah",
    "WI": "Wisconsin",
}

SEARCH_STOP_WORDS = {"school", "district", "high", "middle", "elementary",
                     "academy", "charter"}


def trigger_name_results(
    query, filtered_nces_df, search_index, process_districts,
    threshold=0.3, num_results=1):
  results = search_index.get(query)
  nces_column_to_match = "district_name" if process_districts else "school_name"
  weighted_jaccard_scorer = \
    data_frame_text_search_lib.WeightedTokenJaccardScorer(
        filtered_nces_df[nces_column_to_match])
  jaccard_scorer = data_frame_text_search_lib.TokenJaccardScorer()
  levenshtein_scorer = data_frame_text_search_lib.LevenshteinRatioScorer()
  scored_results = list()
  for result in results:
    hmean_scores = statistics.harmonic_mean([
        weighted_jaccard_scorer.score_df_result(
            query, filtered_nces_df, nces_column_to_match, result),
        # jaccard_scorer.score_df_result(query, filtered_nces_df,
      # nces_column_to_match, result),
        levenshtein_scorer.score_df_result(
            query, filtered_nces_df, nces_column_to_match, result)
    ])
    if hmean_scores == 1.0:
      return [(result, hmean_scores)]
    if hmean_scores > threshold:
      scored_results.append((result, hmean_scores))
  scored_results.sort(key=lambda x: -x[1])
  return scored_results[:num_results]


def read_state_level_csv(path: Text) -> pd.DataFrame:
  """Reads state level file containing either SchoolName or DistrictName for a
  given state from CSV.
  """
  state_abbrev_from_file_name = path.split("/")[-1][:2]
  df = pd.read_csv(path)
  # Validate
  assert (list(set(df["StateAbbrev"]))[0] == state_abbrev_from_file_name), (
    "State abbreviation '%s' doesn't match state name '%s'" % (
    list(set(df["StateAbbrev"]))[0], state_abbrev_from_file_name))
  assert (list(set(df["StateName"]))[0] == STATE_ABBREV_MAPPING[
    state_abbrev_from_file_name])
  # Must contain SchoolName or DistrictName column.
  assert "SchoolName" in df.columns or "DistrictName" in df.columns
  # Unique
  df.drop_duplicates(inplace=True, ignore_index=True)
  return df


def best_effort_merge_nces_data(
    state_df: pd.DataFrame, nces_df: pd.DataFrame,
    process_districts: bool = False
) -> pd.DataFrame:
  state_abbrev = list(set(state_df["StateAbbrev"]))[0]
  # Get NCES data for state.
  filtered_nces_df = nces_df[nces_df["state_location"] == state_abbrev]
  filtered_nces_df.reset_index(drop=True, inplace=True)
  search_index = data_frame_text_search_lib.DataFrameTextSearchInvertedIndex(
    filtered_nces_df,
    "district_name" if process_districts else "school_name")
  output = list()
  progress = ProgressBar(len(state_df.index), note=state_abbrev)
  for index, row in state_df.iterrows():
    query = row["DistrictName" if process_districts else "SchoolName"]
    if pd.isna(query):
      continue
    scored_results = trigger_name_results(
        query, filtered_nces_df, search_index, process_districts)
    has_results = False
    for scored_result in scored_results:
      output_row = [row[c] for c in state_df.columns] +\
                   [filtered_nces_df.iloc[scored_result[0]][c]
                    for c in filtered_nces_df.columns] +\
                   [scored_result[1]]
      output.append(output_row)
      has_results = True
    if not has_results:
      output_row = [row[c] for c in state_df.columns] + \
                   [None for _ in filtered_nces_df.columns] + \
                   [None]
      output.append(output_row)
    progress.increment()
  progress.complete()
  output_df = pd.DataFrame(
      output,
      columns=(
          list(state_df.columns) + list(filtered_nces_df.columns) + [
          "match_score"]))
  output_df.sort_values(by=["match_score"], inplace=True)
  return output_df
