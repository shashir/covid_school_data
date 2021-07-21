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
}

SEARCH_STOP_WORDS = {"school", "district", "high", "middle", "elementary",
                     "academy", "charter"}


def trigger_school_name_results(
    query, schools_df, schools_search_index, threshold=0.3, num_results=1
):
  results = schools_search_index.get(query)
  weighted_jaccard_scorer = \
    data_frame_text_search_lib.WeightedTokenJaccardScorer(
        schools_df["sch_name"])
  jaccard_scorer = data_frame_text_search_lib.TokenJaccardScorer()
  levenshtein_scorer = data_frame_text_search_lib.LevenshteinRatioScorer()
  scored_results = list()
  for result in results:
    hmean_scores = statistics.harmonic_mean([
        weighted_jaccard_scorer.score_df_result(
            query, schools_df, "sch_name", result),
        # jaccard_scorer.score_df_result(query, schools_df, "sch_name", result),
        levenshtein_scorer.score_df_result(
            query, schools_df, "sch_name", result)
    ])
    if hmean_scores == 1.0:
      return [(result, hmean_scores)]
    if hmean_scores > threshold:
      scored_results.append((result, hmean_scores))
  scored_results.sort(key=lambda x: -x[1])
  return scored_results[:num_results]


def read_state_schools_csv(path: Text) -> pd.DataFrame:
  """Reads school names for a given state from CSV."""
  state_abbrev_from_file_name = path.split("/")[-1][:2]
  df = pd.read_csv(path)
  # Validate
  assert (list(set(df["StateAbbrev"]))[0] == state_abbrev_from_file_name)
  assert (list(set(df["State"]))[0] == STATE_ABBREV_MAPPING[
    state_abbrev_from_file_name])
  # Unique
  df.drop_duplicates(inplace=True, ignore_index=True)
  return df


def best_effort_merge_nces_data(
    state_df: pd.DataFrame, nces_schools_df: pd.DataFrame
) -> pd.DataFrame:
  state_abbrev = list(set(state_df["StateAbbrev"]))[0]
  # Get NCES data for state.
  state_schools_df = nces_schools_df[
    nces_schools_df["state"] == state_abbrev]
  state_schools_df.reset_index(drop=True, inplace=True)
  schools_search_index =\
      data_frame_text_search_lib.DataFrameTextSearchInvertedIndex(
          state_schools_df, "sch_name")
  output = list()
  progress = ProgressBar(len(state_df.index), note=state_abbrev)
  for index, row in state_df.iterrows():
    query = row["SchoolName"]
    if pd.isna(query):
      continue
    scored_results = trigger_school_name_results(
        query, state_schools_df, schools_search_index)
    has_results = False
    for scored_result in scored_results:
      output_row = [row[c] for c in state_df.columns] +\
                   [state_schools_df.iloc[scored_result[0]][c]
                    for c in state_schools_df.columns] +\
                   [scored_result[1]]
      output.append(output_row)
      has_results = True
    if not has_results:
      output_row = [row[c] for c in state_df.columns] + \
                   [None for _ in state_schools_df.columns] + \
                   [None]
      output.append(output_row)
    progress.increment()
  progress.complete()
  return pd.DataFrame(
      output,
      columns=(
          list(state_df.columns) + list(state_schools_df.columns) + [
          "match_score"]))
