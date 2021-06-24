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
    "VT": "Vermont"
}

SEARCH_STOP_WORDS = {"school", "district", "high", "middle", "elementary",
                     "academy", "charter"}


def trigger_school_name_results(
    query, schools_df, schools_search_index, threshold=0.3, num_results=1
):
  results = schools_search_index.get(query, "sch_name")
  jaccard_scorer = data_frame_text_search_lib.TokenJaccardScorer()
  levenshtein_scorer = data_frame_text_search_lib.LevenshteinRatioScorer()
  scored_results = list()
  for result in results:
    hmean_scores = statistics.harmonic_mean([
        jaccard_scorer.score_df_result(query, schools_df, "sch_name", result),
        levenshtein_scorer.score_df_result(query, schools_df, "sch_name",
                                           result)
    ])
    if hmean_scores == 1.0:
      return [(result, hmean_scores)]
    if hmean_scores > threshold:
      scored_results.append((result, hmean_scores))
  scored_results.sort(key=lambda x: -x[1])
  return scored_results[:num_results]


def process_state_file(path: Text, nces_schools_df: pd.DataFrame):
  state_abbrev_from_file_name = path.split("/")[-1][:2]
  df = pd.read_csv(path)
  # Validate
  assert (list(set(df["StateAbbrev"]))[0] == state_abbrev_from_file_name)
  assert (list(set(df["State"]))[0] == STATE_ABBREV_MAPPING[
    state_abbrev_from_file_name])
  # Unique
  df.drop_duplicates(inplace=True, ignore_index=True)
  # Get NCES data for state.
  state_schools_df = nces_schools_df[
    nces_schools_df["state"] == state_abbrev_from_file_name]
  state_schools_df.reset_index(drop=True, inplace=True)
  schools_search_index =\
      data_frame_text_search_lib.DataFrameTextSearchInvertedIndex(
          state_schools_df, ["sch_name"])
  output = list()
  progress = ProgressBar(len(df.index), note=state_abbrev_from_file_name)
  for index, row in df.iterrows():
    query = row["SchoolName"]
    scored_results = trigger_school_name_results(
        query, state_schools_df, schools_search_index)
    has_results = False
    for scored_result in scored_results:
      output_row = [row[c] for c in df.columns] +\
                   [state_schools_df.iloc[scored_result[0]][c]
                    for c in state_schools_df.columns] +\
                   [scored_result[1]]
      output.append(output_row)
      has_results = True
    if not has_results:
      output_row = [row[c] for c in df.columns] + \
                   [None for _ in state_schools_df.columns] + \
                   [None]
      output.append(output_row)
    progress.increment()
  return pd.DataFrame(
      output,
      columns=(
          list(df.columns) + list(state_schools_df.columns) + ["match_score"]))
