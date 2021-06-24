import pandas as pd
import re

from Levenshtein.StringMatcher import StringMatcher
from typing import Dict, List, Optional, Set, Text, Tuple


def tokenize(s: Text):
  """Tokenize input string."""
  tokens = list()
  for token in re.split('[^a-zA-Z]', s.lower()):
    if token:
      tokens.append(token)
  return tokens


class ScorerInterface(object):
  def score(self, query: Text, result: Text) -> Optional[float]:
    pass

  def score_df_result(
      self, query: Text, df: pd.DataFrame, field: Text, index: int
  ) -> Optional[float]:
    result = df.iloc[index][field]
    if result:
      return self.score(query, result)
    return None

  def score_df_results(
      self, query: Text, df: pd.DataFrame, field: Text, indexes: Set[int]
  ) -> List[Tuple[int, float]]:
    scored_results: List[Tuple[int, float]] = list()
    for index in indexes:
      result = df.iloc[index][field]
      if result:
        scored_results.append((index, self.score(query, result)))
    scored_results.sort(key=lambda x: -x[1])
    return scored_results


class TokenJaccardScorer(ScorerInterface):
  def score(self, query: Text, result: Text) -> Optional[float]:
    a = set(tokenize(query))
    b = set(tokenize(result))
    intersection = len(a.intersection(b))
    union = (len(a) + len(b)) - intersection
    return float(intersection) / union


class LevenshteinRatioScorer(ScorerInterface):
  def __init__(self):
    self.matcher = StringMatcher()

  def score(self, query: Text, result: Text) -> Optional[float]:
    self.matcher.set_seq1(query)
    self.matcher.set_seq2(result)
    return self.matcher.ratio()


class DataFrameTextSearchInvertedIndex(object):
  def __init__(
      self,
      df: pd.DataFrame, fields_to_index: List[Text], tokenize_fn=tokenize,
      # In case of exact matches.
      key_whole_docs: bool=True,
      stop_words: Set[Text]=frozenset()
  ):
    self.tokenize_fn = tokenize_fn
    # Each field gets an inverted index.
    self.inverted_indexes: Dict[Text, Dict[Text, Set[int]]] = {
        field: dict() for field in fields_to_index}
    for index, row in df.iterrows():
      for field in fields_to_index:
        # Drop empty docs.
        if not row[field] or not isinstance(row[field], str):
          continue

        # Add the tokens to the inverted index.
        tokens: Set[Text] = set(self.tokenize_fn(row[field]))
        tokens.difference(stop_words)
        for token in tokens:
          self.inverted_indexes[field].setdefault(token, set()).add(index)

        # In order to support exact match retrieval, index the whole doc.
        if key_whole_docs:
          self.inverted_indexes[field].setdefault(row[field], set()).add(index)

  def get(self, query: Text, field: Text) -> Set[int]:
    """Given a query, return all indexes into the DataFrame where the field
    value contains a query term.

    Args:
      query: to search for in the field of the DataFrame.
      field: in which to search for the query.

    Returns:
      List of indexes in the DataFrame with matching results.
    """
    results = self.inverted_indexes[field].get(query, set())
    if results:
      return results
    tokens = set(self.tokenize_fn(query))
    for token in tokens:
      results.update(self.inverted_indexes[field].get(token, set()))
    return results

  def get_inverted_index(self, field: Text) -> Dict[Text, Set[int]]:
    return self.inverted_indexes[field]
