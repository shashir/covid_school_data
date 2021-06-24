import math
import pandas as pd
import re

from Levenshtein.StringMatcher import StringMatcher
from typing import Dict, List, Optional, Set, Text


def tokenize(s: Text) -> List[Text]:
  """Tokenize input string."""
  if not isinstance(s, str):
    print(s)
  tokens = list()
  for token in re.split('[^a-zA-Z]', s.lower()):
    if token:
      tokens.append(token)
  return tokens


class ScorerInterface(object):
  """Interface for scoring text matches in a DataFrame column."""

  def score(self, query: Text, result: Text) -> float:
    """Score a query text against a result text."""
    pass

  def score_df_result(
      self, query: Text, df: pd.DataFrame, field: Text, index: int) -> float:
    """Score a query text against the value some index in a DataFrame column."""
    result = df.iloc[index][field]
    if result:
      return self.score(query, result)
    return None


class WeightedTokenJaccardScorer(ScorerInterface):
  """Computes IDF-weighted Jaccard similarity of tokens between query and
  result."""

  def __init__(self, documents: pd.Series):
    self.token_frequency = dict()
    self.total_documents = 0
    for _, document in documents.iteritems():
      self.total_documents += 1
      tokens = tokenize(document)
      for token in tokens:
        if token not in self.token_frequency:
          self.token_frequency[token] = 1
        else:
          self.token_frequency[token] += 1

  def idf(self, token: Text):
    freq = self.token_frequency.get(token, 1)
    return math.log(float(self.total_documents) / freq)

  def score(self, query: Text, result: Text) -> float:
    a = set(tokenize(query))
    b = set(tokenize(result))
    intersection = a.intersection(b)
    if not intersection:
      return 0
    union = a.union(b)
    numerator = sum([self.idf(x) for x in intersection])
    denominator = sum([self.idf(x) for x in union])
    return float(numerator) / denominator


class TokenJaccardScorer(ScorerInterface):
  """Computes Jaccard similarity of tokens between query and result."""
  def score(self, query: Text, result: Text) -> float:
    a = set(tokenize(query))
    b = set(tokenize(result))
    intersection = len(a.intersection(b))
    if intersection == 0:
      return 0
    union = (len(a) + len(b)) - intersection
    return float(intersection) / union


class LevenshteinRatioScorer(ScorerInterface):
  """Computes Levenshtein similarity between query and result."""
  def __init__(self):
    self.matcher = StringMatcher()

  def score(self, query: Text, result: Text) -> float:
    query = " ".join(tokenize(query))
    result = " ".join(tokenize(result))
    self.matcher.set_seq1(query)
    self.matcher.set_seq2(result)
    return self.matcher.ratio()


class DataFrameTextSearchInvertedIndex(object):
  """Class that wraps the inverted index for token-wise search retrieval on
  a text column in a DataFrame."""

  def __init__(
      self,
      df: pd.DataFrame,
      column: Text,
      tokenize_fn=tokenize,
      # In case of exact matches.
      key_whole_docs: bool=True,
      stop_words: Set[Text]=frozenset()
  ):
    """Constructor.

    Args:
      df: DataFrame on which to build the inverted index.
      column: in the DataFrame on which to build the inverted index.
      tokenize_fn: tokenization function
      key_whole_docs: whether to index the whole value of a column in
                      addition to the tokens.
      stop_words: tokens that will not be indexed.
    """
    self.tokenize_fn = tokenize_fn
    self.inverted_index: Dict[Text, Set[int]] = dict()
    self.column = column
    for index, row in df.iterrows():
      # Drop empty docs.
      if not row[column] or not isinstance(row[column], str):
        continue

      # Add the tokens to the inverted index.
      tokens: Set[Text] = set(self.tokenize_fn(row[column]))
      tokens.difference(stop_words)
      for token in tokens:
        self.inverted_index.setdefault(token, set()).add(index)

      # In order to support exact match retrieval, index the whole doc.
      if key_whole_docs:
        self.inverted_index.setdefault(row[column], set()).add(index)

  def get(self, query: Text) -> Set[int]:
    """Given a query, return all indexes into the DataFrame where the column
    value contains a query term.

    Args:
      query: to search for in the field of the DataFrame.

    Returns:
      List of indexes in the DataFrame with matching results.
    """
    results = self.inverted_index.get(query, set())
    if results:
      return results
    tokens = set(self.tokenize_fn(query))
    for token in tokens:
      results.update(self.inverted_index.get(token, set()))
    return results

  def get_inverted_index(self) -> Dict[Text, Set[int]]:
    """Return the whole inverted index on a given field."""
    return self.inverted_index
