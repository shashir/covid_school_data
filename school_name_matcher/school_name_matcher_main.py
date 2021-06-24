import pandas as pd

from absl import app
from absl import flags
from school_name_matcher import school_name_matcher_lib

FLAGS = flags.FLAGS
flags.DEFINE_list("input_files", [], "Files containing SchoolName column.")
flags.DEFINE_string("nces_schools", None, "NCES school data.")


def main(argv):
  assert len(argv) == 1, "Unexpected arguments provided: " + " ".join(argv[1:])
  nces_schools_df = pd.read_csv(
      FLAGS.nces_schools,
      dtype={
          "state": pd.StringDtype(),
          "district_name": pd.StringDtype(),
          "state_leaid": pd.StringDtype(),
          "leaid": pd.StringDtype(),
          "sch_name": pd.StringDtype(),
          "ncessch": pd.StringDtype(),
          "state_schid": pd.StringDtype(),
      })
  for input_file in FLAGS.input_files:
    state_df = school_name_matcher_lib.read_state_schools_csv(input_file)
    joined_df = school_name_matcher_lib.best_effort_merge_nces_data(
        state_df, nces_schools_df)
    joined_df.to_csv(input_file.replace(".csv", "_with_NCES_matches.csv"))


if __name__ == '__main__':
  app.run(main)
