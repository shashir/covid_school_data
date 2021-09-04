import pandas as pd

from absl import app
from absl import flags
from school_name_matcher import school_name_matcher_lib

FLAGS = flags.FLAGS
flags.DEFINE_list("input_files", [], "Files containing SchoolName column.")
flags.DEFINE_string("nces_schools", None, "NCES school data.")
flags.DEFINE_string("nces_districts", None, "NCES district data.")


def main(argv):
  assert len(argv) == 1, "Unexpected arguments provided: " + " ".join(argv[1:])
  process_districts = False
  nces_df = None
  if FLAGS.nces_districts:
    process_districts = True
    assert not FLAGS.nces_schools, (
        "Provide either NCES School data or NCES District data, but not both.")
    nces_df = pd.read_csv(
        FLAGS.nces_districts,
        usecols=["state", "lea_name", "state_leaid", "leaid"],
        dtype={
            "state": pd.StringDtype(),
            "lea_name": pd.StringDtype(),
            "state_leaid": pd.StringDtype(),
            "leaid": pd.StringDtype()
        })
  else:
    assert not FLAGS.nces_districts, (
        "Provide either NCES School data or NCES District data, but not both.")
    nces_df = pd.read_csv(
        FLAGS.nces_schools,
        usecols=["state", "lea_name", "state_leaid", "leaid", "school_name",
                 "ncessch_num", "seasch"],
        dtype={
            "state": pd.StringDtype(),
            "lea_name": pd.StringDtype(),
            "state_leaid": pd.StringDtype(),
            "leaid": pd.StringDtype(),
            "school_name": pd.StringDtype(),
            "ncessch_num": pd.StringDtype(),
            "seasch": pd.StringDtype(),
        })


  for input_file in FLAGS.input_files:
    state_df = school_name_matcher_lib.read_state_level_csv(input_file)
    joined_df = school_name_matcher_lib.best_effort_merge_nces_data(
        state_df, nces_df, process_districts)
    joined_df.to_csv(input_file.replace(".csv", "_with_NCES_matches.csv"),
                     index=False)


if __name__ == '__main__':
  app.run(main)
