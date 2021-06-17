import os
import pandas as pd

from absl import app
from absl import flags
from state_data_mapper import state_data_mapper_lib


FLAGS = flags.FLAGS
flags.DEFINE_string("config", None, "Config filepath.")
flags.DEFINE_string("state_data_dir", ".",
                    "Directory containing state data files.")
flags.DEFINE_string("nces_schools", None, "NCES school data.")
flags.DEFINE_string("nces_districts", None, "NCES district data.")
flags.DEFINE_string("report", None, "Report filepath.")
flags.mark_flag_as_required("config")


def main(argv):
  assert len(argv) == 1, "Unexpected arguments provided: " + " ".join(argv[1:])
  config = state_data_mapper_lib.read_config_file(FLAGS.config)
  nces_schools_df = None
  nces_districts_df = None
  if FLAGS.nces_schools:
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
  if FLAGS.nces_districts:
    nces_districts_df = pd.read_csv(
        FLAGS.nces_districts,
        dtype={
            "state": pd.StringDtype(),
            "district_name": pd.StringDtype(),
            "state_leaid": pd.StringDtype(),
            "leaid": pd.StringDtype(),
        })
  os.chdir(FLAGS.state_data_dir)
  state_dfs, state_report_dfs = state_data_mapper_lib.process_all_states(
      config, report_filepath=FLAGS.report,
      nces_schools_df=nces_schools_df, nces_districts_df=nces_districts_df)
  print(pd.concat(state_report_dfs))


if __name__ == '__main__':
  app.run(main)
