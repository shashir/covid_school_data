import os
import pandas as pd

from absl import app
from absl import flags
from state_data_mapper import state_data_mapper_lib


FLAGS = flags.FLAGS
flags.DEFINE_string("config", None, "Config filepath.")
flags.DEFINE_string("state_data_dir", ".",
                    "Directory containing state data files.")
flags.DEFINE_string("report", None, "Report filepath.")
flags.DEFINE_list("required_columns", [],
                  "List of mandatory output columns. If no data is available, "
                  "they will be empty.")
flags.DEFINE_list("states_to_process", [],
                  "List of states to process from the config. If not "
                  "provided, then all states from the config will be "
                  "processed.")
flags.DEFINE_string("nces_school_metadata", None,
                    "XLSX filepath containing NCESSchoolID, NCESDistrictID, "
                    "Charter, SchoolType columns.")
flags.DEFINE_string("nces_district_metadata", None,
                    "XLSX filepath containing, NCESDistrictID, Charter, "
                    "DistrictType columns.")
flags.mark_flag_as_required("config")


def main(argv):
  assert len(argv) == 1, "Unexpected arguments provided: " + " ".join(argv[1:])
  config = state_data_mapper_lib.read_config_file(FLAGS.config)
  os.chdir(FLAGS.state_data_dir)
  state_dfs, state_report_dfs = state_data_mapper_lib.process_all_states(
      config,
      report_filepath=FLAGS.report,
      required_columns=FLAGS.required_columns,
      states_to_process=FLAGS.states_to_process,
      nces_school_metadata=FLAGS.nces_school_metadata,
      nces_district_metadata=FLAGS.nces_district_metadata)
  print(pd.concat(state_report_dfs))


if __name__ == '__main__':
  app.run(main)
