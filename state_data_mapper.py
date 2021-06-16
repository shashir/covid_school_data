import os
import pandas as pd
import state_data_mapper_lib

from absl import app
from absl import flags

FLAGS = flags.FLAGS
flags.DEFINE_string("config", None, "Config filepath.")
flags.DEFINE_string("source_data_dir", ".", "Directory containing source data files.")
flags.DEFINE_string("report", None, "Report filepath.")
flags.mark_flag_as_required("config")


def main(argv):
  config = state_data_mapper_lib.read_config_file(FLAGS.config)
  os.chdir(FLAGS.source_data_dir)
  state_dfs, state_report_dfs = state_data_mapper_lib.process_all_states(config, report_filepath=FLAGS.report)
  print(pd.concat(state_report_dfs))


if __name__ == '__main__':
  app.run(main)