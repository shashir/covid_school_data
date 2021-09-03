#
# Run:
# python3 nces_joiner_main --nces_lookup_csv="/home/shashir/Downloads/DE_nces.csv" \
#   --state_case_data_csv="/home/shashir/Downloads/DE_schools.csv" \
#   --output_csv="/home/shashir/Downloads/DE_joined.csv" \
#   --process_districts=true

import pandas as pd

from absl import app
from absl import flags
from nces_joiner import nces_joiner_lib

FLAGS = flags.FLAGS
flags.DEFINE_string("state_case_data_csv", None, "Case data for a state.")
flags.DEFINE_list("nces_lookup_csv", None,
                    ("CSV(s) with columns SchoolName, ncessch, either "
                     "is_match or drop."))
flags.DEFINE_string("output_csv", None, "Output CSV.")
flags.DEFINE_bool("process_districts", False,
                  "Whether to capture LEAD ID instead of NCES ID.")


def main(argv):
  assert len(argv) == 1, "Unexpected arguments provided: " + " ".join(argv[1:])
  if FLAGS.process_districts:
    lookups, drops = nces_joiner_lib.read_lea_lookup_csv(FLAGS.nces_lookup_csv)
  else:
    lookups, drops = nces_joiner_lib.read_nces_lookup_csv(FLAGS.nces_lookup_csv)
  state_case_df = pd.read_csv(
      FLAGS.state_case_data_csv,
      dtype={
          "NCESSchoolID": pd.StringDtype(),
          "NCESDistrictID": pd.StringDtype(),
      }
  )
  output_df = nces_joiner_lib.process_state(state_case_df, lookups, drops, FLAGS.process_districts)
  output_df.to_csv(FLAGS.output_csv, index=False)



if __name__ == '__main__':
  app.run(main)
