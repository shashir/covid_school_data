import pandas as pd

from absl import app
from absl import flags
from nces_joiner_tx import nces_joiner_tx_lib

FLAGS = flags.FLAGS
flags.DEFINE_string("state_case_data_csv", None, "Case data for TX.")
flags.DEFINE_list("nces_lookup_csv", None,
                    ("CSV(s) with columns school_name, lea_name, ncessch, "
                     "and leaid."))
flags.DEFINE_string("output_csv", None, "Output CSV.")


def main(argv):
  assert len(argv) == 1, "Unexpected arguments provided: " + " ".join(argv[1:])
  lookups, drops = nces_joiner_tx_lib.read_nces_lookup_csv(
      FLAGS.nces_lookup_csv)
  state_case_df = pd.read_csv(FLAGS.state_case_data_csv)
  output_df = nces_joiner_tx_lib.process_state(state_case_df, lookups, drops)
  output_df.to_csv(FLAGS.output_csv, index=False)



if __name__ == '__main__':
  app.run(main)
