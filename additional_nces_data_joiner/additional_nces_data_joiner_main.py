import pandas as pd

from absl import app
from absl import flags
from additional_nces_data_joiner import additional_nces_data_joiner_lib

FLAGS = flags.FLAGS
flags.DEFINE_string("state_case_data_csv", None, "Case data for a state.")
flags.DEFINE_string("nces_school_demographics_csv", None,
                    "NCES school demographics data.")
flags.DEFINE_string("nces_district_demographics_csv", None,
                    "NCES district demographics data.")
flags.DEFINE_bool("process_districts", False,
                  "Whether to only process districts")
flags.DEFINE_string("output_csv", None, "Output CSV.")

def process_charter(value):
  if pd.isna(value):
    return value
  if value == "Not applicable":
    return "No"
  return value[0].upper() + value[1:]


def main(argv):
  assert len(argv) == 1, "Unexpected arguments provided: " + " ".join(argv[1:])
  state_case_df = pd.read_csv(
      FLAGS.state_case_data_csv,
      dtype={
          "NCESDistrictID": pd.StringDtype(),
          "NCESSchoolID": pd.StringDtype(),
      }
  )
  nces_school_demographics_df = pd.read_csv(
      FLAGS.nces_school_demographics_csv,
      usecols=["state_leaid", "leaid", "charter", "school_type",
               "ncessch_num", "seasch"],
      dtype={
          "state_leaid": pd.StringDtype(),  # StateAssignedDistrictID
          "leaid": pd.StringDtype(),  # NCESDistrictID
          "charter": pd.StringDtype(),  # Charter
          "school_type": pd.StringDtype(),  # SchoolType
          "ncessch_num": pd.StringDtype(),  # NCESSchoolID
          "seasch": pd.StringDtype(),  # StateAssignedDistrictID
      })
  # yes -> Yes and no -> No and "Not applicable" -> No
  nces_school_demographics_df["charter"] = nces_school_demographics_df.apply(
      lambda value: process_charter(value),
      axis=1
  )
  nces_district_demographics_df = pd.read_csv(
      FLAGS.nces_district_demographics_csv,
      usecols=["lea_name", "state_leaid", "leaid", "agency_type", "charter"],
      dtype={
          "lea_name": pd.StringDtype(),  # DistrictName
          "state_leaid": pd.StringDtype(),  # StateAssignedDistrictID
          "leaid": pd.StringDtype(),  # NCESDistrictID
          "agency_type": pd.StringDtype(),  # DistrictType
          "charter": pd.StringDtype(),  # Charter
      })
  # yes -> Yes and no -> No
  nces_district_demographics_df["charter"] =\
    nces_district_demographics_df.apply(
      lambda value: process_charter(value),
      axis=1
  )

  if FLAGS.process_districts:
    output_df = additional_nces_data_joiner_lib.join_nces_district_data(
        state_case_df, nces_district_demographics_df)
  else:
    output_df = additional_nces_data_joiner_lib.join_nces_school_data(
        state_case_df, nces_school_demographics_df,
        nces_district_demographics_df)
  output_df.to_csv(FLAGS.output_csv, index=False)


if __name__ == '__main__':
  app.run(main)
