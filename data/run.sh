python3 ../state_data_mapper/state_data_mapper_main.py \
	--config "../state_data_mapper/state_data_mapper.conf" \
	--report="./report.csv" \
	--state_data_dir="./" \
	--required_columns=StateName,StateAbbrev,DataLevel,Charter,SchoolName,SchoolType,NCESSchoolID,DistrictName,DistrictType,NCESDistrictID,County,TimePeriodInterval,TimePeriodStart,TimePeriodEnd,NewCasesCombined,NewCasesStudents,NewCasesStaff,NewCasesUnknown,TwoWeekPeriodStaffStudentCasesCombined,TwoWeekPeriodCasesStudents,TwoWeekPeriodCasesStaff,CumulativeCasesCombined,CumulativeCasesStudents,CumulativeCasesStaff,CumulativeCasesUnknown,ActiveCasesStaff,ActiveCasesStudents,ActiveCasesUnknown,ActiveCasesCombined \
	--states_to_process="$1" \
	--nces_school_metadata="./nces_metadata/NCES_School_Metadata.xlsx" \
	--nces_district_metadata="./nces_metadata/NCES_District_Metadata.xlsx"