### Assumptions

For now, we assume that you are on `milwaukee.dssg.io` and that you have write permissions
for the directory `/mnt/data/milwaukee`. Moreover, we assume that the data is in that folder
and looks like:

* Ed_DataHub
	* Assessment data 3rd grade.xlsx  
	* Demographic Files 04-05 to 14-15.csv
	* Att 04-05 to 14-15.xlsx         
	* Discipline data 04-05 to 14-15.csv
	
* CJC_DataHub/Adult
	* Case Charge(s)2009.xlsx  
	* Case Charge(s)2014.xlsx  
	* Ref Charge(s)2012.xlsx
	* Case Charge(s)2010.xlsx  
	* Case Charge(s)2015.xlsx  
	* Ref Charge(s)2013.xlsx
	* Case Charge(s)2011.xlsx  
	* Ref Charge(s)2009.xlsx   
	* Ref Charge(s)2014.xlsx
	* Case Charge(s)2012.xlsx  
	* Ref Charge(s)2010.xlsx   
	* Ref Charge(s)2015.xlsx
	* Case Charge(s)2013.xlsx  
	* Ref Charge(s)2011.xlsx  
	
* CJC_DataHub/Juvenile
	* Case Charge(s)2009.xlsx  
	* Case Charge(s)2014.xlsx  
	* Ref Charge(s)2012.xlsx
	* Case Charge(s)2010.xlsx  
	* Case Charge(s)2015.xlsx  
	* Ref Charge(s)2013.xlsx
	* Case Charge(s)2011.xlsx  
	* Ref Charge(s)2009.xlsx  
	* Ref Charge(s)2014.xlsx
	* Case Charge(s)2012.xlsx  
	* Ref Charge(s)2010.xlsx   
	* Ref Charge(s)2015.xlsx
	* Case Charge(s)2013.xlsx  
	* Ref Charge(s)2011.xlsx
	
* Data_Received
	* Juvenile IDs.xlsx  
	* TblAdultCaseChrg Query.xlsx
	* ChargesCategorized.xlsx 

* new_data
	* 2004-2005 attendance 1 row per student per day by school.csv
	* 2005-2006 attendance 1 row per student per day by school.csv
	* 2006-2007 attendance 1 row per student per day by school.csv
	* 2007-2008 attendance 1 row per student per day by school.csv
	* 2008-2009 attendance 1 row per student per day by school.csv
	* 2009-2010 attendance 1 row per student per day by school.csv
	* 2010-2011 attendance 1 row per student per day by school.csv
	* 2011-2012 attendance 1 row per student per day by school.csv
	* 2013-2014 attendance 1 row per student per day by school.csv
	* 2014-2015 attendance 1 row per student per day by school.csv
	* Assessment data.csv
	* Deomographics 0405 through 1415. Us this also for gender.csv
	* Enrollments entry and withdraw by school grade and year 08-09 through 15-16.xlsx
	* Programs.csv
	* Third friday records with gender-other tables dont have gender before 2007-2008.csv
	* cal dates key to date value.csv


### Run the new CJ and Education pipeline
```
python3 etl/cli.py credentials.yml inventory.yml
```

The ETL process creates the criminal justice and education schemas and loads the data from source files. Here are some relevant notes:

#### Criminal Justice (see etl/cli.py#load_cj_data)
This process loads the data files by type (Adult-Case, Juvenile-Case, Adult-Ref, Juvenile-Ref), concatenates all of the files of the same type and outputs them as a `.csv` with a | delimiter. Then, it takes the `.csv`, generates a table schema and adds the table to the database. The table is then populated, and column names are normalized. 

This part of the process results in the following tables in the `cj_schema`:
- `cj_schema.juvenile_case`

#### Education (see etl/cli.py#load_edu_data)
- Normalize race and use most recent records for that and other demographic information. 
- Adding computed 'year' columns for school years in enrollment and demographics tables (using the year containing the start of school), as well as birth year from birth date.
- Trimming assessments. The last 3000 rows of the file have null spaces that caused breaks. Drop the last 3000 rows.
- Trimming programs. The last 10000 rows have erroneous student ids. Drop them.
- Fixing the line breaks in `Assessment.csv` and `Deomographics 0405 through 1415. Us this also for gender.csv`.
- Upload the calendar date key mapping. We take only the first 2 columns of `cal dates key to date value.csv`, which contains `CALENAR_DATE_KEY` and `DATE_KEY`. 

This part of the process results in the following tables in the `edu_schema`:
- `new_assessment`
- `assessment`
- `calendar_date`
- `programs`
- `discipline`
- `enrollment`
- `demographic`
- `new_demographic`
- `most_recent_demographics` (join table to find the most recent instance of several pieces of demographic information like race and gender)

### Generate unique student and offender list

Run `cleaning_offenders.sql` to get a list of unique offenders in both juvenile and adult file. The script filters out records that are CHIPs, i.e. not actual crime. In addition, it removes people who were above 25 at the age of incident in the adult file. The list of dataframes returned are called `cj_schema.real_criminals_juvenile`and `cj_schema.real_criminals_adults`. The below illustrates how to match between the student list and juvenile list (adult list can be matched similarly). 



### Matching students and juvenile

Load the lists of unique students, juveniles and adults as the dataframes: `student`, `juvenile` and `adult` by running
`match.read_data_clean_column(conn)`.

#### Assign names to the dataframes
```
juvenile.name = 'juvenile'
adult.name = 'adult'
student.name = 'student'
```

#### Clean first names and last names for all three dataframes by looping over them
```
filelist = [juvenile, adult, student]

for item in filelist:
    df[item.name] = item
    df[item.name]['firstname_clean'] = df[item.name].firstname.apply(match.clean_first_name)
    df[item.name]['lastname_clean'] = df[item.name].lastname.apply(match.clean_last_name)
    item = df[item.name]
```

#### Apply parallel matching logic

Apply the 3 matching logic: `match.merge_by_1_digitdiff`, `match.merge_ln_bdate_fnjaro`, `match.merge_fn_bdate_lnjaro`. The script returns the matched individual based on each logic and a match ratio. See `match.py` for more detailed description of each logic. Note that `merge_by_1digitdiff` also returns the ratio of exact match, i.e. all three fiels `firstname_clean`, `lastname_clean`, `dob` match.  The order of running each logic does not matter. Based on manual review, we set the jaro distance threshold to 0.8. The example below shows the matching code between juvenile and student.

```
matched_stujuv_df1, matched_stujuv_ratio1, exact_match_ratio = match.merge_by_1digitdiff(student, juvenile)
matched_stujuv_df2, matched_stujuv_ratio2 = match.merge_ln_bdate_fnjaro(student, juvenile,  threshold = 0.8)
matched_stujuv_df3, matched_stujuv_ratio3 = match.merge_fn_bdate_lnjaro(student, juvenile)
```

The final list of the linked individuals is simply an appended list of `matched_stujuv_df1`, `matched_stujuv_df2` and `matched_stujuv_df3`. Specifically, run:

```++
matched_stu_juv_df = matched_stujuv_df1 + matched_stujuv_df2 + matched_stujuv_df3
```




### Storing Results

We store the model results as well as the corresponding feature importance in database. To do that, we need to first create the table shells in the databse. We do that by creating two tables `training.models` and `training.feature_importance`:
```sql
create table training.models (id BIGSERIAL PRIMARY KEY,
  features TEXT, 
  train_set  TEXT,
  test_set TEXT,
  train_label TEXT,
  test_label TEXT,
  train_size TEXT,
  test_size TEXT,
  clf_params  TEXT,
  k NUMERIC,
  precision NUMERIC,
  auc NUMERIC,
  recall numeric,
  cm TEXT
);

CREATE TABLE training.feature_importance (
id BIGSERIAL PRIMARY KEY,
model_id INTEGER REFERENCES training.models (id),
feature TEXT,
importance NUMERIC
);
```





