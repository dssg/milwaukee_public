import csv
import datetime
import glob
import io
import os
import re
import subprocess

import click
import pandas as pd
import sqlalchemy
import yaml

CJ_SCHEMA = 'cj_schema'
EDU_SCHEMA = 'edu_schema'
ASSESSMENT_TABLE_NAME = 'new_assessment'
OLD_ASSESSMENT_TABLE_NAME = 'assessment'
CALENDAR_TABLE_NAME = 'calendar_date'
PROGRAMS_TABLE_NAME = 'programs'
DISCIPLINE_TABLE_NAME = 'discipline'
ENROLLMENT_TABLE_NAME = 'enrollment'
OLD_DEMOGRAPHICS_TABLE_NAME = 'demographic'
DEMOGRAPHICS_TABLE_NAME = 'new_demographic'
MOST_RECENT_DEMOGRAPHICS_TABLE_NAME = 'most_recent_demographics'
ASSESSMENT_TABLE = '{}.{}'.format(EDU_SCHEMA, ASSESSMENT_TABLE_NAME)
OLD_ASSESSMENT_TABLE = '{}.{}'.format(EDU_SCHEMA, OLD_ASSESSMENT_TABLE_NAME)
CALENDAR_TABLE = '{}.{}'.format(EDU_SCHEMA, CALENDAR_TABLE_NAME)
DISCIPLINE_TABLE = '{}.{}'.format(EDU_SCHEMA, DISCIPLINE_TABLE_NAME)
DEMOGRAPHICS_TABLE = '{}.{}'.format(EDU_SCHEMA, DEMOGRAPHICS_TABLE_NAME)
OLD_DEMOGRAPHICS_TABLE = '{}.{}'.format(EDU_SCHEMA, OLD_DEMOGRAPHICS_TABLE_NAME)
ENROLLMENT_TABLE = '{}.{}'.format(EDU_SCHEMA, ENROLLMENT_TABLE_NAME)
MOST_RECENT_DEMOGRAPHICS_TABLE = '{}.{}'.format(EDU_SCHEMA, MOST_RECENT_DEMOGRAPHICS_TABLE_NAME)

RACE_AFAM = 'African-American'
RACE_HIS = 'Hispanic'
RACE_NAT = 'Native American'
RACE_OTHER = 'Other'
RACE_MAPPINGS = {
    'African-Am': RACE_AFAM,
    'Black or African American': RACE_AFAM,
    'HI/PI': RACE_HIS,
    'Hispanic/Latino': RACE_HIS,
    'Native-Am': RACE_NAT,
    'Unknown': RACE_OTHER,
    'Multiple': RACE_OTHER,
    'Multi': RACE_OTHER,
    'Native Hawaiian or Other Pacific Islander': RACE_OTHER,
}

def get_input_and_output(config, type_of_data):
    input_file = os.path.join(config['misc_ed_data']['raw_path'],
                              config[type_of_data]['filename'])
    output_file = os.path.join(config['misc_ed_data']['clean_path'],
                               config[type_of_data]['output_filename'])
    if not os.path.exists(config['misc_ed_data']['clean_path']):
        os.makedirs(config['misc_ed_data']['clean_path'])
    return input_file, output_file


def fix_dates(line, date_names, headers):
    """Fix up the columns in `line` that correspond to dates in the
    format M/D/YY according to the `date_names` and `headers` arguments.

    :param list[str] line: The line to fixup
    :param list[str] date_names: The names of the fields which contain
        American-style dates to be fixed
    :param list[str] headers: The list of field names in the line
    :return: Nothing. line is edited in place.
    """
    date_idxs = [headers.index(date_name) for date_name in date_names]
    for date_idx in date_idxs:
        val = line[date_idx]
        if val:
            # Forget times if they appear
            val = val.split(' ')[0]

            # Sometimes, miraculously, the val is *not* in American format:
            try:
                datetime.datetime.strptime(val, '%Y-%m-%d')
                # In the correct format!
                line[date_idx] = val
                continue
            except ValueError:
                # In the American format
                pass

            try:
                val = datetime.datetime.strptime(val, '%m/%d/%Y')
            except ValueError:
                # No idea what format this is in. Warn and return None
                print("Unreadable date {}".format(val))
                line[date_idx] = None
                continue

            # Sometimes people write dates like 4/1/15. Bump the years to the modern era
            if val.year < 50:
                val = datetime.datetime(val.year + 2000, val.month, val.day)
            elif val.year < 100:
                val = datetime.datetime(val.year + 1900, val.month, val.day)
            val = val.strftime('%Y-%m-%d')
            line[date_idx] = val

def normalize_race(line, headers):
    race_idx = headers.index('student_race')
    race = line[race_idx]
    if race in RACE_MAPPINGS:
        line[race_idx] = RACE_MAPPINGS[race]
    return line

def clean_demographics(config):
    input_file, output_file = get_input_and_output(config, 'demographics')
    if os.path.exists(output_file):
        return False
    with io.open(input_file, mode='r', encoding='latin8') as f, \
            open(output_file, mode='wt', newline='') as g:
        reader = csv.reader(f)
        writer = csv.writer(g)
        headers = next(reader)
        headers = [re.sub(r'\s+', '_', col.strip().lower()) for col in headers]
        writer.writerow(headers)

        date_names = ['collection_date', 'student_birthdate']

        line_chunk = next(reader)
        for line in reader:
            if len(line) == 1:
                line_chunk[-1] = line_chunk[-1].strip() + ' ' + line[0].strip()
            elif len(line) == 0:
                continue
            else:
                fix_dates(line_chunk, date_names, headers)
                normalize_race(line, headers)
                writer.writerow(line_chunk)
                line_chunk = line
        writer.writerow(line_chunk)
    return True

def clean_old_demographics(config):
    input_file, output_file = get_input_and_output(config, 'old_demographics')
    if os.path.exists(output_file):
        return False
    with io.open(input_file, mode='r', encoding='latin8') as f, \
            open(output_file, mode='wt', newline='') as g:
        reader = csv.reader(f)
        writer = csv.writer(g)
        headers = next(reader)
        headers = [re.sub(r'\s+', '_', col.strip().lower()) for col in headers]
        writer.writerow(headers)

        date_names = ['student_birthdate']

        line_chunk = next(reader)
        for line in reader:
            if len(line) == 1:
                line_chunk[-1] = line_chunk[-1].strip() + ' ' + line[0].strip()
            elif len(line) == 0:
                continue
            else:
                fix_dates(line_chunk, date_names, headers)
                normalize_race(line, headers)
                writer.writerow(line_chunk)
                line_chunk = line
        writer.writerow(line_chunk)
    return True

def clean_discipline(config):
    input_file, output_file = get_input_and_output(config, 'discipline')
    if os.path.exists(output_file):
        return False

    with io.open(input_file, mode='r', encoding='latin8') as f, \
            io.open(output_file, mode='wt', newline='') as g:
        reader = csv.reader(f)
        writer = csv.writer(g)
        headers = next(reader)
        headers = [re.sub(r'\s+', '_', col.strip().lower()) for col in headers]
        writer.writerow(headers)

        date_names = ['discipline_start_date',
                      'discipline_end_date',
                      'discipline_expect_return_date']

        line_chunk = next(reader)
        for line in reader:
            if len(line) == 0:
                # Skip empty lines
                continue

            # First handle the *previous* line
            if len(line_chunk) > 30:
                # Write complete line chunks to the fixed file
                fix_dates(line_chunk, date_names, headers)
                assert len(line_chunk) == 32
                writer.writerow(line_chunk)
                line_chunk = line
                continue

            # Now add on this line to the previous line as necessary
            if len(line) < 12:
                # Sometimes multiple newlines appear in free text fields
                line_chunk[-1] += ' '.join(line)
            elif len(line) == 16 and len(line_chunk) == 16:
                # In this case, there's perfect separation in the line. Just join the
                # lines
                line_chunk.extend(line)
            else:
                # The most common case: one of the free text fields breaks
                line_chunk[-1] = line_chunk[-1].strip() + ' ' + line[0].strip()
                line_chunk.extend(line[1:])

        # Write final row
        fix_dates(line_chunk, date_names, headers)
        writer.writerow(line_chunk)
    return True


def clean_assessment(config):
    """
    Clean up the new assessment file where there are extraneous line breaks. Also
    drop last 3000 rows (there's a lot of null bytes).

    Most lines are 44 fields wide, but there is a free text field (the 12th column)
    which we somtimes need to concatenate with the next row (always a 33 wide rows).
    Note that the first column of the 33-wide rows should be appended to the last
    column of the 12-wide row.
    """
    input_file, output_file = get_input_and_output(config, 'assessment')
    if os.path.exists(output_file):
        return False

    # Get total lines so we can drop last 3000 rows
    total_lines = 0
    with io.open(input_file, mode='r', encoding='latin8') as f:
        for line in f:
            total_lines += 1

    # Now go through all the lines and keep those which aren't 44 wide for further processing
    bad_lines = []

    with io.open(input_file, mode='r', encoding='latin8') as f, \
            open(output_file, 'w') as cleaned_f:
        cleaned_writer = csv.writer(cleaned_f)
        reader = csv.reader(f)
        headers = next(reader)
        headers = [re.sub(r'\s+', '_', col.strip().lower()) for col in headers]

        cleaned_writer.writerow(headers)

        num_fields = len(headers)
        for row_number, line in enumerate(csv.reader(f), 1):
            if row_number + 3000 > total_lines:
                # Drop last 3000 rows
                break

            if len(line) != num_fields:
                bad_lines.append(line)
            else:
                cleaned_writer.writerow(line)

    # Fix these remaining lines
    fixed_lines = []
    for line_number, line in enumerate(bad_lines):
        if line_number > 0:  # Skip the first 12-wide row
            if len(line) == 33:
                prev_line = bad_lines[line_number-1]
                prev_line[-1] = (prev_line[-1] + line[0]).strip()
                prev_line.extend(line[1:])
                fixed_lines.append(prev_line)

    # Save fixed lines in the output file
    with open(output_file, 'a') as f:
        cleaned_writer = csv.writer(f)
        cleaned_writer.writerows(fixed_lines)
    return True


def clean_old_assessment(config):
    input_file, output_file = get_input_and_output(config, 'old_assessment')
    if os.path.exists(output_file):
        return False

    df = pd.read_excel(input_file)
    df = df.rename(columns={col: re.sub(r'\s+', '_', col.strip().lower()) for col in df.columns})
    df.to_csv(output_file, index=False)
    return True


def clean_enrollment(config):
    input_file, output_file = get_input_and_output(config, 'enrollment')
    if os.path.exists(output_file):
        return False

    df = pd.read_excel(input_file)
    df = df.rename(columns={col: re.sub(r'\s+', '_', col.strip().lower()) for col in df.columns})
    df.loc[df['student_id'] == '@ERR', 'student_id'] = float('nan')
    df.to_csv(output_file, index=False)
    return True


def clean_programs(config):
    input_file, output_file = get_input_and_output(config, 'programs')
    if os.path.exists(output_file):
        return False

    with io.open(input_file, mode='r', encoding='latin8') as f, \
            open(output_file, 'w') as cleaned_f:
        cleaned_writer = csv.writer(cleaned_f)
        reader = csv.reader(f)
        headers = next(reader)
        headers = [re.sub(r'\s+', '_', col.strip().lower()) for col in headers]
        student_id_idx = headers.index('student_id')

        date_names = ['begin_date', 'end_date', 'membership_begin_date', 'membership_end_date']
        date_idxs = [headers.index(date_name) for date_name in date_names]

        cleaned_writer.writerow(headers)
        bad_rows = 0
        for line in reader:
            if not line[student_id_idx].strip().isdigit():
                bad_rows += 1
                continue

            for date_idx in date_idxs:
                val = line[date_idx]
                if val:
                    val = datetime.datetime.strptime(val, '%m/%d/%Y').strftime('%Y-%m-%d')
                    line[date_idx] = val
            cleaned_writer.writerow(line)
        print("There were {} rows without valid student_ids".format(bad_rows))

    return True


def clean_calendar(config):
    input_file, output_file = get_input_and_output(config, 'calendar')
    if os.path.exists(output_file):
        return False

    with io.open(input_file, mode='r', encoding='latin8') as f, \
            open(output_file, 'w') as cleaned_f:
        cleaned_writer = csv.writer(cleaned_f)
        reader = csv.reader(f)
        headers = next(reader)
        headers = [re.sub(r'\s+', '_', col.strip().lower()) for col in headers]

        date_names = ['date_value']
        cleaned_writer.writerow(headers)

        for line in reader:
            fix_dates(line, date_names, headers)
            cleaned_writer.writerow(line)
    return True


def get_create_statement(filename, delimiter=','):
    """From a filename, use csvkit to get a CREATE statement for the table. Note that
    the table created will have the name 'stdin'. You can change this with string
    replacement.

    :param str filename: The filename for which to create a CREATE statement for
    :param str delimiter: The delimiter of the file
    :return: A Postgres CREATE statement
    :rtype: str
    """
    output = subprocess.check_output(['head', '-n', '10', filename])
    args = ['csvsql', '-i', 'postgresql', '-d', delimiter]
    p = subprocess.Popen(args,
                         stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE)
    output, _ = p.communicate(output)
    retcode = p.wait()
    if retcode != 0:
        raise subprocess.CalledProcessError(retcode, args)
    return output.decode('utf-8')


def copy_data_to_db(filename, schema_name, table_name, credentials, engine, delimiter=','):
    engine.execute("DROP TABLE IF EXISTS {schema}.{table_name};".format(
        schema=schema_name, table_name=table_name))
    create_statement = get_create_statement(filename, delimiter=delimiter)
    create_statement = create_statement.replace('stdin', '{}.{}'.format(schema_name, table_name))
    create_statement = re.sub(r'VARCHAR\(\d+\)', 'TEXT', create_statement)
    create_statement = re.sub(r'NOT NULL', '', create_statement)
    create_statement = re.sub(r'DATE', 'TEXT', create_statement)
    create_statement = re.sub(r'TIME(?:STAMP)? WITHOUT TIME ZONE', 'TEXT', create_statement)
    create_statement = re.sub(r'modifier FLOAT', 'modifier TEXT', create_statement)
    create_statement = re.sub(r'defendant_gender .*\n', 'defendant_gender TEXT,\n',
                              create_statement)
    create_statement = re.sub(r'agency_case_#" .*\n', 'agency_case_#" TEXT,\n', create_statement)
    create_statement = re.sub(r'student_street_number .*\n', 'student_street_number TEXT,\n',
                              create_statement)
    create_statement = create_statement.replace('student_esl_classification INTEGER', 'student_esl_classification TEXT')
    create_statement = create_statement.replace('student_esl_indicator BOOLEAN', 'student_esl_indicator TEXT')
    create_statement = create_statement.replace('student_foodservice_indicator BOOLEAN', 'student_foodservice_indicator TEXT')
    create_statement = create_statement.replace('student_special_ed_indicator BOOLEAN', 'student_special_ed_indicator TEXT')
    create_statement = create_statement.replace('student_at_risk_indicator BOOLEAN', 'student_at_risk_indicator TEXT')
    create_statement = create_statement.replace('student_504_indicator BOOLEAN', 'student_504_indicator TEXT')
    create_statement = create_statement.replace('student_migrant_ed_indicator BOOLEAN', 'student_migrant_ed_indicator TEXT')
    create_statement = create_statement.replace("test_primary_result_code INTEGER", "test_primary_result_code TEXT")
    create_statement = create_statement.replace("program_code INTEGER", "program_code TEXT")
    create_statement = create_statement.replace('discipline_action_type_code INTEGER',
                                                'discipline_action_type_code TEXT')
    create_statement = create_statement.replace("discipline_days INTEGER",
                                                "discipline_days FLOAT")
    for date_name in ['begin_date', 'end_date',
                      'membership_begin_date', 'membership_end_date',
                      'collection_date', 'defendant_dob',
                      'student_birthdate', 'discipline_start_date',
                      'discipline_end_date', 'discipline_expect_return_date',
                      'adm_cd', 'reg_sd', 'reg_cd', 'end_sd', 'end_cd', 'withdraw_date',
                      'date_value']:
        create_statement = re.sub(r'{} \w*'.format(date_name),
                                  '{} DATE'.format(date_name),
                                  create_statement)
    engine.execute(create_statement)

    print("Copying files for {}.{} to db".format(schema_name, table_name))
    with open(filename, 'r') as f:
        conn = engine.raw_connection()
        cur = conn.cursor()
        cur.copy_expert("""COPY {schema_name}.{table_name}
                           FROM stdin
                           WITH CSV HEADER DELIMITER '{delimiter}' QUOTE '"';
                        """.format(schema_name=schema_name,
                                   table_name=table_name,
                                   filename=filename,
                                   delimiter=delimiter), f)
        conn.commit() 


def fixup_zip(x):
    """There are sometimes issues with zip codes. No matter what the format, try
    taking in the zip, turning it into a string, and then making the first five
    numbers an int. If this process fails, return float('nan').

    :param x: The thing to try to convert
    :return: An int if possible, else float('nan')
    :rtype: float | int
    """
    try:
        return int(str(x).strip()[:5])
    except ValueError:
        return float('nan')


def try_cast_to_int(x):
    try:
        return int(x)
    except ValueError:
        return float('nan')


def fix_cc_year(x):
    y = str(x)
    if y == 'nan':
        return float('nan')
    if y.endswith('CF'):
        return int('19' + y[:2])
    return int(x)


def add_year_column(engine, table_name, year_column, source_statement):
    with engine.begin() as connection:
        connection.execute('ALTER TABLE {} ADD COLUMN {} int'.format(table_name, year_column))
        connection.execute('UPDATE {} SET {} = {}'.format(table_name, year_column, source_statement))

def create_most_recent_demographics(engine):
    engine.execute("DROP TABLE IF EXISTS {}".format(MOST_RECENT_DEMOGRAPHICS_TABLE))
    engine.execute("""
        CREATE TABLE {most_recent_demo} AS
        SELECT DISTINCT ON (student_key)
               student_key, firstname, lastname, dob, student_gender, student_race, collection_date
          FROM (
            SELECT student_key,
                   student_first_name firstname,
                   student_last_name lastname,
                   student_birthdate dob,
                   student_gender_code student_gender,
                   student_race,
                   collection_date
              FROM {demographic_table}
             WHERE student_birthdate is not null
               AND student_key is not null
               AND student_last_name is not null
               AND student_first_name is not null
             ORDER BY collection_date DESC
          ) AS no_nulls
        """.format(demographic_table=DEMOGRAPHICS_TABLE,
                   most_recent_demo=MOST_RECENT_DEMOGRAPHICS_TABLE))


def append_calendar_to_assessment(engine):
    engine.execute("""
        DROP TABLE IF EXISTS {edu_schema}.tmp_assessment_table;
        CREATE TABLE {edu_schema}.tmp_assessment_table AS
          SELECT assessment_table.*, calendar_table.date_value AS calendar_date
            FROM {assessment_table} assessment_table
            JOIN {calendar_table} calendar_table
              ON assessment_table.calendar_date_key = calendar_table.calendar_date_key;
        DROP TABLE {assessment_table};
        ALTER TABLE {edu_schema}.tmp_assessment_table RENAME TO {assessment_table_name};
        """.format(assessment_table=ASSESSMENT_TABLE,
                   assessment_table_name=ASSESSMENT_TABLE_NAME,
                   calendar_table=CALENDAR_TABLE,
                   edu_schema=EDU_SCHEMA))


def load_cj_data(engine, credentials, inventory, click):
    engine.execute("CREATE SCHEMA IF NOT EXISTS {}".format(CJ_SCHEMA))
    cj_input_path = inventory['cj_data_path']['raw_path']
    cj_output_path = inventory['cj_data_path']['clean_path']
    if not os.path.exists(cj_output_path):
        os.makedirs(cj_output_path)


    for type_of_file in inventory['case_files']:
        input_path = os.path.join(cj_input_path, type_of_file)
        output_path = os.path.join(cj_output_path, type_of_file + '.csv')
        table_name = "{}_case".format(type_of_file.lower())

        if os.path.exists(output_path):
            click.echo("Reading {} from cached version...".format(type_of_file))
        else:
            click.echo("Combining Case files for {}".format(type_of_file))
            df_list = []
            with click.progressbar(glob.glob(os.path.join(input_path, 'Case*.xlsx'))) as pb:
                for filename in pb:
                    df = pd.read_excel(filename, 'Sheet1')
                    df_list.append(df)

            click.echo("Done reading {} files".format(type_of_file))
            click.echo("Concatenating dataframes....")
            concat_df = pd.concat(df_list)
            del concat_df['First Offender Program-Approved'] # dupe, causes problems
            concat_df = concat_df.reset_index(drop=True).reset_index()\
                                 .rename(columns={'index': 'id'})\
                                 .rename(columns={
                                     col: re.sub(r' ', '_',
                                                 col.lower().strip())
                                     for col in concat_df.columns
                                 })

            # There are sometimes issues with the zip code
            for zip_field in ['defendant_zip', 'incident_zip']:
                if zip_field not in concat_df.columns:
                    continue
                concat_df.loc[~concat_df[zip_field].isnull(), zip_field] = \
                    concat_df.loc[~concat_df[zip_field].isnull(), zip_field].apply(fixup_zip)

            # The count field also has some issues
            concat_df['count_#'] = concat_df['count_#'].apply(try_cast_to_int)
            concat_df['cc#_year'] = concat_df['cc#_year'].apply(fix_cc_year)
            concat_df.to_csv(output_path, index=False)

        copy_data_to_db(output_path, CJ_SCHEMA, table_name,
                              credentials, engine)


def load_edu_data(engine, credentials, inventory, click):
    engine.execute("CREATE SCHEMA IF NOT EXISTS {}".format(EDU_SCHEMA))
    cleaners = {
	'demographics': (clean_demographics, DEMOGRAPHICS_TABLE_NAME),
	'old_demographics': (clean_old_demographics, OLD_DEMOGRAPHICS_TABLE_NAME),
	'assessment': (clean_assessment, ASSESSMENT_TABLE_NAME),
	'old_assessment': (clean_old_assessment, OLD_ASSESSMENT_TABLE_NAME),
	'enrollment': (clean_enrollment, ENROLLMENT_TABLE_NAME),
	'programs': (clean_programs, PROGRAMS_TABLE_NAME),
	'discipline': (clean_discipline, DISCIPLINE_TABLE_NAME),
	'calendar': (clean_calendar, CALENDAR_TABLE_NAME),
    }
    # Clean educational data
    for data_type, data in cleaners.items():
        fn, table_name = data
        click.echo("Cleaning {} data...".format(data_type))
        if not fn(inventory):
            click.echo("Using cached {} data".format(data_type))

        click.echo("Pushing {} data....".format(data_type))
        copy_data_to_db(os.path.join(inventory['misc_ed_data']['clean_path'],
                                           inventory[data_type]['output_filename']),
                              EDU_SCHEMA, table_name,
                              credentials, engine, delimiter=',')

    
    click.echo("Adding year columns")
    add_year_column(engine, ENROLLMENT_TABLE, 'year', 'cast(left(school_year,4) as integer)')
    add_year_column(engine, DEMOGRAPHICS_TABLE, 'year', 'cast(left(collection_year,4) as integer)')
    add_year_column(engine, DEMOGRAPHICS_TABLE, 'birth_year', 'extract (year from student_birthdate)')
    add_year_column(engine, OLD_DEMOGRAPHICS_TABLE, 'year', 'cast(left(school_year,4) as integer)')
    add_year_column(engine, OLD_DEMOGRAPHICS_TABLE, 'birth_year', 'extract (year from student_birthdate)')
    add_year_column(engine, DISCIPLINE_TABLE, 'year', 'case when extract (month from discipline_start_date) > 8 then extract (year from discipline_start_date) else extract (year from discipline_start_date)-1 end')

    click.echo("Appending calendar_date to assessment table...")
    append_calendar_to_assessment(engine)
    add_year_column(engine, ASSESSMENT_TABLE, 'test_year', 'case when extract (month from calendar_date) > 8 then extract (year from calendar_date) else extract (year from calendar_date)-1 end')
    click.echo("Creating most recent demographics table")
    create_most_recent_demographics(engine)


@click.command('')
@click.argument("credentials_file", type=click.Path(exists=True))
@click.argument("inventory_file", type=click.Path(exists=True))
def etl_command(credentials_file, inventory_file):
    """Run the ETL pipeline.

    You must specify database credentials (usual DSN) and in a yaml format, and
    an inventory file as a yaml.
    """
    with open(credentials_file, 'r') as f:
        credentials = yaml.load(f)
    with open(inventory_file, 'r') as f:
        inventory = yaml.load(f)

    engine = sqlalchemy.create_engine('postgresql://', connect_args=credentials)

    load_cj_data(engine, credentials, inventory, click)
    load_edu_data(engine, credentials, inventory, click)


if __name__ == '__main__':
    etl_command()
