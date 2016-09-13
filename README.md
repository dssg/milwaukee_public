# Milwaukee DataShare Project

The City of Milwaukee recently integrated data from across several different departments and systems into a unified platform called DataShare. Information from the health department, police, district attorney’s office, pretrial records, and many more sources can now be combined for analyses that inform city policy and operations and improve quality of life for Milwaukee residents.

DSSG is working with Milwaukee officials on applying this data resource towards intervening with at-risk youth and reducing juvenile crime. The analysis combines data from Milwaukee’s public schools and the DA’s office to determine factors that elevate or reduce the risk of entering the criminal justice system, and identify students in MPS that would benefit from additional support to prevent juvenile citations. 

## Before running the code

Before running the code, the following python packages and toolkits must be installed (python 3 is recommended): 
* numpy
* scipy
* pandas
* scikit-learn
* psycopg2
* sqlalchemy
* jellyfish
* click
* pyyaml
* matplotlib
* csvkit
* xlrd

Additionally, the database credentials needs to be saved on the directory in a `json` file. See `credentials.example` for an example. 

Finally, the configuration file `config.yml` should include a list of features to run, training and test set, as well as the models to run.

## Running the code

The script `main.py` can be run from the command line as follows: 
```
Python main.py [OPTIONS] CREDENTIALS.json config.yml

OPTIONS:

  model         loops through the models_to_run in the config file and posts the model params and results
                (including precision, recall, feature importance) to the database.  
  
  risk_scores   generates risk scores for all students in the `test_table` (specified in the config file)
                using the chosen model specified in `output.py`. The script then saves the list of students
                and their associated risk scores in a csv file on the directory.
```

## Adding new features/labels

New features must be added to `feature_generator.py`. The associated function that extracts the feature must be written under the `feature_code` method. In addition, each feature has the following properties:
* feature_id: you can ignore this for now
* feature_col: must return a string, which is used to refer to the feature in `config.yml`.
* feature_type: 'boolean', 'categorical', or 'numerical'
* feature_description: a brief description of what the feature extracts from the data.

New labels may be added to `labels.py`
