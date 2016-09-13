# Baseline 

The purpose of this code is to create a baseline model for comparsion with our best model. 
The code is implemented based on the specific logic used by MPS. 

### Before running this code

Before running this code, you should add the password to the baseline.py file. 

### Assumptions
The code runs under the following assumptions: 
1. All education data is uploaded to the database 
2. Features tables already exist (specifically feature2013)

### Running the code 
The code is a combination of a Python and SQL. First you should run 'baseline.py' and that scripts creates a new table in the database. 
After that you can run the SQL code. The final output of the SQL code is numbers of TP, FP, TN, FN which you can calculate the precision and recall based on them. 
