# Matching Logic 

We used first name, last name, and date of birth to identify unique individuals (ref: `match.py`). 

In the MPS and CJ data, the names are captured in two different formats.

| Student First Name | Student Last Name |   | Defendant Name |
|--------------------|-------------------|---|----------------|
| Jane               | Doe               |   | Doe, Jane      |

The first task is to split the names in the CJ data and perform some basic normalization on the strings:
* Remove white spaces (before and after string)
* Change all names to lowercase

Then, the first names are cleaned by:
* Removing middle initials
* Removing white spaces
* Removing tricky characters (,, ”, -)
* Removing suffixes (Jr., II, III, etc)

The same steps are performed on the last name with the addition of:
* Splitting hyphenated last names, and keeping only the first one

After the names are cleaned, we accounted for typos by computing the Jaro-Winkler distance for similarities between strings. 
A score of 0 indicates no match while a score of 1 indicates an exact match. We used the JW distance in two methods:

Case 1: Exact match on first name, DOB. Computing the JW distance on the last name, and those that matched with a score higher than 0.8 were considered to be the same individual. 

Case 2: Exact match on last name, DOB. Computing the JW distance on the first name, and those that matched with a score higher than 0.8 were considered to be the same individual.

Lastly, noting that there might be some dates that were entered inconsistently, we allowed for some fuzziness on the dates. With an exact match on first name, last name, and the year for DOB, we allowed up to a 1 digit difference in the dates. For example: 
* '2010-02-04' and '2010-03-04' is a **match**
* '2004-11-09' and '2004-11-22' is **not** considered a match.

## How to Run the Matching Code

This process generates ids for juveniles, writes them to db, then matches across to the student tables and appends the new ids to the overall case files. 

* Run generate_juv_id.py
* Run matching_juv_demo.py
* Run appends_ids.sql

