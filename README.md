# HyriseWorkloadGenerator
A workload generator for hyrise

### Table Generator
The table generator generates tables in a hyrise-suitable format. The specifications of the tables to generate have to be provided as a configuration file. The specifications are defined in json format. An example is given in the config.json file in the tableGenerator directory.

A configuration consists of the following attributes, all of them are mandatory: name, rows, columns, stringsForEachInt, stringLength, uniqueValues.

- **name** (*string*) : name of the table
- **rows** (*int*): number of rows
- **columns** (*int*): number of columns
- **stringsForEachInt** (*int*): inserts X columns with datatype string for each column with datatype integer. The layout is always: integer_column, X string_columns, integer_columns, X string_columns. If the amount does not fit evenely it will insert as many string columns as fit in the last chunk
- **stringLength** (*array of int with two values*): minimum and maximum length of string columns. The exact value is determined randomly
- **uniqueValues** (*array of integer with length = columns or just int*): number of unique values per column. If no array and hence not a single value per column is provided the single int value is taken for all columns

**Usage**:
python generator.py config.json outputDirectory