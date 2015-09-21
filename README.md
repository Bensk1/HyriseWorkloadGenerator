# HyriseWorkloadGenerator

### Workload Generator
The workload generator sends automatically a configurable workload to Hyrise. It does this by simulating days and the workload might change for different days. Some parts of the workload might also occur periodically. Performance will be recorded and statistics calculated. The specifications for the workloads and general simulation are defined in json format. An example is given in *workloadSample.json*.

The workload generator reads the specification and automatically builds json query plans in HYRISE format. Those queries are **not** sent as fast as possible, the workload generator tries to simulate a real workload by not putting the system under full load, but sending some queries approximately every 150ms.

The workload generator is also capable of finding the best index configuration for the defined workload, taking a memory budget into account. It does this by checking at first which of all possible configurations are conform with the memory budget. Afterwards it is going to try all of them. To have slightly more reliable results every configuration is tried multiple times. The amount of tries can be regulated by adjusting REPEAT_BEST_INDEX_RUNS in workload.py. The default value is 3.

#### Workload configuration

A configuration consists of the following attributes, all of them are mandatory: days, secondsPerDay, verbose, compressed, calculateOverallStatistics, statisticsInCycles, indexOptimization, findBestIndexConfiguration, availableBudget, queryClasses, queryClassDistributions, periodicQueryClasses.

- **days** (*int*) : days the generator is going to simulate
- **secondsPerDay** (*int*) : how long should the simulation of one of these days take
- **verbose** (*bool*) : prints additional information during simulation
- **compressed** (*bool*) : decreases size of the json queries significantly. Deactivate for debuggin!
- **verbose** (*bool*) : prints additional information during simulation
- **calculateOverallStatistics** (*bool*) : calculate statistics for all workloads aggregated
- **statisticsInCycles** (*bool*): calculate statistics in cycles,
- **indexOptimization** (*bool*) : whether to trigger index optimization or not
- **findBestIndexConfiguration** (*bool*) : whether to try all possible, but memory budget conforming, index configurations and find the best performing one. **Careful, this setting overwrites indexOptimization. There will of course be no indexOptimization if the generator tries to find the best performing index configuration**
- **availableBudget** (*int*): the memory budget in bytes which is used to find the best performing index
- **queryClasses** (*Array of Objects*) : array that contains QueryClasses
- **queryClassDistributions** (*Array of Objects*) : array that contains QueryClassDistributions
- **periodicQueryClasses** (*Array of Objects*) : array that contains PeriodicQueryClasses

#### QueryClasses

QueryClasses are the differnt parts of workloads. A workload might for example consist of the classes: order processing, sales aggregations and item consolidation. A QueryClass contains the following attributes, all of them are mandatory: description, table, columns, predicateTypes, compoundExpressions, values, auto.

- **description** (*string*) : a textual description for your workload. Be descriptive! The description appears in the statistics
- **table** (*string*) : the name of the table the query class targets
- **columns** (*Array of ints*) : the database columns to be queried
- **predicateTypes** (*Array of strings*) : the types of predicate to be used. Currently supported: EQ, LT, GT
- **compoundExpressions** (*Array of Objects*) : array that cointains CompoundExpressions
- **values** (*string or array of strings/ints*) : if it is set to string, the only possible value can be auto. In this case the workload generator takes a random line from the table and adds the values for the corresponding columns to the query. If it is an array then a value has to be provided for each entry in *columns*.

#### CompoundExpression

CompoundExpressions are necessary for combining the result of scans of multiple columns. If you specified for example `columns: [0, 2, 5]` you have to specify how their results should be combined. A CompoundExpression contains the following attributes, all of them are mandatory: name, type, l, r

- **name** (*string*) : name of the CompoundExpression which is necessary to combine its result with another CompoundExpression
- **type** (*string*) : can only be `and` or `or` for now
- **l** (*int or string*) : if it is set to int, it takes the result of the scan on the column specified as its first input value. If it is set to string it takes the result of another CompoundExpression with the specified name as its first input value.
- **r** (*int or string*) : analogous to `l`, but for the second input value.

#### QueryClassDistribution

QueryClassDistributions are specifying the amounts of queries sent of a specific type in a specific time span. A QueryClassDistribution contains the following attributes, all of them are mandatory: description, validFromDay, distribution

- **description** (*string*) : description of the query class distribution
- **validFromDay** (*int*) : the distribution is valid from the specified day
- **distribution** (*Array of Floats*) : the floats have to add up to 1.0 and have to be multiples of 0.05 (0.0 is a multiple of 0.05). They are applied to the query classes and have to be specified in the same order as in `queryClasses`

#### PeriodicQueryClass

PeriodicQueryClasses are not executed every day, but only periodically. Therefore it adds a single mandatory attribute to the QueryClass:

- **period** (*int*): the QueryClass is executed every n-th day

**Usage**:
python workload.py workload.json tableDirectory [outputFile]

The *tableDirectory* should contain all the *.tbl files being used for the queries.
If *outputFile* is specified the statistical results are written to a file and not printed to stdout.


### Table Generator
The table generator generates tables in a hyrise-suitable format. The specifications of the tables to generate have to be provided as a configuration file. The specifications are defined in json format. An example is given in the *config.json* file in the tableGenerator directory.

#### Table configuration

A configuration consists of the following attributes, all of them are mandatory: name, rows, columns, stringsForEachInt, stringLength, uniqueValues.

- **name** (*string*) : name of the table
- **rows** (*int*): number of rows
- **columns** (*int*): number of columns
- **stringsForEachInt** (*int*): inserts X columns with datatype string for each column with datatype integer. The layout is always: integer_column, X string_columns, integer_columns, X string_columns. If the amount does not fit evenely it will insert as many string columns as fit in the last chunk
- **stringLength** (*array of int with two values*): minimum and maximum length of string columns. The exact value is determined randomly
- **uniqueValues** (*array of integer with length = columns or just int*): number of unique values per column. If no array and hence not a single value per column is provided the single int value is taken for all columns

**Usage**:
python generator.py config.json outputDirectory

#### If you want to generate large tables the use of pypy will speed up the process significantly.