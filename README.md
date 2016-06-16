
A collation and analysis framework to determine data conflicts between different datasources of asset/configuration item information.

1. Pull data from various datasources

 `./bin/run pull all`

 this will gather all data from all registered datasources

2. Merge data together to determine data integrity

 `./bin/run merge`

 this will also spit out a report of unmatched data. these would typically be ci's that do not match against any other ci. examples may include laptops that are not owned by SLAC but have been using our wireless.

3. Dump data to generate report

 `./bin/run dump | tee /scratch/report.tsv`

 this will create a large table of all assets and ci's, with a column defining whether the data has been deemed good or bad. Bad data is usually due to conflicts in data from the various datasources. Examples may include inconsistent serial numbers between datasources.


