# Querying-Moto-Marketplace-MongoDB-Redis-SQL
In this project i query web-scraped data from Greece's largest used Motorcycles Online Marketplace [www.car.gr](https://www.car.gr/classifieds/bikes/?fs=1)</br>
For querying these data, i used Redis (for caching), MongoDB, and SQL.</br>
</br>
The datasets used are the following:</br>
1) **emails_sent.csv** (contains whether emails that car.gr management sent to users with active motorcycle listings where opened or not.)</br>
2) **modified_listings.csv** (contains whether users with active motorcycle listings modified their listing or not.)</br>
3) **BIKES** (contains hundreds of json files with real web-scraped motorcycle listings (more than 35000 listings). In order to iteratively read all these files a bash one-liner command was used (it is in the **bash_oneliner.sh** file). This one liner produces the file **files_list.txt**, which will be used to iteratively
read all the json files with the motorcycle listings.</br>
</br>

R Source files:
1. In *Redis_Queries.R* R source file, there are some queries regarding the emails the users got and their modifications</br>
2. In *MongoDB_Queries.R* R source file, there are queries regarding the motorcycle listings</br>
3. In *SQL_vs_Redis_exec_time.R* R source file, the first 4 queries from *Redis_Queries.R* are being implemented by SQL (MySQL Workbench) </br>
and the execution times between Redis queries and SQL queries are being compared and plotted.
