library(RMySQL)
library(lubridate)

# The numbers below are the execution times of questions 1.1, 1.2, 1.3, and 1.4 concurrently.
# We took those numbers from 
exec_time_redis_1 = 0.001496315
exec_time_redis_2 = 0.0025138
exec_time_redis_3 = 0.001689672
exec_time_redis_4 = 0.001984358

# Importing the data
EMAILS_SENT <- read.csv("C:\\Users\\user\\Dropbox\\My PC (DESKTOP-FQ14F0C)\\Desktop\\M.Sc\\Winter Semester\\Big Data Systems & Architectures\\Assignment_1\\Redis-Mongo Assignment-v1.3\\RECORDED_ACTIONS\\emails_sent.csv")

MODIFIED_LISTINGS <- read.csv("C:\\Users\\user\\Dropbox\\My PC (DESKTOP-FQ14F0C)\\Desktop\\M.Sc\\Winter Semester\\Big Data Systems & Architectures\\Assignment_1\\Redis-Mongo Assignment-v1.3\\RECORDED_ACTIONS\\modified_listings.csv")

# After installing and loading the RMySQL library, we create a database connection object.
mydb = dbConnect(MySQL(), user='root', password='*********', dbname='emails', host='localhost')


# Here we write the dataframes to tables in MySQL
dbSendQuery(mydb, "SET GLOBAL local_infile = true;")
dbWriteTable(mydb, name='emails_sent', value=EMAILS_SENT)
dbWriteTable(mydb, name='modified_listings', value=MODIFIED_LISTINGS)


# Sending a query (here i create the table needed so to make the two implementations, one with Redis and one 
# with SQL to look as similar as possible as it can get)

#########################################################
### How many users modified their listing on January? ###
#########################################################
rs = dbSendQuery(mydb, "
                        CREATE TABLE JanuaryModifications AS
                        SELECT UserID, MonthID, ModifiedListing
                        FROM modified_listings
                        WHERE MonthID = 1 AND ModifiedListing = 1; "
)

start1 = now(tzone = "UTC+2") 

rs1 = dbSendQuery(mydb, "
                          SELECT COUNT(*) 
                          FROM januarymodifications;"
)

# The results of this query remain on the MySQL server, so to access the results in R we need to use the 
# fetch function
data = fetch(rs1, n=-1)

end1 = now(tzone = "UTC+2")
exec_time_sql_1 <- (end1 - start1) # Execution time of Question 1.1 with SQL

print(data)
print(exec_time_sql_1)

# Inserting execution times in a dataframe to plot them

data_comparison <- data.frame(
  name = c("exec_time_redis_1", "exec_time_sql_1"),
  value = c(exec_time_redis_1, exec_time_sql_1)
)
# Plotting the execution times
barplot(height = data_comparison$value, names = data_comparison$name,
        col = "light blue",
        las = 1,
        ylab = "Execution time in seconds",
        main = "Exec. time comparison of Question 1.1 betw. Redis & SQL")
# It appears that this question was executed approximately 14.5 times faster with Redis than with SQL 


###############################################################
### How many users did NOT modify their listing on January? ###
###############################################################

start2 = now(tzone = "UTC+2") 

rs2 = dbSendQuery(mydb, "
                          SELECT COUNT(distinct(UserID))
                          FROM modified_listings
                          WHERE UserID NOT IN ( SELECT UserID 
                                                FROM januarymodifications
                                                );
                        "
)

# The results of this query remain on the MySQL server, so to access the results in R we need to use the 

# fetch function
data = fetch(rs2, n=-1)

end2 = now(tzone = "UTC+2")
exec_time_sql_2 <- (end2 - start2) # Execution time of Question 1.2 with SQL

print(data)
print(exec_time_sql_2)

# Inserting execution times in a dataframe to plot them
data_comparison_2 <- data.frame(
  name = c("exec_time_redis_2", "exec_time_sql_2"),
  value = c(exec_time_redis_2, exec_time_sql_2)
)
# Plotting the execution times
barplot(height = data_comparison_2$value, names = data_comparison_2$name,
        col = "light blue",
        las = 1,
        ylab = "Execution time in seconds",
        main = "Exec. time comparison of Question 1.2 betw. Redis & SQL")
# It appears that this question was executed approximately 39 times faster with Redis than with SQL 

################################################################
##### How many users received at least one e-mail per month ####
##### (at least one e-mail in January and at least one e-mail ##
##### in February and at least one e-mail in March)? ###########
################################################################

# Here we construct 3 tables that "replicate" "EmailsJanuary", "EmailsFebruary", "EmailsMarch" BITMAPS
rs3 = dbSendQuery(mydb,"CREATE TABLE JanReceivedEmails AS 
                        SELECT UserID, MonthID, Emails
                        FROM (
		                        SELECT UserID, MonthID, (CASE WHEN EmailID is not NULL THEN COUNT(EmailID) else 0 END) AS Emails
		                        FROM (
				                          SELECT a.UserID, a.MonthID, a.ModifiedListing, b.EmailID, b.EmailOpened
				                          FROM modified_listings AS a
				                          LEFT JOIN emails_sent AS b
				                          ON a.UserID = b.UserID AND a.MonthID = b.MonthID
			                            ) AS x
                        		WHERE MonthID = 1
                            GROUP BY UserID, MonthID
                            ) AS y; "
)          

rs4 = dbSendQuery(mydb,"CREATE TABLE FebReceivedEmails AS 
                        SELECT UserID, MonthID, Emails
                        FROM (
		                        SELECT UserID, MonthID, (CASE WHEN EmailID is not NULL THEN COUNT(EmailID) else 0 END) AS Emails
		                        FROM (
				                          SELECT a.UserID, a.MonthID, a.ModifiedListing, b.EmailID, b.EmailOpened
				                          FROM modified_listings AS a
				                          LEFT JOIN emails_sent AS b
				                          ON a.UserID = b.UserID AND a.MonthID = b.MonthID
			                            ) AS x
		                        WHERE MonthID = 2
                            GROUP BY UserID, MonthID
                            ) AS y;"
)
rs5 = dbSendQuery(mydb,"CREATE TABLE MarReceivedEmails AS 
                        SELECT UserID, MonthID, Emails
                        FROM (
		                        SELECT UserID, MonthID, (CASE WHEN EmailID is not NULL THEN COUNT(EmailID) else 0 END) AS Emails
		                        FROM (
				                        SELECT a.UserID, a.MonthID, a.ModifiedListing, b.EmailID, b.EmailOpened
				                        FROM modified_listings AS a
				                        LEFT JOIN emails_sent AS b
				                        ON a.UserID = b.UserID AND a.MonthID = b.MonthID
			                          ) AS x
		                        WHERE MonthID = 3
                            GROUP BY UserID, MonthID
                            ) AS y; "
)

start3 = now(tzone = "UTC+2") 

rs6 = dbSendQuery(mydb, "
                          SELECT COUNT(UserID) 
                          FROM janreceivedemails
                          WHERE Emails >=1
                          AND UserID IN (SELECT UserID FROM febreceivedemails WHERE Emails >=1)
                          AND UserID IN (SELECT UserID FROM marreceivedemails WHERE Emails >=1);"
)

# The results of this query remain on the MySQL server, so to access the results in R we need to use the 
# fetch function
data = fetch(rs6, n=-1)

end3 = now(tzone = "UTC+2")
exec_time_sql_3 <- (end3 - start3) # Execution time of Question 1.1 with SQL

print(data)
print(exec_time_sql_3)

# Inserting execution times in a dataframe to plot them
data_comparison <- data.frame(
  name = c("exec_time_redis_3", "exec_time_sql_3"),
  value = c(exec_time_redis_3, exec_time_sql_3)
)
# Plotting the execution times
barplot(height = data_comparison$value, names = data_comparison$name,
        col = "light blue",
        las = 1,
        ylab = "Execution time in seconds",
        main = "Exec. time comparison of Question 1.3 betw. Redis & SQL")

# It appears that this question was executed approximately 52 times faster with Redis than with SQL 


##########################################################################################
###### How many users received an e-mail on January and March but NOT on February? #######
##########################################################################################

start4 = now(tzone = "UTC+2") 

rs7 = dbSendQuery(mydb, "
                          SELECT COUNT(UserID) 
                          FROM janreceivedemails
                          WHERE Emails >=1
                          AND UserID NOT IN (SELECT UserID FROM febreceivedemails WHERE Emails >=1)
                          AND UserID IN (SELECT UserID FROM marreceivedemails WHERE Emails >=1);"
)

# The results of this query remain on the MySQL server, so to access the results in R we need to use the 
# fetch function
data = fetch(rs7, n=-1)

end4 = now(tzone = "UTC+2")
exec_time_sql_4 <- (end4 - start4) # Execution time of Question 1.4 with SQL

print(data)
print(exec_time_sql_4)

# Inserting execution times in a dataframe to plot them

data_comparison <- data.frame(
  name = c("exec_time_redis_4", "exec_time_sql_4"),
  value = c(exec_time_redis_4, exec_time_sql_4)
)
# Plotting the execution times
barplot(height = data_comparison$value, names = data_comparison$name,
        col = "light blue",
        las = 1,
        ylab = "Execution time in seconds",
        main = "Exec. time comparison of Question 1.4 betw. Redis & SQL")

# It appears that this question was executed approximately 23 times faster with Redis than with SQL 

# Closing the connection with MySQL
dbDisconnect(mydb)
