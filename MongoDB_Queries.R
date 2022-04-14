########################################## MongoDB Queries #####################################################

# Loading the necessary packages
require(jsonlite)
require(mongolite)
require(lubridate)
require(stringr)

# Opening a connection to MongoDB and creating the collection that we will use
m <- mongo(collection = "bikes",  db = "my_bikesdb", url = "mongodb://localhost")

##### Adding and cleaning the data #####

# Reading the files_list.txt
files_list <- read.csv2("files_list.txt", header = FALSE)

# Importing the dataset
for (row in 1:nrow(files_list)){
  data <-fromJSON(files_list[row,])
  
  # Data Cleaning
  if (data$ad_data$Price == 'Ask for price'){
    data$ad_data$Price <- "0"
  }
  # Price Cleaning units : €
  data$ad_data$Price <- gsub(".*€", "", data$ad_data$Price)
  data$ad_data$Price <- gsub("\\.", "", data$ad_data$Price)
  data$ad_data$Price <- as.numeric(data$ad_data$Price)
  
  # Mileage Cleaning units : km
  data$ad_data$Mileage <- gsub(" km", "", data$ad_data$Mileage)
  data$ad_data$Mileage <- gsub(",", "", data$ad_data$Mileage)
  data$ad_data$Mileage <- as.numeric(data$ad_data$Mileage)
  
  # Power Cleaning units : bhp
  data$ad_data$Power <- gsub(" bhp.*", "", data$ad_data$Power)
  data$ad_data$Power <- as.numeric(data$ad_data$Power)
  
  # Cubic capacity Cleaning units : cc
  data$ad_data$'Cubic capacity' <- gsub("cc.*", "", data$ad_data$'Cubic capacity')
  data$ad_data$'Cubic capacity' <- as.numeric(data$ad_data$'Cubic capacity')
  
  # Registration cleaning keeping only the year
  data$ad_data$Registration <- gsub(".*/", "", data$ad_data$Registration)
  data$ad_data$Registration <- as.numeric(data$ad_data$Registration)
  
  # Calculating the age of bike
  data$ad_data$Age <- year(Sys.Date()) - data$ad_data$Registration
  
  #Creating an other attribute if the price is Negotiable
  if (str_detect(data$metadata$model , "Negotiable")){
    data$ad_data$Negotiable <- as.logical("TRUE")
  }
  else{
    data$ad_data$Negotiable <- as.logical("FALSE")
  }
  
  # Importing the cleaned data to MongoDB
  data <- toJSON(data, auto_unbox = TRUE)
  m$insert(data)
}

#####	How many bikes are there for sale? #####
m$count('{}') #Calculating the total number of bikes for sales assuming that all ads are for sale

##### What is the average price of a motorcycle ? What is the number of listings ###
##### that were used in order to calculate this average (give a number as well)? ###
##### Is the number of listings used the same as the previous answer? Why? #########

#if we assume that a fair price for sale is around 150 euros, then the number of listings with this price and above are:
count <- m$aggregate('[
  {"$match": {"ad_data.Price": { "$gte": 150 }}}]')

nrow(count) #The number of bike listings that are for sale and their price is gte 150

#Calculating the average price
Average_Price <- m$aggregate(
  '[
  {"$match": {"ad_data.Price": { "$gte": 150 }}},
  {"$group":{"_id": null, "average":{"$avg":"$ad_data.Price"}}}
  ]'
)

round(Average_Price$average,1) #The average price is 3035.8

#####	What is the maximum and minimum price of a motorcycle currently available in the market? #####

#Assuming that a real minimum price for sale is gte to 150 euros then to calculate the min price we can 
#run the following query

MinPrice <- m$aggregate(
  '[
   {"$match": {"ad_data.Price": { "$gt": 150 }}},
   {"$group":{"_id": null, "min":{"$min":"$ad_data.Price"}}}
   ]'
)

MinPrice$min #The min price if a bike is 170 euros

#Similarly we can find the max price 

MaxPrice <- m$aggregate(
  '[
   {"$match": {"ad_data.Price": { "$gt": 150 }}},
   {"$group":{"_id": null, "max":{"$max":"$ad_data.Price"}}}
   ]'
)

MaxPrice$max #The max price for a bike is 89000 euros which can be reasonable for limited edition bikes or custom bikes (buggy in our case)

#####	How many listings have a price that is identified as negotiable? #####

# We have created a new column with a Boolean variable regarding whether the price of a bike is negotiable or not
# so we can simply count the number of True value appearances in this column 

NegotiablePrice <- m$aggregate(
  '[
  {"$match": {"ad_data.Negotiable": true}},
  {"$group": { "_id": null, "Count": { "$sum": 1 }}}
                   ]'
)

NegotiablePrice$Count # There are 1348 bikes with negotiable price

##### What is the motorcycle brand with the highest average price? #####
brands_grouped_max_price <- m$aggregate(
  '[
    {"$group": {"_id": "$metadata.brand", "avg_price": {"$avg" : "$ad_data.Price"}}},
    {"$sort": {"avg_price":-1}},
    {"$limit": 1}
  ]'
)

brands_grouped_max_price #The brand with the highest avg price is Semog (avg price: 15600) (they are buggies not motorcycles)

##### What are the TOP 10 models with the highest average age? #####

# During the data cleaning process we kept only the year in Registration and we also calculated
# the age of each bike so this task is preaty easy now

avg_age <- m$aggregate(
  '[
    {"$group": { "_id": "$metadata.model", "Avg_Age": { "$avg": "$ad_data.Age"}}},
    {"$sort": {"Avg_Age":-1}},
    {"$limit": 10}
  ]'
) 

avg_age

##### How many bikes have "ABS" as an extra #####
abs_count <- m$aggregate(
  '[{"$match": {"extras": { "$eq": "ABS" }}},
  {"$group": { "_id": null, "Count": { "$sum": 1 }}}]') 

abs_count #There are 4025 bikes with ABS as an extra

##### What is the average Mileage of bikes that have "ABS" AND "Led lights" as an extra? #####
abs__led_avg_mls <- m$aggregate(
  '[
    {"$match": { "$and": [ {"extras":"ABS" }, {"extras":"Led lights" }]}},
    {"$group": { "_id": null, "Avg_mileage": { "$avg": "$ad_data.Mileage" }}}
  ]'
) 

abs__led_avg_mls # The average mileage is 30125.7 for bikes with ABS and Led lights