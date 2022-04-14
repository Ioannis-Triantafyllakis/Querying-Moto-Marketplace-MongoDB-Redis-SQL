# Importing the necessary libraries 
library("redux")
library("lubridate")
 

# Create the connection with the local instance of REDIS
r <- redux::hiredis(
  redux::redis_config(
    host = "127.0.0.1", 
    port = "6379"))

#Importing the data
emails <- read.csv("emails_sent.csv")
listings <- read.csv("modified_listings.csv")



##### How many users modified their listing on January? #####

#We have to create a dataframe for January 
listingJan <- listings[which(listings$MonthID ==1),]

#Now we can setbit 1 to each user that modified their listing in January
#we use the dim()[1] to calculate the rows of lisitngJan
for (i in 1:dim(listingJan)[1]){ 
  if (listingJan$ModifiedListing[i] == 1){
    r$SETBIT("ModificationsJanuary", i, "1")
  }
}

#Calculating the number of users that modified their listings
start1 = now()

r$BITCOUNT("ModificationsJanuary")

end1 = now()
exec_time_redis_1 <- end1 - start1
print(exec_time_redis_1)

#####	How many users did NOT modify their listing on January? #####

start2 = now()

#Creating a bitmap with the inversion of the previous
r$BITOP("NOT", "NoModificationsJan", "ModificationsJanuary")

#Calculating the number of users that did not modified their listings in January
r$BITCOUNT("NoModificationsJan") 
# The sum of (NoModificationsJan + ModificationsJanuary) is 
# 20000 1 more than the total number of users (19999 Obs. in ListingJan)

end2 = now()
exec_time_redis_2 <- end2 - start2

print(exec_time_redis_2)

###### How many users received at least one e-mail per month (at least one e-mail in January ###
###### and at least one e-mail in February and at least one e-mail in March)?  #################

#We have to create 3 dataframes, one for each month
#We use the table() to create a column with the number of mails each user received during each month
emailsJan <- as.data.frame(table(emails$UserID[emails$MonthID==1]))
colnames(emailsJan) <- c("User_ID", "No_of_Emails_Jan")

emailsFeb<-as.data.frame(table(emails$UserID[emails$MonthID==2]))
colnames(emailsFeb) <- c("User_ID", "No_of_Emails_Feb")

emailsMar<-as.data.frame(table(emails$UserID[emails$MonthID==3]))
colnames(emailsMar) <- c("User_ID", "No_of_Emails_Mar")

#Creating the merged dataframe which contains the No of mails each user received for all 3 months
allmonths0 <- merge(emailsJan, emailsFeb, by = "User_ID", all.x = T)
allmonths <- merge(allmonths0, emailsMar, by = "User_ID", all.x = T) #The final dataframe

#The final dataframe contains NAs so we choose to convert them to zeros
allmonths[is.na(allmonths)] <- 0

#Now the allmonths dataframe contains the exact number of emails that each user received in each month even if the No is 0
#Now we are ready to create the bitmaps

#January Bitmap
for (i in 1:dim(allmonths)[1]){if(allmonths$No_of_Emails_Jan[i]!=0){r$SETBIT("EmailsJanuary",i,"1")}}

#February Bitmap
for (i in 1:dim(allmonths)[1]){if(allmonths$No_of_Emails_Feb[i]!=0){r$SETBIT("EmailsFebruary",i,"1")}}

#March Bitmap
for (i in 1:dim(allmonths)[1]){if(allmonths$No_of_Emails_Mar[i]!=0){r$SETBIT("EmailsMarch",i,"1")}}

start3 = now()

#With the bitmaps set we are ready to use BITOP and BITCOUNT to find the number of users that received a mail in all months
r$BITOP("AND", "All_months_email", c("EmailsJanuary", "EmailsFebruary", "EmailsMarch"))
r$BITCOUNT("All_months_email") #The total number of users that received at least one mail per month

end3 = now()
exec_time_redis_3 <- end3 - start3
print(exec_time_redis_3)

#####	How many users received an e-mail on January and March but NOT on February?  #####

#January and March
r$BITOP("AND", "JanMar", c("EmailsJanuary", "EmailsMarch"))

#Creating the inversion of February
r$BITOP("NOT", "NotEmailFebruary", "EmailsFebruary")

start4 = now()

#Bitmaps with users that received mails on January and March but not on February
r$BITOP("AND", "AllButFeb", c("NotEmailFebruary", "JanMar"))
r$BITCOUNT("AllButFeb") #Calculating the number of customers that received a mail on January and March but not on February

end4 = now()
exec_time_redis_4 <- end4 - start4
print(exec_time_redis_4)

#####	How many users received an e-mail on January that they did not open but they updated their listing anyway? #####

#We have allready the Modifications of January from the first query in a BITMAP
#We want to create a dataframe with the user_id, the month and the number of emails received 
emails_received_per_month <- as.data.frame(table(emails$UserID, emails$MonthID))
colnames(emails_received_per_month) <- c("User_ID", "Month", "No of Emails")

#The emails_received_per_month dataframe contains the number of mails received each user per month even if the number is zero
#We want to create a similar dataframe but this time only with the opened emails
emails_opened_per_month <- as.data.frame(table(emails$UserID[emails$EmailOpened==1], emails$MonthID[emails$EmailOpened==1]))
colnames(emails_opened_per_month) <- c("User_ID", "Month", "No of opened Emails")

#Now we can merge the two dataframes on User_Id and Month
merged_mails <- merge(emails_received_per_month, emails_opened_per_month, by = c("User_ID", "Month"), all.x = T)

#We convert NAs in No of Opened mails to 0
merged_mails$`No of opened Emails`[is.na(merged_mails$`No of opened Emails`)] <- 0

#Finally we keep only the users that received at least one mail
merged_mails <- merged_mails[merged_mails$`No of Emails` > 0,]

#We subset the above dataframe to keep only January
merged_mails_Jan <- merged_mails[merged_mails$Month == 1,]

#Now we are ready to create the bitmap
for (i in 1:dim(merged_mails_Jan)[1]){
  if (merged_mails_Jan$`No of opened Emails`[i]!=0){
    r$SETBIT("EmailsOpenedJanuary", i, "1")
  }
}

#Calculation the inversion of the above bitmap
r$BITOP("NOT", "EmailsNotOpenedJanuary", "EmailsOpenedJanuary")

#Using BITOP to answer the question as now we have the EmailsNotOpenedJanuary and the ModificationsJanuary
r$BITOP("AND", "NotOpenedButUpdateJan", c("EmailsNotOpenedJanuary", "ModificationsJanuary"))

r$BITCOUNT("NotOpenedButUpdateJan") #The number of users

#####	How many users received an e-mail on January that they did not open but they updated ###########
#### their listing anyway on January OR they received an e-mail on February that they did not open ###
#### but they updated their listing anyway on February OR they received an e-mail on March that they##
#### did not open but they updated their listing anyway on March? ####################################

#Creating a dataframe for each month
merged_mails_Feb <- merged_mails[merged_mails$Month == 2,]
merged_mails_Mar <- merged_mails[merged_mails$Month == 3,]

#Creating a BitMap for EmailsOpened on each month and then finding the inversion of each bitmap

#February Bitmap
for (i in 1:dim(merged_mails_Feb)[1]){
  if (merged_mails_Feb$`No of opened Emails`[i]!=0){
    r$SETBIT("EmailsOpenedFebruary", i, "1")
  }
}

#March Bitmap
for (i in 1:dim(merged_mails_Mar)[1]){
  if (merged_mails_Mar$`No of opened Emails`[i]!=0){
    r$SETBIT("EmailsOpenedMarch", i, "1")
  }
}

r$BITOP("NOT", "EmailsNotOpenedFeb", "EmailsOpenedFebruary") #February Inversion
r$BITOP("NOT", "EmailsNotOpenedMar", "EmailsOpenedMarch") #March Inversion

#Now we want to create the bitmap that contain the users that modified their listing for February and March
listingFeb <- listings[which(listings$MonthID==2),] #The listings for February
listingMar <- listings[which(listings$MonthID==3),] #The listings for March

#February Bitmap
for (i in 1:dim(listingFeb)[1]){
  if (listingFeb$ModifiedListing[i] == 1){
    r$SETBIT("ModificationsFebruary", i, "1")
  }
}

#March Bitmap
for (i in 1:dim(listingMar)[1]){
  if (listingMar$ModifiedListing[i] == 1){
    r$SETBIT("ModificationsMarch", i, "1")
  }
}

#Now we can find the AND Bitops with the users that did not open the mail but they updated anyway
r$BITOP("AND", "NotOpenedButUpdateFeb", c("EmailsNotOpenedFeb", "ModificationsFebruary"))
r$BITOP("AND", "NotOpenedButUpdateMar", c("EmailsNotOpenedMar", "ModificationsMarch"))
r$BITOP("OR", "NotOpenedButUpdateAll", c("NotOpenedButUpdateJan", "NotOpenedButUpdateFeb", "NotOpenedButUpdateMar"))

r$BITCOUNT("NotOpenedButUpdateAll") #The number of customers

##### Does it make any sense to keep sending e-mails with recommendations to sellers? Does this strategy really work? ###
###### How would you describe this in terms a business person would understand?##########################################

# We have the number of people that modified but not opened the mail
r$BITCOUNT("NotOpenedButUpdateAll") # number of people that modified their listings without opening the mail

# Now we want to calculate the number of people that modified their listings after opening the mail
r$BITOP("AND", "OpenedAndUpdateJan", c("EmailsOpenedJanuary", "ModificationsJanuary"))
r$BITOP("AND", "OpenedAndUpdateFeb", c("EmailsOpenedFebruary", "ModificationsFebruary"))
r$BITOP("AND", "OpenedAndUpdateMar", c("EmailsOpenedMarch", "ModificationsMarch"))
r$BITOP("OR", "OpenedAndUpdateAll", c("OpenedAndUpdateJan", "OpenedAndUpdateFeb", "OpenedAndUpdateMar"))

r$BITCOUNT("OpenedAndUpdateAll") #The number of customers that updated their listings after opening the mail



