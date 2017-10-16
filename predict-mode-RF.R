#Bing Map API Request
#Written by Ali.Y, TRIP LABP, JULY, 2017
########################################Goal of the Code########################################
#The Code trains predict the mode of transport using a trained RF model on MTL Trajet dataset.
########################################Goal of the Code########################################

##Please install the following packages ():
# install.packages("randomForest")
# install.packages("rpart.utils")
# install.packages("rpart")
# install.packages("caret")
# install.packages("lattice")
# install.packages("data.table")
# install.packages("RPostgreSQL")
# install.packages("rfUtilities")
# install.packages("e1071")

require(randomForest)
require(rpart.utils)
require(rpart)
require(caret)
require(lattice)
require(rpart.plot)
require(data.table)
require(RPostgreSQL)
require(rfUtilities)
require(e1071)

##################################################Setting the working directory######################################################
setwd("D:/Backups_Linux_September2017/29July_2017/PhD_Thesis/Reports_zak_Bilal/activity_detection")
#Please put the follwoing files in your working directory:
# 1- "Mtl-LandUse-22-codes.csv" (contains the landse categories in MTL)
# 2- "randomforest_mode_detection.rda" (The trainded RF model)
##################################################Preparing data with psql commands######################################################
# create a connection to postgresql
# save the password that we can "hide" it as best as we can by collapsing it
pw <- {
  "postgresql"
}
# loads the PostgreSQL driver
drv <- dbDriver("PostgreSQL")
# creates a connection to the postgres database
# note that "con" will be used later in each connection to the database
con <- dbConnect(drv, dbname = "MtlTrajet_tout_July2017",
                 host = "localhost", port = 5432,
                 user = "postgres", password = pw)

#get the colnames of a table from postgresql
colnames_mode_activity_trip <- dbGetQuery(con, "select column_name from information_schema.columns where
                                          table_name='mode_activity_trip';")

mode_activity_trip <- dbGetQuery(con, "SELECT * from mode_activity_trip")

##################################################Cleaning data with R commands##########################################################
mode_activity_trip$avg_speed[is.na(mode_activity_trip$avg_speed)]<- 0
mode_activity_trip<-mode_activity_trip[!(mode_activity_trip$avg_speed = 0)]

##############################################Importing sociodemographic data############################################
users <- dbGetQuery(con, "select * from users;")
users_neighobour_price<- dbGetQuery(con, "select * from land_use_users_neighobour_price_250_metre;")

###Finding the codes for residential landuse in "userusers_neighobour_price"
liste_code <- read.csv("D:/Backups_Linux_September2017/29July_2017/PhD_Thesis/Reports_zak_Bilal/activity_detection/Mtl-LandUse-22-codes.csv", header = TRUE)
residential_codes<-liste_code[which(liste_code$Category.code == 1),1]
users_neighobour_residentail_price <- users_neighobour_price[users_neighobour_price$utilisatio %in% residential_codes,]
users_neighobour_residentail_price <- aggregate(users_neighobour_residentail_price$average_price, list(users_neighobour_residentail_price$uid), mean)
colnames(users_neighobour_residentail_price)<- c("uid", "avg_price")

for(i in 1:length(mode_activity_trip[,1])){
  mode_activity_trip[i,"age"] <-  users$age_bracket[mode_activity_trip$uid[i]==users$uuid]
}

for(i in 1:length(mode_activity_trip[,1])){
  mode_activity_trip[i,"sex"] <-  users$sex[mode_activity_trip$uid[i]==users$uuid]
  
}

for(i in 1:length(mode_activity_trip[,1])){
  mode_activity_trip[i,"occupation"] <-  users$member_type[mode_activity_trip$uid[i]==users$uuid]
  
}

for(i in 1:length(mode_activity_trip[,1])){
  if(length(users_neighobour_residentail_price$avg_price[mode_activity_trip$uid[i]==users_neighobour_residentail_price$uid])== 0) mode_activity_trip[i,"avg_neibor_price"] <- 0
  else mode_activity_trip[i,"avg_neibor_price"] <-  users_neighobour_residentail_price$avg_price[mode_activity_trip$uid[i]==users_neighobour_residentail_price$uid]
  
}

###################################################Predicting the mode of transport#####################################
#reading the trained RF model
RF <- readRDS("randomforest_mode_detection.rda")
#predict the mode of transport
Prediction <- predict(RF, testing, type="response")
#making the confusion matrix
confmat_RF<-table(Prediction, testing$mode)
confmat_RF<-confusionMatrix(confmat_RF)
#summary of the results
confmat_RF



