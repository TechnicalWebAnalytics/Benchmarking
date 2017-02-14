#---------------------------------------------#
#------------| GA BENCHMARKING |--------------#
#---------------------------------------------#

# DEFINITION
# Takes all GA accounts and pushes various metrics & dimensions to MySQL

# AUTHORS
# Mechelle Warneke <- c(
# Email   = "mechellewarneke@gmail.com",
# Website = "mechellewarneke.com",
# BVACCEL = c(
# 	Email    = "mechelle@bvaccel.com",
# 	Position = "XO Strategist | Technical Web Analyst"
# )
# )

# GA Benchmarking: v1.1
# COPYRIGHT 2016
# LICENSES: MIT ( https://opensource.org/licenses/MIT )

#------------| NOTES |--------------#

# source("/home/mechelle/Google Drive/Work/Syntax/R/Snippets/initialDependencies.R")

# Write to Log Beginning
# x <- toString(Sys.time())
# write <- paste(x,"Impact Radius","Deduping","Tommy John", "Excecution Started", sep=" | ")
# write(write, file = "/home/mechelle/Documents/Logs/CronJobs/RScripts.txt", append=TRUE)

# METRICS / DIMENSIONS
# Revenue, Conv Rate, Revenue By Channel, Conv by Channel,AOV

#CLIENTS
# Tommy John, Mizzen, UnTuckit, MVMT, Boll & Branch, Bombas, Grenco, Kopari, The Balm, Turish T

#------------| Install Dependencies |--------------#

dataToMySQL <- function(db,db_table,dataframe,debug=debug){
  print(db_table)
  dt <- data.table(t(dataframe))
  # print(dt)
  query <- paste("INSERT INTO", db_table,
                 "\nVALUES\n",
                 gsub("c\\(",'\\(',(dt[,paste(.SD,collapse=',\n')])))

  if(debug == TRUE){
    cat(query)
  }else{
    dbGetQuery(db, query)
  }
}

monthSequence <- function(date,numberOfMonths,iterator, default=TRUE){

  numberOfDays <- function(date) {
    m <- format(date, format="%m")

    while (format(date, format="%m") == m) {
      date <- date + 1
    }

    return(as.integer(format(date - 1, format="%d")))
  }

  if(default == TRUE){
    increment <- seq(as.Date(date),by="month",length.out=numberOfMonths)
  }
  startDate <- increment[iterator]
  endDate <- paste0(format(increment[iterator], "%Y-%m-"), numberOfDays(increment[iterator])) # Year with century
  datelist <- list("startDate" = startDate, "endDate" = endDate)
  return(datelist)
  default <- FALSE
}

prnt.test <- function(x){
  cat(x, sep="\n")
}

#-------------| AUTHORIZATIONS |--------------#

con <- dbConnect(MySQL(),
                 user='bva-datastudio', password='bvA1234!',
                 dbname='bvaccel', host='138.68.12.219', port=3306)

on.exit(dbDisconnect(con))

# Get any current
rs <- dbSendQuery(con, "SELECT DISTINCT(View) AS View FROM client_benchmarking")
matchView <- fetch(rs, n=-1)
ds <- dbSendQuery(con, "SELECT DISTINCT(DATE) AS DATE FROM client_benchmarking")
matchDate <- fetch(ds, n=-1)

# Google Analytics
tokens <- c(
  # analytics@bvaccel.com
  "/home/mechelle/Google Drive/Work/Syntax/R/GATokens/.gatoken1.rds",
  # analytics2@bvaccel.com
  "/home/mechelle/Google Drive/Work/Syntax/R/GATokens/.gatoken2.rds")

tokens <- c(
  # analytics@bvaccel.com
  "/home/mechelle/Documents/Scripts/R/GATokens/.gatoken1.rds",
  # analytics2@bvaccel.com
  "/home/mechelle/Documents/Scripts/R/GATokens/.gatoken2.rds")

#-------------| FUNCTIONALITY |--------------#

totalProcessedViews <- 0
totalProcessedRows <- 0

for (autho in 1:length(tokens)) {
  # ------------------------------- Check amount of authorization tokens ( GA )
  print(paste0("Set Authorization: ",autho))
  authorize(cache=tokens[autho])

  #------------| Date Range |--------------#

  # To prevent sampling we need to run a by month extract

  # Amount of months to detect

  # dates$startDate <- "2016-01-01"
  # dates$endDate <- "2016-12-31"

  # Get GA Accounts and Views
  getViews <- list_profiles()
  getAccount <- list_accounts()

  #------------| Begin Data Extraction ( by Starred Views ) |--------------#

  for (i in 1:nrow(getViews)) {
    # --------------------------- Begin View Loop

    months <- 12

    for (countdate in 1:months) {
      # --------------------------- Begin Count Date

      # The beginning date
      dates <- monthSequence("2016-01-01",months,countdate,default=TRUE)

      prnt.test(c("",paste0("Date Increment: ",countdate),""))

      # if(getViews$starred[i] %in% TRUE && "12393183" %in% getViews[1]){
      if(getViews$starred[i] %in% TRUE){
        # 12393183
        # -------------------------------- If View is starred

        print(paste0("START CONSTRUCT: ",getViews$websiteUrl[i]))
        print(paste0("View Order in Account: ", i))

        # Get Property Name
        propertyName <- get_webproperty(accountId = getViews$accountId[i], webPropertyId = getViews$webPropertyId[i])$name

        # Check Industry by matching View ID to Account
        industry <- get_webproperty(accountId = getViews$accountId[i], webPropertyId = getViews$webPropertyId[i])$industryVertical

        # Get Account Name ( Client Name basically )
        for (ii in 1:nrow(getAccount)) {
          if(getViews$accountId[i] %in% getAccount$id[ii]){
            accountName <- getAccount$name[ii]

          }				}

        # If industry is empty return "UNSPECIFIED"
        if(is.null(industry)){
          industry <- "UNSPECIFIED"
        }

        # Revenue, Revenue By Channel, Conv by Channel
        # Conversion Rate: transactions / sessions
        # AOV: total revenue / number of orders

        print("Run GA API")

        if(accountName != "Demo Account (Beta)" && !getViews[i,]$id %in% matchView[,1] ){

          # Get GA Data
          gadata <- get_ga(getViews$id[i],
                           start.date = dates$startDate,
                           end.date = dates$endDate,
                           metrics = "ga:transactionRevenue,ga:sessions,ga:transactions,ga:bounceRate,ga:pageviews,ga:avgSessionDuration",
                           dimensions = "ga:source,ga:medium,ga:channelGrouping,ga:date,ga:deviceCategory,ga:browser,ga:userType",
                           sort = "ga:date",
                           fetch.by = "month"
          )

          # Construct as Dataframe
          gadata <- data.frame(gadata)

        }else{

          print('Data Exists / Demo Account')
          length(gadata) <- 0

        }

        # If no GA data skip
        if(length(gadata) < 1 ){

          print("NO DATA")

        }else{

          # Set DataFrames
          df_account <- NULL
          fuckyeah <- NULL

          # Reconstruct Column Names
          colnames(gadata) <- c(
            "Source",
            "Medium",
            "ChannelGrouping",
            "Date",
            "Device",
            "Browser",
            "UserType",
            "TransactionRevenue",
            "Sessions",
            "Transactions",
            "BounceRate",
            "Pageviews",
            "AvgSessionDuration"
          )

          # Construct GA Account Info
          df_account <- t(c(
            AccountName  = as.character(accountName),
            PropertyName = as.character(propertyName),
            ViewName     = as.character(getViews$name[i]),
            WebsiteURL   = as.character(getViews$websiteUrl[i]),
            Industry     = as.character(industry),
            Currency     = as.character(getViews$currency[i]),
            Timezone     = as.character(getViews$timezone[i]),
            Created      = as.character(getViews$created[i]),
            Permissions  = as.character(getViews$permissions[i]),
            AccountID    = as.character(getViews$accountId[i]),
            UAID         = as.character(getViews$webPropertyId[i]),
            View         = as.character(getViews$id[i])
          ))

          print("Build Data")
          # Merge both DataFrames
          fuckyeah <- rbind(fuckyeah,merge(df_account,gadata))

          print("Push to SQL")
          # Push to MySQL
          dataToMySQL(con,"client_benchmarking",fuckyeah,debug=FALSE)

          totalProcessedRows <- totalProcessedRows + nrow(fuckyeah)
          totalProcessedViews <- totalProcessedViews + 1

          print(paste0("View Increment: ", totalProcessedViews))
          print(paste0("Rows Processed: ",nrow(fuckyeah)))
          print(paste0("CONSTRUCT COMPLETE: ",getViews$websiteUrl[i]))

          # Spaces
          prnt.test(c("",""))
        }
        # -------------------------------- If View is starred
      }
      # --------------------------- Begin View Loop
    }
    # --------------------------- Begin Count Date
  }
  # ------------------------------- Check amount of authorization tokens
}
