#####internal information is replaced by *** #####

rm(list=ls())

library(RJDBC)
library(forecast)
library(tseries)
library(trend)
library(xlsx)

fileCon = file("***", open="r")
user = readLines(fileCon, n=1)
passwd = readLines(fileCon, n=1)
close(fileCon)
drv = JDBC("***","***")
dbConM = dbConnect(drv,"***",user,passwd)

TS_data = dbGetQuery(dbConM, "SELECT *** FROM *** WHERE *** ORDER BY 1,2;")
category = unique(TS_data[,1])

library(doParallel)
cl <- makeCluster(detectCores())
registerDoParallel(cl)

##declare a function for combining row output from each foreach loop
fun_comb <- function(...) {
  mapply('rbind', ..., SIMPLIFY = FALSE)
}

start.t <- Sys.time()
ivec = 1:8
Result <- foreach (i = ivec, .combine = "fun_comb", .packages = c("forecast")) %dopar% {
  
  BI_data = TS_data[TS_data$CATEGORY == category[i],]
  
  start_date = as.POSIXlt(as.Date(min(BI_data[,2])))
  end_date = as.POSIXlt(as.Date(max(BI_data[,2])))
  months_between = 12 * (end_date$year - start_date$year) + (end_date$mon - start_date$mon + 1)
  
  if (months_between == 69) {
    
    BI_data$month_num = as.POSIXlt(as.Date(BI_data$MONTH_BEG_DT))$year*12 + as.POSIXlt(as.Date(BI_data$MONTH_BEG_DT))$mon
    
    BI_forecast_tmp = data.frame(category = numeric(0), y = numeric(0), yhat = numeric(0))
    
    for (j in (1:12)) {
      
      start_nm = 1355 + j
      end_nm = 1355 + j + 56
      
      BI_data_train = BI_data[BI_data$month_num >= start_nm & BI_data$month_num <= end_nm,]
      BI_test = BI_data[BI_data$month_num == end_nm + 1,3]
      
      start_mon = as.POSIXlt(as.Date(BI_data_train[BI_data_train$month_num == start_nm,2]))
      BI_train_ts <- ts(BI_data_train[,3], frequency = 12, start = c(start_mon$year + 1900, start_mon$mon + 1))
      
      BI_model <- auto.arima(BI_train_ts, trace = TRUE)
      BI_predict <- forecast(BI_model, h=1, level=c(95))
      
      BI_forecast_tmp[j,1] = category[i]
      BI_forecast_tmp[j,2] = BI_test
      BI_forecast_tmp[j,3] = as.numeric(BI_predict$mean)
    }
    
    
    error_cat = category[i]
    error_rmse = sqrt(mean((BI_forecast_tmp[,3] - BI_forecast_tmp[,2]) ^ 2))
    BI_mean = mean(BI_forecast_tmp[,2])
    error_pct = error_rmse/BI_mean    
  }
  Error = c(error_cat,error_rmse,BI_mean,error_pct)
  list(BI_forecast_tmp, Error)
}
stopCluster(cl)

end.t <- Sys.time()
end.t - start.t

BI_forecast = Result[[1]]
Error = Result[[2]]

colnames(Error) = c("category","rmse","BI_mean","error_pct")

write.xlsx(x = BI_forecast, file = '***/BI_forecast.xlsx', sheetName = 'Sheet1', 
           col.names = TRUE, row.names = FALSE)

write.xlsx(x = Error, file = '***/Error.xlsx', sheetName = 'Sheet1', 
           col.names = TRUE, row.names = FALSE)
