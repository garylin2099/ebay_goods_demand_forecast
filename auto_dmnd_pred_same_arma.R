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
# getDoParWorkers()

##declare a function for combining row output from each foreach loop
fun_comb <- function(...) {
  mapply('rbind', ..., SIMPLIFY = FALSE)
}

#specify the period that needs predicting
all_pred_month = seq(as.Date("2015-01-01"), as.Date("2018-09-01"), by = "month")
pred_length = length(all_pred_month)

start.t <- Sys.time()
ivec = 1:length(category)
Result <- foreach (i = ivec, .combine = "fun_comb", .packages = c("forecast")) %dopar% {
  
  BI_data = TS_data[TS_data$CATEGORY == category[i],]
  
  start_date = as.POSIXlt(as.Date(min(BI_data[,2])))
  end_date = as.POSIXlt(as.Date(max(BI_data[,2])))
  
  
  BI_data$month_num = as.POSIXlt(as.Date(BI_data$MONTH_BEG_DT))$year*12 + as.POSIXlt(as.Date(BI_data$MONTH_BEG_DT))$mon
  
  BI_forecast_tmp = data.frame(pred_month_beg_dt = all_pred_month, category = rep(category[i], pred_length),
                               y = numeric(pred_length), yhat = numeric(pred_length), arma = rep(NA, pred_length))
  
  #given category i, each for loop below forecasts BI of that category in each month in all_pred_month 
  for (j in 1:pred_length) {
    pred_num = as.POSIXlt(all_pred_month[j])$year*12 + as.POSIXlt(all_pred_month[j])$mon
    start_nm = pred_num - 36
    end_nm = pred_num - 1
    
    BI_data_train = BI_data[BI_data$month_num >= start_nm & BI_data$month_num <= end_nm,]
    BI_test = BI_data[BI_data$month_num == pred_num,3]
    
    #create a time series (ts) object to feed arima model. Since ts needs argument in actual date format instead of month_num,
    #we need to convert the month_num to actual date first
    start_mon = as.POSIXlt(as.Date(BI_data_train[BI_data_train$month_num == start_nm,2]))
    BI_train_ts <- ts(BI_data_train[,3], frequency = 12, start = c(start_mon$year + 1900, start_mon$mon + 1))
    
    BI_model <- auto.arima(BI_train_ts, trace = TRUE)
    BI_predict <- forecast(BI_model, h=1, level=c(95))
    arma <- paste(BI_model[["arma"]], collapse = "")
    
    BI_forecast_tmp[j,3:4] = c(BI_test, as.numeric(BI_predict$mean))
    BI_forecast_tmp[j,5] = arma
  }
  
  
  error_cat = category[i]
  error_rmse = sqrt(mean((BI_forecast_tmp[,4] - BI_forecast_tmp[,3]) ^ 2))
  BI_mean = mean(BI_forecast_tmp[,3])
  error_pct = error_rmse/BI_mean    
  
  Error = c(error_cat,error_rmse,BI_mean,error_pct)
  list(BI_forecast_tmp, Error)
}

end.t <- Sys.time()
end.t - start.t

BI_forecast = Result[[1]]
Error = Result[[2]]
colnames(Error) = c("category","rmse","BI_mean","error_pct")

cat_arima_sel = data.frame (
  category = category[ivec]
)

## check if we are still running in parallel
getDoParWorkers()
## get the most frequently used arima parameter values in each category, store it in a list
par_sel_list <- 
  foreach (i = ivec, .combine = "fun_comb") %dopar% {
    sel_times = sort(table(BI_forecast$arma[BI_forecast$category==category[i]]), decreasing = T)[1]
    sel_par = names(sel_times)
    list(sel_par, sel_times)
  }

cat_arima_sel$parameter_sel = par_sel_list[[1]]
cat_arima_sel$sel_times = par_sel_list[[2]]

cat_arima_sel$AR = substr(cat_arima_sel$parameter_sel, 1, 1)
cat_arima_sel$MA = substr(cat_arima_sel$parameter_sel, 2, 2)
cat_arima_sel$season_AR = substr(cat_arima_sel$parameter_sel, 3, 3)
cat_arima_sel$season_MA = substr(cat_arima_sel$parameter_sel, 4, 4)
cat_arima_sel$period = substr(cat_arima_sel$parameter_sel, 5, 6)
cat_arima_sel$diffrc = substr(cat_arima_sel$parameter_sel, 7, 7)
cat_arima_sel$season_diffrc = substr(cat_arima_sel$parameter_sel, 8, 8)

save(cat_arima_sel, file = "Projects/arima_parameter_each_cat.Rda")


########################################################################
######### Predict Each Category with the Same ARIMA Parameters ########
#######################################################################
load("~/Projects/arima_parameter_each_cat.Rda")
start.t <- Sys.time()
ivec = 86
##the following snippet is almost the same as the first prediction foreach loop except the use of simple arima function
##with parameters specified and fed, as opposed to the auto.arima function above
Result_same_par <- foreach (i = ivec, .combine = "fun_comb", .packages = c("forecast")) %dopar% {
  
  BI_data = TS_data[TS_data$CATEGORY == category[i],]
  
  ##specify the arima parameters to be used in predicting this category
  ##non-seasonal oder (AR, differencing, MA)
  cat_order = as.numeric(cat_arima_sel[i, c(4, 9, 5)])
  ##seasonal (AR, differencing, MA)
  cat_seasonal = as.numeric(cat_arima_sel[i, c(6, 10, 7)])
  
  start_date = as.POSIXlt(as.Date(min(BI_data[,2])))
  end_date = as.POSIXlt(as.Date(max(BI_data[,2])))
  
  
  BI_data$month_num = as.POSIXlt(as.Date(BI_data$MONTH_BEG_DT))$year*12 + as.POSIXlt(as.Date(BI_data$MONTH_BEG_DT))$mon
  
  BI_forecast_tmp = data.frame(pred_month_beg_dt = all_pred_month, category = rep(category[i], pred_length),
                               y = numeric(pred_length), yhat = numeric(pred_length))
  
  for (j in 1:pred_length) {
    pred_num = as.POSIXlt(all_pred_month[j])$year*12 + as.POSIXlt(all_pred_month[j])$mon
    start_nm = pred_num - 36
    end_nm = pred_num - 1
    
    BI_data_train = BI_data[BI_data$month_num >= start_nm & BI_data$month_num <= end_nm,]
    BI_test = BI_data[BI_data$month_num == pred_num,3]
    
    start_mon = as.POSIXlt(as.Date(BI_data_train[BI_data_train$month_num == start_nm,2]))
    BI_train_ts <- ts(BI_data_train[,3], frequency = 12, start = c(start_mon$year + 1900, start_mon$mon + 1))
    
    BI_model <- arima(BI_train_ts, order = cat_order, seasonal = list(order = cat_seasonal, period = 12), method = "ML")
    BI_predict <- forecast(BI_model, h=1, level=c(95))
    
    BI_forecast_tmp[j,3:4] = c(BI_test, as.numeric(BI_predict$mean))
  }
  
  
  error_cat = category[i]
  error_rmse = sqrt(mean((BI_forecast_tmp[,4] - BI_forecast_tmp[,3]) ^ 2))
  BI_mean = mean(BI_forecast_tmp[,3])
  error_pct = error_rmse/BI_mean    
  
  Error = c(error_cat,error_rmse,BI_mean,error_pct)
  list(BI_forecast_tmp, Error)
}
# stopCluster(cl)

end.t <- Sys.time()
end.t - start.t

BI_forecast_same_par = Result_same_par[[1]]
Error_same_par = Result_same_par[[2]]
colnames(Error_same_par) = c("category","rmse","BI_mean","error_pct")









