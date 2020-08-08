library(httr)
library(jsonlite)
library(dplyr)
library(tidyr)
library(lubridate)
library(stringr)
library(CVXR)



### FIX DATES AND TIMES THAT CUT HOUR OFF AT MIDNIGHT
fix_midnight <- function(desired_time){suppressWarnings(ifelse(!is.na(ymd_hms(desired_time)), desired_time, paste(desired_time, '00:00:00')))}



### CHECK IF PROXY DATA EXISTS - DOWNLOAD IF NOT
get_proxy <- function(pollutant, start_proxy){
  print(start_proxy)
  
  start_proxy <- fix_midnight(start_proxy)
  end_proxy <- fix_midnight(as.character(ymd_hms(start_proxy) + 60*60*30))
  
  try(existing_proxy_data <- arrange(read.csv(paste0('results/proxy/', pollutant, '_proxy_data.csv'), stringsAsFactors = F), desc(timestamp))) #### UPDATE PATH AS APPROPRIATE FOR ENVIRONMENT 
  stopifnot('Existing proxy data not found' = exists('existing_proxy_data'))
  
  if(is.na(ymd_hms(start_proxy)) | is.na(ymd_hms(end_proxy))){stop(paste('unable to retreive proxy data from', start_proxy, 'to', end_proxy, '- not of format yyyy-mm-dd hh:mm:ss'))}
  else{}
  
  timestamps_existing <- unique(existing_proxy_data$timestamp)
  timestamps_needed <- as.character(seq(from = ymd_hms(start_proxy), to = ymd_hms(end_proxy), by = 60*60))
  
  timestamps_to_download <- subset(timestamps_needed, timestamps_needed %in% timestamps_existing == F)
  
  if(length(timestamps_to_download) < 1){print('All proxy data for requested date range are already downloaded. Starting calibration with existing parameters.')
    return(existing_proxy_data)}
  
  else{first_date_needed <- as.Date(sort(timestamps_to_download)[1])
  first_hour_needed <- hour(sort(timestamps_to_download)[1])
  
  last_date_needed <- as.Date(sort(timestamps_to_download, decreasing = T)[1])
  last_hour_needed <- hour(sort(timestamps_to_download, decreasing = T)[1])
  
  # Request data from air district
  request_url = paste0('http://www.airnowapi.org/aq/data/?',
                       'startDate=', first_date_needed, 'T', first_hour_needed, '&endDate=', last_date_needed, 'T', last_hour_needed, 
                       '&parameters=', pollutant,
                       '&BBOX=-122.617880,37.639710,-121.706015,38.177130',
                       '&dataType=C&format=application/json&verbose=1&nowcastonly=0&includerawconcentrations=1',
                       '&API_KEY=C05358E3-5508-4216-A03E-E229E0368B7E')
  
  print(paste('Requesting proxy data from Air District for', pollutant, 'from', first_date_needed, first_hour_needed, 'to', last_date_needed, last_hour_needed))
  
  request_call <- GET(url = request_url)
  stop_for_status(request_call, paste('Failed request to URL', request_url))
  request_json <- content(request_call, as = 'text', type = NULL, encoding = 'UTF-8')
  request_df <- arrange(fromJSON(request_json, simplifyDataFrame = TRUE), desc(UTC))
  
  new_proxy_all <- mutate(request_df, timestamp = paste0(substr(UTC, 1, 10), ' ', substr(UTC, 12, 16), ':00'), RawConcentration = ifelse(RawConcentration == -999, Value, RawConcentration))
  new_proxy_unique <- filter(new_proxy_all, timestamp %in% timestamps_existing == F)
  
  random_noise <- runif(nrow(new_proxy_unique), .000001, .00001)
  new_proxy_unique$proxy_rand <- new_proxy_unique$RawConcentration + random_noise
  
  new_proxy_clean <- dplyr::select(new_proxy_unique, -c(UTC, Value, AgencyName, FullAQSCode, IntlAQSCode)) %>% 
    rename('lat'='Latitude', 'lon'='Longitude', 'modality'='Parameter', 'units'='Unit', 'proxy_raw'='RawConcentration', 'proxy_site' = 'SiteName')
  
  updated_proxy_data <- data.frame(rbind(new_proxy_clean, existing_proxy_data))
  write.csv(updated_proxy_data, paste0('results/proxy/', pollutant, '_proxy_data.csv'), row.names = F)
  
  return(updated_proxy_data)
  }
}





### CALIBRATE AQY DATA WITH EXISTING PARAMETERS
calibrate_with_old_params <- function(pollutant, first_hr_existing_aqy, last_hr_existing_aqy){
  
  first_hr_existing_aqy <- fix_midnight(first_hr_existing_aqy)
  last_hr_existing_aqy <- fix_midnight(last_hr_existing_aqy)
  
  # Read in 60-minute AQY data
  aqy_data_directory <- 'C:/Users/18313/Desktop/airMonitoring/downloader/results/concatenated_data'  #### CHANGE PATH AS APPROPRIATE FOR ENVIRONMENT
  aqy_data_path <- list.files(aqy_data_directory, '*freq_60', full.names = T)
  
  if(length(aqy_data_path) < 1){stop(paste('No existing AQY data found in directory', aqy_data_directory))}
  else{}
  
  aqy_data_path_newest <- aqy_data_path[length(aqy_data_path)]
  
  print(paste('Reading most current AQY data found at:', aqy_data_path))
  
  aqy_data_filtered <- read.csv(aqy_data_path_newest, stringsAsFactors = F) %>%
    rename('timestamp'='Time') %>%
    filter(timestamp >= first_hr_existing_aqy & timestamp <= last_hr_existing_aqy)
  

  cal_files_O3_all <- list.files('results', 'cal_params_OZONE*', full.names = T)
  cal_files_O3_newest <- cal_files_O3_all[length(cal_files_O3_all)]
  
  print('Calibrating O3 values with existing parameters')
  
  cal_file_O3 <- filter(read.csv(cal_files_O3_newest, stringsAsFactors = F), !is.na(lat))
  aqy_data_O3 <- inner_join(aqy_data_filtered, cal_file_O3, by = 'ID') %>% 
    filter(timestamp >= start_date & timestamp <= end_date) %>%
    mutate(O3_cal = O3.offset + O3.gain*O3)
  
  if(pollutant == 'NO2'){
    cal_files_NO2_all <- list.files('results', 'cal_params_NO2*', full.names = T)
    cal_files_NO2_newest <- cal_files_NO2_all[length(cal_files_NO2_all)]
    
    cal_file_NO2 <- filter(read.csv(cal_files_NO2_newest, stringsAsFactors = F), !is.na(lat))
    
    print('Calibrating NO2 values with existing parameters')
    
    aqy_data_NO2 <- dplyr::select(aqy_data_O3, ID, timestamp, O3_cal, Ox, NO2) %>%
      inner_join(cal_file_NO2, by = 'ID') %>%
      filter(timestamp >= start_date & timestamp <= end_date) %>%
      mutate(NO2_cal = NO2.b0 + NO2.b1*Ox - NO2.b2*O3_cal)}
  
  # Select correct output and rename columns
  aqy_data_old_params <- switch(pollutant, 'OZONE' = aqy_data_O3, 'NO2' = aqy_data_NO2)
  aqy_data_old_params$aqy_raw <- switch(pollutant, 'OZONE' = aqy_data_old_params$O3, 'NO2' = aqy_data_old_params$NO2)
  aqy_data_old_params$aqy_cal <- switch(pollutant, 'OZONE' = aqy_data_old_params$O3_cal, 'NO2' = aqy_data_old_params$NO2_cal)
  
  return(aqy_data_old_params)
}




### PERFORM DRIFT DETECTION
detect_drift <- function(aqy_id, rolling_aqy_data, rolling_proxy_data){
  
  aqy_drift_detection <- filter(rolling_aqy_data, ID == aqy_id)
  
  proxy_site_for_aqy <- unique(aqy_drift_detection$proxy_site)
  proxy_drift_detection <- subset(rolling_proxy_data, rolling_proxy_data$proxy_site %in% proxy_site_for_aqy)
  
  if(nrow(aqy_drift_detection) <= .75*nrow(proxy_drift_detection) | nrow(aqy_drift_detection)*.75 >= nrow(proxy_drift_detection)){
    print(paste0('Insufficient data for ', aqy_id, '. nrow AQY = ', nrow(aqy_drift_detection), ', nrow proxy =', nrow(proxy_drift_detection), ' - Defaulting to all flags'))
    all_ones <- as.data.frame(cbind(aqy_id, 1, 1, 1, -1, -1))
    return(all_ones)}
  
  else{ 
    drift_detection_data <- na.omit(inner_join(aqy_drift_detection, proxy_drift_detection, by = 'timestamp'))
    
    ks_results <- suppressWarnings(ks.test(drift_detection_data$proxy_rand, drift_detection_data$aqy_cal, exact = F))
    ks_p <- suppressWarnings(ks_results$p.value)
    ks <- ifelse(ks_p <= 0.05, 1, 0)
    
    manual_gain <- sqrt(var(drift_detection_data$proxy_rand) / var(drift_detection_data$aqy_cal))
    gain <- ifelse(manual_gain > 1.3 | manual_gain < .7, 1, 0)
    
    manual_offset <- mean(drift_detection_data$proxy_rand) - mean(drift_detection_data$aqy_cal)*manual_gain
    offset <- ifelse(manual_offset > 5 | manual_offset < -5, 1, 0)
    
    missing <- 0
    
    extreme <- ifelse(mean(drift_detection_data$aqy_cal) >= 1.5*IQR(rolling_aqy_data$aqy_cal, na.rm = T) + quantile(rolling_aqy_data$aqy_cal, .75, na.rm = T) | mean(drift_detection_data$aqy_cal) < 0, -1, 0)
    
    flags_current_72_aqy <- as.data.frame(cbind(aqy_id, ks, gain, offset, missing, extreme))
    
    # Print results
    print(paste(aqy_id, '|| KS P:', round(ks_p, 2), '| GAIN:', round(manual_gain, 2), '| OFFSET:', round(manual_offset, 2)))}
  
  return(flags_current_72_aqy)
}




### ADD UP TODAYS'S FLAGS AND EXISTING FLAGS
process_flags <- function(pollutant, todays_flags_list){
  
  print('Combining todays flags with running flags')
  
  flags_current_72 <- data.frame(matrix(unlist(todays_flags_list), nrow=length(todays_flags_list), ncol = 6, byrow = T))
  colnames(flags_current_72) <- c('ID', 'ks', 'gain', 'offset', 'missing', 'extreme')
  
  try(flags_previous_72 <- read.csv(paste0('results/running_flags/running_flags_', pollutant, '.csv'), stringsAsFactors = F))
  stopifnot('running list of flags not found' = exists('flags_previous_72'))
  
  flags_combined <- full_join(flags_previous_72, flags_current_72, by = 'ID')
  
  flags_running_5days <- mutate(flags_combined, ks = as.integer(ks), gain = as.integer(gain), offset = as.integer(offset)) %>%
    mutate(ks_run = ifelse(ks == 0, 0, ks_run + ks), gain_run = ifelse(gain == 0, 0, gain_run + gain), offset_run = ifelse(offset == 0, 0, offset_run + offset)) %>%
    dplyr::select(-c(ks, gain, offset, missing, extreme))
  
  return(flags_running_5days)
}




### RECALIBRATE OZONE MONITORS
recalibrate_O3 <- function(aqy_id, thirty_day_aqy, thirty_day_proxy){
  
  thirty_day_aqy_filter <- filter(thirty_day_aqy, ID == aqy_id)
  thirty_day_proxy_filter <- filter(thirty_day_proxy, proxy_site == unique(thirty_day_aqy_filter$proxy_site))
  
  thirty_day_data <- na.omit(inner_join(thirty_day_aqy_filter, thirty_day_proxy_filter, by = 'timestamp'))
  
  O3.gain <- sqrt(var(thirty_day_data$proxy_rand) / var(thirty_day_data$O3))
  O3.offset <- mean(thirty_day_data$proxy_rand) - O3.gain*mean(thirty_day_data$O3)
  
  # Combined results
  O3_params <- as.data.frame(cbind(aqy_id, O3.gain, O3.offset))
  
  print(paste0(aqy_id, ': ', 'new offset', O3.offset, ', new gain ', O3.gain))
  
  return(O3_params)
  
}





### RECALIBRATE NO2 MONITORS
# Most pending changes to happen here
recalibrate_NO2 <- function(aqy_id, thirty_day_aqy, thirty_day_proxy){
  
  # Generate data
  thirty_day_aqy_filter <- filter(thirty_day_aqy, ID == aqy_id)
  thirty_day_proxy_filter <- filter(thirty_day_proxy, proxy_site == unique(thirty_day_aqy_filter$proxy_site))
  thirty_day_data <- na.omit(inner_join(thirty_day_aqy_filter, thirty_day_proxy_filter, 'timestamp'))
  
  # Mean-variance moment matching
  NO2.b0 <- mean(thirty_day_data$proxy_rand) - mean(thirty_day_data$Ox - thirty_day_data$O3_cal)
  NO2.b1 <- sqrt(var(thirty_day_data$proxy_rand)/var(thirty_day_data$Ox - thirty_day_data$O3_cal))
  NO2.b2 <- NO2.b1
  
  #Objective function minimization
  ### TO DO - FILL IN BLANK IF TRY FAILS
  ### TO DO - LOOK INTO SOLVER ABILITY TO INITIALIZE WITH SPECIFIED VALUES
  try({
    b0 <- Variable(1)
    b1 <- Variable(1)
    b2 <- Variable(1)
    
    cno2 <- b0 + b1*recal_filter$Ox - b2*recal_filter$O3_cal
    pno2 <- recal_filter$proxy_raw
    
    kl <- kl_div(cno2, pno2)
    kl_obj <- sum(kl)
    
    objective <- Minimize(kl_obj)
    constraints <- list(cno2 >= 0)
    
    kl_min <- Problem(objective, constraints)
    kl_out <- solve(kl_min)
    
    NO2.b0_kl <- kl_out$getValue(b0)
    NO2.b1_kl <- kl_out$getValue(b1)
    NO2.b2_kl <- kl_out$getValue(b2)
  })
  
  warning('Objective function solver failure: defaulting to hand calculated values' = !exists('NO2.b0_kl'))
  
  NO2.b0_kl <- if(exists('NO2.b0_kl')){NO2.b0_kl}
  else{NO2.b0}
  
  NO2.b1_kl <- if(exists('NO2.b1_kl')){NO2.b1_kl}
  else{NO2.b1}
  
  NO2.b2_kl <- if(exists('NO2.b2_kl')){NO2.b2_kl}
  else{NO2.b2}
  
  #Combined results
  NO2_params <- as.data.frame(cbind(aqy_id, NO2.b0, NO2.b1, NO2.b2, NO2.b0_kl, NO2.b1_kl, NO2.b2_kl))
  
  print(paste0(aqy_id, ': ', 'new b0', NO2.b0_kl, ', new b1 ', NO2.b1_kl, ', new b2', NO2.b2_kl))
  
  return(NO2_params)
}
           



### PROCESS NEW PARAMETERS
process_new_params <- function(pollutant, new_params_list, first_day_rolling_72){
  
  flag_detected <- fix_midnight(as.character(ymd_hms(first_day_rolling_72) - 60*60*71 + 60*60*119))
  
  if(is.na(ymd_hms(flag_detected))){stop(paste('new parameter start date of', flag_detected, 'not in format yyyy-mm-dd hh:mm:ss'))}
  else{}
  
  try(new_params <- data.frame(matrix(unlist(new_params_list), nrow=length(new_params_list), byrow = T)))
  if(!exists('new_params')){stop(paste('unable to parse new parameters of type', str(new_params_list)))}
  else{}
  
  O3_names <- c('ID', 'O3.gain', 'O3.offset')
  NO2_names <- c('ID', 'NO2.b0', 'NO2.b1', 'NO2.b2', 'NO2.b0_kl', 'NO2.b1_kl', 'NO2.b2_kl')
  colnames(new_params) <- switch(pollutant, 'OZONE' = O3_names, 'NO2' = NO2_names)
  
  new_params$start_date <- as.character(ymd_hms(flag_detected))
  new_params$end_date <- '9999-12-31 23:59:59'
  
  return(new_params)
}



### COMBINE NEW AND OLD PARAMETERS
combine_old_and_new <- function(pollutant, new_params){
  
  # Edit old parameters to combine with new
  params_existing_path <- list.files('results', paste0(pollutant), full.names = T)
  
  params_existing <- read.csv(params_existing_path[length(params_existing_path)], stringsAsFactors = F)
  params_existing$end_date <- ifelse(params_existing$ID %in% new_params$ID & params_existing$end_date == '9999-12-31 23:59:59', as.character(as.Date(new_params$start_date)-1), params_existing$end_date)
  get_these_cols <- colnames(params_existing)
  
  # Edit new parameters to combine with old
  monitor_info <- dplyr::select(params_existing, c('ID', 'description', 'proxy_site', 'address', 'city', 'lat', 'lon'))
  params_current_info <- inner_join(monitor_info, new_params, by = 'ID')
  params_current <- dplyr::select(params_current_info, get_these_cols)
  
  # Combine parameters
  running_params <- rbind(params_current, params_existing) %>%
    unique() %>%
    arrange(ID, start_date)
  
  return(running_params)
}



#### COMBINE EVERYTHING ABOVE - PERFORM DRIFT DETECTION AND RECALIBRATION ####
calibrate_monitors <- function(start_72, pollutant){
  
  ### SET DATES FOR FILTERING
  start_72 <- fix_midnight(start_72)
  stopifnot('Requires input date of format yyyy-mm-dd hh:mm:ss' = !is.na(ymd_hms(start_72)),
            'Requires input pollutant of OZONE or NO2' = pollutant == 'OZONE' | pollutant == 'NO2')
  
  end_72 <- as.character(ymd_hms(start_72) + 60*60*71)
  start_recal <- as.character(ymd_hms(start_72) - 60*60*24*29) 
  end_recal <- end_72
  
  ### READ AND CALIBRATE NECESSARY DATASETS ###
  proxy_data_720 <- get_proxy(pollutant, start_recal)
  proxy_data_72 <- filter(proxy_data_720, timestamp >= start_72 & timestamp <= end_72)
  aqy_data_720 <- calibrate_with_old_params(pollutant, start_recal, end_recal)
  aqy_data_72 <- filter(aqy_data_720, timestamp >= start_72 & timestamp <= end_72)
  
  ### DETECT MONITOR DRIFT ###
  aqys_in_72_hr_data <- unique(pull(aqy_data_72, ID)) 
  
  if(length(aqys_in_72_hr_data) == 0){warning('no existing AQY data and calibration parameters found for requested range')}
  else{print(paste('Performing drift detection for', length(aqys_in_72_hr_data), 'monitors'))}
  
  flags_current_72 <- lapply(aqys_in_72_hr_data, detect_drift, rolling_aqy_data=aqy_data_72, rolling_proxy_data=proxy_data_72)
  flags_running_120 <- process_flags(pollutant, flags_current_72) 
  
  ### RECALIBRATE MONITORS ###
  flags_running_120 <- mutate(flags_running_120, across(everything(), ~replace_na(.x, 0)))
  flags_running_120$max_flag <- apply(MARGIN = 1, X = flags_running_120[grep('*_run', colnames(flags_running_120))], FUN = max)
  
  needs_calibration <- pull(filter(flags_running_120, max_flag >= 120), ID)
  
  reset_flags <-  mutate(flags_running_120, ks_run = ifelse(ID %in% needs_calibration, 0, ks_run), gain_run = ifelse(ID %in% needs_calibration, 0, gain_run), offset_run = ifelse(ID %in% needs_calibration, 0, offset_run)) %>%
    dplyr::select(-max_flag)
  
  if(length(needs_calibration) < 1){
    write.csv(reset_flags, paste0('results/running_flags/running_flags_', pollutant, '.csv'), row.names = F)
    suppressWarnings(write.csv(as.character(ymd_hms(start_72)), paste0('results/last_start/', 'start_72_', pollutant, '.csv'), col.names = F, row.names = F))
    return(print('No monitors require re-calibration. Writing flags and start date and exiting function.'))}
  else{print('Recalibrating monitor:')}
  
  new_params_list <- switch(pollutant,
                            'OZONE' = lapply(needs_calibration, recalibrate_O3, thirty_day_aqy=aqy_data_720, thirty_day_proxy=proxy_data_720), 
                            'NO2' = lapply(needs_calibration, recalibrate_NO2, thirty_day_aqy=aqy_data_720, thirty_day_proxy=proxy_data_720))
  
  new_params <- process_new_params(pollutant, new_params_list, start_72)
  
  combined_params <- combine_old_and_new(pollutant, new_params)
  
  write.csv(combined_params, paste0('results/cal_params_', pollutant, '_', date(end_recal), '.csv'), row.names = F)
  write.csv(reset_flags, paste0('results/running_flags/running_flags_', pollutant, '.csv'), row.names = F)
  suppressWarnings(write.csv(as.character(ymd_hms(start_72)), paste0('results/last_start/', 'start_72_', pollutant, '.csv'), col.names = F, row.names = F))
  
  return(combined_params)
}




### RUN FUNCTION FOR ALL DAYS BACK TO LAST RE-CALIBRATION
#last_run_O3 <- fix_midnight(read.csv('results/last_start/start_72_OZONE.csv', stringsAsFactors = F))
#last_run_NO2 <- fix_midnight(read.csv('results/last_start/start_72_NO2.csv', stringsAsFactors = F))

#run_this_hour_O3 <- fix_midnight(as.character(ymd_hms(last_run_O3) + 60*60))
#run_this_hour_NO2 <- fix_midnight(as.character(ymd_hms(last_run_NO2) + 60*60))

run_this_hour_O3 <- '2019-12-05 23:00:00'
run_this_hour_NO2 <- '2019-12-05 23:00:00'

yesterday_date <- Sys.Date()-150 #Just running for a subset of dates for now - this will be -1
yesterday_11pm <- paste(yesterday_date, '23:00:00')

run_these_times_O3 <- as.character(seq(from = ymd_hms(run_this_hour_O3), to = ymd_hms(yesterday_11pm), 60*60))
run_these_times_NO2 <- as.character(seq(from = ymd_hms(run_this_hour_NO2), to = ymd_hms(yesterday_11pm), 60*60))

lapply(run_these_times_O3, calibrate_monitors, pollutant = 'OZONE')
lapply(run_these_times_NO2, calibrate_monitors, pollutant = 'NO2')









































