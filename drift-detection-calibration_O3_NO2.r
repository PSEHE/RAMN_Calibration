library(httr)
library(jsonlite)
library(dplyr)
library(lubridate)
library(stringr)
library(CVXR)

get_proxy <- function(start_proxy, end_proxy, pollutant){
  print('Requesting proxy data from Air District')
  
  # Request data from air district
  request_url = paste0('http://www.airnowapi.org/aq/data/?startDate=', 
                       start_proxy, 'T00&endDate=', end_proxy, 
                       'T23&parameters=', pollutant,
                       '&BBOX=-123.201658,37.177199,-121.366941,38.530363',
                       '&dataType=C&format=application/json&verbose=1&nowcastonly=0&includerawconcentrations=1',
                       '&API_KEY=C05358E3-5508-4216-A03E-E229E0368B7E')
  
  request_call <- GET(url = request_url)
  stop_for_status(request_call, 'Invalid API call - no proxy data returned')
  request_json <- content(request_call, as = 'text', type = NULL, encoding = 'UTF-8')
  
  # Clean response data
  request_df <- arrange(fromJSON(request_json, simplifyDataFrame = TRUE), desc(UTC))
  reg_data <- mutate(request_df, timestamp = paste0(substr(UTC, 1, 10), ' ', substr(UTC, 12, 16), ':00'), RawConcentration = ifelse(RawConcentration == -999, Value, RawConcentration))
  
  # Generate random noise to approximate continuous distribution
  rand_noise <- runif(nrow(reg_data), .00001, .00009)
  reg_data$proxy_rand <- reg_data$RawConcentration + rand_noise
  
  # Clean col names
  reg_data <- dplyr::select(reg_data, -c(UTC, Value, AgencyName, FullAQSCode, IntlAQSCode)) %>% 
    rename('lat'='Latitude', 'lon'='Longitude', 'modality'='Parameter', 'units'='Unit', 'proxy_raw'='RawConcentration', 'proxy_site' = 'SiteName')
  
  return(reg_data)
}

calibrate_with_old_params <- function(pollutant){
  print('Calibrating data with existing parameters')
  
  # Read in 60-minute AQY data
  aqy_data_path <- list.files('C:/Users/18313/Desktop/airMonitoring/downloader/results/concatenated_data', '*freq_60', full.names = T)
  aqy_data_full <- read.csv(aqy_data_path[length(aqy_data_path)], stringsAsFactors = F) %>%
    rename('timestamp'='Time')
  
  # Calibrate O3 data
  cal_files_O3_all <- list.files('results', 'cal_params_OZONE*', full.names = T)
  cal_file_O3 <- filter(read.csv(cal_files_O3_all[length(cal_files_O3_all)], stringsAsFactors = F), !is.na(lat))
  aqy_data_O3 <- inner_join(aqy_data_full, cal_file_O3, by = 'ID') %>% 
    filter(timestamp >= start_date & timestamp <= end_date) %>%
    mutate(O3_cal = O3.offset + O3.gain*O3)
  
  # Calibrate NO2 data
  cal_files_NO2_all <- list.files('results', 'cal_params_NO2*', full.names = T)
  cal_file_NO2 <- filter(read.csv(cal_files_NO2_all[length(cal_files_NO2_all)], stringsAsFactors = F), !is.na(lat))
  aqy_data_NO2 <- dplyr::select(aqy_data_O3, ID, timestamp, O3_cal, Ox, NO2) %>%
    inner_join(cal_file_NO2, by = 'ID') %>%
    filter(timestamp >= start_date & timestamp <= end_date) %>%
    mutate(NO2_cal = NO2.b0 + NO2.b1*Ox - NO2.b2*O3_cal)
  
  # Select correct output and rename columns
  aqy_data_old_params <- switch(pollutant, 'OZONE' = aqy_data_O3, 'NO2' = aqy_data_NO2, stop(print('Check your pollutant input. Accepts values of NO2 and OZONE')))
  aqy_data_old_params$aqy_raw <- switch(pollutant, 'OZONE' = aqy_data_old_params$O3, 'NO2' = aqy_data_old_params$NO2)
  aqy_data_old_params$aqy_cal <- switch(pollutant, 'OZONE' = aqy_data_old_params$O3_cal, 'NO2' = aqy_data_old_params$NO2_cal)
  
  return(aqy_data_old_params)
}

detect_drift <- function(aqy, aqy_data_72, proxy_data_72){
  
  aqy_for_cal <- filter(aqy_data_72, ID == aqy)
  
  # Filter to appropriate proxy
  proxy_stn <- unique(aqy_for_cal$proxy_site)
  proxy_for_cal <- subset(proxy_data_72, proxy_data_72$proxy_site %in% proxy_stn)
  
  # Test for monitor drift if sufficient data in AQY and proxy data sets
  if(nrow(aqy_for_cal) <= .75*nrow(proxy_for_cal) | nrow(aqy_for_cal)*.75 >= nrow(proxy_for_cal)){
    print(paste('Did not perform drift detection for', aqy, 'due to insufficient data. NROW AQY = ', nrow(aqy_for_cal), 'NROW Proxy =', nrow(proxy_for_cal)))
    all_zeroes <- as.data.frame(cbind(aqy, 0, 0, 0))
    return(all_zeroes)}
  
  else{ 
    # Kolmogorov-Smirnov test
    ks_results <- suppressWarnings(ks.test(proxy_for_cal$proxy_rand, aqy_for_cal$aqy_cal, exact = F))
    ks_p <- suppressWarnings(ks_results$p.value)
    
    ks <- ifelse(ks_p <= 0.05, 1, 0)
    
    # Mean-variance moment matching for gain
    manual_gain <- sqrt(var(proxy_for_cal$proxy_rand, na.rm = T) / var(aqy_for_cal$aqy_cal, na.rm = T))
    gain <- ifelse(manual_gain > 1.3 | manual_gain < .7, 1, 0)
    
    # Mean-variance moment matching for offset
    manual_offset <- mean(proxy_for_cal$proxy_rand, na.rm = T) - mean(aqy_for_cal$aqy_cal, na.rm = T)*manual_gain
    offset <- ifelse(manual_offset > 5 | manual_offset < -5, 1, 0)
    
    # Combined results
    todays_flags_aqy <- as.data.frame(cbind(aqy, ks, gain, offset))
    
    # Print results
    print(paste(aqy, '|| KS P-VALUE:', round(ks_p, 2), '| MANUAL GAIN:', round(manual_gain, 2), '| MANUAL OFFSET:', round(manual_offset, 2)))}
  
  return(todays_flags_aqy)
}

recalibrate_O3 <- function(aqy, aqy_data_month, proxy_data_month){
  print(paste(aqy))
  
  # Generate data
  aqy_data_month_i <- filter(aqy_data_month, ID == aqy)
  proxy_data_month_i <- filter(proxy_data_month, proxy_site == unique(aqy_data_month_i$proxy_site))
  
  # Mean-Variance Moment Matching
  O3.gain <- sqrt(var(proxy_data_month_i$proxy_rand, na.rm = T)/var(aqy_data_month_i$O3, na.rm = T))
  O3.offset <- mean(proxy_data_month_i$proxy_rand, na.rm = T) - O3.gain*mean(aqy_data_month_i$O3, na.rm = T)
  
  # Combined results
  O3_params <- as.data.frame(cbind(aqy, O3.gain, O3.offset))
  
  return(O3_params)
}

recalibrate_NO2 <- function(aqy, aqy_data_month, proxy_data_month){
  print(aqy)
  
  # Generate data
  aqy_data_month_i <- filter(aqy_data_month, ID == aqy)
  proxy_data_month_i <- filter(proxy_data_month, proxy_site == unique(aqy_data_month_i$proxy_site))
  recal_i <- na.omit(inner_join(aqy_data_month_i, proxy_data_month_i, 'timestamp'))
  
  # Mean-variance moment matching
  NO2.b0 <- mean(proxy_data_month_i$proxy_rand, na.rm = T) - mean(aqy_data_month_i$Ox - aqy_data_month_i$O3_cal, na.rm = T)
  NO2.b1 <- sqrt(var(proxy_data_month_i$proxy_rand, na.rm = T)/var(aqy_data_month_i$Ox - aqy_data_month_i$O3_cal, na.rm = T))
  NO2.b2 <- NO2.b1
  
  #Objective function minimization
  ### TO DO - FILL IN BLANK IF TRY FAILS
  try({
    b0 <- Variable(1)
    b1 <- Variable(1)
    b2 <- Variable(1)
    
    cno2 <- b0 + b1*recal_i$Ox - b2*recal_i$O3_cal
    pno2 <- recal_i$proxy_raw
    
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
  
  #warning(!exists('NO2.b0_kl'), 'Objective function solver failure: defaulting to hand calculated values')
  
  NO2.b0_kl <- if(exists('NO2.b0_kl')){NO2.b0_kl}
  else{NO2.b0}
  
  NO2.b1_kl <- if(exists('NO2.b1_kl')){NO2.b1_kl}
  else{NO2.b1}
  
  NO2.b2_kl <- if(exists('NO2.b2_kl')){NO2.b2_kl}
  else{NO2.b2}
  
  #Combined results
  NO2_params <- as.data.frame(cbind(aqy, NO2.b0, NO2.b1, NO2.b2, NO2.b0_kl, NO2.b1_kl, NO2.b2_kl))
  
  return(NO2_params)
}

process_new_params <- function(pollutant, new_params_list, flag_detected){
  
  new_params <- data.frame(matrix(unlist(new_params_list), nrow=length(new_params_list), byrow = T))
  
  O3_names <- c('ID', 'O3.gain', 'O3.offset')
  NO2_names <- c('ID', 'NO2.b0', 'NO2.b1', 'NO2.b2', 'NO2.b0_kl', 'NO2.b1_kl', 'NO2.b2_kl')
  colnames(new_params) <- switch(pollutant, 'OZONE'=O3_names, 'NO2'=NO2_names)
  
  new_params$start_date <- flag_detected
  new_params$end_date <- '9999-12-31'
  
  return(new_params)
}

combine_old_and_new <- function(pollutant, new_params){
  
  # Edit old parameters to combine with new
  params_existing_path <- list.files('results', paste0(pollutant), full.names = T)
  
  params_existing <- read.csv(params_existing_path[length(params_existing_path)], stringsAsFactors = F)
  params_existing$end_date <- ifelse(params_existing$ID %in% new_params$ID & params_existing$end_date == '9999-12-31', as.character(as.Date(new_params$start_date)-1), params_existing$end_date)
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

### Drift Detection and Recalibration
calibrate_monitors <- function(start_72, pollutant){
  
  ### SET IMPORTANT DATES
  end_72 <- start_72+2
  
  flag_detected <- end_72-5
  
  start_recal <- end_72-30 
  end_recal <- end_72 
  
  ### READ AND CALIBRATE NECESSARY DATASETS ###
  proxy_data_month <- get_proxy(start_recal, end_recal, pollutant)
  proxy_data_72 <- filter(proxy_data_month, timestamp >= start_72 & timestamp <= end_72)
  
  aqy_data_month <- filter(calibrate_with_old_params(pollutant), timestamp >= start_recal & timestamp <= end_recal)
  aqy_data_72 <- filter(aqy_data_month, timestamp >= start_72 & timestamp <= end_72)
  
  ### DETECT MONITOR DRIFT ###
  aqys_in_72_hr_data <- unique(pull(aqy_data_72, ID)) 
  
  print('Performing drift detection using mean-variance moment matching and Kolmogorov Smirnov test')  
  todays_flags_list <- lapply(aqys_in_72_hr_data, detect_drift, aqy_data_72=aqy_data_72, proxy_data_72=proxy_data_72)
  todays_flags <- data.frame(matrix(unlist(todays_flags_list), nrow=length(todays_flags_list), ncol = 4, byrow = T))
  colnames(todays_flags) <- c('ID', 'ks', 'gain', 'offset')
  
  existing_flags <- full_join(read.csv(paste0('results/running_flags/running_flags_', pollutant, '.csv'), stringsAsFactors = F), todays_flags, by = 'ID')
  
  running_flags <- mutate(existing_flags, ks = as.integer(ks), gain = as.integer(gain), offset = as.integer(offset)) %>%
    mutate(ks_run = ifelse(ks == 0, 0, ks_run + ks), gain_run = ifelse(gain == 0, 0, gain_run + gain), offset_run = ifelse(offset == 0, 0, offset_run + offset)) %>%
    dplyr::select(-c(ks, gain, offset))
  
  ### RECALIBRATE MONITORS ###
  running_flags$max_flag <- apply(MARGIN = 1, X = running_flags[grep('*_run', colnames(running_flags))], FUN = max)
  needs_calibration <- pull(filter(running_flags, max_flag >= 5), ID) 
  reset_flags <-  mutate(running_flags, ks_run = ifelse(ID %in% needs_calibration, 0, ks_run), gain_run = ifelse(ID %in% needs_calibration, 0, gain_run), offset_run = ifelse(ID %in% needs_calibration, 0, offset_run)) %>%
    dplyr::select(-max_flag)
  
  if(length(needs_calibration) < 1){
    write.csv(reset_flags, paste0('results/running_flags/running_flags_', pollutant, '.csv'), row.names = F)
    suppressWarnings(write.csv(as.character(start_72), paste0('results/last_start/', 'last_start_', pollutant, '.csv'), col.names = F, row.names = F))
    return(print('No monitors require re-calibration. Writing output and exiting function.'))}
  else{print('Recalibrating monitor:')}
  
  new_params_list <- ifelse(pollutant == 'OZONE', lapply(needs_calibration, recalibrate_O3, aqy_data_month=aqy_data_month, proxy_data_month=proxy_data_month), 
                            lapply(needs_calibration, recalibrate_NO2, aqy_data_month=aqy_data_month, proxy_data_month=proxy_data_month))
  
  new_params <- process_new_params(pollutant, new_params_list, flag_detected)
  
  combined_params <- combine_old_and_new(pollutant, new_params)
  
  write.csv(combined_params, paste0('results/cal_params_', pollutant, '_', end_recal, '.csv'), row.names = F)
  write.csv(reset_flags, paste0('results/running_flags/running_flags_', pollutant, '.csv'), row.names = F)
  suppressWarnings(write.csv(as.character(start_72), paste0('results/last_start/', 'last_start_', pollutant, '.csv'), col.names = F, row.names = F))
  
  return(combined_params)
}


### RUN FUNCTION FOR ALL DAYS BACK TO LAST RE-CALIBRATION
run_date_O3 <- as.Date(read.csv('results/last_start/last_start_OZONE.csv', stringsAsFactors = F)[1,1]) + 1

while(run_date_O3 <= Sys.Date()-3){
  print(run_date_O3)
  calibrate_monitors(run_date_O3, 'OZONE')
  run_date_O3 <- run_date_O3 + 1}

run_date_NO2 <- as.Date(read.csv('results/last_start/last_start_NO2.csv', stringsAsFactors = F)[1,1]) + 1

while(run_date_NO2 <= Sys.Date()-3){
  print(run_date_NO2)
  calibrate_monitors(run_date_NO2, 'NO2')
  run_date_NO2 <- run_date_NO2 + 1}































