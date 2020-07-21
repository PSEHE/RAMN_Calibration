
library(httr)
library(jsonlite)
library(dplyr)
library(lubridate)
library(stringr)
library(CVXR)


### Proxy Data Collection
get_proxy <- function(start_proxy, end_proxy, pollutant){
  request_url = paste0('http://www.airnowapi.org/aq/data/?startDate=', start_proxy, 'T00&endDate=', end_proxy, 'T23&parameters=OZONE,PM25,NO2&BBOX=-123.201658,37.177199,-121.366941,38.530363&dataType=C&format=application/json&verbose=1&nowcastonly=0&includerawconcentrations=1&API_KEY=C05358E3-5508-4216-A03E-E229E0368B7E')
  request_call <- GET(url = request_url)
  request_json <- content(request_call, as = 'text', type = NULL, encoding = 'UTF-8')
  request_df <- arrange(fromJSON(request_json, simplifyDataFrame = TRUE), desc(UTC))
  
  # Clean response data
  reg_data <- mutate(request_df, timestamp = paste0(substr(UTC, 1, 10), ' ', substr(UTC, 12, 16), ':00'), RawConcentration = ifelse(RawConcentration == -999, Value, RawConcentration))
  
  # Generate random noise to approximate continuous distribution
  rand_noise <- runif(nrow(reg_data), .00001, .00009)
  reg_data$proxy_rand <- reg_data$RawConcentration + rand_noise
  
  # Clean col names and filter to pollutant of interest
  reg_data <- dplyr::select(reg_data, -c(UTC, Value, AgencyName, FullAQSCode, IntlAQSCode)) %>% 
    rename('lat'='Latitude', 'lon'='Longitude', 'modality'='Parameter', 'units'='Unit', 'proxy_raw'='RawConcentration', 'proxy_site' = 'SiteName') %>%
    filter(modality == pollutant)
  
  return(reg_data)
}

### Initial Calibration with Old Parameters
calibrate_with_old_params <- function(pollutant){
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
  output <- switch(pollutant, 'OZONE' = aqy_data_O3, 'NO2' = aqy_data_NO2, stop(print('Check your pollutant input. Accepts values of NO2 and OZONE')))
  output$aqy_raw <- switch(pollutant, 'OZONE' = output$O3, 'NO2' = output$NO2)
  output$aqy_cal <- switch(pollutant, 'OZONE' = output$O3_cal, 'NO2' = output$NO2_cal)
  
  return(output)
}

### Drift Detection and Recalibration
calibrate_monitors <- function(start_72, pollutant){
  
  end_72 <- start_72+2
  
  print(paste('DRIFT DETECTION & RECALIBRATION FOR', pollutant, 'FROM', start_72, 'TO', end_72))
  
  ### READ AND CALIBRATE NECESSARY DATASETS ###
  ## Read proxy data
  reg_data <- get_proxy(start_72, end_72, pollutant)
  
  ## Read AQY data
  aqy_data_path <- list.files('C:/Users/18313/Desktop/airMonitoring/downloader/results/concatenated_data', '*freq_60', full.names = T)
  aqy_data_full <- read.csv(aqy_data_path[length(aqy_data_path)], stringsAsFactors = F) %>%
    rename('timestamp'='Time')
  
  ## Calibrate data  
  aqy_data_cal <- calibrate_with_old_params(pollutant)
  aqy_data_72 <- filter(aqy_data_cal, timestamp >= start_72 & timestamp <= end_72)
  
  ### DETECT DRIFT
  ## Generate flags by monitor
  aqy_list_id <- unique(pull(aqy_data_72, ID)) 
  
  todays_flags <- as.data.frame(aqy_list_id) %>%
    mutate(ks = 0, gain = 0, offset = 0) %>%
    rename('ID'='aqy_list_id') 
  
  for(aqy in aqy_list_id){
    aqy_for_cal <- filter(aqy_data_72, ID == aqy)
    
    # Filter to appropriate proxy
    proxy_stn <- unique(aqy_for_cal$proxy_site)
    proxy_for_cal <- subset(reg_data, reg_data$proxy_site %in% proxy_stn)
    
    # Test for monitor drift if sufficient data in AQY and proxy data sets
    if(nrow(aqy_for_cal) <= .75*nrow(proxy_for_cal) | nrow(aqy_for_cal)*.75 >= nrow(proxy_for_cal)){
      print(paste('Did not perform drift detection for', aqy, 'due to insufficient data. NROW AQY = ', nrow(aqy_for_cal), 'NROW Proxy =', nrow(proxy_for_cal)))
    }
    
    else{ 
      # Kolmogorov-Smirnov test
      ks_results <- ks.test(proxy_for_cal$proxy_rand, aqy_for_cal$aqy_cal, exact = F)
      ks_p <- ks_results$p.value # Get p-value from KS test
      
      todays_flags[todays_flags$ID == aqy, 'ks'] <- ifelse(ks_p <= 0.05, 1, 0)
      
      # Mean-variance moment matching for gain
      manual_gain <- sqrt(var(proxy_for_cal$proxy_rand, na.rm = T) / var(aqy_for_cal$aqy_cal, na.rm = T))
      todays_flags[todays_flags$ID == aqy, 'gain'] <- ifelse(manual_gain > 1.3 | manual_gain < .7, 1, 0)
      
      # Mean-variance moment matching for offset
      manual_offset <- mean(proxy_for_cal$proxy_rand, na.rm = T) - mean(aqy_for_cal$aqy_cal, na.rm = T)*manual_gain
      todays_flags[todays_flags$ID == aqy, 'offset'] <- ifelse(manual_offset > 5 | manual_offset < -5, 1, 0)
      
      # Print results
      print(paste(aqy, '|| KS P-VALUE:', ks_p, '| MANUAL GAIN:', manual_gain, '| MANUAL OFFSET:', manual_offset))
    }
  }
  
  ## Combine today's flags with existing flags
  running_flags <- read.csv(paste0('results/running_flags/running_flags_', pollutant, '.csv'), stringsAsFactors = F) %>% 
    full_join(., todays_flags, by = 'ID') %>% 
    mutate(ks_run = ifelse(ks == 0, 0, ks_run + ks), gain_run = ifelse(gain == 0, 0, gain_run + gain), offset_run = ifelse(offset == 0, 0, offset_run + offset)) %>%
    dplyr::select(-c(ks, gain, offset))
  
  write.csv(running_flags, paste0('results/running_flags/running_flags_', pollutant, '.csv'), row.names = F)
  
  # Note last run
  write.csv(as.character(start_72), paste0('results/last_start/', 'last_start_', pollutant, '.csv'), col.names = F, row.names = F)
  
  ### RECALIBRATE ###
  ## Set important dates
  start_recal <- end_72-8 
  end_recal <- end_72 
  flag_detected <- end_72-5
  
  ## Identify monitors needing re-calibration, using highest of flagged columns
  running_flags$max_flag <- apply(MARGIN = 1, X = running_flags[grep('*_run', colnames(running_flags))], FUN = max)
  needs_calibration <- pull(filter(running_flags, max_flag >= 5), ID)
  
  ## Re-calibrate if running flags >= 5 for any monitors
  if(length(needs_calibration) == 0){print(paste('No recalibration is needed for period from', start_72, 'to', end_72, 'for', pollutant))}
  
  else{# Re-calibrate using last eight days of data
    aqy_recal <- filter(aqy_data_cal, ID %in% needs_calibration & timestamp >= start_recal & timestamp <= end_recal)
    proxy_recal <- get_proxy(start_recal, end_recal, pollutant)
    
    # Blank data frame for new parameters
    proxy_sites_recal <- unique(as.data.frame(cbind(ID = aqy_recal$ID, proxy_site = aqy_recal$proxy_site)))
    
    new_params <- as.data.frame(needs_calibration) %>%
      rename('ID'='needs_calibration') %>%
      left_join(proxy_sites_recal, by = 'ID') %>%
      mutate(O3.gain = 1.0, O3.offset = 0.0, NO2.b0 = 0.0, NO2.b1 = 1.0, NO2.b2 = 1.0, NO2.b0_kl = 0.0, NO2.b1_kl = 1.0, NO2.b2_kl =1.0,  
             start_date = as.character(flag_detected), end_date = '9999-12-31')
    
    for(i in 1:nrow(new_params)){
      
      # Isolate data for desired AQY & proxy site
      aqy_recal_i <- filter(aqy_recal, ID == new_params$ID[i])
      proxy_recal_i <- filter(proxy_recal, proxy_site == new_params$proxy_site[i])
      recal_i <- na.omit(inner_join(aqy_recal_i, proxy_recal_i, 'timestamp'))
      
      if(pollutant == 'OZONE'){
        # Calculate new gain and offset for O3
        gain_new_O3 <- sqrt(var(proxy_recal_i$proxy_rand, na.rm = T)/var(aqy_recal_i$O3, na.rm = T))
        offset_new_O3 <- mean(proxy_recal_i$proxy_rand, na.rm = T) - gain_new_O3*mean(aqy_recal_i$O3, na.rm = T)
        
        new_params$O3.gain[i] <- gain_new_O3
        new_params$O3.offset[i] <- offset_new_O3}
      
      else{
        # Calculate new gain and offset for NO2 using mean-variance moment matching
        new_params$NO2.b0[i] <- mean(proxy_recal_i$proxy_rand, na.rm = T) - mean(aqy_recal_i$Ox - aqy_recal_i$O3_cal, na.rm = T)
        new_params$NO2.b1[i] <- sqrt(var(proxy_recal_i$proxy_rand, na.rm = T)/var(aqy_recal_i$Ox - aqy_recal_i$O3_cal, na.rm = T))
        new_params$NO2.b2[i] <- new_params$NO2.b1[i]
        
        # Calculate new gain and offset for NO2 using objective function minimization
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
        
        new_params$NO2.b0_kl[i] <- kl_out$getValue(b0)
        new_params$NO2.b1_kl[i] <- kl_out$getValue(b1)
        new_params$NO2.b2_kl[i] <- kl_out$getValue(b2)
      }
    }
    
    ## Work new parameters into running list
    # Edit old parameters to combine with new
    params_existing_path <- list.files('results', pollutant, full.names = T)
    
    params_existing <- read.csv(params_existing_path[length(params_existing_path)], stringsAsFactors = F)
    print(params_existing)
    
    params_existing$end_date <- ifelse(params_existing$ID %in% new_params$ID & params_existing$end_date == '9999-12-31', # Set new start and end dates for re-calibrated AQYs
                                       as.character(as.Date(new_params$start_date)-1), 
                                       params_existing$end_date)
    
    # Edit new parameters to combine with old
    desired_cols <- colnames(params_existing)
    
    params_current <- dplyr::select(params_existing, c('ID', 'description', 'address', 'city', 'lat', 'lon')) %>%
      right_join(., new_params, by = 'ID') %>%
      dplyr::select(desired_cols)
    
    # Write combined parameters as output
    running_params <- rbind(params_current, params_existing) %>%
      unique() %>%
      arrange(ID, start_date)
    
    write.csv(running_params, paste0('results/cal_params_', pollutant, '_', end_recal, '.csv'), row.names = F)
    
    # Reset running flags for re-calibrated data
    mutate(running_flags, ks_run = ifelse(ID %in% needs_calibration, 0, ks_run),
           gain_run = ifelse(ID %in% needs_calibration, 0, gain_run), 
           offset_run = ifelse(ID %in% needs_calibration, 0, offset_run)) %>%
      dplyr::select(-max_flag) %>%
      write.csv(paste0('results/running_flags/running_flags_', pollutant, '.csv'), row.names = F)
    
  }
}

### RUN FUNCTION FOR ALL DAYS BACK TO LAST RE-CALIBRATION
while(i <= Sys.Date()-3){run_date <- as.Date(read.csv('results/last_start/last_start_OZONE.csv', stringsAsFactors = F)[1,1]) + 1
calibrate_monitors(run_date, 'OZONE')
run_date <- run_date + 1}

while(i <= Sys.Date()-3){run_date <- as.Date(read.csv('results/last_start/last_start_NO2.csv', stringsAsFactors = F)[1,1]) + 1
print(run_date)
calibrate_monitors(run_date, 'NO2')
run_date <- run_date + 1}


































