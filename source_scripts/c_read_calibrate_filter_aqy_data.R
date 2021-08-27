


# Check for raw AQY data in raw_data folder and read in
read_aqy_data <- function(){
  
  files_at_path <- list.files(path = 'raw_data', pattern = '*.csv', full.names = T)
  
  print(files_at_path)
  
  if(length(files_at_path) < 1)
    {stop('No existing AQY data found in raw data directory - please add hourly data')}
  else if(length(files_at_path) > 1)
    {stop('Too many files in raw data directory - please only include hourly data')}
  else
    {aqy_data <- read.csv(files_at_path[1], stringsAsFactors = F)}
  
  aqy_data <- rename(aqy_data, 'timestamp_pacific'='Time') %>%
    
    return(aqy_data)
}



# Check for calibration parameters and read most current - used in functions to calibrate data below
get_current_params <- function(in_pollutant){

  all_params_paths <- list.files('results', in_pollutant, full.names = T)
  
  current_params_path <- all_params_paths[length(all_params_paths)]
  current_params <- read.csv(current_params_path)
  
  print(paste('Calibrating with most recent', in_pollutant, 'params at', current_params_path))
  
  return(current_params)
  
}


# Calibrate O3 Data with existing parameters
calibrate_O3_data <- function(aqy_data_raw){
  
  params_O3 <- get_current_params(in_pollutant='O3')
  
  data_plus_params_O3 <- inner_join(params_O3, aqy_data_raw, by = 'ID') %>%
    filter(timestamp_pacific >= deployment_datetime & timestamp_pacific >= start_date & timestamp_pacific <= end_date) %>%
    rename('O3_raw'='O3', 'Ox_raw'='Ox', 'NO2_raw'='NO2')
  
  aqy_data_O3 <- mutate(data_plus_params_O3, O3 = O3.offset + (O3.gain*O3_raw)) %>%
    dplyr::select(ID, timestamp_pacific, TEMP, RH, DP, Ox_raw, NO2_raw, O3, O3_raw, O3_proxy_site)
  
  return(aqy_data_O3)
  
}


# Calibrate PM25 Data with existing parameters
calibrate_PM25_data <- function(aqy_data_raw){
  
  params_PM25 <- get_current_params(in_pollutant='PM25')
  
  data_plus_params_PM25 <- inner_join(params_PM25, aqy_data_raw, by = 'ID') %>%
    filter(timestamp_pacific >= deployment_datetime & timestamp_pacific >= start_date & timestamp_pacific <= end_date) %>%
    rename('PM25_aqrh'='PM25')
  
  aqy_data_PM25 <- mutate(data_plus_params_PM25, PM25 = PM25.offset + (PM25.gain*PM25_raw)) %>%
    dplyr::select(ID, timestamp_pacific, TEMP, RH, DP, PM25, PM25_aqrh, PM25_raw, PM25_proxy_site)
  
  return(aqy_data_PM25)
  
}


# Calibrate NO2 Data with existing parameters
calibrate_NO2_data <- function(aqy_data_O3){
  
  params_NO2 <- get_current_params(in_pollutant='NO2')
  
  data_plus_params_NO2 <- inner_join(params_NO2, aqy_data_O3, by = 'ID') %>%
    filter(timestamp_pacific >= start_date & timestamp_pacific <= end_date)
  
  aqy_data_NO2 <- mutate(data_plus_params_NO2, NO2 = NO2.offset + NO2.gain.Ox*Ox_raw - NO2.gain.O3*O3) %>%
    dplyr::select(ID, timestamp_pacific, TEMP, RH, DP, Ox_raw, O3, O3_raw, O3_proxy_site, NO2, NO2_raw, NO2_proxy_site)
  
  return(aqy_data_NO2)
  
}


# Filter Data to Desired Period
temporally_filter_data <- function(aqy_proxy_data, look_back_from_time, td_subtract_from_time){
  
  look_back_until_time <- format_timestamp(ymd_hms(format_timestamp(look_back_from_time)) - td_subtract_from_time)
  
  current_action <- ifelse(td_subtract_from_time == td_72hr, 'drift detection', 'recalibration')
  #print(paste('Using data between', look_back_until_time, 'and', look_back_from_time, 'to perform', current_action))
  
  filtered_data <-  filter(aqy_proxy_data, timestamp_pacific <= look_back_from_time & timestamp_pacific > look_back_until_time) %>%
    na.omit()
  
  return(filtered_data)
  
}
