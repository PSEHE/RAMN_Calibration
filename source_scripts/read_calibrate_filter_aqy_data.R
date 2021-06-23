


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
get_current_params <- function(pollutant){
  
  all_params_paths <- list.files('results', paste0(pollutant, '*'), full.names = T)
  current_params_path <- all_params_paths[length(all_params_paths)]
  current_params <- read.csv(current_params_path)
  
  print(paste('Calibrating with most recent', pollutant, 'params at', current_params_path))
  
  return(current_params)
  
}


# Calibrate O3 Data with existing parameters
calibrate_O3_data <- function(aqy_proxy_data){
  
  params_O3 <- get_current_params('OZONE')
  
  aqy_proxy_data_O3 <- inner_join(params_O3, aqy_proxy_data, by = 'ID') %>%
    filter(timestamp_pacific >= deployment_datetime & timestamp_pacific >= start_date & timestamp_pacific <= end_date) %>%
    mutate(O3 = O3.offset + (O3.gain*O3_raw)) %>%
    dplyr::select(ID, timestamp_pacific, TEMP, RH, DP, proxy_rand, O3, O3_raw, Ox_raw, NO2_raw) %>%
    rename('pollutant'='O3', 'pollutant_raw'='O3_raw')
  
  return(aqy_proxy_data_O3)
  
}


# Calibrate NO2 Data with existing parameters
calibrate_NO2_data <- function(aqy_proxy_data_O3){
  
  params_NO2 <- get_current_params('NO2')
  
  joined_aqy_proxy_data_NO2cal <- inner_join(params_NO2, aqy_proxy_data_O3, by = 'ID') %>%
    filter(timestamp_pacific >= start_date & timestamp_pacific <= end_date) %>%
    rename('O3'='pollutant', 'O3_raw'='pollutant_raw') %>%
    mutate(NO2 = NO2.offset + (NO2.gain*NO2_raw)) %>%
    dplyr::select(ID, timestamp_pacific, proxy_rand, TEMP, RH, DP, O3, O3_raw, Ox_raw, NO2, NO2_raw) %>%
    rename('pollutant'='NO2', 'pollutant_raw'='NO2_raw')
  
  return(aqy_proxy_data_NO2)
  
}



# Filter Data to Desired Period
temporally_filter_data <- function(aqy_proxy_data, start_time, td_subtract_from_start){
  
  end_time <- format_timestamp(ymd_hms(format_timestamp(start_time)) - td_subtract_from_start)
  
  current_action <- ifelse(td_subtract_from_start == td.72hr, 'drift detection', 'recalibration')
  print(paste('Using data between', start_time, 'and', end_time, 'to perform', current_action))
  
  filtered_data <-  filter(aqy_proxy_data, timestamp_pacific <= start_time & timestamp_pacific > end_time) %>%
    na.omit()
  
  return(filtered_data)
  
}
