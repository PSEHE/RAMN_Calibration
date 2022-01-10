

########## Check for raw AQY data in raw_data folder and read in ##########
read_aqy_data <- function(){
  
  files_at_path <- list.files(path = 'raw_data', pattern = '*.csv', full.names = T)
  
  print(files_at_path)
  
  if(length(files_at_path) < 1)
    {stop('No existing AQY data found in raw data directory - please add hourly data')}
  else if(length(files_at_path) > 1)
    {stop('Too many files in raw data directory - please only include hourly data')}
  else
    {file_to_read <- files_at_path[1]
     aqy_data <- read.csv(file_to_read, stringsAsFactors = F)}
  
  aqy_data <- rename(aqy_data, 'timestamp_pacific'='Time') %>%
    
    return(aqy_data)
}


########## Check for calibration parameters and read most current - used in functions to calibrate data below ##########
get_current_params <- function(in_pollutant){

  all_params_paths <- list.files('results', in_pollutant, full.names = T)
  
  current_params_path <- all_params_paths[length(all_params_paths)]
  current_params <- read.csv(current_params_path)
  
  print(paste('Calibrating with most recent', in_pollutant, 'params at', current_params_path))
  
  return(current_params)
  
}


########## Calibrate O3 Data with existing parameters ##########
calibrate_O3_data <- function(aqy_data_raw){
  
  params_O3 <- get_current_params(in_pollutant='O3')
  
  data_plus_params_O3 <- inner_join(params_O3, aqy_data_raw, by = 'ID') %>%
    filter(timestamp_pacific >= deployment_datetime & timestamp_pacific >= start_date & timestamp_pacific <= end_date) %>%
    rename('O3_raw'='O3', 'NO2_raw'='NO2', 'Ox_raw'='Ox')
  
  aqy_data_O3 <- mutate(data_plus_params_O3, O3 = O3.offset + (O3.gain*O3_raw)) %>%
    dplyr::select(ID, timestamp_pacific, TEMP, RH, DP, Ox_raw, NO2_raw, proxy_rand, O3, O3_raw)
  
  return(aqy_data_O3)
  
}


########## Calibrate NO2 Data with existing parameters ##########
calibrate_NO2_data <- function(aqy_data_O3){
  
  params_NO2 <- get_current_params(in_pollutant='NO2')
  
  data_plus_params_NO2 <- inner_join(params_NO2, aqy_data_O3, by = 'ID') %>%
    filter(timestamp_pacific >= start_date & timestamp_pacific <= end_date)
  
  aqy_data_NO2 <- mutate(data_plus_params_NO2, NO2 = NO2.offset + NO2.gain.Ox*Ox_raw - NO2.gain.O3*O3) %>%
    dplyr::select(ID, timestamp_pacific, TEMP, RH, DP, Ox_raw, O3, O3_raw, proxy_rand, NO2, NO2_raw)
  
  return(aqy_data_NO2)
  
}


########## Calibrate PM25 Data with existing parameters ##########
calibrate_PM25_data <- function(aqy_data_raw){
  
  params_PM25 <- get_current_params(in_pollutant='PM25')
  
  data_plus_params_PM25 <- inner_join(params_PM25, aqy_data_raw, by = 'ID') %>%
    filter(timestamp_pacific >= deployment_datetime & timestamp_pacific >= start_date & timestamp_pacific <= end_date) %>%
    rename('PM25_aqrh'='PM25')
  
  aqy_data_PM25 <- mutate(data_plus_params_PM25, PM25 = PM25.offset + (PM25.gain*PM25_raw)) %>%
    mutate(time_month = month(timestamp_pacific), 
           time_day_of_month = as.numeric(str_sub(timestamp_pacific, 9, 10)), 
           time_year = year(timestamp_pacific), .after = 'timestamp_pacific') %>%
    dplyr::select(ID, timestamp_pacific, time_month, time_day_of_month, time_year, TEMP, RH, DP, pollutant_proxy_site, proxy_rand, PM25, PM25_aqrh, PM25_raw)
  
  return(aqy_data_PM25)
  
}


########## Put together proxy data and AQY data read functions ##########
read_aqy_join_proxy <- function(in_pollutant){
  
  proxy_sites <- read.csv('proxy_sites.csv') %>%
    rename_with(.cols = starts_with(in_pollutant), .fn = ~str_replace_all(.x, in_pollutant, 'pollutant'))
  
  proxy_data <- get_current_proxy_data(in_pollutant) %>%
    rename('pollutant_proxy_site'='proxy_site')
    
  aqy_data_raw <- read_aqy_data()
  
  aqy_proxy_data_raw <- inner_join(aqy_data_raw, proxy_sites, by = 'ID') %>%
    inner_join(., proxy_data, by = c('timestamp_pacific', 'pollutant_proxy_site'))
  
  return(aqy_proxy_data_raw)
  
}


########## Filter Data to Desired Period ##########
temporally_filter_data <- function(aqy_proxy_data, look_back_from_time, td_subtract_from_time){
  
  look_back_until_time <- format_timestamp(ymd_hms(format_timestamp(look_back_from_time)) - td_subtract_from_time)
  
  current_action <- ifelse(td_subtract_from_time == td_72hr, 'drift detection', 'recalibration')
  #print(paste('Using data between', look_back_until_time, 'and', look_back_from_time, 'to perform', current_action))
  
  filtered_data <-  filter(aqy_proxy_data, timestamp_pacific <= look_back_from_time & timestamp_pacific > look_back_until_time) %>%
    na.omit()
  
  return(filtered_data)
  
}


