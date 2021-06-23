


# Check for existing proxy data - read if exists
get_existing_proxy_data <- function(pollutant){
  
  existing_proxy_data_path <- paste0('results/proxy/', pollutant, '_proxy_data.csv')
  
  try(existing_proxy_data <- read.csv(existing_proxy_data_path, stringsAsFactors = F))
  
  if(!exists('existing_proxy_data', where = environment()))
    {existing_proxy_data <- data.frame()
    print(paste('No existing proxy data for', pollutant, '- creating new'))}
  
  else(print(paste('Reading existing proxy data from', existing_proxy_data_path)))
  
  return(existing_proxy_data)
  
}



# Use existing data to determine most recent time now included in csv - becomes start time for request
find_starttime_proxy_request <- function(existing_proxy_data){
  
  if(nrow(existing_proxy_data) > 0)
  {timestamps_existing <- unique(existing_proxy_data$timestamp_utc)}
  
  else
  {print('No existing data - requesting all starting Dec 2019')
    return(ramn.start.time)}
  
  last_timestamp_recorded <- max(timestamps_existing)
  
  next_timestamp_needed <- format_timestamp(ymd_hms(last_timestamp_recorded) + td.hour)
  
  print(paste('Requesting data starting with', next_timestamp_needed))
  
  return(next_timestamp_needed)
  
}



# Build url for data request based on start date identified
build_proxy_request_url <- function(pollutant, start_request){
  
  month_after_start <- suppressWarnings(as.character(ymd_hms(start_request) + td.30day))
  
  end_request <- min(c(month_after_start, yesterday))
  
  first_date_needed <- as.Date(start_request)
  first_hour_needed <- hour(start_request)
  
  last_date_needed <- as.Date(end_request)
  last_hour_needed <- hour(end_request)
  
  request_url = paste0('http://www.airnowapi.org/aq/data/?',
                       'startDate=', first_date_needed, 'T', first_hour_needed, '&endDate=', last_date_needed, 'T', last_hour_needed, 
                       '&parameters=', pollutant,
                       '&BBOX=-122.617880,37.639710,-121.706015,38.177130',
                       '&dataType=C&format=application/json&verbose=1&nowcastonly=0&includerawconcentrations=1',
                       '&API_KEY=C05358E3-5508-4216-A03E-E229E0368B7E')
  
  print(paste('Requesting proxy data from Air District for', pollutant, 'from', first_date_needed, first_hour_needed, 'to', last_date_needed, last_hour_needed))
  
  return(request_url)
}



# Call API using URL generated at last step
request_new_proxy_data <- function(request_url){
  
  request_call <- GET(url = request_url)
  stop_for_status(request_call, paste('Failed request to URL', request_url))
  
  request_json <- content(request_call, as = 'text', type = NULL, encoding = 'UTF-8')
  request_df <- fromJSON(request_json, simplifyDataFrame = TRUE)
  
  new_proxy_data <- mutate(request_df, 
                           timestamp_utc = paste0(substr(UTC, 1, 10), ' ', substr(UTC, 12, 16), ':00'), 
                           RawConcentration = ifelse(RawConcentration == -999, Value, RawConcentration))
  
  return(new_proxy_data)
  
}



# Clean proxy data obtained from call
clean_new_proxy_data <- function(new_proxy_data){
  
  random_noise <- runif(nrow(new_proxy_data), .0000001, .0001)
  new_proxy_data$proxy_rand <- new_proxy_data$RawConcentration + random_noise
  
  new_proxy_clean <- dplyr::select(new_proxy_data, -c(UTC, Value, AgencyName, FullAQSCode, IntlAQSCode)) %>% 
    rename('lat'='Latitude', 'long'='Longitude', 'modality'='Parameter', 'units'='Unit', 'proxy_raw'='RawConcentration', 'proxy_site' = 'SiteName')
  
  return(new_proxy_clean)
}



# Iterate through dates up to yesterday one week at a time until proxy data current
request_all_dates_needed <- function(pollutant, request_start_time, existing_proxy_data){
  
  proxy_data <- existing_proxy_data
  
  while(request_start_time < yesterday){  
    
    request_url <- build_proxy_request_url(pollutant, request_start_time)
    
    new_proxy_data_raw <- request_new_proxy_data(request_url)
    new_proxy_data <- clean_new_proxy_data(new_proxy_data_raw)
    
    proxy_data <- rbind(proxy_data, new_proxy_data)
    
    month_after_last_start <- format_timestamp(ymd_hms(request_start_time) + td.30day + td.hour)
    
    request_start_time <- min(yesterday, month_after_last_start)
  }
  
  return(proxy_data)
}


# Remove duplicate values and write to csv
process_and_write_proxy_data <- function(pollutant, proxy_data){
  
  proxy_data_no_duplicates <- group_by(proxy_data, proxy_site, timestamp_utc) %>%
    slice_sample() %>%
    ungroup()
  
  print(paste('Proxy data for', pollutant, 'now current - writing to results/proxy'))
  
  write.csv(proxy_data_no_duplicates, paste0('results/proxy/', pollutant, '_proxy_data.csv'), row.names = F)
  
  return(proxy_data_no_duplicates)
}



# Function to Put it Together
get_current_proxy_data <- function(pollutant){
  
  existing_proxy_data <- get_existing_proxy_data(pollutant)
  
  request_start_time <- find_starttime_proxy_request(existing_proxy_data)
  
  updated_proxy_data_raw <- request_all_dates_needed(pollutant, request_start_time, existing_proxy_data)
  
  updated_proxy_data <- process_and_write_proxy_data(pollutant, updated_proxy_data_raw)
  
  # This line to be modified when more monitors included
  updated_filtered_proxy_data <- mutate(updated_proxy_data, timestamp_pacific = format_timestamp(ymd_hms(timestamp_utc) - td.hour*8)) %>%
    filter(proxy_site == 'San Pablo - Rumrill') %>%
    dplyr::select(timestamp_pacific, proxy_rand)
  
  return(updated_filtered_proxy_data)
  
}
