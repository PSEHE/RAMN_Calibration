


# Standard format for timestamps to avoid issue where midnight is deleted from string
format_timestamp <- function(timestamp){
  
  formatted_timestamp <- as.character(format(as.POSIXct(timestamp), '%Y-%m-%d %H:%M:%S'))
  
  return(formatted_timestamp)
}



# Set up time difference objects and key dates
td_hour <- 60*60
td_day <- td_hour*24
td_30day <- td_day*30
td_72hr <- 60*60*72
td_5day <- 60*60*24*5

ramn.start.time <- format_timestamp('2019-12-01 00:00:00')
yesterday <- format_timestamp(round(Sys.time(), 'hour') - td_day)
right.now <- format_timestamp(round(Sys.time(), 'hour'))



# Function to reset all flags for manual use
reset_flags <- function(pollutant){
  
  file_with_flags <- paste0('results/running_flags/running_flags_', pollutant, '.csv')
  
  read.csv(file_with_flags) %>%
    mutate_at(.vars = c('ks', 'gain', 'offset'), .funs = ~.x*0) %>%
    mutate(ks_detected = '2199-01-01 00:00:00', gain_detected = '2199-01-01 00:00:00', offset_detected = '2199-01-01 00:00:00',
           time_of_first_flag = '2199-01-01 00:00:00') %>%
    write.csv(file_with_flags, row.names = F)
  
  print(paste('Reset', pollutant, 'flags - restarting drift detection at zero'))
}

