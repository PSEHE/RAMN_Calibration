


format_timestamp <- function(timestamp){
  
  formatted_timestamp <- as.character(format(as.POSIXct(timestamp), '%Y-%m-%d %H:%M:%S'))
  
  return(formatted_timestamp)
}



td.hour <- 60*60
td.day <- td.hour*24
td.30day <- td.day*30
td.72hr <- 60*60*72
td.5day <- 60*60*24*5

ramn.start.time <- format_timestamp('2019-12-01 00:00:00')
yesterday <- format_timestamp(round(Sys.time(), 'hour') - td.day)
right.now <- format_timestamp(round(Sys.time(), 'hour'))



reset_flags <- function(pollutant){
  
  file_with_flags <- paste0('results/running_flags/running_flags_', pollutant, '.csv')
  
  read.csv(file_with_flags) %>%
    mutate_at(.vars = c('ks', 'gain', 'offset'), .funs = ~.x*0) %>%
    write.csv(file_with_flags, row.names = F)
  
  print(paste('Reset', pollutant, 'flags - restarting drift detection at zero'))
}


