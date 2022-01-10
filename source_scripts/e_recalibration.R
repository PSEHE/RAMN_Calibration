

# Generate new offset and gain using simple formula and thirty days of data
generate_new_gain_and_offset_O3 <- function(joined_data_30day, aqys_needing_recal){
  
  new_gain_and_offset <- filter(joined_data_30day, ID %in% aqys_needing_recal) %>%
    group_by(ID) %>%
    summarize(new.gain = sqrt(var(proxy_rand)/var(pollutant_raw)),
              new.offset = mean(proxy_rand) - new.gain*mean(pollutant_raw),
              .groups = 'drop_last')
  
  new_col_names <- c('ID', 'O3.gain', 'O3.offset')
  colnames(new_gain_and_offset) <- new_col_names
  
  print(as.matrix(new_gain_and_offset))
  
  return(new_gain_and_offset)
  
}


# Generate new offset and gain using simple formula and thirty days of data
generate_new_gain_and_offset_PM25 <- function(joined_midmonth_data){
  
  new_gain_and_offset <- joined_midmonth_data %>%
    group_by(ID, pollutant_proxy_site, time_month, time_year) %>%
    summarize(new.gain = round(sqrt(var(proxy_rand)/var(pollutant_raw)), 5),
              new.offset = round(mean(proxy_rand) - new.gain*mean(pollutant_raw), 5),
              .groups = 'drop_last')
  
  new_col_names <- c('ID', 'proxy_site', 'time_month', 'time_year', 'PM25.gain', 'PM25.offset')
  colnames(new_gain_and_offset) <- new_col_names
  
  new_gain_and_offset <- ungroup(new_gain_and_offset) %>%
    mutate(days_month = days_in_month(time_month), time_month = str_sub(paste0('0', time_month), -2L, -1L),
           start_date = paste(time_year, time_month, '01 00:00:00', sep = '-'), 
           end_date = paste0(time_year, '-', time_month, '-', days_month, ' 23:59:59')) %>%
    dplyr::select(ID, proxy_site, start_date, end_date, PM25.gain, PM25.offset)
  
  return(new_gain_and_offset)
  
}


# Initialize offset and gain for subsequent iteration using simple formula and thirty days of data
generate_initial_gain_and_offset_NO2 <- function(joined_data_30day, aqys_needing_recal){
  
  new_gain_and_offset <- filter(joined_data_30day, ID %in% aqys_needing_recal) %>%
    group_by(ID) %>%
    summarize(new.offset = mean(proxy_rand) - mean(Ox_raw - O3), ## Potential issue - Ox raw signal vs. raw Ox - pull from API?
              new.gain.Ox = sqrt(var(proxy_rand)/var(Ox_raw - O3)),
              new.gain.O3 = new.gain.Ox,
              .groups = 'drop_last')
  
  new_col_names <- c('ID', 'NO2.offset', 'NO2.gain.Ox', 'NO2.gain.O3')
  colnames(new_gain_and_offset) <- new_col_names
  
  print(as.matrix(new_gain_and_offset))
  
  return(new_gain_and_offset)
  
}



# Optimization of NO2 parameters
optimize_params_NO2 <- function(joined_data_30day, new_gain_and_offset){
  
  objective_fxn <- function(joined_data_30day, par){with(joined_data_30day, par[1] + par[2]*Ox_raw - par[3]*O3)}
  
  optim_result <- optim(par = c(new_gain_and_offset$NO2.offset, new_gain_and_offset$NO2.gain.Ox, new_gain_and_offset$NO2.gain.O3), fn = objective_fxn, data = joined_data_30day)
  
  return(optim_result)
  
  
}



# Combine new params with existing - including generating new start and end dates as appropriate
combine_params_get_first_flag <- function(new_params, in_pollutant){
  
  old_params <- get_current_params(in_pollutant)
  
  proxy_col <- paste0(in_pollutant, '_proxy_site')
  
  aqy_metadata <- select(old_params, c('ID', 'deployment_date', 'deployment_datetime', 'Longitude', 'Latitude', proxy_col)) %>%
    unique()
  
  aqys_recalibrated <- new_params$ID
  
  time_first_flagged <- read.csv(paste0('results/running_flags/running_flags_', in_pollutant, '.csv')) %>%
    filter(ID %in% aqys_recalibrated) %>%
    pull(time_of_first_flag) %>%
    unique()

  new_params_metadata <- inner_join(aqy_metadata, new_params, by = 'ID') %>%
    mutate(start_date = format_timestamp(time_first_flagged), end_date = '2199-12-31 23:59:59')

  combined_params <- rbind(old_params, new_params_metadata) %>%
    arrange(ID, start_date) %>%
    group_by(ID) %>%
    mutate(end_date = ifelse(end_date == '2199-12-31 23:59:59' & start_date != max(start_date), 
                             format_timestamp(ymd_hms(max(start_date)) - td_hour), 
                             end_date))

  timestamp_for_path <- str_remove_all(format_timestamp(time_first_flagged), '\\:')
  new_param_path <- paste0('results/', in_pollutant, '_calvals_', timestamp_for_path, '.csv')
  write.csv(combined_params, new_param_path, row.names = F)
  
  return(time_first_flagged)
}



# Reset flags for monitors which have been recalibrated; subtract 5*24
reset_flags_if_recalibrated <- function(in_pollutant){
  
  all_flags <- read.csv(paste0('results/running_flags/running_flags_', pollutant, '.csv'))
  
  flags_to_zero <- filter(all_flags, ks >= 120 | gain >= 120 | offset >= 120)
  flags_zeroed <- mutate_at(flags_to_zero, .vars = c('ks', 'gain', 'offset'), .funs = ~.x*0)
  
  aqys_with_flags_zeroed <-  unique(flags_zeroed$ID)
  
  flags_to_subtract_five_days <- filter(all_flags, ID %in% aqys_with_flags_zeroed == F)
  flags_subtracted_five_days <- mutate_at(flags_to_subtract_five_days, .vars = c('ks', 'gain', 'offset'), .funs = pmax(~.x-119, 0))
  
  updated_flags <- rbind(flags_zeroed, flags_subtracted_five_days) %>%
    arrange(ID)
  
  write.csv(updated_flags, paste0('results/running_flags/running_flags_', in_pollutant, '.csv'), row.names = F)
  
  print('Reset flags - restarting drift detection at zero')
}


