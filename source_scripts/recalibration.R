

generate_new_gain_and_offset <- function(joined_data_30day, aqys_needing_recal, pollutant){
  
  new_gain_and_offset <- filter(joined_data_30day, ID %in% aqys_needing_recal) %>%
    group_by(ID) %>%
    summarize(new.gain = sqrt(var(proxy_rand)/var(pollutant_raw)),
              new.offset = mean(proxy_rand) - new.gain*mean(pollutant_raw),
              .groups = 'drop_last')
  
  new_col_names <- switch(pollutant, 
                          'OZONE'=c('ID', 'O3.gain', 'O3.offset'), 
                          'NO2'=c('ID', 'NO2.gain', 'NO2.offset'))
  colnames(new_gain_and_offset) <- new_col_names
  
  print(as.matrix(new_gain_and_offset))
  
  return(new_gain_and_offset)
  
}



combine_params_get_first_flag <- function(new_params, pollutant, start_time_rolling_72hr){
  
  old_params_path_all <- list.files('results', paste0(pollutant, '*'), full.names = T, recursive = F)
  old_params_path_newest <- old_params_path_all[length(old_params_path_all)]
  old_params <- read.csv(old_params_path_newest)
  
  aqy_metadata <- select(old_params, c('ID', 'deployment_date', 'deployment_datetime', 'Longitude', 'Latitude', 'proxy_site')) %>%
    unique()
  
  time_first_flagged <- format_timestamp(ymd_hms(start_time_rolling_72hr) - td.5day)
  
  new_params_metadata <- inner_join(aqy_metadata, new_params, by = 'ID') %>%
    mutate(start_date = format_timestamp(time_first_flagged), end_date = '2199-12-31 23:59:59', .after = proxy_site)
  
  combined_params <- rbind(old_params, new_params_metadata) %>%
    arrange(ID, start_date) %>%
    group_by(ID) %>%
    mutate(end_date = ifelse(end_date == '2199-12-31 23:59:59' & start_date != max(start_date), 
                             format_timestamp(ymd_hms(max(start_date)) - td_hour), 
                             end_date))
  
  timestamp_for_path <- str_remove_all(format_timestamp(time_first_flagged), '\\:')
  new_param_path <- paste0('results/', pollutant, '_calvals_', timestamp_for_path, '.csv')
  write.csv(combined_params, new_param_path, row.names = F)
  
  return(time_first_flagged)
}



reset_flags_if_recalibrated <- function(pollutant){
  
  all_flags <- read.csv(paste0('results/running_flags/running_flags_', pollutant, '.csv'))
  
  flags_to_reset <- filter(all_flags, ks >= 120 | gain >= 120 | offset >= 120)
  aqys_to_reset <- unique(flags_to_reset$ID)
  flags_cleared <- mutate_at(flags_to_reset, .vars = c('ks', 'gain', 'offset'), .funs = ~.x*0)
  
  flags_to_leave <- filter(all_flags, ID %in% aqys_to_reset == F)
  
  updated_flags <- rbind(flags_to_leave, flags_cleared) %>%
    arrange(ID)
  
  write.csv(updated_flags, paste0('results/running_flags/running_flags_', pollutant, '.csv'), row.names = F)
  
  print('Reset flags - restarting drift detection at zero')
}


