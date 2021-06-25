


# Get flags for current 72-hr period
get_new_flags <- function(joined_data_72hr, look_back_from_time){
  
  drift_tests <- group_by(joined_data_72hr, ID) %>%
    summarize(manual_gain = sqrt(var(proxy_rand)/var(pollutant)),
              manual_offset = mean(proxy_rand) - manual_gain*mean(pollutant),
              ks_p = suppressWarnings(ks.test(proxy_rand, pollutant, exact = F))$p.value, 
              .groups = 'drop_last')
  
  drift_flags <- mutate(drift_tests,
                        time_of_current_flag = look_back_from_time,
                        gain_new = ifelse(manual_gain >= 1.3 | manual_gain <= .7, 1, 0),
                        offset_new = ifelse(manual_offset >= 5 | manual_offset <= -5, 1, 0),
                        ks_new = ifelse(ks_p <= .05, 1, 0))
                        # add test for extreme values relative to rest of data distribution
  
  new_flags <- dplyr::select(drift_flags, -c('manual_gain', 'manual_offset', 'ks_p')) %>%
    mutate_at(.vars = c('ks_new', 'gain_new', 'offset_new'), .funs = as.integer)
  
  aqys_with_flags <- filter(new_flags, gain_new > 0 | offset_new > 0 | ks_new > 0) %>%
    pull(ID)
  
  if(length(aqys_with_flags) > 0)
    {print(paste(aqys_with_flags, 'flagged for this period'))}
  else
    {print('No AQYs flagged for this period')}
  
  return(new_flags)
}


# Add together old and new flags - adjust time of flag detection if needed
sum_old_and_new_flags <- function(pollutant, new_flags, look_back_from_time){
  
  flag_path <- paste0('results/running_flags/running_flags_', pollutant, '.csv')
  existing_flags <- read.csv(flag_path, stringsAsFactors = F)

  joined_flags <- full_join(existing_flags, new_flags, by = 'ID')
  
  sum_if_flagged <- function(old, new){ifelse(new == 0, 0, old + new)}
  
  na_rm_flags <- mutate_at(.tbl = joined_flags, .vars = c('ks', 'ks_new', 'gain', 'gain_new', 'offset', 'offset_new'), .funs = ~replace_na(.x, 0)) %>%
    mutate_at(.tbl = ., .vars = c('time_of_first_flag'), .funs = ~replace_na(.x, '2199-01-01 00:00:00'))
  
  new_old_summed_flags <- mutate(na_rm_flags,
                         ks = sum_if_flagged(ks, ks_new),
                         gain = sum_if_flagged(gain, gain_new),
                         offset = sum_if_flagged(offset, offset_new),
                         time_of_first_flag = ifelse(ks == 0 & gain == 0 & offset == 0, '2199-01-01 00:00:00', 
                                                     ifelse(time_of_current_flag < time_of_first_flag, time_of_current_flag, time_of_first_flag)))
  
  summed_flags <- dplyr::select(new_old_summed_flags, c('ID', 'time_of_first_flag', 'ks', 'gain', 'offset'))

  write.csv(summed_flags, flag_path, row.names = F)
  
  return(summed_flags)
}



get_aqys_needing_recal <- function(summed_flags){
  
  largest_running_flag <- apply(X = summed_flags[3:ncol(summed_flags)], MARGIN = 1, FUN = max)
  
  summed_flags$max_flag <- largest_running_flag
  
  largest_flag_network <- max(largest_running_flag)
  
  print(paste('Largest flag:', largest_flag_network))
  
  aqys_needing_recal <- filter(summed_flags, max_flag >= 24*5) %>%
    pull(ID)
  
  return(aqys_needing_recal)
  
}


