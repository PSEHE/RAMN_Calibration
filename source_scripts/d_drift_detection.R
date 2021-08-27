


# Get flags for current 72-hr period
get_new_flags <- function(joined_data_72hr){

  drift_tests <- group_by(joined_data_72hr, ID) %>%
    summarize(manual_gain = sqrt(var(proxy_rand)/var(pollutant)),
              manual_offset = mean(proxy_rand) - manual_gain*mean(pollutant),
              ks_p = suppressWarnings(ks.test(proxy_rand, pollutant, exact = F))$p.value,
              .groups = 'drop_last')
  
  drift_flags <- mutate(drift_tests,
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



# Assign all flags - for use when there aren't enough data to run tests
### Make more robust later - need to check for monitors that are missing while others are present and this wont work for those purposes
flag_all_monitors <- function(joined_data_72hr, look_back_from_date){
  
  ID <- unique(joined_data_72hr$ID)
  
  all_aqys_flagged <- data.frame(ID) %>%
    mutate(gain_new = 1, offset_new = 1, ks_new = 1)
  
  print('All AQYs flagged for period due to insufficient data')
  
  return(all_aqys_flagged)
  
}


# Add tgether old and new flags - adjust time of flag detection if needed
sum_old_and_new_flags <- function(in_pollutant, new_flags, look_back_from_time){
  
  flag_path <- paste0('results/running_flags/running_flags_', in_pollutant, '.csv')
  existing_flags <- read.csv(flag_path, stringsAsFactors = F)

  joined_flags <- full_join(existing_flags, new_flags, by = 'ID')
  
  sum_if_flagged <- function(old, new){ifelse(new == 0, 0, old + new)}
  
  na_rm_flags <- mutate_at(.tbl = joined_flags, .vars = c('ks', 'ks_new', 'gain', 'gain_new', 'offset', 'offset_new'), .funs = ~replace_na(.x, 0)) %>%
    mutate_at(.tbl = ., .vars = c('time_of_first_flag'), .funs = ~replace_na(.x, '2199-01-01 00:00:00'))
  
  new_old_summed_flags <- mutate(na_rm_flags,
                         current_time = look_back_from_time,
                         ks = sum_if_flagged(ks, ks_new),
                         gain = sum_if_flagged(gain, gain_new),
                         offset = sum_if_flagged(offset, offset_new),
                         ks_detected = ifelse(ks == 0, '2199-01-01 00:00:00', min(ks_detected, current_time)),
                         gain_detected = ifelse(gain == 0, '2199-01-01 00:00:00', min(gain_detected, current_time)),
                         offset_detected = ifelse(offset == 0, '2199-01-01 00:00:00', min(offset_detected, current_time)),
                         time_of_first_flag = min(ks_detected, gain_detected, offset_detected))

  summed_flags <- dplyr::select(new_old_summed_flags, c('ID', 'ks', 'gain', 'offset', 'ks_detected', 'gain_detected', 'offset_detected', 'time_of_first_flag'))

  write.csv(summed_flags, flag_path, row.names = F)
  
  return(summed_flags)
}



get_aqys_needing_recal <- function(summed_flags){

  largest_running_flag <- apply(X = summed_flags[2:4], MARGIN = 1, FUN = max)

  summed_flags$max_flag <- largest_running_flag

  largest_flag_network <- max(largest_running_flag)

  print(paste('Largest flag:', largest_flag_network))
  
  aqys_needing_recal <- filter(summed_flags, max_flag >= 24*5) %>%
    pull(ID)
  
  return(aqys_needing_recal)
  
}


