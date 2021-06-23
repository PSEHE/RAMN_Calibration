


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
  
  # To delete print statements after build out done - simply a test
  results_633 <- filter(drift_tests, ID == 'AQY BB-633')
  results_642 <- filter(drift_tests, ID == 'AQY BB-642')
  print(paste('Results for 633: ks pval =', results_633$ks_p, '| gain =', results_633$manual_gain, '| offset =', results_633$manual_offset))
  print(paste('Results for 642: ks pval =', results_642$ks_p, '| gain =', results_642$manual_gain, '| offset =', results_642$manual_offset))
  
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



sum_old_and_new_flags <- function(pollutant, new_flags){
  
  flag_path <- paste0('results/running_flags/running_flags_', pollutant, '.csv')
  existing_flags <- read.csv(flag_path, stringsAsFactors = F)
  
  joined_flags <- full_join(existing_flags, new_flags, by = 'ID')
  
  cols_to_rmv_na <- colnames(joined_flags)[2:ncol(joined_flags)]
  
  sum_if_flagged <- function(old, new){ifelse(new == 0, 0, old + new)}
  
  summed_flags <- mutate_at(.tbl = joined_flags, .vars = cols_to_rmv_na, .funs = ~replace_na(.x, 0)) %>%
    transmute(ID = ID, 
              ks = sum_if_flagged(ks, ks_new),
              gain = sum_if_flagged(gain, gain_new),
              offset = sum_if_flagged(offset, offset_new))
  
  write.csv(summed_flags, flag_path, row.names = F)
  
  return(summed_flags)
}



get_aqys_needing_recal <- function(summed_flags){
  
  largest_running_flag <- apply(X = summed_flags[2:ncol(summed_flags)], MARGIN = 1, FUN = max)
  
  summed_flags$max_flag <- largest_running_flag
  
  largest_flag_network <- max(largest_running_flag)
  
  print(paste('Largest flag:', largest_flag_network))
  
  aqys_needing_recal <- filter(summed_flags, max_flag >= 24*5) %>%
    pull(ID)
  
  return(aqys_needing_recal)
  
}


