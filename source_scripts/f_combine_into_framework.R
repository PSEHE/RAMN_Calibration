

# Combine functions from source scripts a-e into one function

detect_drift_recalibrate_data <- function(aqy_proxy_data, aqy_proxy_data_raw, in_pollutant, look_back_from_time){

  aqy_proxy_data_72hr <- temporally_filter_data(aqy_proxy_data, look_back_from_time, td_72hr)
  print(paste('Performing drift detection for', look_back_from_time, 'with', nrow(aqy_proxy_data_72hr), 'rows of data'))

  n_deployed <- length(unique(aqy_proxy_data_72hr$ID))
  if(nrow(aqy_proxy_data_72hr) < 0.75*72*n_deployed){
    new_flags <- flag_all_monitors(aqy_proxy_data_raw, look_back_from_time)}
    
    else{
        new_flags <- get_new_flags(aqy_proxy_data_72hr)}
        summed_flags <- sum_old_and_new_flags(in_pollutant, new_flags, look_back_from_time)
        aqys_needing_recal <- get_aqys_needing_recal(summed_flags)
  
  if(length(aqys_needing_recal) < 1){
    new_look_back_from_time <- format_timestamp(ymd_hms(look_back_from_time) + td_hour)
    return(list(look_back_from_time=new_look_back_from_time, calibrated_data=aqy_proxy_data))
  }
  
  else{
    print('Need to recalibrate - filtering to 30 day data')
    start_recal <- format_timestamp(ymd_hms(look_back_from_time) - td_5day)
    aqy_proxy_data_30day <- temporally_filter_data(aqy_proxy_data, start_recal, td_30day)
    
    if(pollutant == 'O3')
    {new_parameters <- generate_new_gain_and_offset_O3(aqy_proxy_data_30day, aqys_needing_recal)
      }
      else{new_parameters <- generate_initial_gain_and_offset_NO2(aqy_proxy_data_30day, aqys_needing_recal)
      ## function for optimization here
      }
    
    first_flag <- combine_params_get_first_flag(new_parameters, in_pollutant)
    new_look_back_from_time <- format_timestamp(ymd_hms(first_flag) + 100*td_hour)
    print(paste('Restarting drift detection at', new_look_back_from_time))
    reset_flags(in_pollutant)
    
    aqy_proxy_data <- calibrate_O3_data(aqy_proxy_data_raw) 
      if(pollutant == 'NO2'){aqy_proxy_data <- calibrate_NO2_data(aqy_proxy_data)}
    
    aqy_proxy_data <- rename_with(aqy_proxy_data, .fn = ~str_replace_all(.x, in_pollutant, 'pollutant'), .cols = everything())
    
    return(list(look_back_from_time=new_look_back_from_time, calibrated_data=aqy_proxy_data))
    }
}