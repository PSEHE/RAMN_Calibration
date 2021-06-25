

# Combine functions from source scripts a-e into one function

detect_drift_recalibrate_data <- function(aqy_proxy_data, aqy_proxy_data_raw, pollutant, look_back_from_time){
  
  aqy_proxy_data_72hr <- temporally_filter_data(aqy_proxy_data, look_back_from_time, td_72hr)
  
  print(paste('Performing drift detection for', look_back_from_time, 'with', nrow(aqy_proxy_data_72hr), 'rows of data'))
  
  new_flags <- get_new_flags(aqy_proxy_data_72hr, look_back_from_time)
  
  summed_flags <- sum_old_and_new_flags(pollutant, new_flags)
  
  aqys_needing_recal <- get_aqys_needing_recal(summed_flags)
  
  if(length(aqys_needing_recal) < 1){
    
    new_look_back_from_time <- format_timestamp(ymd_hms(look_back_from_time) + td_hour)
    
    return(list(look_back_from_time=new_look_back_from_time, calibrated_data=aqy_proxy_data))
    }
  else{
    
    aqy_proxy_data_30day <- temporally_filter_data(aqy_proxy_data, look_back_from_time, td_30day)
    
    new_parameters <- generate_new_gain_and_offset(aqy_proxy_data_30day, aqys_needing_recal, pollutant)
    
    first_flag <- combine_params_get_first_flag(new_parameters, pollutant)

    new_look_back_from_time <- format_timestamp(ymd_hms(first_flag) + 2*td_hour)

    reset_flags(pollutant)
    
    aqy_proxy_data <- calibrate_O3_data(aqy_proxy_data_raw)
    
    return(list(look_back_from_time=new_look_back_from_time, calibrated_data=aqy_proxy_data))
    }
}