# RAMN_Calibration
* This script reads in un-calibrated network data that has been pulled from the Aeroqual API via Python script
* This script requires a proxy to calibrate our data. Bay Area regulatory data is pulled for these purposes. Very simple exploration is done for land use parameters, but for now the most proximate stations are generally assumed to be the best proxies. This assumption will be subject to further analysis at a later time.
  -https://docs.airnowapi.org/Data/docs

### The script is designed to:
1. Read in daily, one-minute air quality monitoring data from Richmond Network, previously downloaded with Python script

2. Call regulatory API and download data for monitoring stations nearest Richmond
   API documentation: https://docs.airnowapi.org/
   2.a. Each day, pull in just the previous day and append to a running log of proxy data

3. Compare data to proxy data for drift detection
3.a. Run Kolmogorov Smirnov test to compare three-day rolling average of each monitor to that of the BAAQMD data
    3.a.i. A flag should be created for each time KS statistic goes below 0.05
        3.a.ii. Create separate column for running count of flag

3.b. Run deviance test to see if mean and variance of our data differ substantially with those of the proxy data
    3.b.i. A separate flag should be created for each of these, with a running flag column 

4. After five consecutive days of flags for any parameter, recalibrate data
    4.a. Calibration uses mean and variance of our data, proxy data to create new slope and offset
    4.b. Data should be re-calibrated back to first day of flag, using eight days of data for slope and offset calculation
        
5. Output - running log of calibration parameters (slope, offset, days valid) for each monitor for NO2 and O3

### Other contents include:
1. Spatial data used for proxy selection and validation
2. Primary literature guiding calibration theory
3. Truncated version of PSE data for script testing
