

# Normal process if token is not fucked. If it is fucked or set to --------


## Install required packages
# install.packages( "RJSONIO" )
# install.packages( "bigrquery" )
# install.packages( "arrow" )

## load required packages
library( bigrquery )
library( RJSONIO )
library("arrow")
library("gargle")

bq_deauth()                 # clear token in this session 


# ensure gargle does not reuse cache
options(gargle_oauth_cache = FALSE)

# explicitly request browser login
bq_auth(email = NA)

bq_user()                   # prints the active email

# sets GCP adress to authenticate with
bq_auth(email = "nino.weerman@sawiday.com")

# get project id
project_id <- "production-247608"

# state query
sql_string <- "
SELECT session_date, user_pseudo_id,
       ga_session_id, traffic_channelgroup,
       user_pseudo_session_id, transactions,
       ecommerce_purchase_revenue, 
       ecommerce_total_item_quantity
FROM `production-247608.ga4_marketing_attribution.ga_nlnl_session_records`
WHERE session_date >= DATE '2026-02-01'
  AND session_date <  DATE '2026-03-01'
"

# run query and store
tb <- bq_project_query( project_id , sql_string )

# ap INT64 -> bit64::integer64 ( no NA overflow )
# this caused issues initially
query_results <- bq_table_download(
  tb , 
  n_max  = Inf ,
  bigint = "integer64"
)

# inspect outpute
str( query_results )
max(query_results$session_date)


###### write output of query to machine

## Write parquet
arrow::write_parquet( query_results , "ga_nlnl_sessions_2025-10.parquet" )

## write JSON
json_results <- toJSON( query_results )
write( json_results, "sawiday_nlnl_sess_2510.json")

## write csv
write.csv( query_results , "Sawiday_MTA_Journeys_NL_FEB_2026.csv ")

## write CSV sample 
# write.csv( query_results[ 1: ( abs( 0.05 * nrow( query_results ) ) ) ,  ], "samp_nl_sess_2510.csv" )

## Get Sample size
cnt_row<- nrow(query_results) # number of rows
smp <- query_results[ 1: ( abs( 0.05 * nrow( query_results ) ) ) ,  ] # get 5% of rows




# Reset to new e-mail -----------------------------------------------------

# ------------------------------------------------------------------------------
# BigQuery auth reset: force a different Google account
# ------------------------------------------------------------------------------

##### Delete the OAuth cache from the library, using the terminal:
## inspect first - TERMINAL

#printenv GOOGLE_APPLICATION_CREDENTIALS
#printenv CLOUDSDK_CONFIG
#ls -l ~/.config/gcloud/application_default_credentials.json

## Remove if present - TERMINAL
#gcloud auth application-default revoke
#rm -f ~/.config/gcloud/application_default_credentials.json

library(bigrquery)
library(gargle)

TARGET_EMAIL <- "nino.weerman@sawiday.com"


# show exactly how gargle is deciding
options(
  gargle_oauth_cache = FALSE,
  gargle_oauth_email = TARGET_EMAIL,
  gargle_oauth_client_type = "installed",
  gargle_oob_default = FALSE,
  gargle_verbosity = "debug"
)

# prevent env-var ADC from hijacking this R session
Sys.unsetenv("GOOGLE_APPLICATION_CREDENTIALS")

# clear any in-memory token
bq_deauth()

 
# CRITICAL: allow ONLY browser-based user OAuth
gargle::cred_funs_set(
  list(credentials_user_oauth2 = gargle::credentials_user_oauth2)
)

# this should now open a browser-based OAuth flow
bq_auth(
  email = TARGET_EMAIL,
  cache = FALSE,
  use_oob = FALSE
)

# verify the active identity
bq_user()

# restore normal credential lookup after auth succeeds
gargle::cred_funs_set_default()
