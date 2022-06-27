
# Load libraries ----------------------------------------------------------

library(tidyverse)
library(fedmatch)
library(DBI)
library(readxl)
library(lubridate)
library(glue)
library(stringii)

# Load data ---------------------------------------------------------------

# Set up server connection
con <- DBI::dbConnect(odbc::odbc(), "DPHE144") 

# Downoload all the cedrs data
cedrs_data <- tbl(con, dbplyr::in_schema("cases", "covid19_cedrs"))%>%
  # But only the variables we want
  select(profileid,firstname,lastname,birthdate) %>%
  # Pull them in
  collect() %>%
  # Make birthdays a date
  mutate(dob = ymd(birthdate)) %>%
  # Choose only one row per profile id
  group_by(profileid) %>%
  slice_head(n=1) %>%
  ungroup()%>%
  # make date of birth a character so it can be used for fuzzy matching
  mutate(dob = as.character(dob),
         firstname = stringi::stri_trans_general(firstname, "latin-ascii"),
         lastname = stringi::stri_trans_general(lastname, "latin-ascii")) %>%
  mutate(firstname = toupper(firstname),
         lastname = toupper(lastname))
  
# Import the mab_data
mab_data <- read_excel(path = "data/mAb_matching_kth_06.23.22.xlsx",
                         sheet="non-matched") %>%
  # Rename teh columns so they match CEDRS
  rename(row = ROW,
         firstname = "First Name",
         lastname = "Last Name",
         dob = "Date Of Birth") %>%
  # Remove that weird column I don't like
  select(-`Name + DOB`) %>%
  # make date of birth a character so it can be used for fuzzy matching

  mutate(dob = as.character(dob),
         firstname = stringi::stri_trans_general(firstname, "latin-ascii"),
         lastname = stringi::stri_trans_general(lastname, "latin-ascii")) %>%
  mutate(firstname = toupper(firstname),
         lastname = toupper(lastname))

# Now, can we do the fuzzy match real fast? ------------------------------

# Figure out how many CPU cores I have
cpu_cores <- parallel::detectCores()
# Use all but 2 for the fuzzy match
cores_to_use <- round(cpu_cores/2,0)

# Log the start time
start_match <- now()

# Do the match!
x <- fedmatch::merge_plus(
  
  # Choose the data sources
  data1 = mab_data,
  data2 = cedrs_data,
  
  # Match across mulitple variables
  match_type = "multivar",
  
  # Choose the matching variables
  by = c(
    "firstname",
    "lastname",
    "dob"
  ),
  
  # For output, decide how to name the variables
  suffixes = c("_mab", "_cedrs"),
  
  # Identify the unique key for each data source
  unique_key_1 = "row",
  unique_key_2 = "profileid", 
  
  # Set up the finickier match settings
  multivar_settings = build_multivar_settings(
    # How to comapre each variable
    compare_type = c("stringdist","stringdist","stringdist"),
    # What weight to give each variable
    wgts = c(0.30, 0.35, 0.35), 
    # How many CPU cores to use
    nthread = 1
  )
)

# Print a nice message to the console
print(
  glue::glue(
    "
    It only took {now() - start_match} mins!
    
    Isn't this an efficient system?
    "
  )
)

# Output the matches to a data frame
matches <- x[["matches"]] %>%
  select(ends_with("_mab"),
         ends_with("_cedrs"),
         ends_with("compare"),
         multivar_score) %>%
  arrange(desc(multivar_score))

# Look at the dataframe
view(matches)

matches_join <- left_join(matches,cedrs_data,
          by = c("firstname_cedrs" = "firstname",
                 "lastname_cedrs" = "lastname",
                 "dob_cedrs" = "dob"))

# Write that dataframe
write_csv(matches_join,
          paste0("mab_cedrs_fuzzy_match",
                 Sys.Date(),
                 ".csv"
          )
)
