
# Libraries
library("RMySQL")
library(tidyverse)

# Functions
dbDisconnectAll <- function(){
  ile <- length(dbListConnections(MySQL())  )
  lapply( dbListConnections(MySQL()), function(x) dbDisconnect(x) )
  cat(sprintf("%s connection(s) closed.\n", ile))
}

# General SQL query
query <- function(q)
{
  # Remote DB with password - for MySQL
  con = dbConnect(MySQL(),
                  user='guest',
                  password = 'guest',
                  dbname='meadows',
                  port = 3306, host='sxouse.ddns.net')
  
  rs1 = dbSendQuery(con, q)
  return(as_tibble(fetch(rs1, n=-1)))
  dbDisconnectAll()
}

# Load the database
GetTheData <-  function()
{
  con = dbConnect(MySQL(),
                  user='guest',
                  password = 'guest',
                  dbname='meadows',
                  port = 3306, host='sxouse.ddns.net')
  
  q <- sprintf('select survey_id, assembly_name, quadrat_count, community, quadrat_id, visit_date, records_id, species.species_id, 
    species.species_name from surveys
      join quadrats on quadrats.survey_id = surveys_id
      join visit_dates on quadrats.vd_id = visit_dates.vds_id
      join records on records.quadrat_id = quadrats_id
      join species on species.species_id = records.species_id
    # Two surveys have 0 quadrat count; exclude A.capillaris_stolonifera; exclude 
    # some odd surveys with no assigned community
    where quadrat_count > 0 and species.species_id != 4 and community is not null;') 
  # NOTE: this extract includes "MG5", i.e. some MG5 communities where the team have not decided
  # on a sub-group.
  
  rs1 = dbSendQuery(con, q)
  return(as_tibble(fetch(rs1, n=-1)))
  dbDisconnectAll()
}

# Return raw data extract (but don't display _id columns)
AllTheData <- function(t_d) 
{
  return(t_d %>% select("assembly_name", "quadrat_count", "visit_date", "species_name"))
}

# Return species frequencies in the data, average and measures of spread
GrossFrequency <- function(t_d) 
{
  # Gross frequency for each species.
  # Need hits and trials (quadrats)
  species_freq <- t_d %>% group_by(species_id, species_name) %>% summarise(hits = n())
  trials <- n_distinct(t_d$quadrat_id)
  return(species_freq %>% mutate(trials)
         %>% mutate(freq = hits/trials)
         %>%  mutate(CrI5 = qbeta(0.05, hits+1, 1+trials-hits))
         %>%  mutate(median = qbeta(0.5, hits+1, 1+trials-hits)) # For comparison with frequency as hits/trials
         %>%  mutate(CrI95 = qbeta(0.95, hits+1, 1+trials-hits))
  )
}

# Return species frequencies by community, average and measures of spread
FrequencyByCommunity <- function(t_d) # the_data
{
  # Need trials for each community
  trials_by_community <- t_d %>% group_by(community) %>% summarise(trials = n_distinct(quadrat_id))
  # Need hits for each community and species
  hits_by_community <- t_d %>% group_by(community, species_name, species_id) %>% summarise(hits = n_distinct(records_id))
  return(left_join(hits_by_community, trials_by_community, by = "community")
                        %>%  mutate(freq = hits/trials)
                        %>%  mutate(CrI5 = qbeta(0.05, hits+1, 1+trials-hits))
                        %>%  mutate(median = qbeta(0.5, hits+1, 1+trials-hits)) # For comparison with frequency as hits/trials
                        %>%  mutate(CrI95 = qbeta(0.95, hits+1, 1+trials-hits)))
}

# Return species frequencies by survey, average and measures of spread
FrequencyBysurvey <- function(t_d)
{
  d <- t_d %>% select(survey_id, species_id, species_name)
  # Hits for each species in each survey
  species_hits <- (d %>% group_by(survey_id, species_name) 
                   %>% summarise(hits = n()))
  # Trials (quadrats) - quadrat count for each survey (is indepenedent of species!)
  t <- (t_d %>% select(survey_id, quadrat_id)
        %>% group_by(survey_id)
        %>% summarise(trials = n_distinct(quadrat_id)))
  # Frequency of each species in each survey, hits/trials
  species_freq <- (left_join(species_hits, t, by = "survey_id")
                   %>% mutate(freq = hits/trials)
                   %>% mutate(CrI5 = qbeta(0.05, hits+1, 1+trials-hits))
                   %>% mutate(median = qbeta(0.5, hits+1, 1+trials-hits)) # For comparison with frequency as hits/trials
                   %>% mutate(CrI95 = qbeta(0.95, hits+1, 1+trials-hits)))
  # Include community
  nvcs <- t_d %>% select(survey_id, community) %>% distinct()
  data <- left_join(species_freq, nvcs, by = "survey_id") 
  # Include assembly_name
  surveys <- t_d %>%  select(survey_id, assembly_name) %>% distinct()
  data <- (left_join(data, surveys, by = "survey_id")
           %>% select(survey_id, assembly_name, community, species_name, 
                      hits, trials, freq, CrI5, median, CrI95)) # reorder
  d <- the_data %>% select(survey_id, species_id, species_name)
  # Hits for each species in each survey
  species_hits <- (d %>% group_by(survey_id, species_name) 
                   %>% summarise(hits = n()))
  # Trials (quadrats) - quadrat count for each survey (is indepenedent of species!)
  t <- (the_data %>% select(survey_id, quadrat_id)
        %>% group_by(survey_id)
        %>% summarise(trials = n_distinct(quadrat_id)))
  # Frequency of each species in each survey, hits/trials
  species_freq <- (left_join(species_hits, t, by = "survey_id")
                   %>% mutate(freq = hits/trials)
                   %>% mutate(CrI5 = qbeta(0.05, hits+1, 1+trials-hits))
                   %>% mutate(median = qbeta(0.5, hits+1, 1+trials-hits)) # For comparison with frequency as hits/trials
                   %>% mutate(CrI95 = qbeta(0.95, hits+1, 1+trials-hits)))
  # Include community
  nvcs <- t_d %>% select(survey_id, community) %>% distinct()
  data <- left_join(species_freq, nvcs, by = "survey_id") 
  # Include assembly_name
  surveys <- t_d %>%  select(survey_id, assembly_name) %>% distinct()
  data <- (left_join(data, surveys, by = "survey_id")
           %>% select(survey_id, assembly_name, community, species_name, 
                      hits, trials, freq, CrI5, median, CrI95)) # reorder
  # Include species_id, dropped somewhere along the way
  data <- (left_join(data, t_d 
                     %>% select(species_id, species_name) 
                     %>% distinct(), by = "species_name"))
  data <- data %>% select(survey_id, assembly_name, community, 
                     species_id, species_name, hits, trials,freq, 
                     CrI5, median,CrI95) 
}

# Return quadrat species count per community, average and measures of spread.
CommunitySpeciesCounts <- function(t_d) # the_data
{
  # 1. Species counts per quadrat
  sp_cnt <- (t_d %>% select(quadrat_id, species_id)
             %>% group_by(quadrat_id) %>% summarise(sp_count = n()))
  # 2. Join communities, 
  communities <- t_d %>% select(community, quadrat_id) %>% distinct()
  d1 <- left_join(sp_cnt, communities, by = "quadrat_id")
  # 3. Summarise - mean and measures of spread
  data <- (d1 %>% group_by(community) 
           %>% summarise(mean_species_count = mean(sp_count), sd = sd(sp_count), 
                         CrI5 = quantile(sp_count, 0.05), median = median(sp_count), 
                         CrI95 = quantile(sp_count, 0.95)))
  return(data)
}

# Return quadrat species count per survey, average and measures of spread
surveySpeciesCounts <- function(t_d) # the_data
{
  # 1. Species counts per quadrat
  sp_cnt <- (t_d %>% select(quadrat_id, species_id)
             %>% group_by(quadrat_id) %>% summarise(sp_count = n()))
  
  # 2. Join surveys, summarise - mean and measures of spread
  surveys <- t_d %>% select(survey_id, quadrat_id) %>% distinct()
  d1 <- left_join(sp_cnt, surveys, by = "quadrat_id")
  d2 <- d1 %>% group_by(survey_id) %>% summarise(mean_species_count = mean(sp_count), sd = sd(sp_count), CrI5 = quantile(sp_count, 0.05), median = median(sp_count), CrI95 = quantile(sp_count, 0.95))
  
  # 3.Add assembly_name
  ass_names <- t_d %>% select(assembly_name, survey_id) %>% distinct()
  data <- (left_join(d2, ass_names, by = "survey_id") 
           %>% select(survey_id, assembly_name, mean_species_count, sd, CrI5, median, CrI95))
  return(data)
  }

########################## MAIN ##############################
# Following useful for testing the functions. Comment out in general
# GET DATA FROM DB
the_data <- GetTheData()
# 
# Make various digests of the_data the_data but pass a selection, e.g. community, year, species.
species_freq <-  GrossFrequency(the_data)
# freq_by_community <- FrequencyByCommunity(the_data)
# freq_by_survey <- FrequencyBysurvey(the_data)
# community_species_counts <- CommunitySpeciesCounts(the_data)
# survey_species_counts <- surveySpeciesCounts(the_data)

# General procedure for sending an SQL query

# q <- "SELECT Community, species_id, p_central FROM meadows.mg_rodwell where Community like 'MG%';"
# t <- query(q)