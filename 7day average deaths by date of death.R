library(haven)
library(arrow)
library(tidyverse)
library(slider)
library(lubridate)


cases <- read_parquet("T:/ACDC-Shared Files/nCoV- 2019 Novel Coronavirus/Data Management/Daily_Run_Data/parquet_datasets/cases_reduced_vars.parquet") 

cases_deaths <- cases %>% 
  filter(death==1) %>% 
  rename(death_dt=date_death)%>% 
  mutate(death_dt_use=as.Date(ifelse(!is.na(death_dt) & death_dt>=as.Date("2020/1/1"),death_dt,ifelse(!is.na(create_dt) & death==1,create_dt,NA)), origin="1970-01-01")) %>% 
  count(death_dt_use)

all_dates_death<-data.frame(seq(as.Date("2020/1/1"),as.Date(Sys.Date()),"day"))%>%
  rename(death_dt_use=1) %>% arrange(death_dt_use) 


all_deaths<- cases_deaths %>% arrange(death_dt_use)%>% 
  right_join(all_dates_death, by="death_dt_use") %>% replace_na(list(n=0)) %>% arrange(death_dt_use)

all_dt_index <- all_deaths %>%
  mutate(avg_7days_dths = slide_index_dbl(n,
                  .i=death_dt_use,
                  .f=~mean(.x, na.rm=TRUE),
                  .before=days(6)))


write.csv(all_dt_index, "H:/deaths_output.csv")

  