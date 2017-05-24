# 
# Разбор и размещение ежемесячной статистики большой группы аккаунтов -----
# 

library(tidyverse)
library(stringr)
library(lubridate)

source("reference.R")

files_all <- list.files(
  path = "Data",
  full.names = TRUE
)

if(file.exists("BIG_files_parsed.lines")) {
  big_files_parsed <- read_lines("BIG_files_parsed.lines")
} else {
  big_files_parsed <- NULL
}

if(file.exists("BIG_accounts.csv")) {
  big_accounts <- read_csv("BIG_accounts.csv")
}

if(file.exists("BIG_accounts_stat.csv")) {
  big_accounts_stat <- 
    read_csv(
      "BIG_accounts_stat.csv", 
      col_types = cols(
        .default = col_number(),
        account_id = col_integer(),
        b_type = col_character()
      )
    )
}

files <- 
  files_all[grepl("M,account_info.+\\.RObj", files_all) & ! files_all %in% big_files_parsed]

if(length(files) > 0) for(file in files) {
  load(file)
  # Извлекаем статистику для каждого аккаунта:
  tmp_accounts <- 
    account_info %>% 
    lapply(function(accounts_group) {
      tmp_timestamp <- accounts_group$timestamp %>% as.integer()
      lapply(accounts_group$data, function(account) {
        data_frame(
          account_id = ifelse(
            is.null(account$account_id), NA, account$account_id
          ),
          last_battle_time = ifelse(
            is.null(account$last_battle_time), NA, account$last_battle_time
          ),
          global_rating = ifelse(
            is.null(account$global_rating), NA, account$global_rating
          ),
          timestamp = tmp_timestamp
        )
      }) %>% 
        bind_rows()
    }) %>% 
    bind_rows() %>% 
    mutate_if(is.numeric, as.integer)
  
  if(exists("big_accounts")) {
    big_accounts <- 
      bind_rows(big_accounts, tmp_accounts) %>% 
      arrange(timestamp) %>% 
      group_by(account_id, last_battle_time) %>% 
      summarise_all(last)
  } else {
    big_accounts <- tmp_accounts
  }
  
  tmp_accounts_stat <- 
    account_info %>% 
    lapply(function(accounts_group) {
      tmp_timestamp <- accounts_group$timestamp %>% as.integer()
      lapply(accounts_group$data, function(account) {
        tmp_account_id <- account$account_id %>% as.integer()
        tmp_statistics <- account$statistics
        names(tmp_statistics)[names(tmp_statistics) %in% ref_stat_fields$type] %>%
          lapply(function(battle_type) {
            # Извлекаем значение каждого собираемого показателя:
            ref_stat_fields$name %>%
              lapply(function(variable_name){
                tmp_value <- tmp_statistics[[c(battle_type, variable_name)]]
                list(
                  account_id = tmp_account_id,
                  timestamp = tmp_timestamp,
                  b_type = battle_type,
                  variable = variable_name,
                  value = ifelse(is.null(tmp_value), NA, tmp_value)
                )
              }) %>%
              bind_rows()
          }) %>%
          bind_rows()
      }) %>% 
        bind_rows()
    }) %>% 
    bind_rows() %>% 
    filter(! is.na(value)) %>% 
    spread(variable, value)
  
  if(exists("big_accounts_stat")) {
    big_accounts_stat <- 
      bind_rows(big_accounts_stat, tmp_accounts_stat) %>% 
      arrange(timestamp) %>% 
      group_by(account_id, b_type) %>% 
      mutate(prev_battles = dplyr::lag(battles, default = -1)) %>% 
      filter(prev_battles != battles) %>% 
      select(- prev_battles)
  } else {
    big_accounts_stat <- tmp_accounts_stat
  }
  
  write_csv(big_accounts, "BIG_accounts.csv")
  write_csv(big_accounts_stat, "BIG_accounts_stat.csv")
  
  big_files_parsed <- 
    c(big_files_parsed, file) %>% sort() %>% unique()
  write_lines(big_files_parsed, "BIG_files_parsed.lines")
}

rm(list = ls()[
  ls() %in% c("file", "files", "account_info", "tmp_accounts", "tmp_accounts_stat")
  ])
