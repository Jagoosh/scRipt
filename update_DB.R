# 
# Настройка окружения для разбора данных и их размещения в базу
# 

library(tidyverse)
library(stringr)
library(lubridate)

if(file.exists("DB_files_parsed.lines")) {
  files_parsed <- read_lines("DB_files_parsed.lines")
} else {
  files_parsed <- NULL
}

files_all <- list.files(
  path = "Data",
  full.names = TRUE
)

source("reference.R")

# 
# Разбор и размещение сведений об участии в кланах -----
# 

if(file.exists("DB_clan_members.csv")) {
  clan_members <- read_csv("DB_clan_members.csv")
}

files <- 
  files_all[grepl("H,clans_info.+\\.RObj", files_all) & ! files_all %in% files_parsed]

if(length(files) > 0) for(file in files) {
  load(file)
  
  tmp_clan_members <- 
    clans_info %>% 
    select(account_id, clan_id, joined_at, timestamp) %>% 
    mutate_all(as.integer)
  
  if(exists("clan_members")) {
    clan_members <- 
      tmp_clan_members %>% 
      bind_rows(clan_members) %>% 
      group_by(account_id, clan_id, joined_at) %>% 
      summarise(timestamp = max(timestamp))
  } else {
    clan_members <- 
      tmp_clan_members
  }
  
  write_csv(clan_members, "DB_clan_members.csv")
  
  files_parsed <- 
    c(files_parsed, file) %>% sort() %>% unique()
  write_lines(files_parsed, "DB_files_parsed.lines")
}

rm(list = ls()[
  ls() %in% c("file", "files", "clans_info", "tmp_clan_members")
  ])

# 
# Разбор и размещение статистики промресурса -----
# 

if(file.exists("DB_resources.csv")) {
  resources <- read_csv("DB_resources.csv")
}

files <- 
  files_all[grepl("H,stronghold_accountstats.+\\.RObj", files_all) & ! files_all %in% files_parsed]

if(length(files) > 0) for(file in files) {
  load(file)
  
  tmp_resources <- 
    stronghold_accountstats %>% 
    lapply(function(accounts_group) {
      if(accounts_group$status == "ok") {
        tmp_account_id <- names(accounts_group$data) %>% as.integer()
        tmp_timestamp <- accounts_group$timestamp %>% as.integer()
        lapply(tmp_account_id, function(by_account){
          tmp_by_account <- accounts_group[[c("data", by_account)]]
          if(! is.null(tmp_by_account)) {
            as_data_frame(
              tmp_by_account
            ) %>% 
              mutate(account_id = by_account, timestamp = tmp_timestamp)
          } else {
            data_frame(clan_id = NA, total_resources_earned = NA) %>% 
              mutate(account_id = by_account, timestamp = tmp_timestamp)
          }
        }) %>% bind_rows()
      }
    }) %>% bind_rows()
  
  if(exists("resources")) {
    resources <- 
      bind_rows(resources, tmp_resources) %>% 
      arrange(timestamp) %>% 
      group_by(account_id) %>% 
      mutate(prev_earned = dplyr::lag(total_resources_earned, default = -1)) %>% 
      filter(prev_earned != total_resources_earned) %>% 
      select(- prev_earned)
  } else {
    resources <- tmp_resources
  }
  
  write_csv(resources, "DB_resources.csv")
  
  files_parsed <- 
    c(files_parsed, file) %>% sort() %>% unique()
  write_lines(files_parsed, "DB_files_parsed.lines")
}

rm(list = ls()[
  ls() %in% c("file", "files", "stronghold_accountstats", "tmp_resources")
  ])

# 
# Разбор и размещение статистики аккаунтов в целом -----
# 

if(file.exists("DB_accounts.csv")) {
  accounts <- read_csv("DB_accounts.csv")
}

if(file.exists("DB_accounts_stat.csv")) {
  accounts_stat <- 
    read_csv(
      "DB_accounts_stat.csv", 
      col_types = cols(
        .default = col_number(),
        account_id = col_integer(),
        b_type = col_character()
      )
    )
}

files <- 
  files_all[grepl("H,account_info.+\\.RObj", files_all) & ! files_all %in% files_parsed]

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
          created_at = ifelse(
            is.null(account$created_at), NA, account$created_at
          ),
          nickname = ifelse(
            is.null(account$nickname), NA, account$nickname
          ),
          timestamp = tmp_timestamp
        )
      }) %>% 
        bind_rows()
    }) %>% 
    bind_rows() %>% 
    mutate_if(is.numeric, as.integer)
  
  if(exists("accounts")) {
    accounts <- 
      bind_rows(accounts, tmp_accounts) %>% 
      arrange(timestamp) %>% 
      group_by(account_id, last_battle_time) %>% 
      summarise_all(last)
  } else {
    accounts <- tmp_accounts
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
  
  if(exists("accounts_stat")) {
    accounts_stat <- 
      bind_rows(accounts_stat, tmp_accounts_stat) %>% 
      arrange(timestamp) %>% 
      group_by(account_id, b_type) %>% 
      mutate(prev_battles = dplyr::lag(battles, default = -1)) %>% 
      filter(prev_battles != battles) %>% 
      select(- prev_battles)
  } else {
    accounts_stat <- tmp_accounts_stat
  }
  
  write_csv(accounts, "DB_accounts.csv")
  write_csv(
    accounts_stat %>% 
      mutate_at(vars(-contains("b_type"), -contains("avg_")), as.integer),
    "DB_accounts_stat.csv"
  )
  
  files_parsed <-
    c(files_parsed, file) %>% sort() %>% unique()
  write_lines(files_parsed, "DB_files_parsed.lines")
}

rm(list = ls()[
  ls() %in% c("file", "files", "account_info", "tmp_accounts", "tmp_accounts_stat")
  ])

# 
# Разбор и размещение статистики аккаунтов по танкам -----
# 

if(file.exists("DB_tanks_stat.csv")) {
  tanks_stat <- 
    read_csv(
      "DB_tanks_stat.csv", 
      col_types = cols(
        .default = col_number(),
        account_id = col_integer(),
        tank_id = col_integer(),
        b_type = col_character()
      )
    )
}

files <- 
  files_all[grepl("H,tanks_stats.+\\.RObj", files_all) & ! files_all %in% files_parsed]

# Извлекаем статистику для каждого файла:
if(length(files) > 0) for(file in files) {
  load(file)
  # Извлекаем статистику для каждого аккаунта:
  tmp_tanks_stat <- 
    tanks_stats %>% 
    lapply(function(tanks_stats_by_account) {
      if(tanks_stats_by_account$status == "ok") {
        tmp_account_id <- names(tanks_stats_by_account$data) %>% as.integer()
        tmp_timestamp <- tanks_stats_by_account$timestamp
        # Извлекаем статистику для каждого танка:
        tanks_stats_by_account$data[[1]] %>% 
          lapply(function(tanks_stats_by_tank) {
            tmp_tank_id <- tanks_stats_by_tank$tank_id
            # Извлекаем статистику для каждого типа боя:
            ref_stat_fields$type[ref_stat_fields$type %in% names(tanks_stats_by_tank)] %>%
              lapply(function(battle_type) {
                # Извлекаем значение каждого собираемого показателя:
                ref_stat_fields$name %>% 
                  lapply(function(variable_name){
                    tmp_value <- tanks_stats_by_tank[[c(battle_type, variable_name)]]
                    list(
                      account_id = tmp_account_id,
                      timestamp = tmp_timestamp,
                      tank_id = tmp_tank_id,
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
      }
    }) %>% 
    bind_rows() %>% 
    filter(! is.na(value)) %>% 
    spread(variable, value)
  
  if(exists("tanks_stat")) {
    tanks_stat <- 
      bind_rows(tanks_stat, tmp_tanks_stat) %>% 
      arrange(timestamp) %>% 
      group_by(account_id, tank_id, b_type) %>% 
      mutate(prev_battles = dplyr::lag(battles, default = -1)) %>% 
      filter(prev_battles != battles) %>% 
      select(- prev_battles)
  } else {
    tanks_stat <- tmp_tanks_stat
  }
  
  write_csv(
    tanks_stat %>% 
      mutate_at(-contains("b_type"), vars(-contains("avg_")), as.integer),
    "DB_tanks_stat.csv"
  )
  
  files_parsed <- 
    c(files_parsed, file) %>% sort() %>% unique()
  write_lines(files_parsed, "DB_files_parsed.lines")
}

rm(list = ls()[
  ls() %in% c("file", "files", "tanks_stats", "tmp_tanks_stat")
  ])
