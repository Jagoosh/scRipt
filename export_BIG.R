library(tidyverse)

accounts <- 
  read_csv("BIG_accounts.csv") %>% 
  # na.omit() %>%
  group_by(account_id, last_battle_time) %>%
  top_n(1, timestamp) %>% 
  ungroup()

accounts_stat <- 
  read_csv(
    "BIG_accounts_stat.csv", 
    col_types = cols(
      .default = col_number(),
      account_id = col_integer(),
      b_type = col_character()
    )
  )


accounts %>% 
  arrange(last_battle_time) %>% 
  group_by(account_id) %>% 
  mutate(next_battle_time = lead(last_battle_time %>% as.numeric(), default = Inf))


accounts_data <- 
  inner_join(
    accounts %>% 
      arrange(last_battle_time) %>% 
      group_by(account_id) %>% 
      mutate(next_battle_time = lead(last_battle_time %>% as.numeric(), default = Inf)) %>% 
      select(-timestamp) %>% 
      ungroup(),
    accounts_stat,
    by = "account_id"
  ) %>% 
  group_by(account_id, b_type, last_battle_time) %>% 
  filter(timestamp >= last_battle_time, timestamp < next_battle_time) %>% 
  top_n(1, timestamp) %>% 
  ungroup() %>% 
  select(-next_battle_time, -timestamp, -b_type)

accounts_data %>% 
  mutate(
    last_battle_time = last_battle_time %>% as.POSIXct(origin = "1970-01-01", tz = "UTC")
  ) %>% 
  arrange(account_id) %>% 
  write_csv("BIG_accounts_data.csv")
