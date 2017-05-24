# 
# Перенос в архивы разобранных файлов данных
# 

# !!! Всегда выполнять построчно, вручную !!!

library(tidyverse)
library(stringr)
library(lubridate)

files_all <- list.files(
  path = "Data",
  full.names = TRUE
)

if(file.exists("DB_files_parsed.lines")) {
  files_parsed <- read_lines("DB_files_parsed.lines")
} else {
  files_parsed <- NULL
}

files_archive <- 
  data_frame(
    fullname = files_all[files_all %in% files_parsed],
    date = str_extract(fullname, "[0-9]{4}-[0-9]{2}-[0-9]{2}") %>%
      as.Date(),
    period = strftime(date, "%Y-%m")
  ) %>% 
  group_by(period) %>% 
  summarise(fullnames = paste(fullname, collapse = " ")) %>% 
  mutate(
    archive = paste0(
      "7za u Archive/WoTstat_Data,", period, ".7z ", fullnames
    ),
    remove = paste0(
      "rm ", fullnames
    )
  )

# Распределяем разобранные файлы по архивам:
for(cmd in files_archive$archive) system(cmd)

# !!! Если всё хорошо, удаляем заархивированные файлы !!!:
for(cmd in files_archive$remove) system(cmd)
