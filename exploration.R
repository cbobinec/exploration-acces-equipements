library(dplyr)
library(duckdb)

# URL data.gouv pour travail à la volée
url <- "https://static.data.gouv.fr/resources/donnees-sur-la-localisation-et-lacces-de-la-population-aux-equipements/20250715-113610/donnees-2024-reg52.parquet"

# Récupération des données utiles filtrées --------------------------------
conn <- DBI::dbConnect(drv = duckdb())
# pour lecture à la volée
DBI::dbExecute(conn, "LOAD httpfs;")

data <- conn %>% 
  tbl(paste0("read_parquet('",url,"')"))

nantes <- data %>% 
  filter(depcom == "44109") %>%
  collect()

DBI::dbDisconnect(conn, shutdown = TRUE)

