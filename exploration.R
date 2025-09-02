library(dplyr)
library(duckdb)

# URL data.gouv pour travail à la volée
url <- "https://static.data.gouv.fr/resources/donnees-sur-la-localisation-et-lacces-de-la-population-aux-equipements/20250715-113610/donnees-2024-reg52.parquet"

# Conf duckdb -------------------------------------------------------------
conn <- DBI::dbConnect(drv = duckdb())
# pour lecture à la volée
DBI::dbExecute(conn, "LOAD httpfs;")
data <- conn %>% 
  tbl(paste0("read_parquet('",url,"')"))

# Récupération des données par carreau sur une commune --------------------------------
nantes <- data %>% 
  filter(depcom == "44109") %>%
  # medecin
  filter(typeeq_id == "D265") %>%
  collect()

# Calcul temps moyen par commune --------------------------------
tps_moy_44 <- data %>%
  filter(dep == "44") %>%
  filter(typeeq_id == "D265") %>%
  select(pop, duree, depcom) %>%
  group_by(depcom) %>%
  summarise(duree_moyenne = sum(duree * pop, na.rm = TRUE) / sum(pop, na.rm = TRUE)) %>%
  collect()

# Nettoyage --------------------------------
DBI::dbDisconnect(conn, shutdown = TRUE)
