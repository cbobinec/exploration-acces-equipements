library(dplyr)
library(duckdb)
library(sf)
library(geojsonsf)

# URL data.gouv pour travail à la volée
url <- "https://static.data.gouv.fr/resources/donnees-sur-la-localisation-et-lacces-de-la-population-aux-equipements/20250715-113610/donnees-2024-reg52.parquet"
fond <- "input/commune_reg_52_2025.gpkg"
output <- "html/duree_moyenne_par_commune.geojson"

# Conf duckdb -------------------------------------------------------------
conn <- DBI::dbConnect(drv = duckdb())
# pour lecture à la volée : pas nécessaire?
# DBI::dbExecute(conn, "LOAD httpfs;")
data <- conn %>% 
  tbl(paste0("read_parquet('",url,"')"))

# Calcul temps moyen par commune --------------------------------
duree_moyenne_par_commune <- data %>%
  filter(typeeq_id == "D265") %>%
  select(pop, duree, depcom) %>%
  group_by(depcom) %>%
  summarise(
    duree_moyenne = sum(duree * pop, na.rm = TRUE) / sum(pop, na.rm = TRUE)
  ) %>%
  collect()

# Nettoyage --------------------------------
DBI::dbDisconnect(conn, shutdown = TRUE)

# Fusion avec le fond commune ---------------------------------------------
duree_moyenne_par_commune_geo <- st_read("input/commune_reg_52_2025.gpkg") %>% 
  left_join(
    y = duree_moyenne_par_commune,
    by = join_by(code == depcom)
  ) %>% 
  select(code, libelle, duree_moyenne)

write(sf_geojson(duree_moyenne_par_commune_geo), output)
