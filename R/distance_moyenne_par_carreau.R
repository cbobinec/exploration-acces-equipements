library(dplyr)
library(duckdb)
library(sf)
library(geojsonsf)
library(classInt)
library(gridy)

# URL data.gouv pour travail à la volée
url <- "https://static.data.gouv.fr/resources/donnees-sur-la-localisation-et-lacces-de-la-population-aux-equipements/20250715-113610/donnees-2024-reg52.parquet"
output <- "html/moyenne_par_carreau/moyenne_par_carreau.geojson"

# Conf duckdb -------------------------------------------------------------
conn <- DBI::dbConnect(drv = duckdb())
# pour lecture à la volée : pas nécessaire?
# DBI::dbExecute(conn, "LOAD httpfs;")
data <- conn %>% 
  tbl(paste0("read_parquet('",url,"')"))

# Calcul temps moyen par commune --------------------------------
moyenne_par_carreau <- data %>%
  filter(depcom %in% c("44109", "44143", "44020", "44190", "44009", "44215", "44071", "44198")) %>%
  filter(typeeq_id == "F307") %>%
  select(pop, distance, idSrc) %>%
  group_by(idSrc) %>%
  summarise(
    distance_moyenne = sum(distance * pop, na.rm = TRUE) / sum(pop, na.rm = TRUE)
  ) %>%
  collect()

# Nettoyage --------------------------------
DBI::dbDisconnect(conn, shutdown = TRUE)

# Création de la géométrie des carreaux ---------------------------
# on recrée l'identifiant inspire plus standard
# et pour exploiter gridy pour générer la grille de carreau
moyenne_par_carreau_geo <- moyenne_par_carreau %>% 
  tidyr::separate(idSrc, into = c("id_E", "id_N"), sep = "_") %>% 
  mutate(id_inspire = glue::glue("CRS3035RES200mN{id_N}E{id_E}")) %>% 
  select(-id_E, -id_N) %>% 
  # plus lisible en mètres?
  mutate(distance_moyenne = as.integer(distance_moyenne * 1000))

grille <- sf::st_sf(
  moyenne_par_carreau_geo,
  geometry = sf::st_sfc(lapply(moyenne_par_carreau_geo[["id_inspire"]], gridy::make_contour_car)),
  crs= st_crs(3035)
) %>% 
  st_transform(crs = 4326)

write(sf_geojson(grille), output)

# Calcul de seuils ---------------------------------------------
seuils <- classIntervals(
  grille$distance_moyenne, 
  n=5, 
  style="jenks"
)

message("Seuils Jenks calculés :")
seuils
