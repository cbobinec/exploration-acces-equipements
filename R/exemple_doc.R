#la base est au format parquet, on utilise le package duckdb pour l'exploiter

library(dplyr)
library(duckdb)
library(DBI)

con_duck <- DBI::dbConnect(
  drv = duckdb(), 
  dbdir = "myduckdb4.db", 
  config = list(
    memory_limit = "30G",
    threads = "4")
)


chemin <- 'chemin_base/donnees_acces_equipements_2023' 

base <- tbl(con_duck, paste0("read_parquet('", chemin, "/**/*.parquet')"))

#recuperer les donnees pour une commune

aubagne <- base %>% filter(depcom == "13005") %>% collect()

#calcul du temps moyen sur un équipement et les communes d'un département


base_filtree <- base %>% filter(typeeq_id == "B201") %>% filter(dep == "13")

tps_moy <-
  base_filtree %>%
  select(pop, duree, depcom) %>%
  group_by(depcom) %>%
  summarise(duree_moyenne = sum(duree * pop, na.rm = TRUE) / sum(pop, na.rm = TRUE)) %>%
  collect()

#calcul du temps moyen sur 2 équipements regroupés

base_modifiee <-
  base %>%
  filter(typeeq_id %in% c("A101", "A104")) %>%
  group_by(idSrc, depcom, iris) %>%
  slice_min(duree, n = 1, with_ties = FALSE) %>%
  ungroup()

tps_moy <-
  base_modifiee %>%
  select(pop, duree, depcom) %>%
  group_by(depcom) %>%
  summarise(duree_moyenne = sum(duree * pop, na.rm = TRUE) / sum(pop, na.rm = TRUE)) %>%
  collect()



