library(tidyverse)
library(cwi)

acs_year <- 2019
params <- list(
  bridgeport = list(region = "Fairfield County", town = "Bridgeport"),
  hartford = list(region = "Greater Hartford", town = c("Hartford", "West Hartford")),
  stamford = list(region = "Fairfield County", town = "Stamford"),
  new_haven = list(region = "Greater New Haven", town = "New Haven")
)


# same as last time, indicators t = table, m = mappable (percents)
cdc_meta <- read_csv("utils/cdc_indicators.txt") %>%
  mutate(topic = fct_recode(topic, life_expectancy = "expectancy")) %>%
  select(topic, indicator, display, long_display = new_display)
acs_meta <- read_csv("utils/indicator_headings.txt")
meta <- bind_rows(acs_meta, cdc_meta) %>%
  mutate(indicator = str_replace(indicator, " ", "X")) %>%
  select(-long_display)

acs_file <- file.path("input_data", "acs_to_prep_for_viz_2019.rds")
if (!file.exists(acs_file)) {
  download.file("https://github.com/CT-Data-Haven/2019acs/raw/main/output_data/acs_to_prep_for_viz_2019.rds", 
                destfile = acs_file)
}
cdc_file <- file.path("input_data", "cdc_health_all_lvls_nhood_2020.rds")
if (!file.exists(cdc_file)) {
  download.file("https://github.com/CT-Data-Haven/cdc_aggs20/raw/main/output_data/cdc_health_all_lvls_nhood_2020.rds",
                destfile = cdc_file)
}

acs_by_city <- readRDS(acs_file) %>%
  map(mutate, year = as.character(acs_year)) %>%
  map(pivot_longer, estimate:share, names_to = "type", values_drop_na = TRUE) %>%
  map(unite, indicator, type, group, sep = "X") %>%
  map(left_join, meta, by = c("topic", "indicator"))

cdc <- readRDS(cdc_file) %>%
  mutate(type = "share",
         topic = fct_recode(topic, outcomes = "HLTHOUT", prevention = "PREVENT", behaviors = "UNHBEH")) %>%
  arrange(topic, level) %>%
  left_join(meta, by = c("topic", "question" = "display")) %>%
  mutate(topic = fct_recode(topic, health_outcomes = "outcomes", unhealthy_behaviors = "behaviors")) %>%
  unite(indicator, type, indicator, sep = "X") %>%
  rename(display = question)

cdc_by_city <- params %>%
  imap(function(p, cty) {
    fltr <- c(unlist(p), "Connecticut")
    cdc %>%
      filter(name %in% fltr | city == camiller::clean_titles(cty, cap_all = TRUE))
  }) 

out_by_city <- list(acs_by_city, cdc_by_city) %>%
  map_depth(2, select, -city) %>%
  transpose() %>%
  map(bind_rows) %>%
  map(janitor::remove_empty, "cols")

out_by_city %>%
  iwalk(function(df, city) {
    df %>%
      distinct(name, display, year, .keep_all = TRUE) %>%
      pivot_wider(id_cols = c(level, name), names_from = c(display, year)) %>%
      write_csv(file.path("to_distro", str_glue("{city}_nhood_{acs_year}_acs_health_comb.csv")))
  })

saveRDS(out_by_city, file.path("output_data", str_glue("all_nhood_{acs_year}_acs_health_comb.rds")))
