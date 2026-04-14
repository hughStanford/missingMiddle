library(pacman)

p_load(tidyverse, janitor, arrow, glmmTMB)

# 1. load data and helper content ---------------------------------------------------

infile <- "[insert filepath]"
outfile <- "[insert filepath]"

df <- read_parquet(
  file.path(outfile,
            "missingMiddle",
            "modelVariables",
            "combined_df.parquet")
)

# 2. standardize variables ------------------------------------------------

vars_to_standardize <- c(
  "seifa_disadv",
  "born_overseas_pct",
  "hhold_w_dep_pct",
  "area_sqm",
  "lot_shape_index",
  "density_index",
  "slope_sd",
  "median_price_house_3br",
  "dist_from_cbd_m",
  "vacant_land_density_kde",
  "composite_index_average"
)

df[vars_to_standardize] <- lapply(df[vars_to_standardize], scale)

# 2. build models for testing ---------------------------------------------

build_models <- function(city){
  
  valid_cities <- c("all_cities", "melbourne", "sydney", "brisbane", "adelaide", "perth")
  if(!city %in% valid_cities){
    stop('Incorrept city input. City needs to be "all_cities", or "melbourne", "sydney", "brisbane", "adelaide", or "perth".')
  }
  
  if(city == "all_cities"){
    df_city <- df
    
    m <- glmmTMB(is_med_density ~ seifa_disadv
                 + area_sqm
                 + lot_shape_index
                 + density_index
                 + median_price_house_3br
                 + vacant_land_density_kde
                 + dist_from_cbd_m
                 + composite_index_average
                 + slope_sd
                 + born_overseas_pct
                 + hhold_w_dep_pct
                 + (1|lga_name25)
                 + (1|city_name),
                 data = df_city,
                 family = binomial(link = "logit"))
    
  } else {
    df_city <- df %>% 
      filter(city_name ==  city)
    
    m <- glmmTMB(is_med_density ~ seifa_disadv
                 + area_sqm
                 + lot_shape_index
                 + density_index
                 + median_price_house_3br
                 + vacant_land_density_kde
                 + dist_from_cbd_m
                 + composite_index_average
                 + slope_sd
                 + born_overseas_pct
                 + hhold_w_dep_pct
                 + (1|lga_name25),
                 data = df_city,
                 family = binomial(link = "logit"))
  }
  
  saveRDS(m, file = file.path(outfile,
                              "missingMiddle",
                              sprintf("%s_model_for_analysis.rds",
                                      city)
  ))
  
  summary(m)
  
}

# 3. Run model for all cities ---------------------------------------------

cities <- c(
  "all_cities" # either " all_cities" or each cities individually: "melbourne", "sydney", "brisbane", "adelaide", "perth"
)

lapply(cities, build_models)
