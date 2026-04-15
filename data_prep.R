library(pacman)

p_load(tidyverse, sf, janitor, readxl, terra, arrow, pbapply, spatstat)

# 1. Helper content ---------------------------------------------------

infile <- "[insert filepath]"
outfile <- "[insert filepath]"

# infile <- "D:/University/R/ARC_hotCities/raw_data"
# outfile <- "D:/University/R/ARC_hotCities/processed_data"

cities <- c(
  "melbourne",
  "sydney",
  "brisbane",
  "adelaide",
  "perth"
)

read_and_clean <- function(filepath, crs) { 
  st_read(filepath) %>% 
    st_transform(crs) %>% 
    clean_names(case = "snake")
}

read_and_clean_parquet <- function(filepath, crs){ 
  read_parquet(filepath) %>% 
    mutate(geometry = st_as_sfc(geometry, crs = crs)) %>%
    st_as_sf() %>%
    clean_names(case = "snake")
}

read_and_clean_csv <- function(filepath, crs) { 
  read.csv(filepath) %>%
    clean_names(case = "snake")
}

get_city_crs <- function(city_name) {
  case_when(
    city_name == "melbourne" ~ 28355,
    city_name %in% c("sydney", "brisbane") ~ 28356,
    city_name == "adelaide" ~ 28354,
    city_name == "perth" ~ 28350
  )
}

# 2. Load and prep data ------------------------------------------------------------
  
## 2.1 geographies -------------------------------------------------------------

ucl_raw <- read_and_clean(
  file.path(infile,
            "abs_geographies",
            "UCL_2021_AUST_GDA2020",
            "UCL_2021_AUST_GDA2020.shp"
  ),
  4326) %>% 
  filter(ucl_name21 %in% c("Perth (WA)", "Melbourne", "Adelaide", "Sydney", "Brisbane"))

sa2_raw <- read_and_clean(
  file.path(infile,
            "abs_geographies",
            "SA2_2021_AUST_SHP_GDA2020",
            "SA2_2021_AUST_GDA2020.shp"
  ),
  4326)

sa1_raw <- read_and_clean(
  file.path(infile,
            "abs_geographies",
            "SA1_2021_AUST_SHP_GDA2020",
            "SA1_2021_AUST_GDA2020.shp"
  ),
  4326) %>% 
  filter(!str_detect(sa1_code21, "Z"))

lga_raw <- read_and_clean(
  file.path(infile,
            "abs_geographies",
            "LGA_2025_AUST_GDA2020",
            "LGA_2025_AUST_GDA2020.shp"
  ),
  4326)

mb_raw <- read_and_clean(
  file.path(infile,
            "abs_geographies",
            "MB_2021_AUST_SHP_GDA2020",
            "MB_2021_AUST_GDA2020.shp"
  ),
  4326)

open_space_raw <- read_and_clean(
  file.path(infile,
            "OSM_Australia_GeoFabrik_24022026",
            "gis_osm_landuse_a_free_1.shp"),
  4326) %>% 
  filter(fclass %in% c("park", "recreation_ground", "nature_reserve", "forest"))

waterways_raw <- read_and_clean(
  file.path(infile,
            "OSM_Australia_GeoFabrik_24022026",
            "gis_osm_waterways_free_1.shp"),
  4326)

water_raw <- read_and_clean(
  file.path(infile,
            "OSM_Australia_GeoFabrik_24022026",
            "gis_osm_water_a_free_1.shp"),
  4326)

railways_raw <- read_and_clean(
  file.path(infile,
            "OSM_Australia_GeoFabrik_24022026",
            "gis_osm_railways_free_1.shp"),
  4326) %>% 
  filter(fclass %in% c("rail", "light_rail", "tram"))

## 2.2 ABS derived data ----------------------------------------------------

### 2.2.1 SEIFA data --------------------------------------------------------------


seifa <- read_excel(
  file.path(
    infile,
    "ABS_SEIFA",
    "Statistical Area Level 1, Indexes, SEIFA 2021.xlsx"
  ),
  sheet = "Table 1",
  skip = 5
) %>% 
  rename(
    "sa1_code21" = 1,
    "seifa_disadv" = 2,
    "seifa_adv_disadv" = 4
  ) %>% 
  mutate(
    seifa_disadv = as.numeric(seifa_disadv),
    seifa_adv_disadv = as.numeric(seifa_adv_disadv),
    sa1_code21 = as.numeric(sa1_code21)
  ) %>% 
  dplyr::select(
    sa1_code21, seifa_disadv, seifa_adv_disadv
  )


### 2.2.2 Census data -------------------------------------------------------

# born overseas data

born_raw_1 <- read.csv(
  file.path(
    infile,
    "ABS_census_2021_SA1",
    "2021 Census GCP Statistical Area 1 for AUS",
    "2021Census_G09F_AUST_SA1.csv"
  )
) %>% 
  clean_names(case = "snake") %>% 
  rename(sa1_code21 = sa1_code_2021)

born_raw_2 <- read.csv(
  file.path(
    infile,
    "ABS_census_2021_SA1",
    "2021 Census GCP Statistical Area 1 for AUS",
    "2021Census_G09H_AUST_SA1.csv"
  )
) %>% 
  clean_names(case = "snake") %>% 
  rename(sa1_code21 = sa1_code_2021)

born_combined <- left_join(born_raw_1, born_raw_2, by = "sa1_code21")

born_overseas <- born_combined %>%
  mutate(born_overseas_tot = p_tot_tot - p_cob_ns_tot - p_australia_tot,
         born_overseas_pct = born_overseas_tot/(p_tot_tot - p_cob_ns_tot),
         sa1_code21 = as.numeric(sa1_code21)
  ) %>% 
  filter(p_tot_tot != 0,
         born_overseas_tot >= 0) %>% 
  dplyr::select(sa1_code21, born_overseas_pct)

# households with dependents

hholds_w_dep <- read.csv(
  file.path(
    infile,
    "ABS_census_2021_SA1",
    "2021 Census GCP Statistical Area 1 for AUS",
    "2021Census_G35_AUST_SA1.csv"
  )
) %>% 
  clean_names(case = "snake") %>% 
  rename(sa1_code21 = sa1_code_2021) %>% 
  filter(total_total != 0) %>% 
  mutate(hhold_w_dep_pct = total_fam_hhold/total_total,
         sa1_code21 = as.numeric(sa1_code21)) %>% 
  dplyr::select(sa1_code21, hhold_w_dep_pct)

### 2.2.3 Combining ABS data --------------------------------------------------------

sa1 <- sa1_raw %>%
  st_join(ucl_raw, join = st_intersects) %>%
  filter(!is.na(ucl_name21)) %>% 
  mutate(sa1_code21 = as.numeric(sa1_code21)) %>% 
  st_drop_geometry() %>% 
  dplyr::select(ucl_name21, sa1_code21)

abs_stats_sa1 <- sa1 %>% 
  left_join(seifa, by = "sa1_code21") %>% 
  left_join(born_overseas, by = "sa1_code21") %>% 
  left_join(hholds_w_dep, by = "sa1_code21")

## 2.3 Lot derived data ----------------------------------------------------

# establish principal function

calculate_lot_statistics <- function(city, abs_geography){
  
  crs <- get_city_crs(city)
  
  dev_raw <- read_and_clean(
    file.path(infile,
              "model_outputs",
              city,
              "development_summary",
              sprintf("%s_complete-dev_2023_2019_summary_nonres_mbcat_corrected.gpkg",
                      city)),
    crs) %>% 
    filter(complete.cases(x1st_pred_prob_23)) %>% 
    rename(geometry = geom) %>% 
    mutate(id = paste0(city, "_", row_number()))
  
  dev <- dev_raw %>% 
    mutate(
      area_sqm = as.numeric(st_area(geometry)),
      perim_m = as.numeric(st_perimeter(geometry)),
      area_perim_ratio = as.numeric(area_sqm/perim_m),
      # convexity_ratio controls for lots with jagged edges
      convexity_ratio = as.numeric(area_sqm/ st_area(st_convex_hull(geometry))),
      # rectangularity controls for non-rectangular shapes (i.e. triangles)
      rectangularity_index = as.numeric(area_sqm / st_area(st_minimum_rotated_rectangle(geometry))),
      lot_shape_index = convexity_ratio * rectangularity_index,
      city_name = city
    )
  
  centroids <- st_centroid(dev)
  
  city_sa1 <- abs_geography %>% 
    st_transform(crs) %>% 
    mutate(sa1_code21 = as.numeric(sa1_code21)) %>% 
    dplyr::select(sa1_code21, geometry)
  
  centroids_w_sa1 <- st_join(centroids, city_sa1, join = st_within, left = TRUE) %>%
    st_drop_geometry() %>% 
    dplyr::select(sa1_code21)
  
  if(sum(is.na(centroids_w_sa1$sa1_code21)) > 0){
    warning(sprintf("City: %s — %d centroids did not intersect any SA1 polygons or was on boundary and will have NA for sa1.",
                    city, sum(is.na(centroids_w_sa1$sa1_code21))))
  }
  
  dev <- dev %>%
    bind_cols(centroids_w_sa1) %>% 
    st_drop_geometry() %>% 
    dplyr::select(id, area_sqm, lot_shape_index, city_name, mb_code21)
  
  return(dev)
  
}

# run function to calculate lot statistics

lot_stats <- lapply(cities, function(city){
  calculate_lot_statistics(city, sa1_raw)
}) %>% 
  do.call(bind_rows, .)


## 2.4 Building type density ---------------------------------------------------

# establish principal function

calculate_dwelling_type_pct <- function(city, abs_geography){
  
  crs <- get_city_crs(city)
  
  geog <- abs_geography %>% 
    st_transform(crs) %>% 
    dplyr::select(geometry, mb_code21)
  
  stock <- read_and_clean_parquet(
    file.path(infile,
              "model_outputs",
              city,
              ifelse(city == "perth",
                     "perth_2019_prediction_20250506_155428_TabularSwinSLoraModel.parquet",
                     sprintf("%s_2019_prediction_20250605_215138_TabularSwinSLoraModel.parquet",
                             city))),
    crs) %>% 
    mutate(stock_type = case_when(first_predicted_class == "cluster" ~ "medium_density",
                                  first_predicted_class == "multi-unit-occupancy" ~ "medium_density",
                                  first_predicted_class == "low-rise-apartments" ~ "medium_density",
                                  TRUE ~ first_predicted_class),
           stock_subtype = case_when(first_predicted_class == "cluster" & gnaf_count <= 4 ~ "lower_medium_density",
                                     first_predicted_class == "multi-unit-occupancy" & gnaf_count <= 4 ~ "lower_medium_density",
                                     first_predicted_class == "cluster" & gnaf_count > 4 ~ "upper_medium_density",
                                     first_predicted_class == "multi-unit-occupancy" & gnaf_count > 4 ~ "upper_medium_density",
                                     TRUE ~ first_predicted_class)) %>% 
    filter(!is.na(stock_type)) %>% 
    dplyr::select(!mb_code21)
  
  # re-establish mb_code21 values through spatial match
  stock_centroid <- st_centroid(stock)
  stock_spatial_match <- st_join(stock_centroid, geog, join = st_within, left = TRUE) %>%
    st_drop_geometry() %>% 
    mutate(mb_code21 = as.numeric(mb_code21)) %>% 
    dplyr::select(pin_2019, mb_code21)
  stock <- stock %>% 
    left_join(stock_spatial_match, by = "pin_2019")
  
  total_dwelling <- stock %>% 
    st_drop_geometry() %>% 
    group_by(mb_code21) %>% 
    summarise(total_dwelling_count = sum(stock_type %in% c("single-house",
                                                           "medium_density",
                                                           "high-rise-apartments",
                                                           "rural"), na.rm = TRUE)
    ) %>%
    ungroup()
  
  # calculate pct of dwellings made up by each type/sub-type
  
  single_house <- stock %>% 
    st_drop_geometry() %>% 
    group_by(mb_code21) %>% 
    summarise(single_house_count = sum(stock_type == "single-house", na.rm = TRUE)) %>% 
    ungroup() %>% 
    left_join(total_dwelling, by = "mb_code21") %>% 
    filter(total_dwelling_count != 0) %>% 
    mutate(single_house_pct = single_house_count/total_dwelling_count) %>% 
    dplyr::select(!total_dwelling_count)
  
  medium_density <- stock %>% 
    st_drop_geometry() %>% 
    group_by(mb_code21) %>% 
    summarise(medium_density_count = sum(stock_type == "medium_density", na.rm = TRUE)) %>% 
    ungroup() %>% 
    left_join(total_dwelling, by = "mb_code21") %>% 
    filter(total_dwelling_count != 0) %>% 
    mutate(medium_density_pct = medium_density_count/total_dwelling_count) %>% 
    dplyr::select(!total_dwelling_count)
  
  high_rise_apartments <- stock %>% 
    st_drop_geometry() %>% 
    group_by(mb_code21) %>% 
    summarise(high_rise_apartments_count = sum(stock_type == "high-rise-apartments", na.rm = TRUE)) %>% 
    ungroup() %>% 
    left_join(total_dwelling, by = "mb_code21") %>% 
    filter(total_dwelling_count != 0) %>% 
    mutate(high_rise_apartments_pct = high_rise_apartments_count/total_dwelling_count) %>% 
    dplyr::select(!total_dwelling_count)
  
  rural <- stock %>% 
    st_drop_geometry() %>% 
    group_by(mb_code21) %>% 
    summarise(rural_count = sum(stock_type == "rural", na.rm = TRUE)) %>% 
    ungroup() %>% 
    left_join(total_dwelling, by = "mb_code21") %>% 
    filter(total_dwelling_count != 0) %>% 
    mutate(rural_pct = rural_count/total_dwelling_count) %>% 
    dplyr::select(!total_dwelling_count)
  
  # combine individual dfs
  # weights: rural = 0 (very low), single_house = 1 (low),
  #          medium_density = 2 (medium), high_rise_apartments = 3 (high)
  
  dwelling_type_pct <- left_join(single_house, medium_density, by = "mb_code21") %>% 
    left_join(high_rise_apartments, by = "mb_code21") %>% 
    left_join(rural, by = "mb_code21") %>%
    mutate(
      city = city,
      density_index = (0 * rural_pct) +
        (1 * single_house_pct) +
        (2 * medium_density_pct) +
        (3 * high_rise_apartments_pct)
    ) %>% 
    dplyr::select(mb_code21,
                  density_index,
                  city)
  
  # quality control check
  
  pct_sums <- round(
    single_house$single_house_pct +
      medium_density$medium_density_pct +
      high_rise_apartments$high_rise_apartments_pct +
      rural$rural_pct, 5)
  
  if(any(pct_sums != 1)){
    stop(sprintf("Sum of percentages of dwelling types in %s does not equal 1 for %d mesh blocks. Confirm calculation is working as intended",
                 city, sum(pct_sums != 1)))
  }
  
  return(dwelling_type_pct)
}

# run function to calculate building type density

dwelling_type_stats <- pblapply(cities, function(city){
  calculate_dwelling_type_pct(city, mb_raw)
}) %>% 
  do.call(bind_rows, .)


## 2.5 Slope regularity ----------------------------------------------------

# establish principal function

get_topography_stats <- function(city){
  
  crs <- get_city_crs(city)
  
  # load unprocessed DEM
  
  dem <- rast(
    file.path(
      outfile,
      "inputData",
      "elevation_by_ucl",
      sprintf("%s_DEM_1sec_elevation_fullCity.tif",
              city))
  ) %>% 
    project(paste0("EPSG:", crs))
  
  # load city boundary
  
  ucl <- ucl_raw %>% 
    filter(ucl_name21 == ifelse(city == "perth",
                                "Perth (WA)",
                                str_to_title(city))) %>%
    dplyr::select(geometry, ucl_name21)
  
  city_sa1 <- st_join(sa1_raw, ucl, join = st_intersects) %>%
    filter(!is.na(ucl_name21)) %>%
    mutate(sa1_code21 = as.numeric(sa1_code21)) %>% 
    st_transform(crs)
  
  city_sa1_vect <- vect(city_sa1)
  
  # Calculate slope across the entire DEM
  slope <- terrain(dem, v = "slope", unit = "degrees")
  
  # extract slope stats for each sa1
  
  slope_mean <- extract(slope, city_sa1_vect, fun = mean, na.rm = TRUE) %>%
    rename(slope_mean = slope)
  
  slope_sd <- extract(slope, city_sa1_vect, fun = sd, na.rm = TRUE) %>%
    rename(slope_sd = slope)
  
  # calculate number of cells per sa1 for diagnostic purposes (to check if small sa1s are creating outliers)
  
  slope_n <- extract(slope, city_sa1_vect, fun = function(x) sum(!is.na(x))) %>%
    rename(slope_n = slope)
  
  # combine stats into a single output object
  
  sa1_slope_stats <- city_sa1 %>%
    mutate(ID = row_number()) %>%
    left_join(slope_mean, by = "ID") %>%
    left_join(slope_sd, by = "ID") %>% 
    left_join(slope_n, by = "ID") %>% 
    mutate(city_name = city) %>% 
    st_drop_geometry() %>% 
    dplyr::select(city_name, slope_mean, slope_sd, sa1_code21)
  
}

# run function to calculate slope regularity

topo_stats <- lapply(cities, get_topography_stats) %>% 
  do.call(bind_rows, .)


## 2.6 Median property price -----------------------------------------------

# load price data

prices <- read_and_clean_csv(
  file.path(
    infile,
    "OpenStats_propertyPrices",
    "prices_3br_house.csv"
  )
) %>% 
  mutate(date = as.Date(date)) %>% 
  filter(year(date) <= 2019) %>%
  group_by(geo_name) %>%
  slice_max(date, n = 1, with_ties = FALSE) %>%
  ungroup()

# establish principal function

get_median_prices <- function(city){
  
  crs <- get_city_crs(city)
  
  # load city boundary
  
  ucl <- ucl_raw %>% 
    filter(ucl_name21 == ifelse(city == "perth",
                                "Perth (WA)",
                                str_to_title(city))) %>%
    dplyr::select(geometry, ucl_name21)
  
  city_sa2 <- st_join(sa2_raw, ucl, join = st_intersects) %>%
    filter(!is.na(ucl_name21)) %>%
    st_transform(crs)
  
  city_prices <- prices %>% 
    filter(geo_name %in% city_sa2$sa2_name21) %>% 
    mutate(city_name = city) %>%
    rename(sa2_name21 = geo_name,
           median_price_house_3br = value) %>% 
    dplyr::select(sa2_name21, median_price_house_3br, city_name)
}

# run function to calculate median house prices

price_stats <- lapply(cities, get_median_prices) %>% 
  do.call(bind_rows, .)


## 2.7 Distance from CBD ---------------------------------------------------

# establish case specific helper function

get_point_zero <- function(city_name) {
  coords  <- list(
    melbourne = c(144.963028, -37.81384),
    brisbane  = c(153.028173, -27.468174),
    sydney    = c(151.207699, -33.867716),
    adelaide  = c(138.600183, -34.9261),
    perth     = c(115.859,    -31.9522)
  )
  output <- st_sf(
    geometry = st_sfc(
      st_point(coords[[city_name]]),
      crs = 4326)
  )
  
  return(output)
}

# establish principal function

get_distance_from_cbd <- function(city){
  
  crs <- get_city_crs(city)
  
  city_point_zero <- get_point_zero(city) %>% 
    st_transform(crs)
  
  dev_raw <- read_and_clean(
    file.path(infile,
              "model_outputs",
              city,
              "development_summary",
              sprintf("%s_complete-dev_2023_2019_summary_nonres_mbcat_corrected.gpkg",
                      city)),
    crs) %>% 
    filter(complete.cases(x1st_pred_prob_23)) %>% 
    rename(geometry = geom) %>% 
    mutate(id = paste0(city, "_", row_number()))
  
  centroids <- st_centroid(dev_raw)
  
  dev <- dev_raw %>% 
    mutate(dist_from_cbd_m = st_distance(centroids, city_point_zero),
           city_name = city) %>% 
    st_drop_geometry() %>% 
    dplyr::select(id, city_name, dist_from_cbd_m)
  
  return(dev)
}

# run function to calculate distance from CBD

dist_stats <- lapply(cities, get_distance_from_cbd) %>% 
  do.call(bind_rows, .)


## 2.8 vacant lot density --------------------------------------------------

# establish principal function

get_vacant_land_density <- function(city){
  
  crs <- get_city_crs(city)
  
  # identify likely vacant land
  
  stock_raw <- read_and_clean(
    file.path(infile,
              "model_outputs",
              city,
              ifelse(city == "perth",
                     "perth_2023_2019_20251004135315_20251006002916_preproc_nonres_mbcat_corrected.gpkg",
                     sprintf("%s_2023_2019_20251004151320_20251006014834_preproc_nonres_mbcat_corrected.gpkg",
                             city))),
    crs) %>% 
    filter(complete.cases(x1st_pred_prob_23)) %>% 
    rename(geometry = geom)
  
  ucl <- ucl_raw %>% 
    st_transform(crs) %>% 
    filter(ucl_name21 == ifelse(city == "perth",
                                "Perth (WA)",
                                str_to_title(city))) %>%
    dplyr::select(geometry, ucl_name21)
  
  open_space <- open_space_raw %>%
    st_transform(crs) %>% 
    st_join(ucl, join = st_intersects) %>%
    filter(!is.na(ucl_name21))
  
  waterways <- waterways_raw %>% 
    st_transform(crs) %>% 
    st_join(ucl, join = st_intersects) %>%
    filter(!is.na(ucl_name21))
  
  water <- water_raw %>% 
    st_transform(crs) %>% 
    st_join(ucl, join = st_intersects) %>%
    filter(!is.na(ucl_name21))
  
  railways <- railways_raw %>% 
    st_transform(crs) %>% 
    st_join(ucl, join = st_intersects) %>%
    filter(!is.na(ucl_name21))
  
  vacant <- stock_raw %>% 
    filter(str_detect(proc_pred_19, "vegetation|bare-earth"),
           undev_flag_19 == FALSE,
           mb_cat21 == "Residential") %>% 
    # filter out odd shaped lots
    mutate(
      area_sqm = as.numeric(st_area(geometry)),
      perim_m = as.numeric(st_perimeter(geometry)),
      mbr_area = as.numeric(st_area(st_minimum_rotated_rectangle(geometry))),
      polsby_popper = as.numeric((4 * pi * area_sqm) / (perim_m^2)), # removes long and skinny lots
      rectangularity = area_sqm / mbr_area, # removes windy lots
      city_name = city) %>% 
    filter(polsby_popper > 0.35, # numbers derived from visual assessment. Tried balancing false +ve and -ve. erring to remove more false +ve.
           rectangularity > 0.3) %>% 
    # remove lots that are likely to be public land
    st_join(open_space, join = st_intersects) %>%
    filter(is.na(fclass)) %>% 
    dplyr::select(!fclass) %>% 
    st_join(waterways, join = st_intersects) %>%
    filter(is.na(fclass)) %>% 
    dplyr::select(!fclass) %>% 
    st_join(water, join = st_intersects) %>%
    filter(is.na(fclass)) %>% 
    dplyr::select(!fclass) %>% 
    st_join(railways, join = st_intersects) %>%
    filter(is.na(fclass)) %>% 
    dplyr::select(!fclass)
  
  vacant_centroids <- vacant %>%
    st_centroid() %>%
    st_coordinates() %>%
    as.data.frame()
  
  # calculate weights based on vacant land area
  weights <- vacant$area_sqm
  
  # create ppp object for calculating KDE in spatstat package
  vacant_ppp <- ppp(
    x = vacant_centroids$X,
    y = vacant_centroids$Y,
    window = as.owin(st_bbox(ucl)),
    marks = weights
  )
  
  # calculate raster resolution required for each city
  bbox <- st_bbox(ucl)
  city_width  <- bbox["xmax"] - bbox["xmin"]
  city_height <- bbox["ymax"] - bbox["ymin"]
  target_res <- 50
  dimyx <- c(
    round(city_height / target_res),
    round(city_width  / target_res)
  )
  
  # run kde calculation
  kde <- density.ppp(
    vacant_ppp,
    sigma = 600,         # how far each vacant lot impacts the density curve. Subjective decision. Chosen as 600m is a "neighbourhood"-ish
    weights = weights,
    dimyx = dimyx,
    positive = TRUE # forces all values to be positive. cleans up oddities at edges where vacant lot density reads as negative (which is nonsensical)
  )
  
  # Convert to ppp object to terra raster
  kde_rast <- rast(kde)
  crs(kde_rast) <- paste0("EPSG:", crs)
  
  # extract vacant land density values for each developed lot
  dev_raw <- read_and_clean(
    file.path(infile,
              "model_outputs",
              city,
              "development_summary",
              sprintf("%s_complete-dev_2023_2019_summary_nonres_mbcat_corrected.gpkg",
                      city)),
    crs) %>% 
    filter(complete.cases(x1st_pred_prob_23)) %>% 
    rename(geometry = geom) %>% 
    mutate(id = paste0(city, "_", row_number()))
  
  dev_centroids <- dev_raw %>%
    st_centroid()
  
  kde_extract <- terra::extract(
    kde_rast,
    vect(dev_centroids),
    na.rm = TRUE
  ) %>% 
    rename(vacant_land_density_kde = lyr.1)
  
  dev <- dev_raw %>%
    mutate(ID = row_number()) %>%
    left_join(kde_extract, by = "ID") %>%
    mutate(city_name = city) %>% 
    st_drop_geometry() %>% 
    dplyr::select(city_name, vacant_land_density_kde, id)
  
  # QUALITY CHECK: confirm no NAs introduced by centroids falling outside raster extent
  n_na <- sum(is.na(dev$vacant_land_density_kde))
  if (n_na > 0) {
    warning(
      sprintf(
        "%d lot centroid(s) in %s returned NA for vacant_land_density_kde — likely outside the UCL boundary or raster extent",
        n_na,
        city
      ))
  }
  
  return(dev)
  
}

# run function for calculating vacant lot density

vac_density_stats <- lapply(cities, get_vacant_land_density) %>% 
  do.call(bind_rows, .)


## 2.9 Public transport access ---------------------------------------------

# load case specific data

snamuts_raw <- read_and_clean(
  file.path(infile,
            "SNAMUTS_transportConnectivity",
            "snamuts_indicators_areas_2021_.json"
  ),
  4326)

# establish principal function

get_snamuts <- function(city){
  
  crs <- get_city_crs(city)
  
  ucl <- ucl_raw %>%
    st_transform(crs) %>% 
    filter(ucl_name21 == ifelse(city == "perth",
                                "Perth (WA)",
                                str_to_title(city))) %>%
    dplyr::select(geometry, ucl_name21)
  
  city_snamuts <- snamuts_raw %>% 
    st_transform(crs) %>% 
    st_join(ucl, join = st_intersects) %>%
    filter(!is.na(ucl_name21)) %>%
    dplyr::select(!sa1_code21) %>% # appears to be an error in snamuts sa1 codes as they don't line up with abs codes.
    st_buffer(-5)
  
  city_sa1 <- sa1_raw %>% 
    st_transform(crs) %>% 
    st_join(ucl, join = st_intersects) %>%
    filter(!is.na(ucl_name21)) %>% 
    mutate(sa1_code21 = as.numeric(sa1_code21))
  
  output <- st_join(city_sa1, city_snamuts, join = st_intersects) %>% 
    mutate(composite_index_average = replace_na(composite_index_average, 0),
           city_name = city) %>% 
    dplyr::select(composite_index_average, sa1_code21, city_name) %>% 
    st_drop_geometry()
  
}

# run function for calculating transport accessibility

transport_stats <- lapply(cities, get_snamuts) %>% 
  do.call(bind_rows, .)


## 2.10 Response variable _ medium density ---------------------------------

# establish principal function

get_resp <- function(city){
  
  crs <- get_city_crs(city)
  
  dev_raw <- read_and_clean(
    file.path(infile,
              "model_outputs",
              city,
              "development_summary",
              sprintf("%s_complete-dev_2023_2019_summary_nonres_mbcat_corrected.gpkg",
                      city)),
    crs) %>% 
    filter(complete.cases(x1st_pred_prob_23)) %>% 
    rename(geometry = geom) %>%
    mutate(id = paste0(city, "_", row_number()))
  
  dev <- dev_raw %>% 
    mutate(is_med_density = case_when(proc_pred_23 %in% c("cluster",
                                                          "low-rise-apartments",
                                                          "multi-unit-occupancy"
    ) ~ 1,
    TRUE ~ 0)) %>% 
    dplyr::select(is_med_density, id)
  
  dev_centroid <- st_centroid(dev)
  
  lga <- lga_raw %>% 
    st_transform(crs) %>% 
    dplyr::select(lga_name25)
  
  dev <- st_join(dev_centroid, lga, join = st_within, left = TRUE) %>%
    st_drop_geometry()
  
  return(dev)
  
}

# run function for calculating response variable

resp <- pblapply(cities, get_resp) %>% 
  bind_rows()


# 3. Combine all variables ------------------------------------------------

# establish specific geometries for combining variables

ucl_for_combining <- ucl_raw %>% 
  mutate(city_name = tolower(word(ucl_name21, 1)))

geography_combined <- mb_raw %>%
  st_join(ucl_for_combining, join = st_intersects) %>%
  filter(!is.na(ucl_name21)) %>% 
  mutate(sa1_code21 = as.numeric(sa1_code21),
         mb_code21 = as.numeric(mb_code21)) %>% 
  st_drop_geometry() %>% 
  dplyr::select(ucl_name21, mb_code21, sa1_code21, sa2_code21, sa2_name21, city_name)

get_dev_lots <- function(city, abs_geography){
  
  crs <- get_city_crs(city)
  
  geog <- abs_geography %>% 
    st_transform(crs) %>% 
    dplyr::select(geometry)
  
  dev_raw <- read_and_clean(
    file.path(infile,
              "model_outputs",
              city,
              "development_summary",
              sprintf("%s_complete-dev_2023_2019_summary_nonres_mbcat_corrected.gpkg",
                      city)),
    crs) %>% 
    filter(complete.cases(x1st_pred_prob_23)) %>% 
    rename(geometry = geom) %>%
    mutate(id = paste0(city, "_", row_number())) %>% 
    dplyr::select(id, mb_code21)
  
  dev_centroid <- st_centroid(dev_raw)
  
  output <- st_join(dev_centroid, geog, join = st_within, left = TRUE) %>%
    st_drop_geometry()
  
  return(output)
  
}

dev_lots <- lapply(cities, function(city){
  get_dev_lots(city, mb_raw)
}) %>% 
  bind_rows()

case_study <- dev_lots %>% 
  left_join(geography_combined, by = "mb_code21") %>% 
  dplyr::select(id, mb_code21, sa1_code21, sa2_code21, sa2_name21, ucl_name21, city_name)

# combine all variables

df <- resp %>% 
  left_join(case_study, by = "id") %>% 
  left_join(abs_stats_sa1, by = "sa1_code21") %>% 
  left_join(lot_stats, by = "id") %>% 
  left_join(dwelling_type_stats, by = "mb_code21") %>%  
  left_join(topo_stats, by = "sa1_code21") %>%
  left_join(price_stats, by = "sa2_name21") %>% 
  left_join(dist_stats, by = "id") %>% 
  left_join(vac_density_stats, by = "id") %>% 
  left_join(transport_stats, by = "sa1_code21") %>% 
  drop_na()

# 4. Save outputs ----------------------------------------------------------

output_dir <-file.path(outfile,
                       "missingMiddle",
                       "modelVariables")
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

write_parquet(df,
              file.path(output_dir,
                        "combined_df.parquet")
)

