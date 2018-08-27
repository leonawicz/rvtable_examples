library(dplyr)
library(snapprep)
inputs <- clim_inputs_table()
load(snapdef()$akcan1km2km)
cells <- filter(cells, Source == "akcan2km") %>% ungroup %>% select(-Source, -Cell)
cells$Location[cells$Location=="Department of Defense (DOD) and Department of Ene*"] <-
  "Department of Defense (DOD) and Department of Energy"

# Extract monthly spatial distributions
set.seed(23)
# split sequence among multiple Atlas nodes if available
# memory-intensive; use only two cores on a given Atlas node.
for(i in 1:nrow(inputs)){
  clim_dist_monthly(i, cells, inputs)
  print(nrow(inputs) - i)
}

# Calculate seasonal spatial distributions
set.seed(62)
variable <- c("pr", "tas", "tasmin", "tasmax") # iterate using four Atlas nodes
files <- list.files(recursive = TRUE, pattern=paste0("^", variable[1], "_.*."))
parallel::mclapply(seq_along(files), clim_dist_seasonal, files = files, mc.cores = 32)

# Calculate monthly and seasonal statistics
monDir <- "clim_2km"
seaDir <- "clim_2km_seasonal"
set.seed(62)
variables <- c("pr", "tas", "tasmin", "tasmax")

files <- file.path(monDir, list.files(monDir, recursive = TRUE))
files <- split(files, basename(dirname(dirname(files))))
for(i in seq_along(files)){
  clim_stats_ar5(files = files[[i]], type = "monthly", 
                 out_dir = snapdef()$ar5dir_dist_stats[1], mc.cores = 32)
}

files <- file.path(seaDir, list.files(seaDir, recursive = TRUE))
files <- split(files, basename(dirname(dirname(files))))
for(i in seq_along(files)){
  clim_stats_ar5(files = files[[i]], type = "seasonal", 
                 out_dir = snapdef()$ar5dir_dist_stats[2], mc.cores = 32)
}
