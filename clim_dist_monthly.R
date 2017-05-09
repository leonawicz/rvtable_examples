setwd("/workspace/Shared/Tech_Projects/DeltaDownscaling/project_data/downscaled")
library(parallel)
library(raster)
library(dplyr)
library(purrr)
library(rvtable)

outDir <- "/workspace/UA/mfleonawicz/data/clim_2km"
vars <- c("pr", "tas", "tasmin", "tasmax")
models <- c("GFDL-CM3", "GISS-E2-R", "IPSL-CM5A-LR", "MRI-CGCM3", "NCAR-CCSM4", "ts40")
rcps <- map(models, ~rep(list.files(.x), each=length(vars))) %>% map(~.x[.x != "rcp26"])
models <- map2(models, rcps, ~rep(.x, each=length(.y)))
vars <- map(models, ~rep(vars, length=length(.x)))
zero.min <- map(vars, ~.x=="pr")
inputs <- data.frame(rcp=unlist(rcps), model=unlist(models), var=unlist(vars), zero=unlist(zero.min))

get_files <- function(rcp, model, variable, dir=getwd()){
  mos.lab <- c(paste0(0, 1:9), 10:12)
  files <- list.files(file.path(dir, model, rcp, variable), pattern=".tif$", full=TRUE)
  n <- nchar(files)
  yrs <- as.numeric(substr(files, n-7, n-4))
  mos <- substr(files, n-10, n-9)
  ord <- order(paste(yrs, mos))
  list(files=files[ord], years=yrs[ord], months=mos[ord])
}

load("/atlas_scratch/mfleonawicz/projects/DataExtraction/workspaces/shapes2cells_akcan1km2km.RData")
cells <- filter(cells, Source=="akcan2km") %>% ungroup %>% select(-Source, -Cell)
cells$Location[cells$Location=="Department of Defense (DOD) and Department of Ene*"] <-
  "Department of Defense (DOD) and Department of Energy"

get_data <- function(model.index, cells, inputs, inDir, outDir,
                     na.rm=TRUE, density.args=list(n=200, adjust=0.1),
                     sample.size=10000, verbose=TRUE, overwrite=FALSE){
  verbose <- if(verbose & model.index==1) TRUE else FALSE
  inputs <- inputs[model.index,]
  rcp <- inputs$rcp
  model <- inputs$model
  variable <- inputs$var
  zero.min <- inputs$zero

  files <- get_files(rcp, model, variable, inDir)
  x0 <- as.matrix(raster::stack(files$files, quick=TRUE))
  if(verbose) print("Matrix in memory...")
  if(na.rm) x0 <- x0[!is.na(x0[,1]),]
  for(i in unique(cells$LocGroup)){
    cells.i <- filter(cells, LocGroup==i)
    for(j in unique(cells.i$Location)){
      dir.create(grpDir <- file.path(outDir, i,  j), showWarnings=FALSE, recursive=TRUE)
      file <- paste0(grpDir, "/", variable, "_", rcp, "_", model, ".rds")
      if(!overwrite && exists(file)) next
      if(verbose) print(paste("Compiling data for", j, "..."))
      cells.ij <- filter(cells.i, Location==j)
      idx <- if(na.rm) cells.ij$Cell_rmNA else cells.ij$Cell
      x <- x0[idx,]
      use_sample <- nrow(x) > sample.size
      if(use_sample) x <- x[sort(sample(1:nrow(x), sample.size)),]
      yrs <- as.integer(rep(files$years, each=nrow(x)))
      mos <- as.integer(rep(files$months, each=nrow(x)))
      x <- as.numeric(x)
      x <- split(x, paste(yrs, c(paste0(0, 1:9), 10:12)[mos]))
      nam <- names(x)
      if(verbose) print(paste("Number of time slices:", length(nam)))
      x <- mclapply(x, rvtable, density.args=density.args, mc.cores=32)
      x <- map2(x, nam, ~mutate(.x, Year=as.integer(substr(.y, 1, 4)), Month=as.integer(substr(.y, 6, 7)))) %>%
        bind_rows %>% tbl_df %>% dplyr::select(Year, Month, Val, Prob)
      if(zero.min && any(x$Val < 0)) warning("Density includes values less than zero.")
      saveRDS(x, file)
    }
  }
  NULL
}

set.seed(23)
# split sequence among multiple Atlas nodes if available
# memory-intensive; use only two cores on a given Atlas node.
mclapply(1:nrow(inputs), get_data, cells=cells, inputs=inputs, inDir=getwd(), outDir=outDir, mc.cores=2)
