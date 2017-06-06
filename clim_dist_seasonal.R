setwd("/workspace/UA/mfleonawicz/data/clim_2km")
library(parallel)
library(dplyr)
library(purrr)
library(rvtable)

outDir <- "/workspace/UA/mfleonawicz/data/clim_2km_seasonal"

get_seasonal <- function(i, files, outDir, density.args=list(n=200, adjust=0.1)){
  .seasonal <- function(x, season, density.args){
    .season <- function(x, months){
      yrs <- range(x$Year)
      x <- filter(x, Month %in% months) 
      if(any(months==12)){
        y <- mutate(x, Year=ifelse(Month==12, Year+1L, Year)) %>% filter(Year > yrs[1] & Year <= yrs[2])
        x <- filter(x, Year==yrs[1]) %>% bind_rows(y)
      }
      rvtable(x)
    }
    x <- switch(season,
      "annual"=rvtable(x), "winter"=.season(x, c(1,2,12)), "spring"=.season(x, 3:5), 
      "summer"=.season(x, 6:8), "autumn"=.season(x, 9:11))
    marginalize(x, "Month", density.args=density.args)
  }
  file <- files[i]
  dir.create(outDir <- file.path(outDir, dirname(file)), showWarnings=FALSE, recursive=TRUE)
  outfile <- file.path(outDir, strsplit(basename(file), "\\.")[[1]][1])
  x <- readRDS(file)
  y <- .seasonal(x, "annual", density.args=density.args)
  saveRDS(y, paste0(outfile, "_annual.rds"))
  y <- .seasonal(x, "winter", density.args=density.args)
  saveRDS(y, paste0(outfile, "_winter.rds"))
  y <- .seasonal(x, "spring", density.args=density.args)
  saveRDS(y, paste0(outfile, "_spring.rds"))
  y <- .seasonal(x, "summer", density.args=density.args)
  saveRDS(y, paste0(outfile, "_summer.rds"))
  y <- .seasonal(x, "autumn", density.args=density.args)
  saveRDS(y, paste0(outfile, "_autumn.rds"))
}

set.seed(62)
variable <- c("pr", "tas", "tasmin", "tasmax") # iterate using four Atlas nodes
files <- list.files(recursive=TRUE, pattern=paste0("^", variable[1], "_.*."))
mclapply(seq_along(files), get_seasonal, files=files, outDir=outDir, mc.cores=32)
