setwd("/workspace/UA/mfleonawicz/data/clim_2km")
library(parallel)
library(dplyr)
library(purrr)
library(rvtable)

outDir <- "/workspace/UA/mfleonawicz/data/clim_2km_seasonal"

get_seasonal <- function(i, files, outDir, density.args=list(n=200, adjust=0.1)){
  .seasonal <- function(x, season, density.args=density.args){
    .season <- function(x, months){
      yrs <- range(x$Year)
      x <- filter(x, Month %in% months) %>% mutate(Year=ifelse(Month==12, Year+1L, Year))
      filter(x, Year >= yrs[1] + 1 & Year <= yrs[2]) %>% rvtable
    }
    x <- rvtable(x)
    f <- function(x) marginalize(x, "Month", density.args=density.args)
    if(season=="annual") return(f(x))
    if(season=="winter") return(f(.season(x, c(1,2,12))))
    if(season=="spring") return(f(.season(x, 3:5)))
    if(season=="summer") return(f(.season(x, 6:8)))
    if(season=="autumn") return(f(.season(x, 9:11)))
  }
  file <- files[i]
  dir.create(outDir <- file.path(outDir, dirname(file)), showWarnings=FALSE, recursive=TRUE)
  outfile <- file.path(outDir, strsplit(basename(file), "\\.")[[1]][1])
  x <- readRDS(file)
  y <- .seasonal(x, "annual")
  saveRDS(y, paste0(outfile, "_annual.rds"))
  y <- .seasonal(x, "winter")
  saveRDS(y, paste0(outfile, "_winter.rds"))
  y <- .seasonal(x, "spring")
  saveRDS(y, paste0(outfile, "_spring.rds"))
  y <- .seasonal(x, "summer")
  saveRDS(y, paste0(outfile, "_summer.rds"))
  y <- .seasonal(x, "autumn")
  saveRDS(y, paste0(outfile, "_autumn.rds"))
}

set.seed(62)
variable <- c("pr", "tas", "tasmin", "tasmax") # iterate using four Atlas nodes
files <- list.files(recursive=TRUE, pattern=paste0("^", variable[1], "_.*."))
mclapply(seq_along(files), get_seasonal, files=files, outDir=outDir, mc.cores=28)
