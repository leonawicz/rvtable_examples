setwd("/workspace/UA/mfleonawicz/data")
library(parallel)
library(dplyr)
library(purrr)
library(rvtable)

monDir <- "clim_2km"
seaDir <- "clim_2km_seasonal"
dir.create(outDir <- "/workspace/UA/mfleonawicz/data/clim_2km_stats", showWarnings=FALSE)

set.seed(62)
variables <- c("pr", "tas", "tasmin", "tasmax")
files <- file.path(seaDir, list.files(seaDir, recursive=TRUE))
files <- split(files, basename(dirname(dirname(files))))

get_stats <- function(files, outDir, mc.cores=32){
  grpDir <- dirname(files[1])
  locgrp <- dirname(grpDir)
  loc <- basename(grpDir)
  dir.create(outDir <- file.path(outDir, grpDir), showWarnings=FALSE, recursive=TRUE)
  rcp_levels <- c("Historical", "4.5", "6.0", "8.5")
  relabel_rcps <- function(x) sapply(x, function(x) switch(x, historical="Historical", rcp45="4.5", rcp60="6.0", rcp85="8.5"))
  model_levels <- c("CRU 4.0", "NCAR-CCSM4", "GFDL-CM3", "GISS-E2-R", "IPSL-CM5A-LR", "MRI-CGCM3")
  season_levels <- c("Annual", "Winter", "Spring", "Summer", "Autumn")
  relabel_seasons <- function(x) sapply(x, function(x) switch(x, annual="Annual", winter="Winter", spring="Spring", summer="Summer", autumn="Autumn"))
  files <- cbind(files, loc, do.call(rbind, strsplit(basename(files), "_"))) %>% 
    data.frame(stringsAsFactors=FALSE) %>% tbl_df %>% mutate(V6=gsub("\\.rds", "", V6))
  names(files) <- c("files", "Region", "Var", "RCP", "GCM", "Season")
  files <- mutate(files,
    Region=factor(Region, levels=sort(unique(loc))),
    Var=factor(Var, levels=c("pr", "tas", "tasmin", "tasmax")),
    RCP=factor(relabel_rcps(RCP), levels=rcp_levels),
    GCM=factor(factor(ifelse(GCM=="ts40", "CRU 4.0", GCM), levels=model_levels)),
    #GCM=factor(ifelse(GCM=="ts40", factor("CRU 4.0", levels=model_levels), factor(GCM, levels=model_levels))),
    Season=factor(relabel_seasons(Season), levels=season_levels))
  .stats <- function(i, files){
    print(paste("File", i, "of", nrow(files), "..."))
    readRDS(files$files[i]) %>% rvtable %>% sample_rvtable %>% group_by(RCP, GCM, Var, Year, Season) %>%
      summarise(Mean=round(mean(Val), 1), SD=round(sd(Val), 1), Min=round(min(Val), 1),
        Pct_05=round(quantile(Val, 0.05), 1), Pct_10=round(quantile(Val, 0.10), 1), Pct_25=round(quantile(Val, 0.25), 1), 
        Pct_50=round(quantile(Val, 0.50), 1), Pct_75=round(quantile(Val, 0.75), 1), Pct_90=round(quantile(Val, 0.90), 1),
        Pct_95=round(quantile(Val, 0.95), 1), Max=round(max(Val), 1)) %>% ungroup %>%
      mutate(Region=files$Region[i], Var=files$Var[i], RCP=files$RCP[i], GCM=files$GCM[i], Season=files$Season[i])
  }
  files <- split(files, loc)
  for(j in seq_along(files)){
    x <- mclapply(seq_along(files[[j]]), .stats, files=files[[j]], mc.cores=mc.cores)
    print(x)
    x <- bind_rows(x) %>% select(RCP, GCM, Region, Var, Year, Season, 
                                 Mean, SD, Min, Pct_05, Pct_10, Pct_25, Pct_50, Pct_75, Pct_90, Pct_95, Max) %>%
      arrange(RCP, GCM, Region, Var, Year, Season)
    outfile <- file.path(outDir, unique(as.character(files[[j]]$Region)), "_climate.rds")
    saveRDS(x, outfile)
  }
  NULL
}

for(i in seq_along(files)){
  get_stats(files=files[[i]], outDir=outDir, mc.cores=32)
}
