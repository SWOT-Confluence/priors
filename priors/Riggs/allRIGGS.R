##Author: Ryan Riggs
##Date: 11/29/2022
##French gauge downloads.
library(jsonlite)
library(data.table)
library(BBmisc)
library(bomWater)
library(dplyr)
library(rvest)
library(tidyhydat)
library(RSelenium)
library(rvest)

##Author: Ryan Riggs
##Date: 3/15/2022
#aus


##Altered By SCoss 11.6.23
#quebec

#Saf
library(lubridate)
library(httr)
library(stringr)
library(tibble)

##Author: Ryan Riggs
##Date: 11/18/2021
#braz

#link = "https://www.snirh.gov.br/hidroweb/rest/api/documento/convencionais?tipo=3&documentos="
##Author: Ryan Riggs
##Date: 3/15/2022
##Altered By SCoss 11.6.23
#quebec

#Canada

library(tidyhydat)

# hy_default_db(hydat_path = "/tmp/Hydat.sqlite3")
# hy_set_default_db(hydat_path = download_hydat(dl_hydat_here = "/tmp", ask = FALSE ))
# download_hydat(dl_hydat_here = "/opt/hydroshare/Hydat.sqlite3", ask = FALSE )
print("pulling hydat")
download_hydat(ask = FALSE )
hy_dir()
hy_src()
print("finished pulling hydat")
can = try(hy_daily_flows("02OA004"))
if(is.error(can)){
  # return(NA)
  print("hydat failed...")
}else{
  can$Q = can$Value
  can$date =  as.character(can$Date)
  print(can)
  print("hydat worked")
}



 .get_start_date <- function(start_date) {
  ## Format start date
  if (is.null(start_date)) {
    start_date <- "1900-01-01"
  }
  return(start_date)
}

.get_end_date <- function(end_date) {
  ## Format end date - if not specified then use current date
  if (is.null(end_date))
    end_date <- Sys.time() %>%
      as.Date() %>%
      format("%Y-%m-%d")
  return(end_date)
}

.get_column_name <- function(variable) {
  ## Get appropriate column name for return object
  if (variable == "stage") {
    colnm <- "H"
  } else if (variable == "discharge") {
    colnm <- "Q"
  } else {
    stop(sprintf("Variable %s is not available", variable))
  }
  return(colnm)
}


# South africa
# FMr= self.downloadQ_saf(site,'discharge',self.start_date, self.end_date)
qdownload_Saf <- function(site,
                          variable = "discharge",
                          start_date = NULL,
                          end_date = NULL,
                          sites = FALSE,
                          ...) {
  
  print("starting saf pull......---------")
  if (sites) {
    print("returned sites...")
    return(southAfrican_sites)
  }
  print("actually pulling....")
  start_date <- .get_start_date(start_date)
  end_date <- .get_end_date(end_date)
  column_name <- .get_column_name(variable)
  original_data <- try(download_sa_data(
    site, variable, start_date, end_date, primary = FALSE
  ),silent=TRUE)
  # print("original data")
  # print(original_data)
  if(is.error(original_data)==TRUE|length(original_data)==0){stop('This gauge does not have a record associated with it and/or the agency website is down.')}
  data <- original_data %>%
    mutate(DATE = as.Date(.data$DATE, format = "%Y%m%d"))
  if (variable == "stage") {
    data <- data %>%
      mutate(across(starts_with("COR_"), as.numeric)) %>%
      rename(Date = "DATE") %>%
      group_by(.data$Date) %>%
      summarize(!!column_name := mean(.data$COR_LEVEL))
  } else {
    data <- data %>%
      rename(Date = "DATE", !!column_name := "D_AVG_FR") %>%
      dplyr::select(all_of(c("Date", column_name)))
  }
  print("test 2")
  out <- new_tibble(
    data,
    original = original_data,
    class = "rr_tbl"
  )
  out$date=as.character(as.Date(out$Date, format = "%Y%m%d"))
  out$Q=as.numeric(data$Q)
  print("Successfully pulled saf gague")
  # print(out)
  return(out)
  # data$date=as.character(as.Date(data$DATE, format = "%Y%m%d"))
  # #data$Q=as.numeric(data$D_AVG_FR)
  # return(data)
}


##Author: Ryan Riggs
##Date: 3/15/2022
#japan
#path = "http://www1.river.go.jp/cgi-bin/DspWaterData.exe?KIND=6&ID="

##Author: Ryan Riggs
##Date: 3/15/2022
#UK
##Note: There are quality codes in this database. 
##They are part of the output and nothing is currently filtered out. 
newWeb = "http://environment.data.gov.uk/hydrology/id/measures/"
##############################################################################
##Discharge download functions. 
##############################################################################
#aus
qdownload_a = function(site,sd,ed){
  discharge = try(get_daily(parameter_type = 'Water Course Discharge',
                            station_number=site, 
                            start_date = sd, 
                            end_date = ed))
  if(!is.error(discharge)){
    discharge$Q = discharge$Value
    discharge$Date = as.character(as.Date(discharge$Timestamp, format = "%Y-%m-%d"))
    return(discharge)
  }
  return(NA)
  #discharge$Q = discharge$Value
  #discharge$date =  as.Date(discharge$Timestamp)
  #discharge$Date =  as.character(as.Date(discharge$Timestamp))
  #return(discharge)
  #if(is.error(discharge)){next}else{
    #return(NA)
  #}
}
################################################################################################
##Functions. 
################################################################################################
#brazil
is.error <- function(
    expr,
    tell=FALSE,
    force=FALSE
)
{
  expr_name <- deparse(substitute(expr))
  test <- try(expr, silent=TRUE)
  iserror <- inherits(test, "try-error")
  if(tell) if(iserror) message("Note in is.error: ", test)
  if(force) if(!iserror) stop(expr_name, " is not returning an error.", call.=FALSE)
  iserror
}

substrRight <- function(x, n){
  substr(x, nchar(x)-n+1, nchar(x))
}
#Chile
qdownload_ch = function(site){
  Sys.sleep(.25)
  outpath = tempfile()
  website = paste0(original, site, ending)
  file = try(html_session(website)%>%html_element('body')%>%html_text('url'))
  if(is.error(file)){next}
  page = gsub(".*https", "", file)
  page = gsub("}}}", "", page)
  page = paste0("https", page)
  page = noquote(page)
  page = gsub('"', '', page)
  download.file(page, outpath)
  sttn = fread(outpath)
  sttn$Date = paste(sttn$agno, sttn$mes, sttn$dia, sep="-")
  sttn$Date = as.Date(sttn$Date, format = "%Y-%m-%d")
  sttn$valor = as.numeric(sttn$valor)
  sttn$Q = sttn$valor
  return(data.table(Date=sttn$Date[order(sttn$Date)], Q=sttn$Q[order(sttn$Date)]))
}
#qubec
qdownload_q=function(f){
  location='https://www.cehq.gouv.qc.ca/depot/historique_donnees/fichier/'
  website=paste0(location,f,'_Q.txt')
  outpath=tempfile()
  downloading = try(download.file(website, outpath))
  if(is.error(downloading)){return(NA)}
  data=fread(outpath, fill=TRUE)
  removeInfo=grep('Date', data$V2)
  data=data[(removeInfo+1):nrow(data),2:3]
  colnames(data)=c('Date','Q')
  data$date=as.character(as.Date(data$Date))
  data$Q=as.numeric(data$Q)
  data$Station=as.character(f)
  return(data)
}

qdownload_b = function(site){
  link = "https://www.snirh.gov.br/hidroweb/rest/api/documento/convencionais?tipo=3&documentos="
  outpath = tempfile()
  outpath2 = tempfile()
  files = paste0(link, site)
  out = paste0(outpath, site, ".zip")
  try(download.file(files, out, method = "curl", quiet = TRUE))
  a = unzip(out)
  data = suppressWarnings(try(read.table(unzip(a[grep("vazoes", a)]), sep = ";", header = TRUE, fileEncoding = "latin1")))
  if(!is.error(data)){
    data1 = data[9:nrow(data),]
    cols = data1[1:78]
    data1 = data1[79:length(data1)]
    starts = data1 == as.character(site)
    starts = which(starts)
  }else{
    return()
  }
  
  df = as.data.frame(matrix(numeric(), nrow =length(data1)/length(cols), ncol = length(unlist(cols))))
  colnames(df) = cols
  for(j in 1:length(starts)){
    start = starts[j]
    end = starts[j]+77
    dt = data1[start:end]
    dt = gsub(",", ".", dt)
    df[j,1:length(cols)] = dt
  }
  tab2 = df
  monthCols = grep("Vazao", colnames(tab2))
  monthCols = monthCols[-grep("Status",colnames(tab2)[monthCols])]
  tab2 = melt.data.table(as.data.table(tab2), measure.vars = colnames(tab2)[monthCols])
  tab2$Day = substrRight(as.character(tab2$variable), 2)
 
  tab2$month = substr(tab2$Data, 4,5)
  tab2$year = substr(tab2$Data, 7,10)
  tab2$Date = paste(tab2$year, tab2$month, tab2$Day, sep = "")
  out = data.frame(Date=tab2$Date, Q = as.numeric(tab2$value))
  out = out[order(out$Date),]
  # print(out)
  return(out)
}
################################################################################
##Discharge download functions. 
################################################################################
#canada
qDownload_c = function(site){
  
  can = try(hy_daily_flows(site))
  if(is.error(can)){
    return(NA)
  }else{
    can$Q = can$Value
    can$date =  as.character(can$Date)
    return(can)
  }
}
#################################################################################
##Discharge download functions. 
#################################################################################
#japan
################################################################################
##Discharge download functions. 
################################################################################
#quebec
qdownload_q=function(f){
  location='https://www.cehq.gouv.qc.ca/depot/historique_donnees/fichier/'
  website=paste0(location,f,'_Q.txt')
  outpath=tempfile()
  downloading = try(download.file(website, outpath))
  if(is.error(downloading)){return(NA)}
  data=fread(outpath, fill=TRUE)
  removeInfo=grep('Date', data$V2)
  data=data[(removeInfo+1):nrow(data),2:3]
  colnames(data)=c('Date','Q')
  data$date=as.character(as.Date(data$Date))
  data$Q=as.numeric(data$Q)
  data$Station=as.character(f)
  return(data)
}

qDownload_j = function(i, start, end){
  path = "http://www1.river.go.jp/cgi-bin/DspWaterData.exe?KIND=6&ID="
  site=i
  beginning = start
  start = paste0(beginning, "0101")
  end = end
  ending = paste0(end, "1231")
  rng = beginning:end
  
  dayrng = c('01','02','03','04','05','06','07','08','09','10','11','12')
  
  dtList = list()
  for(r in 1:length(rng)){
    year = rng[r]
    v = as.vector(12)
    for(j in 1:length(dayrng)){
      v[j] = paste0(year,dayrng[j],'01')
    }
    dtList[[r]] = v  
  }
  dates = unlist(dtList)
  
  tab = list()
  for(k in 1:length(dates)){
    day = dates[k]
    website = paste0(path, site, "&BGNDATE=", day, "&ENDDATE=", ending)
    file = try(session(website)%>%
                 read_html()%>%html_element('body'))
    if(is.error(file)){next}
    file1 = try(file%>%html_table())
    if(is.error(file1)){next}
    ##
    df = file1[5:nrow(file1),]
    dts = as.Date(df$X1, format="%Y/%m/%d")
    df = df[,2:ncol(df)]
    df = suppressWarnings(apply(df, 2, as.numeric))
    df = as.data.frame(df)
    df1 = rowMeans(df, na.rm = TRUE)
    df1 = as.data.frame(df1)
    df1$Date = dts
    tab[[k]] = df1
    
  }
  
  tabOut=rbindlist(tab)
  tabOut = tabOut[!is.na(tabOut$df1)]
  colnames(tabOut) = c("Q", "Date")
  tabout=tabOut[order(tabOut$Date),]
  tabout$date =  as.character(tabout$Date)
  return(tabout)
}
################################################################################
##Discharge download functions. 
################################################################################
#UK
newWeb = "http://environment.data.gov.uk/hydrology/id/measures/"
dischargeLoc = "-flow-m-86400-m3s-qualified/readings.csv"

qdownload_uk = function(site){
  web = site
  first = strsplit(web, "/")
  id = first[[1]][[7]]
  first[[1]][[1]] = paste0(first[[1]][[1]], "/")
  outPath = tempfile()
  download.file(paste0(newWeb,id, dischargeLoc), outPath)
  df = fread(outPath)
  df$Date = as.character(df$date)
  df$Q = df$value
  return(df)
}

################################################################################
##Discharge download functions. 
################################################################################
#Chile


##Chile
#libraries needed already imported
##Web address. 



qdownload_ch = function(site){
  Sys.sleep(.25)
  original = "https://explorador.cr2.cl/request.php?options={%22variable%22:{%22id%22:%22qflxDaily%22,%22var%22:%22caudal%22,%22intv%22:%22daily%22,%22season%22:%22year%22,%22stat%22:%22mean%22,%22minFrac%22:80},%22time%22:{%22start%22:-946771200,%22end%22:1631664000,%22months%22:%22A%C3%B1o%20completo%22},%22anomaly%22:{%22enabled%22:false,%22type%22:%22dif%22,%22rank%22:%22no%22,%22start_year%22:1980,%22end_year%22:2010,%22minFrac%22:70},%22map%22:{%22stat%22:%22mean%22,%22minFrac%22:10,%22borderColor%22:%227F7F7F%22,%22colorRamp%22:%22Jet%22,%22showNaN%22:false,%22limits%22:{%22range%22:[5,95],%22size%22:[4,12],%22type%22:%22prc%22}},%22series%22:{%22sites%22:[%22"
  ending = "%22],%22start%22:null,%22end%22:null},%22export%22:{%22map%22:%22Shapefile%22,%22series%22:%22CSV%22,%22view%22:{%22frame%22:%22Vista%20Actual%22,%22map%22:%22roadmap%22,%22clat%22:-18.0036,%22clon%22:-69.6331,%22zoom%22:5,%22width%22:461,%22height%22:2207}},%22action%22:[%22export_series%22]}"
  outpath = tempfile()
  website = paste0(original, site, ending)
  file = try(session(website)%>%html_element('body')%>%html_text('url'))
  if(is.error(file)){return()}
  page = gsub(".*https", "", file)
  page = gsub("}}}", "", page)
  page = paste0("https", page)
  page = noquote(page)
  page = gsub('"', '', page)
  download.file(page, outpath)
  sttn = fread(outpath)
  sttn$Date = paste(sttn$agno, sttn$mes, sttn$dia, sep="-")
  sttn$Date = as.character(as.Date(sttn$Date, format = "%Y-%m-%d"))
  # sttn$Date = as.Date(sttn$Date, format = "%Y-%m-%d")
  sttn$valor = as.numeric(sttn$valor)
  sttn$Q = sttn$valor
  return(data.table(Date=sttn$Date[order(sttn$Date)], Q=sttn$Q[order(sttn$Date)]))
}

##########################################################################################################
##Download Q fun: Conversion of divide by 1000 is in the function.
##########################################################################################################
qdownload_f =function(site){
  ##website
  ##########################################################################################################
  # "https://hubeau.eaufrance.fr/api/v2/hydrometrie/observations_tr?format=json&code_entite=K060001001&size=20000"

  # station_specific = 'https://hubeau.eaufrance.fr/api/v2/hydrometrie/observations_tr?format=json&?code_entite='
  # web = paste0(station_specific,site,'&date_debut_obs_elab=1800-01-01&date_fin_obs_elab=',Sys.Date(),'&grandeur_hydro_elab=QmnJ&size=20000')

  station_specific <- 'https://hubeau.eaufrance.fr/api/v2/hydrometrie/obs_elab?format=json'
  web <- paste0(station_specific,
                '&code_entite=', site,
                '&date_debut_obs_elab=1800-01-01',
                '&date_fin_obs_elab=', Sys.Date(),
                '&grandeur_hydro_elab=QmnJ',
                '&size=20000')

  df =fromJSON(web)$data

  if(is.null(nrow(df))){return(NULL)}else{
    df=data.table(Q=as.numeric(df$resultat_obs)/1000,
                  Date=as.character(as.Date(df$date_obs)))
    df = df[df$Q>=0,]
  }
  # print(head(df))

  return(df)
}
##########################################################################################################
##All the code below runs saf
##########################################################################################################

.get_start_date <- function(start_date) {
  ## Format start date
  if (is.null(start_date)) {
    start_date <- "1900-01-01"
  }
  return(start_date)
}

.get_end_date <- function(end_date) {
  ## Format end date - if not specified then use current date
  if (is.null(end_date))
    end_date <- Sys.time() %>%
      as.Date() %>%
      format("%Y-%m-%d")
  return(end_date)
}

.get_column_name <- function(variable) {
  ## Get appropriate column name for return object
  if (variable == "stage") {
    colnm <- "H"
  } else if (variable == "discharge") {
    colnm <- "Q"
  } else {
    stop(sprintf("Variable %s is not available", variable))
  }
  return(colnm)
}

# qdownload_Saf <- function(site,
#                           variable = "discharge",
#                           start_date = NULL,
#                           end_date = NULL,
#                           sites = FALSE,
#                           ...) {
  
#   if (sites) {
#     return(southAfrican_sites)
#   }
#   start_date <- .get_start_date(start_date)
#   end_date <- .get_end_date(end_date)
#   column_name <- .get_column_name(variable)
#   original_data <- try(download_sa_data(
#     site, variable, start_date, end_date, primary = FALSE
#   ),silent=TRUE)
#   if(is.error(original_data)==TRUE|length(original_data)==0){stop('This gauge does not have a record associated with it and/or the agency website is down.')}
#   data <- original_data %>%
#     mutate(DATE = as.Date(.data$DATE, format = "%Y%m%d"))
#   if (variable == "stage") {
#     data <- data %>%
#       mutate(across(starts_with("COR_"), as.numeric)) %>%
#       rename(Date = "DATE") %>%
#       group_by(.data$Date) %>%
#       summarize(!!column_name := mean(.data$COR_LEVEL))
#   } else {
#     data <- data %>%
#       rename(Date = "DATE", !!column_name := "D_AVG_FR") %>%
#       dplyr::select(all_of(c("Date", column_name)))
#   }
#   out <- new_tibble(
#     data,
#     original = original_data,
#     class = "rr_tbl"
#   )
#   out$date=as.character(as.Date(out$Date, format = "%Y%m%d"))
#   out$Q=as.numeric(data$Q)
#   return(out)
#   # data$date=as.character(as.Date(data$DATE, format = "%Y%m%d"))
#   # #data$Q=as.numeric(data$D_AVG_FR)
#   # return(data)
# }

construct_endpoint <- function(site, data_type, chunk_start_date, chunk_end_date) {
  chunk_start_date <- format(chunk_start_date, "%Y-%m-%d")
  chunk_end_date <- format(chunk_end_date, "%Y-%m-%d")
  endpoint <- paste0(
    "https://www.dws.gov.za/Hydrology/Verified/HyData.aspx?",
    "Station=", site, "100.00",
    "&DataType=", data_type,
    "&StartDT=", chunk_start_date,
    "&EndDT=", chunk_end_date,
    "&SiteType=RIV"
  )
  return(endpoint)
}

download_sa_data <- function(site,
                             variable,
                             start_date,
                             end_date,
                             primary) {
  ## Convert to date
  start_date=as.Date(start_date)
  end_date=as.Date(end_date)
  
  ## divide timeseries into months, because we can only
  ## scrape data one month at a time.
  ts <- seq(start_date, end_date, by = "1 day")
  years <- year(ts) %>%
    unique() %>%
    sort()
  n_years <- length(years)
  if (primary || (variable == "stage")) {
    ## We have to download stage data from primary data, which
    ## can only be downloaded one year at a time
    chunk_size <- 1 # Chunk size = num years
    n_chunks <- n_years
    data_type <- "Point"
    header <- c(
      "DATE", "TIME", "COR_LEVEL",
      "COR_LEVEL_QUAL", "COR_FLOW", "COR_FLOW_QUAL"
    )
  } else {
    chunk_size <- 20
    n_chunks <- ceiling(n_years / chunk_size)
    data_type <- "Daily"
    header <- c("DATE", "D_AVG_FR", "QUAL")
  }
  ## Number of data columns
  n_cols <- length(header)
  data_list <- list()
  for (i in 1:n_chunks) {
    chunk_start_date <- start_date + years((i-1) * chunk_size)
    endpoint <- construct_endpoint(site, data_type, chunk_start_date, end_date)
    ## data <- session(endpoint) %>%
    ##   html_element('body') %>%
    ##   html_text('pre')
    response <- GET(endpoint)
    data <- content(response) %>%
      html_element("body") %>%
      # html_text("pre")
      html_text(TRUE)
    data <- str_split(data, '\n')
    data <- unlist(data)
    ## Find out whether there is any data for
    ## the requested time period
    header_row <- grep("^DATE", data)
    if (length(header_row) == 0) {
      next
    } else {
      header_row <- header_row[1]
    }
    data_rows <- grep("^[0-9]{8} ", data)
    ## Convert to list
    data <- as.list(data)
    ## Get header
    data <- data[data_rows]
    data_sub <- lapply(data, function(x){
      sub <- x %>% str_split(' +')
      sub <- unlist(sub)
      if (length(sub) > n_cols) {
        sub <- sub[1:n_cols]
      } else if (length(sub) < n_cols) {
        sub <- c(sub[1], rep(NA, n_cols - 1))
      }
      sub
    })
    data <- do.call("rbind", data_sub)
    colnames(data) <- header
    data <- data %>% as_tibble()
    data_list[[i]] <- data
  }
  original_data <- do.call("rbind", data_list)
  return(original_data)
}