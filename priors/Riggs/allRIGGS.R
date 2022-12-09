##Author: Ryan Riggs
##Date: 3/15/2022
#aus
devtools::install_github('buzacott/bomWater')
library(bomWater)
library(BBmisc)
library(data.table)

##Author: Ryan Riggs
##Date: 11/18/2021
#braz
library(data.table)
#link = "https://www.snirh.gov.br/hidroweb/rest/api/documento/convencionais?tipo=3&documentos="
##Author: Ryan Riggs
##Date: 3/15/2022
#Canada
library(tidyhydat)
 x=hy_version()
 if  (x$Date<Sys.Date()){
   fp=hy_dir()
   ffp=paste(fp,"/*",sep = "")
   unlink(ffp, recursive = T, force = T)
   print("hydat version older than today's date. Downloading hydat.")
   download_hydat(dl_hydat_here = NULL, ask = FALSE)
 }

##Author: Ryan Riggs
##Date: 3/15/2022
#japan
#path = "http://www1.river.go.jp/cgi-bin/DspWaterData.exe?KIND=6&ID="
library(RSelenium)
library(rvest)
library(data.table)
library(BBmisc)
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


qdownload_b = function(site){
  link = "https://www.snirh.gov.br/hidroweb/rest/api/documento/convencionais?tipo=3&documentos="
  outpath = tempfile()
  outpath2 = tempfile()
  files = paste0(link, site)
  out = paste0(outpath, site, ".zip")
  try(download.file(files, out, method = "curl", quiet = TRUE))
  print(out)
  print(getwd)
  a = unzip(out)
  data = suppressWarnings(try(read.table(unzip(a[grep("vazoes", a)]), sep = ";", header = TRUE),silent = TRUE))
  if(!is.error(data)){
    data1 = data[9:nrow(data),]
    cols = data1[1:78]
    data1 = data1[79:length(data1)]
    starts = data1 == as.character(site)
    starts = which(starts)
  }else{next}
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
original = "https://explorador.cr2.cl/request.php?options={%22variable%22:{%22id%22:%22qflxDaily%22,%22var%22:%22caudal%22,%22intv%22:%22daily%22,%22season%22:%22year%22,%22stat%22:%22mean%22,%22minFrac%22:80},%22time%22:{%22start%22:-946771200,%22end%22:1631664000,%22months%22:%22A%C3%B1o%20completo%22},%22anomaly%22:{%22enabled%22:false,%22type%22:%22dif%22,%22rank%22:%22no%22,%22start_year%22:1980,%22end_year%22:2010,%22minFrac%22:70},%22map%22:{%22stat%22:%22mean%22,%22minFrac%22:10,%22borderColor%22:%227F7F7F%22,%22colorRamp%22:%22Jet%22,%22showNaN%22:false,%22limits%22:{%22range%22:[5,95],%22size%22:[4,12],%22type%22:%22prc%22}},%22series%22:{%22sites%22:[%22"
01201003
ending = "%22],%22start%22:null,%22end%22:null},%22export%22:{%22map%22:%22Shapefile%22,%22series%22:%22CSV%22,%22view%22:{%22frame%22:%22Vista%20Actual%22,%22map%22:%22roadmap%22,%22clat%22:-18.0036,%22clon%22:-69.6331,%22zoom%22:5,%22width%22:461,%22height%22:2207}},%22action%22:[%22export_series%22]}"




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