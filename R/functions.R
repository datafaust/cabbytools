#Functions below---------------------------
#author: Fausto Lopez
#purpose: to run rbin file processing efficiently in conjunction with popular sql joins
#and to amplify access to shared ride data for ease of use


#' Pull trips from shl, fhv or yellow records for a given date range
#'
#' This function allows you to access rbin files on the shared ride to query quickly trip data from any of our services.
#' @param service the service you want in quotes: 'fhv'.
#' @param dt_start start date of range.
#' @param dt_end end date of range.
#' @param features a character vector of desired variables.
#' @param hack_fil a character vector of desired hacks to filter on.
#' @param dt_convert do you want dates to be automatically converted, defaults to T.
#' @param merge_ent if True merges against entity automatically, defaults to FALSE.
#' @param odbc_con for accessing sql tables in function, pass your odbc connection string.
#' @param query a character string encompassing the sql query.
#' @param left_key the left join key in the trip records.
#' @param right_key the right join key in entity.
#' @param join_type the type of join desired, currently experimental, defaults to left.
#' @keywords get
#' @export
#' @examples
#' get_trips("fhv",dt_start = "2017-06-01", dt_end = "2017-06-01", features = c("hack","plate","pudt","dodt"))

get_trips = function(service
                     ,dt_start = NULL
                     ,dt_end =NULL
                     ,features = NULL
                     ,hack_fil= NULL
                     ,dt_convert = T
                     ,merge_ent = F
                     ,odbc_con = NULL
                     ,query = NULL
                     ,left_key = NULL
                     ,right_key = NULL
                     ,join_type = NULL) {


  #all possible directories
  fun_dirs = list(
    med = "I:/COF/COF/_M3trics2/records/med",
    shl = "I:/COF/COF/_M3trics2/records/shl",
    fhv = "I:/COF/COF/_M3trics2/records/fhv",
    share = "I:/COF/COF/_M3trics2/records/fhv_share"
  )

  #check parameters and logic, spit errors
  if(is.null(service)) { stop("please enter a type of service: med,fhv,shl")}
  if(is.null(odbc_con) & merge_ent ==T) { stop("please enter an RODBC connection")}
  if(is.null(query) & merge_ent ==T) { stop("please enter a query")}
  if(is.null(dt_start)) { dt_start = as.Date("2015-01-01")}
  if(is.null(dt_end)) { dt_end = as.Date("2015-01-03")}
  #features are tested later in script

  #create date seq
  dates = seq.Date(as.Date(dt_start), as.Date(dt_end), by = "days", features = T)

  if(merge_ent == T & service != "share") {

    #loop
    setwd(fun_dirs[[service]])
    trips = rbindlist(
      pbapply::pblapply(dates,function(x) {
        pull = fst::read.fst(list.files(pattern = as.character(x)), as.data.table = T)
        #correct pudt and dodt for fhv
        if(service == "fhv") { names(pull)[names(pull) == "pud"] = "pudt"} else {pull}
        pull = if(service == "fhv" & x < as.Date("2017-06-01")) {pull = pull[,dodt:=NA]} else {pull}
        #test hack filter
        pull =  if(is.null(hack_fil)) { pull } else { pull[hack %in% hack_fil,]}
        #test feature select
        pull =  if(is.null(features)) { pull } else { pull[,features, with = F]}
        #print(pull)
        pull[,names(pull) := lapply(.SD, function(x) trimws(toupper(x)))]
        #print(pull)
        #test date conversion
        pull = if(dt_convert==F) { pull } else { pull = pull[
          ,':='(pudt = fasttime::fastPOSIXct(pudt,tz = "GMT")
                ,dodt = fasttime::fastPOSIXct(dodt,tz = "GMT")
          )
          ]
        }
      })
    )



    #now query the data base
    #pull entity
    con = RODBC::odbcConnect(odbc_con)
    sqlz = RODBC::sqlQuery(con, query, as.is = T)
    RODBC::odbcCloseAll()

    #merge
    trips = merge(trips,sqlz, by.x = left_key, by.y = right_key, all.x = T)[,industry:=service]




  } else if(merge_ent == T & service =='share') {

    #pull trips
    setwd(fun_dirs[[service]])
    trips = rbindlist(
      pbapply::pblapply(dates,function(x) {
        pull = fst::read.fst(list.files(pattern = as.character(x)), as.data.table = T)
        #test hack filter
        pull =  if(is.null(hack_fil)) { pull } else { pull[hack %in% hack_fil,]}
        pull =  if(is.null(features)) { pull } else { pull[,features, with = F]}
        #print(pull)
        pull[,names(pull) := lapply(.SD, function(x) trimws(toupper(x)))]
        pull = if(dt_convert==F) { pull } else { pull = pull[
          ,':='(share_pudt = fasttime::fastPOSIXct(share_pudt,tz = "GMT")
                ,share_dodt = fasttime::fastPOSIXct(share_dodt,tz = "GMT")
          )
          ]
        }

      })
    )

    #now query the data base
    #pull entity
    con = RODBC::odbcConnect(odbc_con)
    sqlz = RODBC::sqlQuery(con, query, as.is = T)
    RODBC::odbcCloseAll()

    #merge
    trips = merge(trips,sqlz, by.x = left_key, by.y = right_key, all.x = T)[,industry:=service]


  } else if (merge_ent != T & service =='share') {

    #pull trips
    setwd(fun_dirs[[service]])
    trips = rbindlist(
      pbapply::pblapply(dates,function(x) {
        pull = fst::read.fst(list.files(pattern = as.character(x)), as.data.table = T)
        #test hack filter
        pull =  if(is.null(hack_fil)) { pull } else { pull[hack %in% hack_fil,]}
        pull =  if(is.null(features)) { pull } else { pull[,features, with = F]}
        #print(pull)
        pull[,names(pull) := lapply(.SD, function(x) trimws(toupper(x)))]
        pull = if(dt_convert==F) { pull } else { pull = pull[
          ,':='(share_pudt = fasttime::fastPOSIXct(share_pudt,tz = "GMT")
                ,share_dodt = fasttime::fastPOSIXct(share_dodt,tz = "GMT")
          )
          ]
        }

      })
    )[,industry:=service]

  } else if(merge_ent !=T & service !='share') {

    setwd(fun_dirs[[service]])
    trips = rbindlist(
      pbapply::pblapply(dates,function(x) {
        pull = fst::read.fst(list.files(pattern = as.character(x)), as.data.table = T)
        #correct pudt and dodt for fhv
        if(service == "fhv") { names(pull)[names(pull) == "pud"] = "pudt"} else {pull}
        pull = if(service == "fhv" & x < as.Date("2017-06-01")) {pull = pull[,dodt:=NA]} else {pull}
        #test hack filter
        pull =  if(is.null(hack_fil)) { pull } else { pull[hack %in% hack_fil,]}
        #test feature select
        pull =  if(is.null(features)) { pull } else { pull[,features, with = F]}
        #print(pull)
        pull[,names(pull) := lapply(.SD, function(x) trimws(toupper(x)))]
        #print(pull)
        #test date conversion
        pull = if(dt_convert==F) { pull } else { pull = pull[
          ,':='(pudt = fasttime::fastPOSIXct(pudt,tz = "GMT")
                ,dodt = fasttime::fastPOSIXct(dodt,tz = "GMT")
          )
          ]
        }
      })
    )[,industry:=service]

  }
}

#' Quickly pull up text pipeline for using fst database with daily data
#'
#' This function allows you quickly access the pipeline necesarry to pull daily trip files through the fst database.
#' @keywords get
#' @export
#' @examples
#' get_trips_days()

get_trips_days = function(){
  code = print("
               #all the directories of importance
               dirs = list(
               med = 'I:/COF/COF/_M3trics2/records/med',
               shl = 'I:/COF/COF/_M3trics2/records/shl',
               fhv = 'I:/COF/COF/_M3trics2/records/fhv',
               share = 'I:/COF/COF/_M3trics2/records/fhv_share')

               #enter your directory, default is fhv
               setwd(dirs$fhv)

               #loop is written out for you
               my_result =
               seq.Date(as.Date(),as.Date(),by = 'days') %>%
               pbapply::pblapply(function(x){
               #enter your directory again in case of outputting cache somewhere else
               setwd(dirs$fhv)
               fst::read.fst(list.files(as.character(x), as.data.table = T)) %>%
               #begin filtering and dplyr functions below


               }) %>% rbindlist()
               ")
  clipr::write_clip(code)
  message("Code ready to paste")}


#' Quickly pull up text pipeline for using fst database with monthly data
#'
#' This function allows you quickly access the pipeline necesarry to pull daily trip files through the fst database.
#' @keywords get
#' @export
#' @examples
#' get_trips_months()
get_trips_months = function(){
  code = print("

               #all the directories of importance
               dirs = list(
               med = 'I:/COF/COF/_M3trics2/records/med',
               shl = 'I:/COF/COF/_M3trics2/records/shl',
               fhv = 'I:/COF/COF/_M3trics2/records/fhv',
               share = 'I:/COF/COF/_M3trics2/records/fhv_share')

               #enter your directory, default is fhv
               setwd(dirs$fhv)

               #loop is written out for you
               my_result =
               substr(seq.Date(as.Date(), as.Date(), by  ='months'),1,7) %>%
               pblapply(function(the_month){

               #enter your directory again in case of outputting cache somewhere else
               setwd()

               #extract all files that match the month and pull the data
               trips_month =
               list.files(pattern = the_month) %>%
               pblapply(function(the_days) {
               read.fst(the_days, as.data.table = T) %>%
               #begin filtering with dplyr or data table below

               }) %>% rbindlist()

               #if you need to do further calculations on the monthly trip data above you can do so
               #below before aggregating
               #example: trips_agg = trips %>% group_by(month(pudt)) %>% count

               }) %>% rbindlist()
               ")
  clipr::write_clip(code)
  message("Code ready to paste")}




#' Quickly pull up text pipeline for using fst database with daily data in parallel
#'
#' This function allows you quickly access the pipeline necesarry to pull daily trip files through the fst database in parallel.
#' @param cl the number of cores you want
#' @keywords get
#' @export
#' @examples
#' get_trips_days_p()
get_trips_days_p = function(){
  code = print("
#enter your libraries
libs = c('data.table','parallel','RODBC', 'lubridate'
               , 'fasttime', 'pbapply', 'dplyr', 'parallel'
               ,'zoo','fst')
               lapply(libs, require, character.only = T)

               #all the directories of importance
               dirs = list(
               med = 'I:/COF/COF/_M3trics2/records/med',
               shl = 'I:/COF/COF/_M3trics2/records/shl',
               fhv = 'I:/COF/COF/_M3trics2/records/fhv',
               share = 'I:/COF/COF/_M3trics2/records/fhv_share')

               #cluster setup
               cl = makeCluster() #enter the number of clusters you want to use, run detectCores()-1 for recommended cores
               clusterExport(cl,c('libs','dirs')) #enter the vectors to export, you'll want to add any other vectors
               clusterEvalQ(cl, lapply(libs, require, character.only = T)) #export your libraries to each core

               #enter your directory, default is fhv
               setwd(dirs$fhv)

               #loop is written out for you
               my_result =
               seq.Date(as.Date(),as.Date(),by = 'days') %>%
               pbapply::pblapply(function(x){
               #enter your directory again in case of outputting cache somewhere else
               setwd(dirs$fhv)
               fst::read.fst(list.files(as.character(x), as.data.table = T)) %>%
               #begin filtering and dplyr functions below


               },cl = cl #assigns cluster for parallelization
               ) %>% rbindlist()

               stopCluster(cl) #kill the cluster
               rm(cl) #remove it
               gc() #clear memory

")
  clipr::write_clip(code)
  message("Code ready to paste")
}

#' Spatial Join function to quickly run a spatial join
#'
#' This function allows you to quickly run spatial joins
#' @param data_set the data set name no quotes
#' @param shp_file the shape file name no quotes
#' @param projection the projection to transform the shape file in quotes
#' @param long longitude
#' @param lat latitude
#' @keywords spatial
#' @export
#' @import data.table rgdal sp
#' @examples
#'  spatial_join(bc, zones ,"+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs" , "longitude" , "latitude")
spatial_join = function(data_set,shp_file,projection, long, lat) {

  cols = c(long,lat)

  #first I like to subset the long lat points
  fs_points = data_set[, .SD, .SDcols = cols]
  names(fs_points) = c("x", "y")

  #code any NAs out
  fs_points[,x:= as.numeric(as.character(x))][
    ,y:= as.numeric(as.character(y))] #here I make sure things are numberical

  #this bit is very cutomizable but these are parameters I have set to clean out
  #crazy points and turn them to 0
  #rgdal doesnt like NAs when doin a spatial join
  #so this makes sure they will be coded out and the script will run without errors
  fs_points[is.na(fs_points$x)] = 0.00000 #here we are saying if something is NA, turn it into the number 0
  fs_points[is.na(fs_points$y)] = 0.00000
  #fs_points[y > 80 | y < 0
  #          ,y:= 0.00000][x > -65 | x < -85
  #                        ,x:=0.00000] #same thing is being done in this chain with conditions on range

  #transform projection (this is done to make sure the projections align)
  pointsSP = SpatialPoints(fs_points,
                           proj4string=CRS(projection))

  #match projection
  pointsSP = spTransform(pointsSP, proj4string(shp_file)) #another step towards matching the projection

  #test equivelency, should read TRUE (debug check)
  print(proj4string(shp_file) == proj4string(pointsSP)) #this will be True to show they are equivalent

  # Use 'over' to get _indices_ of the Polygons object  containing each point
  indices = setDT(over(pointsSP, shp_file)) #this is the actual spatial join; note I use the shape file
  names(indices) = tolower(names(indices)) #I lower the columns for easier use, you can omit

  head(indices) #here you can print and check what the new file indices looks like (this will be a whole bunch of NAS, that measn they are outside the polygons)

  #now we take the new spatially joined file and column bind it back to our original file
  final = cbind(data_set, indices)
  rm(indices,pointsSP,fs_points)
  gc()
  final
}
