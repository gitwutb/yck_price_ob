# SELECT DISTINCT belong_project,project_name FROM yck_tableau_daily_regular
# WHERE belong_project NOT in (1,2,3,4,5,6,7,8,9,20,21,22,24,25,26,27,28,29,30,31,32,37,42,41,44,45,48,50,51,52)
# SELECT car_platform,id_che300,brand,series,model_name,model_price,color,auto,location,province,regional,regDate,user_years,mile,quotes
# FROM (SELECT DISTINCT car_series1 FROM yck_price_config_project a
#       INNER JOIN analysis_che300_cofig_info b ON a.id_che300=b.car_id) m
# INNER JOIN analysis_wide_table n ON m.car_series1=n.series

rm(list = ls(all=T))
gc()
library(RODBC)
library(reshape2)
library(dplyr)
library(ggplot2)
library(RMySQL)
library(stringr)
library(e1071) 
library(tcltk)
library(lubridate)
library(truncnorm)
library(cluster)
library(Rtsne)
library(xlsx)
library(parallel)
library(rlist)
price_model_loc<-gsub("\\/code","",tryCatch(dirname(rstudioapi::getActiveDocumentContext()$path),error=function(e){getwd()}))
local_defin<-data.frame(user = 'root',host='192.168.0.111',password= '000000',dbname='yck-data-center',stringsAsFactors = F)
loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
dbSendQuery(loc_channel,'SET NAMES gbk')
output_view<-dbFetch(dbSendQuery(loc_channel,"SELECT DISTINCT id_che300 FROM yck_price_config_project;"),-1)
input_orig<-dbFetch(dbSendQuery(loc_channel,"SELECT car_platform,id_che300,brand,series,model_year,model_name,model_price,color,auto,location,province,regional,regDate,update_time,user_years,mile,quotes
     FROM analysis_wide_table;"),-1)
dbDisconnect(loc_channel)
input_orig<-data.frame(input_orig,quotes_p=round(input_orig$quotes/input_orig$model_price,2))
input_orig<-input_orig%>%filter(quotes_p>0.05&quotes_p<0.95)
input_orig<-input_orig%>%filter(user_years>0&user_years<10)
input_orig<-input_orig%>%filter(mile>0&mile<50)
input_orig<-input_orig%>%filter(substr(update_time,1,7)>substr(Sys.Date()-365,1,7))
input_orig<-input_orig%>%filter(year(regDate)-as.numeric(model_year)>=-2)

analysis_wide_table_cous<-dplyr::summarise(group_by(input_orig,series),count_s=n())
write.csv(analysis_wide_table_cous,paste0(price_model_loc,"\\file\\analysis_wide_table_cous.csv",sep=""),
          row.names = F,fileEncoding = "UTF-8",quote = F)
rm(analysis_wide_table_cous)

#####挑选精确款型,款型量不够再放宽条件
output_view<-merge(output_view,input_orig,by='id_che300')
tab<-output_view%>%group_by(model_name,series,auto)%>%dplyr::summarise(cou=n())%>%as.data.frame()

output_add<-NULL
for (i in 1:nrow(tab)) {
  if(tab$cou[i]<800){
    linshi<-input_orig%>%filter(series==tab$series[i],auto==tab$auto[i],model_year==substr(tab$model_name[i],1,4))
    output_add<-rbind(output_add,linshi)
  }else{print("is ok")}
}
output_view<-rbind(output_view,output_add)
#output_view<-data.frame(output_view,user_years_d=cut(output_view$user_years,breaks = c(seq(0,10,0.75),10)))


write.csv(output_view,paste0(price_model_loc,"\\file\\yck_price_sh_car.csv",sep=""),
          row.names = F,fileEncoding = "UTF-8",quote = F)
loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
dbSendQuery(loc_channel,'SET NAMES gbk')
dbSendQuery(loc_channel,"TRUNCATE TABLE yck_price_sh_car")
dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE '",price_model_loc,"/file/yck_price_sh_car.csv'",
                               " INTO TABLE yck_price_sh_car CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;",sep=""))
dbDisconnect(loc_channel)

series<-data.frame(series=tab$series%>%unique())
ana2_1<-merge(input_orig,series,by="series")
ana2_2<-dplyr::summarise(group_by(ana2_1,brand,series,province),count_n=n())
write.csv(ana2_2,paste0(price_model_loc,"\\file\\yck_price_shcar_province.csv",sep=""),
          row.names = F,fileEncoding = "UTF-8",quote = F)
loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
dbSendQuery(loc_channel,'SET NAMES gbk')
dbSendQuery(loc_channel,"TRUNCATE TABLE yck_price_shcar_province")
dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE '",price_model_loc,"/file/yck_price_shcar_province.csv'",
                               " INTO TABLE yck_price_shcar_province CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;",sep=""))
#####analysis_wide_table_cous表的定时车系技术
dbSendQuery(loc_channel,"TRUNCATE TABLE analysis_wide_table_cous")
dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE '",price_model_loc,"/file/analysis_wide_table_cous.csv'",
                               " INTO TABLE analysis_wide_table_cous CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;",sep=""))
dbDisconnect(loc_channel)



#################*********录入阿里云*********#################
loc_channel<-dbConnect(MySQL(),user = "yckdc",host="47.106.189.86",password= "YckDC888",dbname="yck-data-center")
dbSendQuery(loc_channel,'SET NAMES gbk')
dbSendQuery(loc_channel,"TRUNCATE TABLE yck_price_sh_car")
dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE '",price_model_loc,"/file/yck_price_sh_car.csv'",
                               " INTO TABLE yck_price_sh_car CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;",sep=""))
dbDisconnect(loc_channel)

##########
loc_channel<-dbConnect(MySQL(),user = "yckdc",host="47.106.189.86",password= "YckDC888",dbname="yck-data-center")
dbSendQuery(loc_channel,'SET NAMES gbk')
dbSendQuery(loc_channel,"TRUNCATE TABLE yck_price_shcar_province")
dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE '",price_model_loc,"/file/yck_price_shcar_province.csv'",
                               " INTO TABLE yck_price_shcar_province CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;",sep=""))
#####analysis_wide_table_cous表的定时车系技术
dbSendQuery(loc_channel,"TRUNCATE TABLE analysis_wide_table_cous")
dbSendQuery(loc_channel,paste0("LOAD DATA LOCAL INFILE '",price_model_loc,"/file/analysis_wide_table_cous.csv'",
                               " INTO TABLE analysis_wide_table_cous CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;",sep=""))
dbDisconnect(loc_channel)