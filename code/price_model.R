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
library(xlsx)
library(parallel)
library(rlist)
##########数据输入
###########加载自定义函数###########paste0(price_model_loc,"\\function")
#price_model_loc<-gsub("\\/main","",dirname(rstudioapi::getActiveDocumentContext()$path))
price_model_loc<-c("C:/Users/Administrator/Desktop/YCK_DC_SHARE/code_model_ml/model_rea")
local_defin<-data.frame(user = 'root',host='192.168.0.111',password= '000000',dbname='yck-data-center',stringsAsFactors = F)
source(paste0(price_model_loc,"\\function\\fun_model_price.R"),echo=FALSE,encoding="utf-8")
############################模型链条完善##############################
loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
dbSendQuery(loc_channel,'SET NAMES gbk')
result_output<-dbFetch(dbSendQuery(loc_channel,"SELECT a.car_id,project_name,is_sold,cost,deal_price,detection_level,operate_status FROM yck_tableau_daily_regular a
  INNER JOIN yck_tableau_daily_financial b ON a.car_id=b.car_id;"),-1)
select_input<-dbFetch(dbSendQuery(loc_channel,"SELECT id_che300 select_model_id,license_reg_date select_regDate,kilometre select_mile,DATE_FORMAT(sell_time,'%Y-%m-%d') select_partition_month,car_id FROM yck_tableau_daily_regular a 
  INNER JOIN yck_price_config_project b ON a.type_name=b.type_name AND a.belong_project=b.belong_project AND is_sold=1;"),-1)
select_input1<-dbFetch(dbSendQuery(loc_channel,"SELECT id_che300 select_model_id,license_reg_date select_regDate,kilometre select_mile,DATE_FORMAT(sell_time,'%Y-%m-%d') select_partition_month,car_id FROM yck_tableau_daily_regular a 
  INNER JOIN yck_price_config_project b ON a.type_name=b.type_name AND a.belong_project=b.belong_project AND is_sold=0 AND operate_status=4 AND is_future=0;"),-1)
select_input2<-dbFetch(dbSendQuery(loc_channel,"SELECT id_che300 select_model_id,license_reg_date select_regDate,kilometre select_mile,DATE_FORMAT(sell_time,'%Y-%m-%d') select_partition_month,car_id FROM yck_tableau_daily_regular a 
  INNER JOIN yck_price_config_project b ON a.type_name=b.type_name AND a.belong_project=b.belong_project AND is_future=1 AND is_sold=0;"),-1)
select_input_future1<-dbFetch(dbSendQuery(loc_channel,"SELECT id_che300 select_model_id,'2018-07-01' select_regDate,'' select_mile,'2019-07-01' select_partition_month FROM yck_price_config_project where is_future=2;"),-1)
dbDisconnect(loc_channel)
select_input_future<-NULL
for (i in 1:5) {
  linshi_input<-select_input1
  linshi_input$select_partition_month<-as.character(today()+31*(i-1))
  select_input<-rbind(select_input,linshi_input)
  linshi_input<-select_input2
  linshi_input$select_partition_month<-as.character(today()+31*(i-1))
  select_input<-rbind(select_input,linshi_input)
  linshi_input<-select_input_future1
  linshi_input$select_partition_month<-as.character(as.Date("2019-07-02")+31*(i-3))
  select_input_future<-rbind(select_input_future,linshi_input)
  linshi_input<-select_input_future1
  linshi_input$select_partition_month<-as.character(as.Date("2020-07-02")+31*(i-3))
  select_input_future<-rbind(select_input_future,linshi_input)
}
rm(select_input1,linshi_input,select_input_future1)
aa<-select_input%>%group_by(select_model_id,select_regDate)%>%dplyr::summarise(a=n())%>%
  top_n(1,a)%>%top_n(1,select_regDate)%>%as.data.frame()%>%dplyr::select(select_model_id,select_regDate1=select_regDate)
select_input<-left_join(select_input,aa,by="select_model_id")
select_input$select_regDate[which(select_input$select_regDate=='1899-12-31')]<-select_input$select_regDate1[which(select_input$select_regDate=='1899-12-31')]
select_input<-select_input%>%dplyr::select(-select_regDate1)
select_input$select_partition_month[which(is.na(select_input$select_partition_month))]<-as.character(Sys.Date())
select_input$select_mile<-3*as.numeric(round(difftime(as_datetime(select_input$select_partition_month),as_datetime(select_input$select_regDate),units="days")/365,2))
select_input_future$select_mile<-4*as.numeric(round(difftime(as_datetime(select_input_future$select_partition_month),as_datetime(select_input_future$select_regDate),units="days")/365,2))

##########2018-09-18使用多线程代替
# start_time<-Sys.time()
# pb <- tkProgressBar("进度","已完成 %", 0, 100) #开启进度条 
# u<-1:nrow(select_input)
# output_pre<-NULL
# output_sample<-NULL
# for (i in 1:nrow(select_input)) {
#   linshi<-fun_pred(select_input[i,])
#   linshi1<-linshi[[1]]
#   output_pre<-rbind(output_pre,linshi1)
#   info<- sprintf("已完成 %d%%", round(i*100/length(u)))   #进度条
#   setTkProgressBar(pb, i*100/length(u),sprintf("进度 (%s)  耗时:(%ss)",info,round(unclass(as.POSIXct(Sys.time()))-unclass(as.POSIXct(start_time)))),info)  #进度条
# }
# close(pb)  #关闭进度条
# Sys.time()-start_time
start_time<-Sys.time()
select_input_org<-select_input
x<-1:nrow(select_input_org)
cl<-makeCluster(4)
clusterExport(cl,c("select_input_org","price_model_loc","local_defin"))
clusterEvalQ(cl,c(library(RODBC),
                  library(reshape2),
                  library(dplyr),
                  library(ggplot2),
                  library(RMySQL),
                  library(stringr),
                  library(e1071) ,
                  library(tcltk),
                  library(lubridate),
                  source(paste0(price_model_loc,"\\function\\fun_model_price.R"),echo=FALSE,encoding="utf-8")))
output_pre<-parLapply(cl,x,fun_pred_round)
output_pre<-list.rbind(list.rbind(output_pre))
stopCluster(cl)
Sys.time()-start_time
result_tag<-dplyr::summarise(group_by(output_pre,car_id,select_model_id,select_model_name,select_model_price,select_regDate,
                               select_partition_month,select_mile),fb_price=round(mean(fb),2),pm_price=round(mean(pm),2))%>%ungroup()%>%as.data.frame()
result_reg<-dplyr::summarise(group_by(output_pre,select_model_id,select_model_name,select_model_price,
                            province),fb_price=round(mean(fb),2),pm_price=round(mean(pm),2))%>%ungroup()%>%as.data.frame()


#################
result_output<-merge(result_tag,result_output,by='car_id')
result_output$deal_price[which(result_output$is_sold==0)]<-10000*result_output$pm_price[which(result_output$is_sold==0)]


write.csv(result_output,"E:/Work_table/gitwutb/git_project/yck_price_ob/file/yck_price_pred_ml.csv",
          row.names = F,fileEncoding = "UTF-8",quote = F)
write.csv(result_reg,"E:/Work_table/gitwutb/git_project/yck_price_ob/file/yck_price_pred_ml_province.csv",
          row.names = F,fileEncoding = "UTF-8",quote = F)
loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
dbSendQuery(loc_channel,'SET NAMES gbk')
dbSendQuery(loc_channel,"TRUNCATE TABLE yck_price_pred_ml")
dbSendQuery(loc_channel,"LOAD DATA LOCAL INFILE 'E:/Work_table/gitwutb/git_project/yck_price_ob/file/yck_price_pred_ml.csv'
            INTO TABLE yck_price_pred_ml CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;")
dbSendQuery(loc_channel,"TRUNCATE TABLE yck_price_pred_ml_province")
dbSendQuery(loc_channel,"LOAD DATA LOCAL INFILE 'E:/Work_table/gitwutb/git_project/yck_price_ob/file/yck_price_pred_ml_province.csv'
            INTO TABLE yck_price_pred_ml_province CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;")
dbDisconnect(loc_channel)


###################################2018-09-05添加，考量期车#####################
start_time<-Sys.time()
pb <- tkProgressBar("进度","已完成 %", 0, 100) #开启进度条 
u<-1:nrow(select_input_future)
output_pre<-NULL
output_sample<-NULL
for (i in 1:nrow(select_input_future)) {
  linshi<-fun_pred(select_input_future[i,])
  linshi1<-linshi[[1]]
  output_pre<-rbind(output_pre,linshi1)
  info<- sprintf("已完成 %d%%", round(i*100/length(u)))   #进度条
  setTkProgressBar(pb, i*100/length(u),sprintf("进度 (%s)  耗时:(%ss)",info,round(unclass(as.POSIXct(Sys.time()))-unclass(as.POSIXct(start_time)))),info)  #进度条
}
close(pb)  #关闭进度条
yck_price_pred_ml_future<-output_pre
yck_price_pred_ml_future$select_partition_month<-substr(yck_price_pred_ml_future$select_partition_month,1,4)
yck_price_pred_ml_future<-dplyr::summarise(group_by(yck_price_pred_ml_future,select_model_id,select_model_name,select_model_price,select_regDate,
                               select_partition_month),fb_price=round(mean(fb),2),pm_price=round(mean(pm),2))%>%ungroup()%>%
  as.data.frame()%>%dplyr::mutate(add_date=Sys.Date())

write.csv(yck_price_pred_ml_future,"E:/Work_table/gitwutb/git_project/yck_price_ob/file/yck_price_pred_ml_future.csv",
          row.names = F,fileEncoding = "UTF-8",quote = F)
loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
dbSendQuery(loc_channel,'SET NAMES gbk')
dbSendQuery(loc_channel,paste0("DELETE FROM yck_price_pred_ml_future WHERE add_date='",Sys.Date(),"'"))
dbSendQuery(loc_channel,"LOAD DATA LOCAL INFILE 'E:/Work_table/gitwutb/git_project/yck_price_ob/file/yck_price_pred_ml_future.csv'
            INTO TABLE yck_price_pred_ml_future CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;")
dbDisconnect(loc_channel)



##################第三部分：2018-9-6临时增加项目####
select_input_future_b<-read.csv("E:/Work_table/gitwutb/git_project/yck_price_ob/file/select_inputst.csv",header = T)
output_pre<-NULL
for (i in 1:nrow(select_input_future_b)) {
  linshi<-fun_pred(select_input_future_b[i,])
  linshi1<-linshi[[1]]
  output_pre<-rbind(output_pre,linshi1)
}
yck_price_pred_ml_future_b<-dplyr::summarise(group_by(output_pre,select_model_name,select_model_price,select_regDate,
                                             select_partition_month),fb_price=round(mean(fb),2),pm_price=round(mean(pm),2))%>%ungroup()%>%
  as.data.frame()%>%dplyr::mutate(add_date=Sys.Date())
write.csv(yck_price_pred_ml_future_b,"E:/Work_table/gitwutb/git_project/yck_price_ob/file/yck_price_pred_ml_future_b.csv",
          row.names = F,fileEncoding = "UTF-8",quote = F)
loc_channel<-dbConnect(MySQL(),user = local_defin$user,host=local_defin$host,password= local_defin$password,dbname=local_defin$dbname)
dbSendQuery(loc_channel,'SET NAMES gbk')
dbSendQuery(loc_channel,paste0("DELETE FROM yck_price_pred_ml_future_b WHERE add_date='",Sys.Date(),"'"))
dbSendQuery(loc_channel,"LOAD DATA LOCAL INFILE 'E:/Work_table/gitwutb/git_project/yck_price_ob/file/yck_price_pred_ml_future_b.csv'
            INTO TABLE yck_price_pred_ml_future_b CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;")
dbDisconnect(loc_channel)








###############*********************录入阿里云********************###########################
loc_channel<-dbConnect(MySQL(),user = "yckdc",host="47.106.189.86",password= "YckDC888",dbname="yck-data-center")
dbSendQuery(loc_channel,'SET NAMES gbk')
dbSendQuery(loc_channel,"TRUNCATE TABLE yck_price_pred_ml")
dbSendQuery(loc_channel,"LOAD DATA LOCAL INFILE 'E:/Work_table/gitwutb/git_project/yck_price_ob/file/yck_price_pred_ml.csv'
            INTO TABLE yck_price_pred_ml CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;")
dbSendQuery(loc_channel,"TRUNCATE TABLE yck_price_pred_ml_province")
dbSendQuery(loc_channel,"LOAD DATA LOCAL INFILE 'E:/Work_table/gitwutb/git_project/yck_price_ob/file/yck_price_pred_ml_province.csv'
            INTO TABLE yck_price_pred_ml_province CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;")
dbDisconnect(loc_channel)

#######
loc_channel<-dbConnect(MySQL(),user = "yckdc",host="47.106.189.86",password= "YckDC888",dbname="yck-data-center")
dbSendQuery(loc_channel,'SET NAMES gbk')
dbSendQuery(loc_channel,paste0("DELETE FROM yck_price_pred_ml_future WHERE add_date='",Sys.Date(),"'"))
dbSendQuery(loc_channel,"LOAD DATA LOCAL INFILE 'E:/Work_table/gitwutb/git_project/yck_price_ob/file/yck_price_pred_ml_future.csv'
            INTO TABLE yck_price_pred_ml_future CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;")
dbDisconnect(loc_channel)

######
loc_channel<-dbConnect(MySQL(),user = "yckdc",host="47.106.189.86",password= "YckDC888",dbname="yck-data-center")
dbSendQuery(loc_channel,'SET NAMES gbk')
dbSendQuery(loc_channel,paste0("DELETE FROM yck_price_pred_ml_future_b WHERE add_date='",Sys.Date(),"'"))
dbSendQuery(loc_channel,"LOAD DATA LOCAL INFILE 'E:/Work_table/gitwutb/git_project/yck_price_ob/file/yck_price_pred_ml_future_b.csv'
            INTO TABLE yck_price_pred_ml_future_b CHARACTER SET utf8 FIELDS TERMINATED BY ',' lines terminated by '\r\n' IGNORE 1 LINES;")
dbDisconnect(loc_channel)
