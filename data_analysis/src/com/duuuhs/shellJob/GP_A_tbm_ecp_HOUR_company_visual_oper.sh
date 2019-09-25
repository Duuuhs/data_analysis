##################################################################
##FUNTION: LoadData :  tbl_****_company_visual_visitor
##DATA REGEX		:  tbl_****_company_visual_visitor 
##@author dumingyuan
##【EBDA】基本参数提示(固定的)######################################
##参数编号：0                            1              2               3                                               4                       7
##参数：sh ebda_exec_Sh.sh flowID       jobID   jobName                                 jobPath                                                 EBDA_UDC_BIZ_DATE     jobStatus       args
##示例：
##功能： 商城可视化指标数据加工                                                                             ccbcom_load_ccbcom              com.ccb.services.Ccbcom_import_ccbcom    20190811                5                       arg1            arg2            arg3
##################################################################
##【EBDA】作业参数设置(EDIT)
##################################################################
######################################################################################################
#商务企业商城运营可视化分析业务--时间分布
######################################################################################################
EBDA_DIR=/home/ap/dip/appjob/shelljob/****
#EBDA_DIR="/home/hadoop/workspace_EBDA"
EBDA_jobSh=${EBDA_DIR}/jobSh
EBDA_BIN=${EBDA_jobSh}/platform
EBDA_LOG=${EBDA_DIR}/logs
EBDA_etc=${EBDA_DIR}/ebda_etc/ebda.env
EBDA_FLOWID=${1}
EBDA_JOBID=${2}
EBDA_JOBNAME=${3}
EBDA_JOBPATH=${4}
EBDA_UDC_BIZ_DATE=${5}
UDC_BIZ_DATE=${EBDA_UDC_BIZ_DATE}
EBDA_JOBSTATUS=${6}
HOST_USER=`whoami`
HOST_NAME=`hostname`
#last_date=`date -d "- 14 day ${UDC_BIZ_DATE}" +%Y%m%d` 
##################################################################
##【EBDA】方法：如果程序执行出错，exit 1(固定的)
##################################################################
issucc()
{
    if [ $? -ne 0 ]; then
        echo "last command execute failed!!!"
        exit 1
    fi
}

echo "【EBDA_INFO】Job:"${EBDA_JOBNAME}"###STARTED!#########Date"`date +"%Y-%m-%d %H:%M:%S"`
echo "【JOB_INFO】FLOWID["${EBDA_FLOWID}"]JOBID["${EBDA_JOBID}"]JOBNAME["${EBDA_JOBNAME}"]JOBPATH["${EBDA_JOBPATH}"]UDC_BIZ_DATE["${UDC_BIZ_DATE}"]JOBSTATUS"${EBDA_JOBSTATUS}
#########################################
##【STEP1】Variables Initialized
#########################################
echo "【EBDA_INFO】Step1:Initialize Started"
EBDA_YEAR=${UDC_BIZ_DATE:0:4}
EBDA_MON=${UDC_BIZ_DATE:4:2}
EBDA_DAY=${UDC_BIZ_DATE:6:2}

###InputJob Variables--3.0 DataClear SqlArray
array_dataclear[0]="truncate table ****_app.tbm_web_page_view_hour;"
array_dataclear[1]="truncate table ****_app.tbm_****_pc_pv_hour_01;"
array_dataclear[2]="truncate table ****_app.tbm_****_pc_visitor_hour_01;"
array_dataclear[3]="truncate table ****_app.tbm_****_pc_visitor_hour_02;"
array_dataclear[4]="truncate table ****_app.tbm_****_pc_visitor_hour_03;"
array_dataclear[5]="truncate table ****_app.tbm_****_pc_time_all_hour_01;"
array_dataclear[6]="truncate table ****_app.tbm_****_pc_time_ave_hour_01;"
array_dataclear[7]="truncate table ****_app.tbm_rule_comp2_hour_01;"
array_dataclear[8]="truncate table ****_app.tbm_****_pc_web_hour_01;"
array_dataclear[9]="truncate table ****_app.tbm_****_pc_shop_hour_01;"
array_dataclear[10]="truncate table ****_app.tbm_****_pc_order_hour_01;"
array_dataclear[11]="truncate table ****_app.tbm_****_pc_new_hour_01;"
array_dataclear[12]="truncate table ****_app.tbm_****_pc_new_hour_02;"
array_dataclear[13]="delete from ****_app.tbl_****_company_visual_visitor where udc_biz_date like '${EBDA_YEAR}-${EBDA_MON}-${EBDA_DAY}%' and regional_class='06';"

###InputJob Variables--3.1 Partition SqlArray
array_partition=()

###InputJob Variables--3.2 load Sql Array
array_load=()

##InputJob Variables--3.3 update Sql Array
array_update=()

###InputJob Variables--3.4 insert Sql Array

###########开始处理数据...
#1 访问时段预处理
array_insert[0]="insert into ****_app.tbm_web_page_view_hour 
(UDC_BIZ_DATE,
UDC_SESSION_ID,
UDC_PAGE_ID,
UDC_PAGE_VISIT_TIME,
UDC_SOURCE_URL,
UDC_CURRENT_URL,
OSVT_CHANNEL_ID,
UDC_CHANNEL_HOUR) 
select '${UDC_BIZ_DATE}',  --业务日期
		UDC_SESSION_ID,
		UDC_PAGE_ID,
		UDC_PAGE_VISIT_TIME,
		UDC_SOURCE_URL,
		UDC_CURRENT_URL,
		OSVT_CHANNEL_ID,
       case floor(UDC_PAGE_HOUR) 
         when 0 then '00:00-00:59' when 1 then '01:00-01:59' when 2 then '02:00-02:59' 
         when 3 then '03:00-03:59' when 4 then '04:00-04:59' when 5 then '05:00-05:59' 
         when 6 then '06:00-06:59' when 7 then '07:00-07:59' when 8 then '08:00-08:59' 
         when 9 then '09:00-09:59' when 10 then '10:00-10:59' when 11 then '11:00-11:59'
         when 12 then '12:00-12:59' when 13 then '13:00-13:59' when 14 then '14:00-14:59' 
         when 15 then '15:00-15:59' when 16 then '16:00-16:59' when 17 then '17:00-17:59' 
         when 18 then '18:00-18:59' when 19 then '19:00-19:59' when 20 then '20:00-20:59' 
         when 21 then '21:00-21:59' when 22 then '22:00-22:59' when 23 then '23:00-23:59' end time_slot --时间分段，每间隔1小时为一段，共24段 
  from (select 	UDC_SESSION_ID,
				UDC_PAGE_ID,
				UDC_PAGE_VISIT_TIME,
				UDC_SOURCE_URL,
				UDC_CURRENT_URL,
				OSVT_CHANNEL_ID,
               extract(hour from to_timestamp(UDC_PAGE_VISIT_TIME, 'YYYYMMDDHH24MISSMS')) UDC_PAGE_HOUR --从会话时间提取小时数
          from TBL_WEB_PAGE_VIEW
         where UDC_CHANNEL_ID='04'
		   and OSVT_CHANNEL_ID in ('05', '06','07','08','09') 
           --and UDC_PAGE_VISIT_TIME ~ '^[0-9]{17}$'  --过滤不合格式数据
           and substr(UDC_PAGE_VISIT_TIME, 1, 8) = '${UDC_BIZ_DATE}') b;"
		  
#2 页面浏览数、访问次数
array_insert[1]="insert into ****_app.tbm_****_pc_pv_hour_01 
(udc_biz_date,
channel_flag,
channel_hour,
pandect_pv,
channel_visit_times) 
select '${UDC_BIZ_DATE}',  --业务日期
       OSVT_CHANNEL_ID,  --渠道ID
	   UDC_CHANNEL_HOUR,
	   count(1),
	   count(distinct udc_session_id)
from 	
****_app.tbm_web_page_view_hour 
group by    
OSVT_CHANNEL_ID,UDC_CHANNEL_HOUR;"
		  
#3 访客数
array_insert[2]="insert into ****_app.tbm_****_pc_visitor_hour_01 
(udc_biz_date,
channel_flag,
channel_hour,
channel_visitor) 
select 
'${UDC_BIZ_DATE}',
visitor1.OSVT_CHANNEL_ID,
visitor1.UDC_CHANNEL_HOUR,
coalesce(visitor1.num,0)+coalesce(visitor2.num,0)		--预防遇到null值结果为null的情况
from
(
select a.OSVT_CHANNEL_ID,  --渠道ID
	   b.UDC_CHANNEL_HOUR,
	   count(distinct a.udc_customer_id) num
from
****_app.TBL_VISITOR_IDENTIFICATION a,****_app.tbm_web_page_view_hour b
where 
a.udc_biz_date=b.udc_biz_date
and a.UDC_SESSION_ID=b.UDC_SESSION_ID
and a.OSVT_CHANNEL_ID=b.OSVT_CHANNEL_ID
and a.udc_biz_date='${UDC_BIZ_DATE}' 
and a.udc_customer_id is not null 
and a.udc_customer_id!=''
group by a.OSVT_CHANNEL_ID,b.UDC_CHANNEL_HOUR) visitor1 
left join 
(
select a.OSVT_CHANNEL_ID,  --渠道ID
	   b.UDC_CHANNEL_HOUR,
	   count(distinct a.udc_cookie) num 
from
****_app.TBL_VISITOR_IDENTIFICATION a,****_app.tbm_web_page_view_hour b
where 
a.udc_biz_date=b.udc_biz_date
and a.UDC_SESSION_ID=b.UDC_SESSION_ID
and a.OSVT_CHANNEL_ID=b.OSVT_CHANNEL_ID
and a.udc_biz_date='${UDC_BIZ_DATE}' 
and (a.udc_customer_id is null or a.udc_customer_id='')
group by a.OSVT_CHANNEL_ID,b.UDC_CHANNEL_HOUR) visitor2 
on visitor1.OSVT_CHANNEL_ID=visitor2.OSVT_CHANNEL_ID 
and visitor1.UDC_CHANNEL_HOUR=visitor2.UDC_CHANNEL_HOUR;"
		  

#4 平均网站访问时长,预处理(总访问时长)
array_insert[3]="insert into ****_app.tbm_****_pc_time_all_hour_01 
(udc_biz_date,
channel_flag,
channel_hour,
channel_page_time) 
select 
'${UDC_BIZ_DATE}' udc_biz_date,  --业务日期
t.OSVT_CHANNEL_ID,
t.UDC_CHANNEL_HOUR,
sum(extract(EPOCH from t.udc_page_stay_time)) udc_page_stay_time --将时长interval类型转换为数字（秒）
from
(select  OSVT_CHANNEL_ID,  --渠道ID
		 UDC_CHANNEL_HOUR, 
		 lead(to_timestamp(UDC_PAGE_VISIT_TIME,'YYYYMMDDHH24MISSMS')) over(partition by udc_session_id order by UDC_PAGE_VISIT_TIME) - to_timestamp(UDC_PAGE_VISIT_TIME, 'YYYYMMDDHH24MISSMS') udc_page_stay_time
  from ****_app.tbm_web_page_view_hour) t
   group by t.OSVT_CHANNEL_ID,t.UDC_CHANNEL_HOUR;"
				
#5 平均网站访问时长(平均访问时长)
array_insert[4]="insert into ****_app.tbm_****_pc_time_ave_hour_01 
(channel_flag,channel_hour,udc_tmp_visit_time)
select 
a.channel_flag,
a.channel_hour,
lpad(trunc(a.channel_page_time/b.channel_visit_times/3600),2,'0')||':'||lpad(trunc(a.channel_page_time/b.channel_visit_times%3600/60),2,'0')||':'||lpad(trunc(a.channel_page_time/b.channel_visit_times%3600%60%60),2,'0')||'' 
from 
tbm_****_pc_time_all_hour_01 a,
tbm_****_pc_pv_hour_01 b 
where 
a.channel_flag=b.channel_flag and 
a.channel_hour=b.channel_hour;"	

#6 网站跳出次数
array_insert[5]="insert into ****_app.tbm_****_pc_web_hour_01 
(channel_flag,channel_hour,channel_web_bounce) 
select 
b.OSVT_CHANNEL_ID,
b.UDC_CHANNEL_HOUR,
count(1)
from
(select 
UDC_SESSION_ID,
count(1) as num
from 
****_app.tbm_web_page_view_hour 
group by UDC_SESSION_ID
having count(1)=1 ) a				--筛选出跳出为1的情况
left join ****_app.tbm_web_page_view_hour  b
on a.UDC_SESSION_ID=b.UDC_SESSION_ID 
group by b.OSVT_CHANNEL_ID,b.UDC_CHANNEL_HOUR;"	

#7 浏览商品次数
array_insert[6]="insert into ****_app.tbm_****_pc_shop_hour_01 
(udc_biz_date,
channel_flag,
channel_hour,
channel_pv) 
select 
'${UDC_BIZ_DATE}',  --业务日期
OSVT_CHANNEL_ID,
UDC_CHANNEL_HOUR,
count(1)
from
****_app.tbm_web_page_view_hour 
where (UDC_CURRENT_URL like 'http://****/space/show.php%'
or UDC_CURRENT_URL like 'http://****/****/view/sell/detail%'
or UDC_CURRENT_URL like 'http://****/space/sell_show.php%'
or UDC_CURRENT_URL like 'http://****/****/view/sell/snap-detail%'
or UDC_CURRENT_URL like 'http://****/****/view/productDetail/show%'
or UDC_CURRENT_URL like 'http://****/****/mobile/views/productDetail/detail.html%'
or UDC_CURRENT_URL like 'http://****/****/view/productDetail/showSnap%'
or UDC_CURRENT_URL like 'http://****/****/mobile/views/productDetail/snap_detail.html%'
or UDC_CURRENT_URL like 'http://****/products/pd_%'
or UDC_CURRENT_URL like 'http://****/products/m_%'
or UDC_CURRENT_URL like 'http://****/client/cpd_%'
) group by OSVT_CHANNEL_ID,UDC_CHANNEL_HOUR;"
   
#8 下单完成次数
array_insert[7]="insert into ****_app.tbm_****_pc_order_hour_01 
(udc_biz_date,
channel_flag,
channel_hour,
channel_pv) 
select 
'${UDC_BIZ_DATE}',  --业务日期
OSVT_CHANNEL_ID,
UDC_CHANNEL_HOUR,
count(distinct udc_session_id)
from
****_app.tbm_web_page_view_hour 
where (UDC_CURRENT_URL like 'http://****/****/view/orderView/orderSubmit%'  
or UDC_CURRENT_URL like 'http://****/member/unitAddOrder.jhtml%')
group by OSVT_CHANNEL_ID,UDC_CHANNEL_HOUR;"


#9 新访次数 (一条处理) --create on 2019/07/31
array_insert[8]="insert into ****_app.tbm_****_pc_new_hour_02 
(channel_flag,channel_hour,udc_tmp_visit_time)
select 
d.osvt_channel_id,
d.UDC_CHANNEL_HOUR, 
count(distinct d.udc_session_id)
from
(
(select 
 a.udc_session_id,a.osvt_channel_id
 from 
 (select udc_session_id,udc_customer_id,osvt_channel_id from ****_app.tbl_visitor_identification 
  where udc_biz_date='${UDC_BIZ_DATE}' 
  and udc_customer_id is not null
  and udc_customer_id !=''
  ) a 
  right join 
 (select udc_visitor_sequence from ****_app.tbl_visitor_first_identification
  where udc_biz_date='${UDC_BIZ_DATE}'
  and udc_visitor_type='01'
  ) b
  on a.udc_customer_id=b.udc_visitor_sequence
) 												--客户udc_session_id
union 
(select 
 a.udc_session_id,a.osvt_channel_id
 from 
 (select udc_session_id,udc_cookie,osvt_channel_id from ****_app.tbl_visitor_identification 
  where udc_biz_date='${UDC_BIZ_DATE}' 
  and (udc_customer_id is null or udc_customer_id='')
  ) a 
  right join 
 (select udc_visitor_sequence from ****_app.tbl_visitor_first_identification
  where udc_biz_date='${UDC_BIZ_DATE}'
  and udc_visitor_type='02'
  ) b
  on a.udc_cookie=b.udc_visitor_sequence
)												--游客udc_session_id
) as c											--当天新增访客数的所有udc_session_id
inner join 
****_app.tbm_web_page_view_hour d
on c.udc_session_id=d.udc_session_id
and c.osvt_channel_id=d.osvt_channel_id
group by d.OSVT_CHANNEL_ID,d.UDC_CHANNEL_HOUR;"


#10 数据插入最终表
array_insert[9]="insert into ****_app.tbl_****_company_visual_visitor
(regional_class,
regional_type,
regional_visitor,
regional_visit_times,
regional_pv,
regional_web_bounce,
regional_visit_number_ave,
regional_visit_time_ave,
regional_visit_times_new,
regional_visit_percent_new,
regional_visit_comm_times,
regional_order_conv,
regional_flag,
udc_biz_date)
select  
'06',
coalesce(a.channel_hour,'0'),
coalesce(to_char(b.channel_visitor,'FM999,999,999,999,999'),'0'),
coalesce(to_char(a.channel_visit_times,'FM999,999,999,999,999'),'0'),
coalesce(to_char(a.pandect_pv,'FM999,999,999,999,999'),'0'),
coalesce(round((e.channel_web_bounce)*100/a.channel_visit_times,2)||'%','0.00%'),
coalesce(round(a.pandect_pv/a.channel_visit_times,0),'0'),
coalesce(d.udc_tmp_visit_time,'0'),
coalesce(t.udc_tmp_visit_time,'0'),
coalesce(round((t.udc_tmp_visit_time)*100/a.channel_visit_times,2)||'%','0.00%'),
coalesce(to_char(f.channel_pv,'FM999,999,999,999,999'),'0'),
coalesce(round((g.channel_pv)*100/a.channel_visit_times,2)||'%','0.00%'),
coalesce(a.channel_flag,'0'),
'${EBDA_YEAR}-${EBDA_MON}-${EBDA_DAY}'||' 00:00:00'
from 
tbm_****_pc_pv_hour_01 a left join
tbm_****_pc_visitor_hour_01 b on a.channel_flag=b.channel_flag and a.channel_hour=b.channel_hour left join
tbm_****_pc_time_ave_hour_01 d on a.channel_flag=d.channel_flag and a.channel_hour=d.channel_hour left join
tbm_****_pc_web_hour_01 e on a.channel_flag=e.channel_flag and a.channel_hour=e.channel_hour left join
tbm_****_pc_shop_hour_01 f on a.channel_flag=f.channel_flag and a.channel_hour=f.channel_hour left join
tbm_****_pc_order_hour_01 g on a.channel_flag=g.channel_flag and a.channel_hour=g.channel_hour left join
tbm_****_pc_new_hour_02 t on a.channel_flag=t.channel_flag and a.channel_hour=t.channel_hour;"

#########################################
##【STEP2】Data_Clear
#########################################
echo "【EBDA_INFO】Step2:DataClear Method Started,Status: "${EBDA_JOBSTATUS}

###if [ ${EBDA_JOBSTATUS} -eq 5 ];then
echo "【EBDA_INFO】,jobStatus is 5 ==> DataClear Started"
###Clear the data on Hive
for sql in "${array_dataclear[@]}"
do
	echo "[EBDA_INFO]Step3. Current_SQL[${sql}]"
	${EBDA_BIN}/ebda_psql_exec.sh -c "${sql}"
	issucc
done
###fi

#########################################
##【STEP3】Job Execution Part
#########################################
echo "【EBDA_INFO】Step3.1,3.2,3.3,3.4: Partition,Load,Update,Insert sql"
for sql in "${array_partition[@]}" "${array_load[@]}" "${array_update[@]}" "${array_insert[@]}"
do
	echo "[EBDA_INFO]Step3. Current_SQL[${sql}]"
	${EBDA_BIN}/ebda_psql_exec.sh -c "${sql}"
	issucc
done

#########################################
##【STEP4】Variables recycle
#########################################
echo "【EBDA_INFO】Step4:Vars Destroy Started"

echo "【EBDA_INFO】Job:"${EBDA_JOBNAME}"###FINISH!#########Date"`date +"%Y-%m-%d %H:%M:%S"`
echo "【EBDA_INFO】Job: ${EBDA_JOBNAME} run times:$SECONDS s"
exit 0
