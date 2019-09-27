##################################################################
##FUNTION: LoadData :  tbl_****_company_visual_visitor
##DATA REGEX		:  tbl_****_company_visual_visitor 
##@author dumingyuan
##【EBDA】基本参数提示(固定的)######################################
##参数编号：0                            1              2               3                                               4                       7
##参数：sh ebda_exec_Sh.sh flowID       jobID   jobName                                 jobPath                                                 EBDA_UDC_BIZ_DATE     jobStatus       args
##示例：
##功能： 善融商城可视化指标数据加工                                                                             ccbcom_load_ccbcom              com.ccb.services.Ccbcom_import_ccbcom    20160811                5                       arg1            arg2            arg3
##################################################################
##【EBDA】作业参数设置(EDIT)
##################################################################
######################################################################################################
#商务企业商城运营可视化分析业务--地区分布
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
array_dataclear[1]="truncate table ****.tbm_ip_session;"
array_dataclear[2]="truncate table ****.tbm_web_page_view_region;"
array_dataclear[3]="truncate table ****.tbm_****_pc_pv_region_01;"
array_dataclear[4]="truncate table ****.tbm_****_pc_visitor_region_01;"
array_dataclear[5]="truncate table ****.tbm_****_pc_visitor1_region_01;"
array_dataclear[6]="truncate table ****.tbm_****_pc_visitor2_region_01;"
array_dataclear[7]="truncate table ****.tbm_****_pc_time_all_region_01;"
array_dataclear[8]="truncate table ****.tbm_****_pc_time_ave_region_01;"
array_dataclear[9]="truncate table ****.tbm_rule_comp2_region_01;"
array_dataclear[10]="truncate table ****.tbm_****_pc_web_region_01;"
array_dataclear[11]="truncate table ****.tbm_****_pc_order_region_01;"
array_dataclear[12]="truncate table ****.tbm_****_pc_shop_region_01;"
array_dataclear[14]="truncate table ****.tbm_****_pc_new_region_02;"
array_dataclear[15]="delete from ****.tbl_****_company_visual_visitor where udc_biz_date like '${EBDA_YEAR}-${EBDA_MON}-${EBDA_DAY}%' and regional_class='05';"

###InputJob Variables--3.1 Partition SqlArray
array_partition=()

###InputJob Variables--3.2 load Sql Array
array_load=()

##InputJob Variables--3.3 update Sql Array
array_update=()

###InputJob Variables--3.4 insert Sql Array

###########预处理IP
#1 IP处理
array_insert[0]="insert into ****.tbm_ip_session 
(UDC_SESSION_ID,
IP) 
select 
distinct a.UDC_SESSION_ID,
a.UDC_IP
from 
****.TBL_WEB_SESSION a
where 
a.udc_biz_date='${UDC_BIZ_DATE}'
and a.UDC_CHANNEL_ID='04';"

#2 页面浏览表预处理
array_insert[1]="insert into ****.tbm_web_page_view_region   
(UDC_BIZ_DATE,
UDC_SESSION_ID,
UDC_PAGE_ID,
UDC_PAGE_VISIT_TIME,
UDC_SOURCE_URL,
UDC_CURRENT_URL,
OSVT_CHANNEL_ID,
UDC_CHANNEL_REGION) 
select 
'${UDC_BIZ_DATE}',
a.UDC_SESSION_ID,
a.UDC_PAGE_ID,
a.UDC_PAGE_VISIT_TIME,
a.UDC_SOURCE_URL,
a.UDC_CURRENT_URL,
a.OSVT_CHANNEL_ID,
c.pcode
from 
****.TBL_WEB_PAGE_VIEW a 
left join tbm_ip_session b on a.UDC_SESSION_ID=b.UDC_SESSION_ID
left join tbl_ip_province c on b.ip=c.ip
and a.udc_biz_date='${UDC_BIZ_DATE}'
and a.UDC_CHANNEL_ID='04';"


###########开始处理企业PC数据...
#3 页面浏览数、访问次数
array_insert[2]="insert into ****.tbm_****_pc_pv_region_01 
(udc_biz_date,
channel_flag,
channel_pcode,
pandect_pv,
channel_visit_times) 
select 
'${UDC_BIZ_DATE}',
OSVT_CHANNEL_ID,
UDC_CHANNEL_REGION, --省份编码
count(1),
count(distinct udc_session_id) 
from 
****.tbm_web_page_view_region
group by OSVT_CHANNEL_ID,UDC_CHANNEL_REGION;"

#4 访客数
array_insert[3]="insert into ****.tbm_****_pc_visitor_region_01   
(udc_biz_date,
channel_flag,
channel_region,
channel_visitor) 
select 
'${UDC_BIZ_DATE}',
visitor1.OSVT_CHANNEL_ID,
visitor1.UDC_CHANNEL_REGION,
coalesce(visitor1.num,0)+coalesce(visitor2.num,0)		--预防遇到null值结果为null的情况
from
(
select a.OSVT_CHANNEL_ID,  --渠道ID
	   b.UDC_CHANNEL_REGION,
	   count(distinct a.udc_customer_id) num
from
****.TBL_VISITOR_IDENTIFICATION a,****.tbm_web_page_view_region b
where 
a.udc_biz_date=b.udc_biz_date
and a.UDC_SESSION_ID=b.UDC_SESSION_ID
and a.OSVT_CHANNEL_ID=b.OSVT_CHANNEL_ID
and a.udc_biz_date='${UDC_BIZ_DATE}' 
and a.udc_customer_id is not null 
and a.udc_customer_id!=''
group by a.OSVT_CHANNEL_ID,b.UDC_CHANNEL_REGION) visitor1 
left join 
(
select a.OSVT_CHANNEL_ID,  --渠道ID
	   b.UDC_CHANNEL_REGION,
	   count(distinct a.udc_cookie) num 
from
****.TBL_VISITOR_IDENTIFICATION a,****.tbm_web_page_view_region b
where 
a.udc_biz_date=b.udc_biz_date
and a.UDC_SESSION_ID=b.UDC_SESSION_ID
and a.OSVT_CHANNEL_ID=b.OSVT_CHANNEL_ID
and a.udc_biz_date='${UDC_BIZ_DATE}' 
and (a.udc_customer_id is null or a.udc_customer_id='')
group by a.OSVT_CHANNEL_ID,b.UDC_CHANNEL_REGION) visitor2 
on visitor1.OSVT_CHANNEL_ID=visitor2.OSVT_CHANNEL_ID 
and visitor1.UDC_CHANNEL_REGION=visitor2.UDC_CHANNEL_REGION;"

#5 平均网站访问时长,预处理(总访问时长)
array_insert[4]="insert into ****.tbm_****_pc_time_all_region_01 
(channel_flag,channel_ip,udc_tmp_visit_time)
select 
t.OSVT_CHANNEL_ID,
t.UDC_CHANNEL_REGION , --省份编码
sum(extract(EPOCH from udc_page_stay_time)) udc_page_stay_time --将时长interval类型转换为数字（秒）
from (select a.OSVT_CHANNEL_ID,a.UDC_CHANNEL_REGION,a.udc_session_id,a.udc_page_id,lead(to_timestamp(a.UDC_PAGE_VISIT_TIME,'YYYYMMDDHH24MISSMS')) over(partition by a.udc_session_id order by a.UDC_PAGE_VISIT_TIME) - to_timestamp(a.UDC_PAGE_VISIT_TIME, 'YYYYMMDDHH24MISSMS') udc_page_stay_time
      from ****.tbm_web_page_view_region a  
         where a.UDC_PAGE_VISIT_TIME ~ '^[0-9]{17}$' --过滤不合格式的数据
               --group by a.OSVT_CHANNEL_ID,a.UDC_CHANNEL_REGION,a.udc_session_id,a.udc_page_id,a.UDC_PAGE_VISIT_TIME
			   ) t group by t.OSVT_CHANNEL_ID,t.UDC_CHANNEL_REGION;"
				
#6 平均网站访问时长,预处理(平均访问时长)
array_insert[5]="insert into ****.tbm_****_pc_time_ave_region_01 
(channel_flag,channel_ip,udc_tmp_visit_time)
select 
a.channel_flag,
a.channel_ip, --省份编码
lpad(trunc(a.udc_tmp_visit_time/b.channel_visit_times/3600),2,'0')||':'||lpad(trunc(a.udc_tmp_visit_time/b.channel_visit_times%3600/60),2,'0')||':'||lpad(trunc(a.udc_tmp_visit_time/b.channel_visit_times%3600%60%60),2,'0')||'' 
from 
tbm_****_pc_time_all_region_01 a,
tbm_****_pc_pv_region_01 b 
where 
a.channel_flag=b.channel_flag
and a.channel_ip=b.channel_pcode
group by a.channel_flag,a.channel_ip,a.udc_tmp_visit_time,b.channel_visit_times;"				

#7 网站跳出次数
array_insert[6]="insert into ****.tbm_rule_comp2_region_01   
(channel_flag,channel_ip,udc_tmp_count) 
select 
b.OSVT_CHANNEL_ID,
b.UDC_CHANNEL_REGION,
count(1)
from
(select 
UDC_SESSION_ID,
count(1) as num
from 
****.tbm_web_page_view_region 
group by UDC_SESSION_ID
having count(1)=1 ) a				--筛选出跳出为1的情况
left join ****.tbm_web_page_view_region  b
on a.UDC_SESSION_ID=b.UDC_SESSION_ID 
group by b.OSVT_CHANNEL_ID,b.UDC_CHANNEL_REGION;"	

#8 浏览商品次数
array_insert[7]="insert into ****.tbm_****_pc_shop_region_01 
(channel_flag,channel_ip,udc_tmp_visit_time) 
select 
OSVT_CHANNEL_ID,
UDC_CHANNEL_REGION , --省份编码
count(distinct udc_session_id) 
from 
****.tbm_web_page_view_region
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
) group by OSVT_CHANNEL_ID,UDC_CHANNEL_REGION;"

#9 下单完成次数
array_insert[8]="insert into ****.tbm_****_pc_order_region_01 
(channel_flag,channel_ip,channel_order) 
select 
OSVT_CHANNEL_ID,
UDC_CHANNEL_REGION, --省份编码
count(distinct udc_session_id) 
from 
****.tbm_web_page_view_region
where (UDC_CURRENT_URL like 'http://****/****/view/orderView/orderSubmit%'
or UDC_CURRENT_URL like 'http://****/member/unitAddOrder.jhtml%')
group by OSVT_CHANNEL_ID,UDC_CHANNEL_REGION;"


#10 新访次数 (一条处理) --create on 2019/07/31
array_insert[9]="insert into ****.tbm_****_pc_new_region_02 
(channel_flag,channel_ip,udc_tmp_visit_time)
select 
d.OSVT_CHANNEL_ID,
d.UDC_CHANNEL_REGION, --省份编码 
count(distinct d.udc_session_id)
from
(
(select 
 a.udc_session_id,a.osvt_channel_id
 from 
 (select udc_session_id,udc_customer_id,osvt_channel_id from ****.tbl_visitor_identification 
  where udc_biz_date='${UDC_BIZ_DATE}' 
  and udc_customer_id is not null
  and udc_customer_id !=''
  ) a 
  right join 
 (select udc_visitor_sequence from ****.tbl_visitor_first_identification
  where udc_biz_date='${UDC_BIZ_DATE}'
  and udc_visitor_type='01'
  ) b
  on a.udc_customer_id=b.udc_visitor_sequence
) 												--客户udc_session_id
union 
(select 
 a.udc_session_id,a.osvt_channel_id
 from 
 (select udc_session_id,udc_cookie,osvt_channel_id from ****.tbl_visitor_identification 
  where udc_biz_date='${UDC_BIZ_DATE}' 
  and (udc_customer_id is null or udc_customer_id='')
  ) a 
  right join 
 (select udc_visitor_sequence from ****.tbl_visitor_first_identification
  where udc_biz_date='${UDC_BIZ_DATE}'
  and udc_visitor_type='02'
  ) b
  on a.udc_cookie=b.udc_visitor_sequence
)												--游客udc_session_id
) as c											--当天新增访客数的所有udc_session_id
inner join 
****.tbm_web_page_view_region d
on c.udc_session_id=d.udc_session_id
and c.osvt_channel_id=d.osvt_channel_id
group by d.OSVT_CHANNEL_ID,d.UDC_CHANNEL_REGION;"


#11 数据插入最终表
array_insert[10]="insert into ****.tbl_****_company_visual_visitor
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
udc_biz_date
--channel_index
) 
select  
'05',
coalesce(c.area_name,'其他'),
coalesce(to_char(b.channel_visitor,'FM999,999,999,999,999'),'0'),
coalesce(to_char(a.channel_visit_times,'FM999,999,999,999,999'),'0'),
coalesce(to_char(a.pandect_pv,'FM999,999,999,999,999'),'0'),
coalesce(round((e.udc_tmp_count)*100/a.channel_visit_times,2)||'%','0.00%'),
coalesce(round(a.pandect_pv/a.channel_visit_times,0),'0'),
coalesce(d.udc_tmp_visit_time,'0'),
coalesce(t.udc_tmp_visit_time,'0'),
coalesce(round((t.udc_tmp_visit_time)*100/a.channel_visit_times,2)||'%','0.00%'),
coalesce(to_char(f.udc_tmp_visit_time,'FM999,999,999,999,999'),'0'),
coalesce(round((g.channel_order)*100/a.channel_visit_times,2)||'%','0.00%'),
a.channel_flag,
'${EBDA_YEAR}-${EBDA_MON}-${EBDA_DAY}'||' 00:00:00'
from 
tbm_****_pc_pv_region_01 a left join
tbv_province_city_code c on a.channel_pcode=c.area_num left join
tbm_****_pc_visitor_region_01 b on a.channel_flag=b.channel_flag and a.channel_pcode=b.channel_region left join
tbm_****_pc_time_ave_region_01 d on a.channel_flag=d.channel_flag and a.channel_pcode=d.channel_ip left join
tbm_rule_comp2_region_01 e on a.channel_flag=e.channel_flag and a.channel_pcode=e.channel_ip left join
tbm_****_pc_shop_region_01 f on a.channel_flag=f.channel_flag and a.channel_pcode=f.channel_ip left join
tbm_****_pc_order_region_01 g on a.channel_flag=g.channel_flag and a.channel_pcode=g.channel_ip left join
tbm_****_pc_new_region_02 t on a.channel_flag=t.channel_flag and a.channel_pcode=t.channel_ip;" 

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
