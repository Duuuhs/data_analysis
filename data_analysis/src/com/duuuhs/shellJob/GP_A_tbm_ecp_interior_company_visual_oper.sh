##################################################################
##FUNTION: LoadData :  tbl_ecp_company_visual_oper
##DATA REGEX		:  tbl_ecp_company_visual_oper 
##@author dumingyuan
##【EBDA】基本参数提示(固定的)######################################
##参数编号：0                            1              2               3                                               4                       7
##参数：sh ebda_exec_Sh.sh flowID       jobID   jobName                                 jobPath                                                 EBDA_UDC_BIZ_DATE     jobStatus       args
##示例：
##功能： 商城可视化指标数据加工                                                                             ccbcom_load_ccbcom              com.ccb.services.Ccbcom_import_ccbcom    20160811                5                       arg1            arg2            arg3
##################################################################
##【EBDA】作业参数设置(EDIT)
##################################################################
######################################################################################################
#善融商务企业商城运营可视化分析业务--内部引流
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
echo "==================""${last_date}"
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
array_dataclear[1]="truncate table tbm_rule_comp2_interior_01;"
array_dataclear[2]="truncate table tbm_ecp_pc_pv_interior_01;"
array_dataclear[3]="truncate table tbm_ecp_pc_visitor_interior_01;"
array_dataclear[4]="truncate table tbm_web_page_view_interior_01;"
array_dataclear[5]="truncate table tbm_ecp_pc_visitor_interior_03;"
array_dataclear[6]="truncate table tbm_ecp_pc_web_interior_01;"
array_dataclear[7]="truncate table tbm_ecp_pc_time_all_interior_01;"
array_dataclear[8]="truncate table tbm_ecp_pc_time_ave_interior_01;"
array_dataclear[9]="truncate table tbm_ecp_pc_shop_interior_01;"
array_dataclear[10]="truncate table tbm_ecp_pc_order_interior_01;"
array_dataclear[12]="truncate table tbm_ecp_pc_new_interior_02;"
array_dataclear[13]="delete from tbl_ecp_sources_flow_detail where sources_flag='02' and udc_biz_date like '${EBDA_YEAR}-${EBDA_MON}-${EBDA_DAY}'||' 00:00:00';"

###InputJob Variables--3.1 Partition SqlArray
array_partition=()

###InputJob Variables--3.2 load Sql Array
array_load=()

##InputJob Variables--3.3 update Sql Array
array_update=()

###InputJob Variables--3.4 insert Sql Array

###########开始处理企业PC数据...
#1 预处理
array_insert[0]="insert into tbm_web_page_view_interior_01    -----新建表
(UDC_BIZ_DATE,
UDC_SESSION_ID,
UDC_PAGE_ID,
UDC_PAGE_VISIT_TIME,
UDC_SOURCE_URL,
UDC_CURRENT_URL,
OSVT_CHANNEL_ID,
UDC_CHANNEL_INTERIOR) 
select 
UDC_BIZ_DATE,
UDC_SESSION_ID,
UDC_PAGE_ID,
UDC_PAGE_VISIT_TIME,
UDC_SOURCE_URL,
UDC_CURRENT_URL,
OSVT_CHANNEL_ID,
(case 
when substr(UDC_SOURCE_URL,1,30) like '%type=ccb%' then '建行网站群'
when substr(UDC_SOURCE_URL,1,30) like '%srswqysc&source_type=mccb%' then '移动门户' 
when UDC_SOURCE_URL like '%EBSB2B&adv_id=qywytcgg01%' then '企业网银' 
when UDC_SOURCE_URL like '%srswqysc&source_type=qysjyh%' then '企业手机银行' 
when UDC_SOURCE_URL like '%srswqysc&source_type=appshfw%' then '商户服务平台' 
when substr(UDC_SOURCE_URL,1,30) like '%type=grsjyh%' then '个人手机银行' 
when substr(UDC_SOURCE_URL,1,30) like '%type=wxyh%' then '微信银行' 
when UDC_SOURCE_URL like '%sft.ccb.com%' then '****' 
when UDC_SOURCE_URL like '%http://****/bepay/thirdPartAPI.php%' then '外联-****对接' 
when UDC_SOURCE_URL like '%http://****/alliance/thirdPartAPI.php%' then '外联-主站对接' 
when UDC_SOURCE_URL like '%jc.ccb.com%' then '****频道' 
when UDC_SOURCE_URL like '%/bepay/m_index_noauth.php%' then '企业版手机银行****引流'
else '其他' end)	
from 
TBL_WEB_PAGE_VIEW
where 
udc_biz_date='${UDC_BIZ_DATE}'
and UDC_CHANNEL_ID='04';"

#2 页面浏览数、访问次数
array_insert[1]="insert into tbm_ecp_pc_pv_interior_01 
(udc_biz_date,
channel_flag,
udc_channel_interior,
pandect_pv,
channel_visit_times) 
select 
'${UDC_BIZ_DATE}',
a.OSVT_CHANNEL_ID,
a.UDC_CHANNEL_INTERIOR,
count(1),
count(distinct a.udc_session_id) 
from 
tbm_web_page_view_interior_01 a 
where a.UDC_CHANNEL_INTERIOR !='其他'
group by a.OSVT_CHANNEL_ID,a.UDC_CHANNEL_INTERIOR;"

#3 访客数
array_insert[2]="insert into tbm_ecp_pc_visitor_interior_01 
(udc_biz_date,
channel_flag,
udc_channel_interior,
channel_visitor) 
select 
'${UDC_BIZ_DATE}',
visitor1.OSVT_CHANNEL_ID,
visitor1.UDC_CHANNEL_INTERIOR,
coalesce(visitor1.num,0)+coalesce(visitor2.num,0)		--预防遇到null值结果为null的情况
from
(
select a.OSVT_CHANNEL_ID,  --渠道ID
	   b.UDC_CHANNEL_INTERIOR,
	   count(distinct a.udc_customer_id) num
from
TBL_VISITOR_IDENTIFICATION a,tbm_web_page_view_interior_01 b
where 
a.udc_biz_date=b.udc_biz_date
and a.UDC_SESSION_ID=b.UDC_SESSION_ID
and a.OSVT_CHANNEL_ID=b.OSVT_CHANNEL_ID
and a.udc_biz_date='${UDC_BIZ_DATE}' 
and a.udc_customer_id is not null 
and a.udc_customer_id!=''
and b.UDC_CHANNEL_INTERIOR !='其他'
group by a.OSVT_CHANNEL_ID,b.UDC_CHANNEL_INTERIOR) visitor1 
left join 
(
select a.OSVT_CHANNEL_ID,  --渠道ID
	   b.UDC_CHANNEL_INTERIOR,
	   count(distinct a.udc_cookie) num 
from
TBL_VISITOR_IDENTIFICATION a,tbm_web_page_view_interior_01 b
where 
a.udc_biz_date=b.udc_biz_date
and a.UDC_SESSION_ID=b.UDC_SESSION_ID
and a.OSVT_CHANNEL_ID=b.OSVT_CHANNEL_ID
and a.udc_biz_date='${UDC_BIZ_DATE}' 
and (a.udc_customer_id is null or a.udc_customer_id='')
and b.UDC_CHANNEL_INTERIOR !='其他'
group by a.OSVT_CHANNEL_ID,b.UDC_CHANNEL_INTERIOR) visitor2 
on visitor1.OSVT_CHANNEL_ID=visitor2.OSVT_CHANNEL_ID 
and visitor1.UDC_CHANNEL_INTERIOR=visitor2.UDC_CHANNEL_INTERIOR;"

#4 平均网站访问时长,预处理(总访问时长)
array_insert[3]="insert into tbm_ecp_pc_time_all_interior_01 
(channel_flag,udc_channel_interior,udc_tmp_visit_time)
select 
b.OSVT_CHANNEL_ID,
b.UDC_CHANNEL_INTERIOR,
sum(udc_page_stay_time) 
from 
(select 
OSVT_CHANNEL_ID,
UDC_CHANNEL_INTERIOR,
extract(EPOCH from udc_page_stay_time) udc_page_stay_time --将时长interval类型转换为数字（秒）
from (select udc_session_id,OSVT_CHANNEL_ID,udc_page_id,UDC_CHANNEL_INTERIOR,lead(to_timestamp(UDC_PAGE_VISIT_TIME,'YYYYMMDDHH24MISSMS')) over(partition by udc_session_id,OSVT_CHANNEL_ID order by UDC_PAGE_VISIT_TIME) - to_timestamp(UDC_PAGE_VISIT_TIME, 'YYYYMMDDHH24MISSMS') udc_page_stay_time
      from tbm_web_page_view_interior_01 
         where UDC_PAGE_VISIT_TIME ~ '^[0-9]{17}$' --过滤不合格式的数据
                ) a where a.udc_page_stay_time<'1800') b
				where b.UDC_CHANNEL_INTERIOR !='其他'
						group by b.OSVT_CHANNEL_ID,b.UDC_CHANNEL_INTERIOR;--筛选异常数据"
				
#5 平均网站访问时长,预处理(平均访问时长)
array_insert[4]="insert into tbm_ecp_pc_time_ave_interior_01 
(channel_flag,udc_channel_interior,udc_tmp_visit_time)
select 
a.channel_flag,
a.UDC_CHANNEL_INTERIOR,
lpad(trunc(a.udc_tmp_visit_time/b.channel_visit_times/3600),2,'0')||':'||lpad(trunc(a.udc_tmp_visit_time/b.channel_visit_times%3600/60),2,'0')||':'||lpad(trunc(a.udc_tmp_visit_time/b.channel_visit_times%3600%60%60),2,'0')||'' 
from 
tbm_ecp_pc_time_all_interior_01 a,
tbm_ecp_pc_pv_interior_01 b 
where 
a.channel_flag=b.channel_flag
and a.UDC_CHANNEL_INTERIOR=b.UDC_CHANNEL_INTERIOR;"				

#6 网站跳出率，预处理
array_insert[5]="insert into tbm_rule_comp2_interior_01
(channel_flag,udc_channel_interior,channel_web_bounce)
select 
b.OSVT_CHANNEL_ID,
b.UDC_CHANNEL_INTERIOR,
count(1)
from
(select 
UDC_SESSION_ID,
count(1) as num
from 
tbm_web_page_view_interior_01 
group by UDC_SESSION_ID
having count(1)=1 ) a				--筛选出跳出为1的情况
left join tbm_web_page_view_interior_01  b
on a.UDC_SESSION_ID=b.UDC_SESSION_ID 
where b.UDC_CHANNEL_INTERIOR !='其他'
group by b.OSVT_CHANNEL_ID,b.UDC_CHANNEL_INTERIOR;"	

#7 浏览商品次数
array_insert[6]="insert into tbm_ecp_pc_shop_interior_01
(channel_flag,
udc_channel_interior,
udc_tmp_visit_time) 
select
a.OSVT_CHANNEL_ID,
a.UDC_CHANNEL_INTERIOR,
count(distinct a.udc_session_id) 
from 
tbm_web_page_view_interior_01 a 
where a.UDC_CHANNEL_INTERIOR !='其他'
group by a.OSVT_CHANNEL_ID,a.UDC_CHANNEL_INTERIOR;"

#8 下单次数
array_insert[7]="insert into tbm_ecp_pc_order_interior_01
(channel_flag,
udc_channel_interior,
udc_tmp_visit_time) 
select
a.OSVT_CHANNEL_ID,
a.UDC_CHANNEL_INTERIOR,
count(distinct a.udc_session_id) 
from 
tbm_web_page_view_interior_01 a 
where (UDC_CURRENT_URL like 'http://****/ecp/view/orderView/orderSubmit%'  
or UDC_CURRENT_URL like 'http://****/member/unitAddOrder.jhtml%')
and a.UDC_CHANNEL_INTERIOR !='其他'
group by a.OSVT_CHANNEL_ID,a.UDC_CHANNEL_INTERIOR;"


#9 新访次数 (一条处理) --create on 2019/07/31
array_insert[8]="insert into tbm_ecp_pc_new_interior_02 
(channel_flag,udc_channel_interior,udc_tmp_visit_time) 
select 
d.osvt_channel_id,
d.udc_channel_interior,
count(distinct d.udc_session_id)
from
(
(select 
 a.udc_session_id,a.osvt_channel_id
 from 
 (select udc_session_id,udc_customer_id,osvt_channel_id from tbl_visitor_identification 
  where udc_biz_date='${UDC_BIZ_DATE}' 
  and udc_customer_id is not null
  and udc_customer_id !=''
  ) a 
  right join 
 (select udc_visitor_sequence from tbl_visitor_first_identification
  where udc_biz_date='${UDC_BIZ_DATE}'
  and udc_visitor_type='01'
  ) b
  on a.udc_customer_id=b.udc_visitor_sequence
) 												--客户udc_session_id
union 
(select 
 a.udc_session_id,a.osvt_channel_id
 from 
 (select udc_session_id,udc_cookie,osvt_channel_id from tbl_visitor_identification 
  where udc_biz_date='${UDC_BIZ_DATE}' 
  and (udc_customer_id is null or udc_customer_id='')
  ) a 
  right join 
 (select udc_visitor_sequence from tbl_visitor_first_identification
  where udc_biz_date='${UDC_BIZ_DATE}'
  and udc_visitor_type='02'
  ) b
  on a.udc_cookie=b.udc_visitor_sequence
)												--游客udc_session_id
) as c											--当天新增访客数的所有udc_session_id
inner join 
tbm_web_page_view_interior_01 d
on c.udc_session_id=d.udc_session_id
and c.osvt_channel_id=d.osvt_channel_id
where d.udc_channel_interior !='其他'
group by d.OSVT_CHANNEL_ID,d.udc_channel_interior;"


#10 数据入最终表
array_insert[9]="insert into tbl_ecp_sources_flow_detail 
(sources_visitor,
sources_visit_times,
sources_pv,
sources_web_bounce,
sources_visit_number_ave,
sources_visit_time_ave,
sources_visit_times_new,
sources_visit_percent_new,
sources_visit_comm_times,
sources_order_conv,
sources_type,
sources_flag,
channel_flag,
udc_biz_date) 
select 
coalesce(to_char(b.channel_visitor,'FM999,999,999,999,999'),'0'),
coalesce(to_char(a.channel_visit_times,'FM999,999,999,999,999'),'0'),
coalesce(to_char(a.pandect_pv,'FM999,999,999,999,999'),'0'),
coalesce(round((d.channel_web_bounce)*100/a.channel_visit_times,2)||'%','0.00%'),
coalesce(round(a.pandect_pv/a.channel_visit_times),'0'),
coalesce(c.udc_tmp_visit_time,'0'),
coalesce(t.udc_tmp_visit_time,'0'),
coalesce(round((t.udc_tmp_visit_time)*100/a.channel_visit_times,2)||'%','0.00%'),
coalesce(e.udc_tmp_visit_time,'0'),
coalesce(round((f.udc_tmp_visit_time)*100/a.channel_visit_times,2)||'%','0.00%'),
coalesce(a.udc_channel_interior,'null'),
'02',
a.channel_flag,
'${EBDA_YEAR}-${EBDA_MON}-${EBDA_DAY}'||' 00:00:00'
from 
tbm_ecp_pc_pv_interior_01 a left join
tbm_ecp_pc_visitor_interior_01 b on a.channel_flag=b.channel_flag and a.udc_channel_interior=b.udc_channel_interior left join
tbm_ecp_pc_time_ave_interior_01 c on a.channel_flag=c.channel_flag and a.udc_channel_interior=c.udc_channel_interior left join
tbm_rule_comp2_interior_01 d on a.channel_flag=d.channel_flag and a.udc_channel_interior=d.udc_channel_interior left join
tbm_ecp_pc_shop_interior_01 e on a.channel_flag=e.channel_flag and a.udc_channel_interior=e.udc_channel_interior left join
tbm_ecp_pc_order_interior_01 f on a.channel_flag=f.channel_flag and a.udc_channel_interior=f.udc_channel_interior left join 
tbm_ecp_pc_new_interior_02 t on a.channel_flag=t.channel_flag and a.udc_channel_interior=t.udc_channel_interior;"


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
