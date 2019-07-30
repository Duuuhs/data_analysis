##################################################################
##FUNTION: LoadData :  tbl_ecp_company_visual_oper
##DATA REGEX		:  tbl_ecp_company_visual_oper 
##@author dumingyuan
##【EBDA】基本参数提示(固定的)######################################
##参数编号：0                            1              2               3                                               4                       7
##参数：sh ebda_exec_Sh.sh flowID       jobID   jobName                                 jobPath                                                 EBDA_UDC_BIZ_DATE     jobStatus       args
##示例：
##功能： 善融商城可视化指标数据加工                                                                             ccbcom_load_ccbcom              com.ccb.services.Ccbcom_import_ccbcom    20190701                5                       arg1            arg2            arg3
##################################################################
##【EBDA】作业参数设置(EDIT)
##################################################################
######################################################################################################
#善融商务企业商城运营可视化分析业务--站内搜索
######################################################################################################
EBDA_DIR=/home/ap/dip/appjob/shelljob/dzyh
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
array_dataclear[0]="truncate table dzyh_app.tbm_ecp_internal_serach_pv_01;"
array_dataclear[1]="truncate table dzyh_app.tbm_ecp_internal_serach_users_01;"
array_dataclear[2]="delete from dzyh_app.tbl_ecp_web_search_detail where udc_biz_date like '${EBDA_YEAR}-${EBDA_MON}-${EBDA_DAY}%' ;"




###InputJob Variables--3.1 Partition SqlArray
array_partition=()



###InputJob Variables--3.2 load Sql Array
array_load=()



##InputJob Variables--3.3 update Sql Array
array_update=()



###InputJob Variables--3.4 insert Sql Array
#1、 站内搜索次数,站内搜索访问次数
array_insert[0]="insert into dzyh_app.tbm_ecp_internal_serach_pv_01 
(udc_biz_date,
osvt_channel_id,
udc_internal_search_keyword,
search_insta_times,
search_insta_visit_times
) 
select 
'${UDC_BIZ_DATE}',
osvt_channel_id,
udc_internal_search_keyword,
count(udc_session_id),				--站内搜索次数
count(distinct(udc_session_id))		--站内搜索访问次数
from 
dzyh_app.TBL_CUST_SEARCH_KEYWORD
where
udc_biz_date='${UDC_BIZ_DATE}'
and udc_channel_id='04'
and udc_internal_search_keyword !=''
and udc_internal_search_keyword is not null
group by osvt_channel_id,udc_internal_search_keyword;"							

#2、 站内搜索用户数
array_insert[1]="insert into dzyh_app.tbm_ecp_internal_serach_users_01 
(udc_biz_date,
osvt_channel_id,
udc_internal_search_keyword,
search_insta_users) 
select 
'${UDC_BIZ_DATE}',
a.osvt_channel_id,
a.udc_internal_search_keyword,
count(distinct(b.udc_customer_id))		--站内搜索用户数
from
dzyh_app.TBL_CUST_SEARCH_KEYWORD a, dzyh_app.TBL_VISITOR_IDENTIFICATION b
where a.UDC_BIZ_DATE=b.UDC_BIZ_DATE
and a.UDC_CHANNEL_ID=b.UDC_CHANNEL_ID
and a.UDC_SESSION_ID=b.UDC_SESSION_ID
and a.udc_biz_date = '${UDC_BIZ_DATE}'
and a.UDC_CHANNEL_ID='04'
and a.udc_internal_search_keyword !=''
and a.udc_internal_search_keyword is not null
and b.udc_customer_id !=''
and b.udc_customer_id is not null
group by a.osvt_channel_id,a.udc_internal_search_keyword;" 

#3、 数据插入到最终表
array_insert[2]="insert into dzyh_app.tbl_ecp_web_search_detail
(udc_biz_date,			
 channel_flag,  		
 search_name,
 search_insta_times,
 search_insta_visit_times,
 search_insta_users											
 )
 select
 '${EBDA_YEAR}-${EBDA_MON}-${EBDA_DAY}'||' 00:00:00',							
 a.osvt_channel_id,
 a.udc_internal_search_keyword,													--关键词名称
 coalesce(to_char(a.search_insta_times,'FM999,999,999,999,999'),'0'),			--站内搜索次数
 coalesce(to_char(a.search_insta_visit_times,'FM999,999,999,999,999'),'0'),		--站内搜索访问次数
 coalesce(to_char(b.search_insta_users,'FM999,999,999,999,999'),'0')			--站内搜索用户数
 from 
 dzyh_app.tbm_ecp_internal_serach_pv_01 a 
 left join dzyh_app.tbm_ecp_internal_serach_users_01 b on a.udc_biz_date=b.udc_biz_date 
 and a.osvt_channel_id=b.osvt_channel_id 
 and a.udc_internal_search_keyword=b.udc_internal_search_keyword;" 









#########################################
##【STEP2】Data_Clear
#########################################
echo "【EBDA_INFO】Step2:DataClear Method Started,Status: "${EBDA_JOBSTATUS}

#if [ ${EBDA_JOBSTATUS} -eq 5 ];then
echo "【EBDA_INFO】,jobStatus is 5 ==> DataClear Started"
##Clear the data on Hive
for sql in "${array_dataclear[@]}"
do
	echo "[EBDA_INFO]Step3. Current_SQL[${sql}]"
	${EBDA_BIN}/ebda_psql_exec.sh -c "${sql}"
	issucc
done
#fi

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
