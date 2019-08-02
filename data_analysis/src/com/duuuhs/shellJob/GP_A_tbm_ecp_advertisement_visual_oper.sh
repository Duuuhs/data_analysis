##################################################################
##FUNTION: LoadData :  tbl_ecp_company_visual_oper
##DATA REGEX		:  tbl_ecp_company_visual_oper 
##@author Duuuhs
##【EBDA】基本参数提示(固定的)######################################
##参数编号：0                            1              2               3                                               4                       7
##参数：sh ebda_exec_Sh.sh flowID       jobID   jobName                                 jobPath                                                 EBDA_UDC_BIZ_DATE     jobStatus       args
##示例：
##功能： 商城可视化指标数据加工                                                                             ccbcom_load_ccbcom              com.ccb.services.Ccbcom_import_ccbcom    20190706                5                       arg1            arg2            arg3
##################################################################
##【EBDA】作业参数设置(EDIT)
##################################################################
######################################################################################################
#商务企业商城运营可视化分析业务--营销分析--广告位效果分析
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
array_dataclear[0]="truncate table tbm_ecp_advertisement_pv_01;"
array_dataclear[1]="truncate table tbm_ecp_advertisement_click_01;"
array_dataclear[2]="truncate table tbm_ecp_advertisement_pv_times_01;"
array_dataclear[3]="truncate table tbm_ecp_advertisement_click_times_01;"
array_dataclear[4]="truncate table tbm_ecp_advertisement_order_01;"
array_dataclear[5]="truncate table tbm_ecp_advertisement_strike_01;"
array_dataclear[6]="delete from tbl_ecp_ad_result_analyze_detail_01 where udc_biz_date like '${EBDA_YEAR}-${EBDA_MON}-${EBDA_DAY}%' ;"




###InputJob Variables--3.1 Partition SqlArray
array_partition=()

###InputJob Variables--3.2 load Sql Array
array_load=()

##InputJob Variables--3.3 update Sql Array
array_update=()

###InputJob Variables--3.4 insert Sql Array




#1、 预处理,广告浏览数据(获取相应的广告浏览数据以及对应的渠道id)
array_insert[0]="insert into tbm_ecp_advertisement_pv_01 
(udc_biz_date,
udc_page_id,
udc_session_id,
udc_ad_dttm,
udc_ad_id,
osvt_channel_id ) 
select 
'${UDC_BIZ_DATE}',
a.udc_page_id,
a.udc_session_id,
a.udc_ad_dttm,
a.udc_ad_id,
b.osvt_channel_id 		 											
from
TBL_PAGE_AD_DISPLAY a, TBL_VISITOR_IDENTIFICATION b 
where 
a.udc_biz_date=b.udc_biz_date
and a.udc_session_id=b.udc_session_id 
and a.udc_channel_id=b.udc_channel_id
and a.udc_biz_date='${UDC_BIZ_DATE}'
and a.udc_channel_id='04'
and a.udc_ad_id in ('广告限定条件未给...'); "	




#2、 预处理,广告点击数据(获取相应的广告点击数据以及对应的渠道id)
array_insert[1]="insert into tbm_ecp_advertisement_click_01 
(udc_biz_date,
udc_page_id,
udc_session_id,
udc_ad_click_dttm,
udc_ad_id,
osvt_channel_id) 
select 
'${UDC_BIZ_DATE}',
a.udc_page_id,
a.udc_session_id,
a.udc_ad_click_dttm,
a.udc_ad_id,
b.osvt_channel_id 		 											
from
TBL_CUSTOMER_AD_CLICKS a, TBL_VISITOR_IDENTIFICATION b 
where 
a.udc_biz_date=b.udc_biz_date
and a.udc_session_id=b.udc_session_id 
and a.udc_channel_id=b.udc_channel_id
and a.udc_biz_date='${UDC_BIZ_DATE}'
and a.udc_channel_id='04'
and a.udc_ad_id in ('广告限定条件未给...'); "




#3、 广告展示次数
array_insert[2]="insert into tbm_ecp_advertisement_pv_times_01 
(udc_biz_date,
osvt_channel_id,
udc_ad_id,
udc_advertisement_pv_times)
select 						
'${UDC_BIZ_DATE}',
osvt_channel_id,
udc_ad_id,
count(1)		 											
from
tbm_ecp_advertisement_pv_01  
where udc_biz_date='${UDC_BIZ_DATE}'
group by osvt_channel_id,udc_ad_id; "



#4、 广告点击次数 
array_insert[3]="insert into tbm_ecp_advertisement_click_times_01 
(udc_biz_date,
osvt_channel_id,
udc_ad_id,
udc_advertisement_click_times)
select 						
'${UDC_BIZ_DATE}',
osvt_channel_id,
udc_ad_id,
count(1)		 											
from
tbm_ecp_advertisement_click_01 
where 
udc_biz_date='${UDC_BIZ_DATE}'
group by osvt_channel_id,udc_ad_id; "



#5、 广告下单数
array_insert[4]="insert into tbm_ecp_advertisement_order_01 
(udc_biz_date,
osvt_channel_id,
udc_ad_id,
udc_advertisement_order_times)
select 
'${UDC_BIZ_DATE}',
a.osvt_channel_id,
a.udc_ad_id,
count(1)
from tbm_ecp_advertisement_click_01 a, TBL_WEB_PAGE_VIEW b
where a.udc_biz_date=b.udc_biz_date
and a.osvt_channel_id=b.osvt_channel_id
and a.udc_page_id=b.udc_page_id
and a.udc_session_id=b.udc_session_id
and b.udc_channel_id='04'
and b.udc_current_url like '限定条件未给...'
group by a.osvt_channel_id,a.udc_ad_id;"



#6、 广告成交数
array_insert[5]="insert into tbm_ecp_advertisement_strike_01 
(udc_biz_date,
osvt_channel_id,
udc_ad_id,
udc_advertisement_strike_times)
select 
'${UDC_BIZ_DATE}',
a.osvt_channel_id,
a.udc_ad_id,
count(1)
from tbm_ecp_advertisement_click_01 a, TBL_WEB_PAGE_VIEW b
where a.udc_biz_date=b.udc_biz_date
and a.osvt_channel_id=b.osvt_channel_id
and a.udc_page_id=b.udc_page_id
and a.udc_session_id=b.udc_session_id
and b.udc_channel_id='04'
and b.udc_current_url like '限定条件未给...'
group by a.osvt_channel_id,a.udc_ad_id;"






#8、 B2B--数据插入到最终表
array_insert[7]="insert into tbl_ecp_ad_result_analyze_detail_01
(udc_biz_date,			  			--日期
 channel_flag,			  			--渠道id
 udc_ad_id,			  	  			--广告id	
 udc_advertisement_pv_times,  		--广告展示次数
 udc_advertisement_click_times, 	--广告点击次数
 udc_advertisement_click_rate, 		--广告点击率
 udc_advertisement_order_times, 	--广告下单数量
 udc_advertisement_strike_times,	--广告成交数量
 udc_advertisement_strike_amount,	--广告成交金额
 udc_advertisement_register_members --广告新增注册会员数
 )
 select
 '${EBDA_YEAR}-${EBDA_MON}-${EBDA_DAY}'||' 00:00:00',										 --日期
 a.osvt_channel_id,																			 --渠道id
 a.udc_ad_id,																				 --广告id
 coalesce(to_char(a.udc_advertisement_pv_times,'FM999,999,999,999,999'),'0'),				 --广告展示次数   
 coalesce(to_char(b.udc_advertisement_click_times,'FM999,999,999,999,999'),'0'),			 --广告点击次数
 round(coalesce(b.udc_advertisement_click_times,0)*100/a.udc_advertisement_pv_times,2)||'%', --广告点击率
 coalesce(to_char(a.udc_advertisement_pv_times,'FM999,999,999,999,999'),'0'), 				 --广告下单数量
 coalesce(to_char(a.udc_advertisement_pv_times,'FM999,999,999,999,999'),'0'),				 --广告成交数量
 '0',																						 --广告成交金额
 '0'																						 --广告新增注册会员数
 from 
 dzyh_app.tbm_ecp_advertisement_pv_times_01 a 
 left join tbm_ecp_advertisement_click_times_01 b on a.udc_biz_date=b.udc_biz_date and a.osvt_channel_id=b.osvt_channel_id and a.udc_ad_id=b.udc_ad_id
 left join tbm_ecp_advertisement_order_01 c on a.udc_biz_date=c.udc_biz_date and a.osvt_channel_id=c.osvt_channel_id and a.udc_ad_id=c.udc_ad_id
 left join tbm_ecp_advertisement_strike_01 d on a.udc_biz_date=d.udc_biz_date and a.osvt_channel_id=d.osvt_channel_id and a.udc_ad_id=d.udc_ad_id;" 
 
 







#########################################
##【STEP2】Data_Clear
#########################################
echo "【EBDA_INFO】Step2:DataClear Method Started,Status: "${EBDA_JOBSTATUS}

if [ ${EBDA_JOBSTATUS} -eq 5 ];then
echo "【EBDA_INFO】,jobStatus is 5 ==> DataClear Started"
###Clear the data on Hive
for sql in "${array_dataclear[@]}"
do
	echo "[EBDA_INFO]Step3. Current_SQL[${sql}]"
	${EBDA_BIN}/ebda_psql_exec.sh -c "${sql}"
	issucc
done
fi

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
