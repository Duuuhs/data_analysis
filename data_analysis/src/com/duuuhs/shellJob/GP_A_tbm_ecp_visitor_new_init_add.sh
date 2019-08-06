##################################################################
##FUNTION: LoadData :  tbl_ecp_company_visual_top
##DATA REGEX		:  tbl_ecp_company_visual_top 
##@author dumingyuan
##【EBDA】基本参数提示(固定的)######################################
##参数编号：0                            1              2               3                                               4                       7
##参数：sh ebda_exec_Sh.sh flowID       jobID   jobName                                 jobPath                                                 EBDA_UDC_BIZ_DATE     jobStatus       args
##示例：
##功能： 商城可视化指标数据加工                                                                             ccbcom_load_ccbcom              com.ccb.services.Ccbcom_import_ccbcom    20190730                5                       arg1            arg2            arg3
##################################################################
##【EBDA】作业参数设置(EDIT)
##################################################################
######################################################################################################
#商务运营可视化分析业务--访客增量数据
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
array_dataclear[1]="delete from dzyh_app.tbl_visitor_first_identification where udc_biz_date >= '${UDC_BIZ_DATE}';"

###InputJob Variables--3.1 Partition SqlArray
array_partition=()

###InputJob Variables--3.2 load Sql Array
array_load=()

##InputJob Variables--3.3 update Sql Array
array_update=()

###InputJob Variables--3.4 insert Sql Array

###########开始处理访客增量数据...
array_insert[0]="insert into tbl_visitor_first_identification
(udc_biz_date,udc_visitor_sequence,udc_visitor_type)
select 
udc_biz_date,udc_visitor_sequence,udc_visitor_type
from
  (select 
  udc_biz_date,
   (
    case
    when udc_customer_id is not null and udc_customer_id !=''
    then udc_customer_id
    else
    udc_cookie
    end 
   )as udc_visitor_sequence,	--用户唯一标志序列号(udc_customer_id/udc_cookie)
   (
    case
    when udc_customer_id is not null and udc_customer_id !=''
    then '01'
    else
    '02'
    end 
   )as udc_visitor_type			--用户类型(01:客户;02:游客)
  from TBL_VISITOR_IDENTIFICATION 
  where udc_biz_date='${UDC_BIZ_DATE}'
 ) a
where
exists
(select udc_visitor_sequence from tbl_visitor_first_identification where udc_visitor_sequence  = a.udc_visitor_sequence);"


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
