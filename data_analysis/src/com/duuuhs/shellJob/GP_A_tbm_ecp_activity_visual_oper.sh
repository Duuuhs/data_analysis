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
#企业商城运营可视化分析业务--营销分析(postgresql)
######################################################################################################
EBDA_DIR=/home/ap/dip/appjob/shelljob/***
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
array_dataclear[0]="truncate table tbm_ecp_activity_all_pv_01;"
array_dataclear[1]="truncate table tbm_ecp_activity_visitor_01;"
array_dataclear[2]="truncate table tbm_ecp_activity_time_all_01;"
array_dataclear[3]="truncate table tbm_ecp_activity_time_ave_01;"
array_dataclear[4]="truncate table tbm_ecp_activity_outside_01;"
array_dataclear[5]="truncate table tbm_ecp_activity_trade_pv_01;"
array_dataclear[6]="truncate table tbm_ecp_activity_trade_order_01;"
array_dataclear[7]="delete from tbl_ecp_special_activity where udc_biz_date like '${EBDA_YEAR}-${EBDA_MON}-${EBDA_DAY}%' ;"




###InputJob Variables--3.1 Partition SqlArray
array_partition=()



###InputJob Variables--3.2 load Sql Array
array_load=()



##InputJob Variables--3.3 update Sql Array
array_update=()



###InputJob Variables--3.4 insert Sql Array
#1、 活动数量,访问次数
array_insert[0]="insert into tbm_ecp_activity_all_pv_01 
(udc_biz_date,
channel_id,
activity_url,
pandect_pv,
channel_visit_times) 
select 
'${UDC_BIZ_DATE}',
OSVT_CHANNEL_ID,
UDC_CURRENT_URL,						--活动url
count(1),						 		--活动数量
count(distinct UDC_SESSION_ID) 		 	--访问次数
from
TBL_WEB_PAGE_VIEW 
where
udc_biz_date='${UDC_BIZ_DATE}'
and (UDC_CURRENT_URL like 'http://buy.ccb.com/secondchannel/mppc%'						--B2C,PC
or UDC_CURRENT_URL like 'http://buy.ccb.com/secondchannel/mTouchErJi.jhtml?pageId=%'	--B2C,APP/TP
or UDC_CURRENT_URL like 'http://sale.mall.ccb.com/sale/%')								--B2B
and UDC_CHANNEL_ID='04'
group by OSVT_CHANNEL_ID,UDC_CURRENT_URL;"							


#2、 B2B--营销分析--专题活动--访客数
array_insert[1]="insert into tbm_ecp_activity_visitor_01 
(udc_biz_date,channel_id,activity_url,activity_visitor) 
select 
'${UDC_BIZ_DATE}',
visitor1.OSVT_CHANNEL_ID,
visitor1.udc_current_url,
coalesce(visitor1.num,0)+coalesce(visitor2.num,0)		
from
(
select 
a.OSVT_CHANNEL_ID,
b.UDC_CURRENT_URL,
count(distinct a.udc_customer_id) as num 
from TBL_VISITOR_IDENTIFICATION a, TBL_WEB_PAGE_VIEW b 
where 
a.udc_biz_date=b.udc_biz_date
and a.UDC_SESSION_ID=b.UDC_SESSION_ID
and a.OSVT_CHANNEL_ID=b.OSVT_CHANNEL_ID
and a.UDC_CHANNEL_ID=b.UDC_CHANNEL_ID
and a.udc_biz_date='${UDC_BIZ_DATE}' 
and a.udc_customer_id is not null and a.udc_customer_id !=''
and (b.UDC_CURRENT_URL like 'http://buy.ccb.com/secondchannel/mppc%'		--B2C,PC
or b.UDC_CURRENT_URL like 'http://buy.ccb.com/secondchannel/mTouchErJi.jhtml?pageId=%'	--B2C,APP/TP
or b.UDC_CURRENT_URL like 'http://sale.mall.ccb.com/sale/%')				--B2B
and b.UDC_CHANNEL_ID='04'
group by a.OSVT_CHANNEL_ID,b.UDC_CURRENT_URL
) as visitor1 left join  --客户数
(
select 
a.OSVT_CHANNEL_ID,
b.UDC_CURRENT_URL,
count(distinct a.udc_cookie) as num from TBL_VISITOR_IDENTIFICATION a, TBL_WEB_PAGE_VIEW b 
where 
a.udc_biz_date=b.udc_biz_date
and a.UDC_SESSION_ID=b.UDC_SESSION_ID
and a.OSVT_CHANNEL_ID=b.OSVT_CHANNEL_ID
and a.UDC_CHANNEL_ID=b.UDC_CHANNEL_ID
and a.udc_biz_date='${UDC_BIZ_DATE}' 
and (a.udc_customer_id is null or a.udc_customer_id='')
and (b.UDC_CURRENT_URL like 'http://buy.ccb.com/secondchannel/mppc%'		--B2C,PC
or b.UDC_CURRENT_URL like 'http://buy.ccb.com/secondchannel/mTouchErJi.jhtml?pageId=%'	--B2C,APP/TP
or b.UDC_CURRENT_URL like 'http://sale.mall.ccb.com/sale/%')				--B2B
and a.UDC_CHANNEL_ID='04'
group by a.OSVT_CHANNEL_ID,b.UDC_CURRENT_URL
) as visitor2 --游客数 
on visitor1.OSVT_CHANNEL_ID=visitor2.OSVT_CHANNEL_ID
and visitor1.UDC_CURRENT_URL=visitor2.UDC_CURRENT_URL;"


#3、 B2B--营销分析--专题活动--平均访问时长--总访问时长
array_insert[2]="insert into tbm_ecp_activity_time_all_01 
(udc_biz_date,channel_id,activity_url,activity_visit_time)
select 
'${UDC_BIZ_DATE}',
a.OSVT_CHANNEL_ID,
a.udc_current_url,
sum(extract(EPOCH from a.udc_page_stay_time))  udc_page_stay_time --将时长interval类型转换为数字（秒）
from 
(
select
'${UDC_BIZ_DATE}', 
OSVT_CHANNEL_ID,
--udc_session_id,
udc_current_url,
lead(to_timestamp(UDC_PAGE_VISIT_TIME,'YYYYMMDDHH24MISSMS')) over(partition by udc_session_id order by UDC_PAGE_VISIT_TIME) - to_timestamp(UDC_PAGE_VISIT_TIME, 
'YYYYMMDDHH24MISSMS') udc_page_stay_time
from TBL_WEB_PAGE_VIEW 
where UDC_PAGE_VISIT_TIME ~ '^[0-9]{17}$' --过滤不合格式的数据
and udc_biz_date = '${UDC_BIZ_DATE}'
and UDC_CHANNEL_ID='04'
) a 
where  (a.UDC_CURRENT_URL like 'http://buy.ccb.com/secondchannel/mppc%'		--B2C,PC
or a.UDC_CURRENT_URL like 'http://buy.ccb.com/secondchannel/mTouchErJi.jhtml?pageId=%'	--B2C,APP/TP
or a.UDC_CURRENT_URL like 'http://sale.mall.ccb.com/sale/%')				--B2B
group by a.OSVT_CHANNEL_ID,a.udc_current_url;"


#4、 B2B--营销分析--专题活动--平均访问时长
array_insert[3]="insert into tbm_ecp_activity_time_ave_01 
(udc_biz_date,channel_id,activity_url,activity_visit_time_ave)
select 
'${UDC_BIZ_DATE}',
a.channel_id,
a.activity_url,
lpad(trunc(a.activity_visit_time/b.channel_visit_times/3600),2,'0')||':'||lpad(trunc(a.activity_visit_time/b.channel_visit_times%3600/60),2,'0')||':'||lpad(trunc(a.activity_visit_time/b.channel_visit_times%3600%60%60),2,'0')||'' 
from 
tbm_ecp_activity_time_all_01 a,
tbm_ecp_activity_all_pv_01 b 
where
a.udc_biz_date=b.udc_biz_date 
and a.channel_id=b.channel_id
and a.activity_url=b.activity_url;"


#5、 B2B--营销分析--专题活动--跳出率--跳出次数(跳出次数在入库表中计算)
array_insert[4]="insert into tbm_ecp_activity_outside_01 
(udc_biz_date,channel_id,activity_url,channel_web_bounce) 
select 
'${UDC_BIZ_DATE}',
b.OSVT_CHANNEL_ID,
b.udc_current_url,
count(1)
from
(select 
'${UDC_BIZ_DATE}',
UDC_SESSION_ID,
count(1) as num
from 
TBL_WEB_PAGE_VIEW 
where 
UDC_CHANNEL_ID='04'
and udc_biz_date='${UDC_BIZ_DATE}'
group by UDC_SESSION_ID
having count(1)=1 ) a				--筛选出跳出为1的情况
left join TBL_WEB_PAGE_VIEW b
on a.UDC_SESSION_ID=b.UDC_SESSION_ID 
where 
b.udc_biz_date='${UDC_BIZ_DATE}'
and (b.UDC_CURRENT_URL like 'http://buy.ccb.com/secondchannel/mppc%'		--B2C,PC
or b.UDC_CURRENT_URL like 'http://buy.ccb.com/secondchannel/mTouchErJi.jhtml?pageId=%'	--B2C,APP/TP
or b.UDC_CURRENT_URL like 'http://sale.mall.ccb.com/sale/%')				--B2B
group by b.OSVT_CHANNEL_ID,b.UDC_CURRENT_URL;"


##5、 B2B--营销分析--专题活动--跳出率--跳出次数(跳出次数在入库表中计算)
#array_insert[4]="insert into tbm_ecp_activity_outside_01 
#(udc_biz_date,channel_id,activity_id,channel_web_bounce) 
#select 
#'${UDC_BIZ_DATE}',
#a.OSVT_CHANNEL_ID,
#a.activity_id,
#count(1)
#from 
#(select 
#'${UDC_BIZ_DATE}',
#OSVT_CHANNEL_ID,
#UDC_SESSION_ID,
#UDC_CURRENT_URL,
#split_part(split_part(UDC_CURRENT_URL,'mppc_',2),'.jhtml',1) as activity_id,  --活动id
#count(1) as num
#from 
#TBL_WEB_PAGE_VIEW 
#where 
#udc_biz_date='${UDC_BIZ_DATE}'
#and UDC_CURRENT_URL like 'http://buy.ccb.com/secondchannel/mppc_%'   --统计跳出的情况
#group by OSVT_CHANNEL_ID,UDC_SESSION_ID,UDC_CURRENT_URL ) a 
#where a.num=1 group by a.OSVT_CHANNEL_ID,a.activity_id;"


#######(无url)
#6、 B2B--营销分析--专题活动--成交订单转化率--页面总访问次数 
array_insert[5]="insert into tbm_ecp_activity_trade_pv_01 
(udc_biz_date,
channel_id,
channel_url,
channel_pv,
channel_visit_times) 
select 
'${UDC_BIZ_DATE}',
t.OSVT_CHANNEL_ID,
t.UDC_SOURCE_URL,
t.channel_pv,
t.channel_visit_times
from 
(select 
'${UDC_BIZ_DATE}',
OSVT_CHANNEL_ID,
UDC_SOURCE_URL,
count(1) as channel_pv,
count(distinct udc_session_id) as channel_visit_times
from 
TBL_WEB_PAGE_VIEW a 
where 
a.udc_biz_date='${UDC_BIZ_DATE}' 
and (a.UDC_SOURCE_URL like '暂无url' )  --http://buy.ccb.com/%
group by a.OSVT_CHANNEL_ID,a.UDC_SOURCE_URL) t;"


#######(无url)
#7、 B2B--营销分析--专题活动--成交订单转化率--页面有下单成交访问次数   
array_insert[6]="insert into tbm_ecp_activity_trade_order_01 
(udc_biz_date,
channel_id,
channel_url,
channel_pv) 
select 
'${UDC_BIZ_DATE}',
a.OSVT_CHANNEL_ID,
b.channel_url,
count(distinct a.udc_session_id) as channel_pv      
from 
TBL_WEB_PAGE_VIEW a, tbm_ecp_pc_trade_pv_02 b
where 
a.UDC_SOURCE_URL=b.channel_url
and a.OSVT_CHANNEL_ID=b.channel_id
and a.udc_biz_date='${UDC_BIZ_DATE}'  
and (a.UDC_CURRENT_URL like 'url暂无...'
or a.UDC_CURRENT_URL like 'url暂无...')
group by a.OSVT_CHANNEL_ID,b.channel_url ;"


#8、 B2B--数据插入到最终表
array_insert[7]="insert into tbl_ecp_special_activity
(channel_name,			--活动名称
 channel_num,			--活动数量
 channel_visitor_num, 	--访客数
 channel_visit, 		--访问次数
 channel_time_ave, 		--平均访问时长
 channel_jump_percent, 	--跳出率
 channel_order_percent, --成交订单转化率
 channel_number,  		--新增注册会员数
 channel_flag,  		--分类（PC或者TP或者APP）
 udc_biz_date			--业务日期
 )
 select
 coalesce((
	case a.channel_id
	when '05'		--B2B,PC
	then  split_part(a.activity_url,'http://sale.mall.ccb.com/sale/',2)
	when '09'		--B2B,TP
	then  split_part(a.activity_url,'http://sale.mall.ccb.com/sale/',2)
	when '07'		--B2C,PC
	then 
		case split_part(split_part(a.activity_url,'http://buy.ccb.com/secondchannel/mppc_',2),'.jhtml',1) 
		when '01021' then '学生惠'
		when '00620' then '电子券专区'
		when 'PB001' then '私人银行'
		else '快速专题活动'
		end
	else 			--B2C,APP/TP
		case split_part(a.activity_url,'http://buy.ccb.com/secondchannel/mTouchErJi.jhtml?pageId=',2) 
		when '00092' then '分期优选'
		when '00367' then '学生惠'
		when '00124' then '地方特色'
		when '00126' then '积分购'
		when '00791' then '善融好货'
		when '00835' then '善融好货'
		when '00128' then '品牌馆'
		when '00120' then '用券专区'
		when '00121' then '名酒馆'
		when '00167' then '手机数码'
		when '00166' then '电脑家电'
		when '00164' then '食品茶饮'
		when '00156' then '美容个户'
		when '00168' then '家居百货'
		when '00173' then '流行时尚'
		when 'PB002' then '私人银行'
		when '00856' then '老字号荟萃'
		when '01234' then '老字号荟萃'
		else '快速专题活动'
		end
	end
 ),''),																			--活动名称
 coalesce(to_char(a.pandect_pv,'FM999,999,999,999,999'),'0'),					--活动数量
 coalesce(to_char(d.activity_visitor,'FM999,999,999,999,999'),'0'),				--访客数
 coalesce(to_char(a.channel_visit_times,'FM999,999,999,999,999'),'0'),			--访问次数
 coalesce(b.activity_visit_time_ave,'0'),										--平均访问时长
 coalesce(round((c.channel_web_bounce)*100/a.channel_visit_times,2)||'%','0'),	--跳出率
 '0',												 							--成交订单转化率(无url) --round((f.channel_pv)*100/e.channel_pv,2)||'%'
 '0',																			--新增注册会员数
 a.channel_id,																	--渠道id
 '${EBDA_YEAR}-${EBDA_MON}-${EBDA_DAY}'||' 00:00:00'							--日期
 from 
 tbm_ecp_activity_all_pv_01 a 
 left join tbm_ecp_activity_time_ave_01 b on a.udc_biz_date=b.udc_biz_date and a.channel_id=b.channel_id and a.activity_url=b.activity_url
 left join tbm_ecp_activity_outside_01 c on a.udc_biz_date=c.udc_biz_date and a.channel_id=c.channel_id and a.activity_url=c.activity_url
 left join tbm_ecp_activity_visitor_01 d on a.udc_biz_date=d.udc_biz_date and a.channel_id=d.channel_id and a.activity_url=d.activity_url;" 
 #left join tbm_ecp_activity_trade_pv_01 e on a.udc_biz_date=e.udc_biz_date and a.channel_id=e.channel_id 
 #left join tbm_ecp_activity_trade_order_01 f on a.udc_biz_date=f.udc_biz_date and a.channel_id=f.channel_id 










#########################################
##【STEP2】Data_Clear
#########################################
echo "【EBDA_INFO】Step2:DataClear Method Started,Status: "${EBDA_JOBSTATUS}

##if [ ${EBDA_JOBSTATUS} -eq 5 ];then
echo "【EBDA_INFO】,jobStatus is 5 ==> DataClear Started"
##Clear the data on Hive
for sql in "${array_dataclear[@]}"
do
	echo "[EBDA_INFO]Step3. Current_SQL[${sql}]"
	${EBDA_BIN}/ebda_psql_exec.sh -c "${sql}"
	issucc
done
##fi

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
