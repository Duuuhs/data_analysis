##################################################################
##FUNTION: LoadData :  tbl_****_company_visual_oper
##DATA REGEX		:  tbl_****_company_visual_oper 
##@author dumingyuan
##【EBDA】基本参数提示(固定的)######################################
##参数编号：0                            1              2               3                                               4                       7
##参数：sh ebda_exec_Sh.sh flowID       jobID   jobName                                 jobPath                                                 EBDA_UDC_BIZ_DATE     jobStatus       args
##示例：
##功能： 商城可视化指标数据加工                                                                             ccbcom_load_ccbcom              com.ccb.services.Ccbcom_import_ccbcom    20190711                5                       arg1            arg2            arg3
##################################################################
##【EBDA】作业参数设置(EDIT)
##################################################################
######################################################################################################
#商务企业商城运营可视化分析业务--漏斗分析
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
array_dataclear[0]="truncate table tbm_****_funnel_homepage_01;"
array_dataclear[1]="truncate table tbm_****_funnel_homepage_02;"
array_dataclear[3]="truncate table tbm_****_funnel_goodsdatail_01;"
array_dataclear[4]="truncate table tbm_****_funnel_shoppingcart_01;"
array_dataclear[5]="truncate table tbm_****_funnel_unitaddorder_01;"
array_dataclear[6]="truncate table tbm_****_funnel_analyze_sub;"
array_dataclear[9]="truncate table tbm_****_funnel_analyze_visit;"
array_dataclear[10]="truncate table tbm_****_funnel_analyze_visitor;"
array_dataclear[11]="truncate table tbm_****_funnel_analyze_visitor_num;"
array_dataclear[12]="truncate table tbm_****_funnel_analyze_loss;"
array_dataclear[13]="delete from tbl_****_funnel_analyze_detail where udc_biz_date like '${EBDA_YEAR}-${EBDA_MON}-${EBDA_DAY}%' ;"




###InputJob Variables--3.1 Partition SqlArray
array_partition=()



###InputJob Variables--3.2 load Sql Array
array_load=()



##InputJob Variables--3.3 update Sql Array
array_update=()



###InputJob Variables--3.4 insert Sql Array
#1、到达着陆页面的session_id以及时间戳						
array_insert[0]="insert into tbm_****_funnel_homepage_01			
(udc_biz_date,		--20190428
udc_session_id,
osvt_channel_id,
udc_page_visit_time)
select
'${UDC_BIZ_DATE}',
a.udc_session_id,
a.osvt_channel_id,
a.udc_page_visit_time		--每个session_id的首次访问时间戳
from 
(select 
 udc_session_id,
 osvt_channel_id,
 udc_page_visit_time,			
 rank() over(partition by udc_session_id,osvt_channel_id order by udc_page_visit_time) num
 from
 TBL_WEB_PAGE_VIEW 
 where
 udc_biz_date='${UDC_BIZ_DATE}'
 and (udc_current_url='http://****'
 or udc_current_url='http://****/****/mobile/#home'
 or udc_current_url='http://****/****/mobile/index.html#home'
 or udc_current_url='http://****'
 or udc_current_url='http://****/index.html'
 or udc_current_url='http://****/page_20140530.html'
 or udc_current_url='http://****/client/#'
 or udc_current_url='http://****/client/index.jhtml#')   
 and udc_channel_id='04') a
where a.num=1;"


#2、过滤出到达着陆页面后session_id的访问记录
array_insert[1]="insert into tbm_****_funnel_homepage_02
(udc_biz_date,
udc_session_id,
osvt_channel_id,
udc_current_url,
udc_source_url,
udc_page_id,
udc_page_visit_time
)
select 
'${UDC_BIZ_DATE}',
a.udc_session_id,
a.osvt_channel_id,
a.udc_current_url,
a.udc_source_url,
a.udc_page_id,
a.udc_page_visit_time
from
TBL_WEB_PAGE_VIEW a, tbm_****_funnel_homepage_01 b
where
a.udc_session_id=b.udc_session_id
and a.udc_biz_date=b.udc_biz_date
and a.osvt_channel_id=b.osvt_channel_id
and a.udc_channel_id='04'
and a.udc_page_visit_time>=b.udc_page_visit_time;"


#3、着陆页面的访客信息
array_insert[2]="insert into tbm_****_funnel_analyze_visitor
(udc_biz_date,
osvt_channel_id,
udc_session_id,
udc_visitor_sequence,	--访客唯一标志序列号(udc_customer_id/udc_cookie)
udc_visitor_type		--访客类型(01:客户;02:游客)
)
select 
'${UDC_BIZ_DATE}',
a.osvt_channel_id,
a.udc_session_id,
(
 case
 when b.udc_customer_id is not null and b.udc_customer_id !=''
 then b.udc_customer_id
 else
 b.udc_cookie
 end 
),
(
 case
 when b.udc_customer_id is not null and b.udc_customer_id !=''
 then '01'
 else
 '02'
 end 
)
from 
tbm_****_funnel_homepage_01 a, TBL_VISITOR_IDENTIFICATION b
where
a.osvt_channel_id=b.osvt_channel_id
and a.udc_session_id=b.udc_session_id
and b.UDC_BIZ_DATE='${UDC_BIZ_DATE}'
and b.udc_channel_id='04';"



#4、着陆页面的访问次数
array_insert[3]="insert into tbm_****_funnel_analyze_visit
(udc_biz_date,
osvt_channel_id,
page_type,				--页面类型(01:着陆页面)
channel_pv,				--页面浏览数
channel_visit_num		--访问次数
)
select 
'${UDC_BIZ_DATE}',
a.osvt_channel_id,
'01',
count(1),
count(distinct a.udc_session_id)
from 
tbm_****_funnel_homepage_02 a
--(select 
--udc_session_id, 
--osvt_channel_id,
--udc_page_visit_time
--lead(udc_current_url) over (partition by udc_session_id,osvt_channel_id order by udc_page_visit_time) udc_next_url
--lead(udc_current_url) over (partition by udc_session_id,osvt_channel_id order by udc_page_visit_time) udc_next_url     --下一个页面
--from tbm_****_funnel_02) b		--过滤刷新页面的中间表
where 
 a.udc_current_url='http://****'
 or a.udc_current_url='http://****/****/mobile/#home'
 or a.udc_current_url='http://****/****/mobile/index.html#home'
 or a.udc_current_url='http://****'
 or a.udc_current_url='http://****/index.html'
 or a.udc_current_url='http://****/page_20140530.html'
 or a.udc_current_url='http://****/client/#'
 or a.udc_current_url='http://****/client/index.jhtml#'
group by a.osvt_channel_id;"


#5、着陆页面的访客数
array_insert[4]="insert into tbm_****_funnel_analyze_visitor_num
(udc_biz_date,
osvt_channel_id,
page_type,
channel_visitor
)
select 
'${UDC_BIZ_DATE}',
a.osvt_channel_id,
'01',
count(distinct a.udc_visitor_sequence)
from
tbm_****_funnel_analyze_visitor a, tbm_****_funnel_homepage_01 b
where 
a.osvt_channel_id=b.osvt_channel_id
and a.udc_session_id=b.udc_session_id
group by a.osvt_channel_id;"


#6、到达商品详情页面的session_id以及时间戳						
array_insert[5]="insert into tbm_****_funnel_goodsdatail_01			
(udc_biz_date,		
udc_session_id,
osvt_channel_id,
udc_page_visit_time)
select
'${UDC_BIZ_DATE}',
a.udc_session_id,
a.osvt_channel_id,
a.udc_page_visit_time		--每个session_id的首次访问时间戳
from 
(select 
 udc_session_id,
 osvt_channel_id,
 udc_page_visit_time,			
 rank() over(partition by udc_session_id,osvt_channel_id order by udc_page_visit_time) num
 from
 tbm_****_funnel_homepage_02 
 where
 (udc_current_url like 'http://****/****/view/sell/detail%'
 or udc_current_url like 'http://****/****/view/sell/snap-detail%	'
 or udc_current_url = 'http://****/****/mobile/views/productDetail/detail.html'
 or udc_current_url = 'http://****/****/mobile/views/productDetail/snap_detail.html'
 or udc_current_url like '%.ccb.com/products/pd_%')
) a
where a.num=1;"


#7、商品详情页面的访问次数
array_insert[6]="insert into tbm_****_funnel_analyze_visit
(udc_biz_date,
osvt_channel_id,
page_type,				--页面类型(02:商品详情页面)
channel_pv,				--页面浏览数
channel_visit_num		--访问次数
)
select 
'${UDC_BIZ_DATE}',
a.osvt_channel_id,
'02',
count(1),
count(distinct(a.udc_session_id))
from 
tbm_****_funnel_goodsdatail_01 a, tbm_****_funnel_homepage_02 b
where 
a.udc_session_id=b.udc_session_id
and a.osvt_channel_id=b.osvt_channel_id
and	b.udc_page_visit_time>=a.udc_page_visit_time	--按照session_id最早出现的时间戳筛选记录
and (b.udc_current_url like 'http://****/****/view/sell/detail%'
or b.udc_current_url like 'http://****/****/view/sell/snap-detail%'
or b.udc_current_url = 'http://****/****/mobile/views/productDetail/detail.html'
or b.udc_current_url = 'http://****/****/mobile/views/productDetail/snap_detail.html'
or b.udc_current_url like '%.ccb.com/products/pd_%')
group by a.osvt_channel_id;"


#8、商品详情页面的访客数
array_insert[7]="insert into tbm_****_funnel_analyze_visitor_num
(udc_biz_date,
osvt_channel_id,
page_type,
channel_visitor
)
select 
'${UDC_BIZ_DATE}',
a.osvt_channel_id,
'02',
count(distinct a.udc_visitor_sequence)
from
tbm_****_funnel_analyze_visitor a, tbm_****_funnel_goodsdatail_01 b
where 
a.osvt_channel_id=b.osvt_channel_id
and a.udc_session_id=b.udc_session_id
group by a.osvt_channel_id;"


#9、着陆页面的流失次数
array_insert[8]="insert into tbm_****_funnel_analyze_loss
(udc_biz_date,
osvt_channel_id,
page_type,				--页面类型(01:着陆页面)
udc_loss_num		
)
select
'${UDC_BIZ_DATE}',
sub1.osvt_channel_id,
'01',
coalesce(sub1.channel_visit_num,0)-coalesce(sub2.channel_visit_num,0)	--	流失次数
from
(select osvt_channel_id,channel_visit_num from tbm_****_funnel_analyze_visit where page_type='01') sub1	--着陆页面的访问次数
left join
(select osvt_channel_id,channel_visit_num from tbm_****_funnel_analyze_visit where page_type='02') sub2  --商品详情页面的访问次数
on sub1.osvt_channel_id=sub2.osvt_channel_id;"


#10、到达购物车页面的session_id以及时间戳						
array_insert[9]="insert into tbm_****_funnel_shoppingcart_01			
(udc_biz_date,		
udc_session_id,
osvt_channel_id,
udc_page_visit_time)
select
'${UDC_BIZ_DATE}',
a.udc_session_id,
a.osvt_channel_id,
a.udc_page_visit_time		--每个session_id的首次访问时间戳
from 
(select 
 a.udc_session_id,
 a.osvt_channel_id,
 a.udc_page_visit_time,			
 rank() over(partition by a.udc_session_id,a.osvt_channel_id order by a.udc_page_visit_time) num
 from
 tbm_****_funnel_homepage_02 a, tbm_****_funnel_goodsdatail_01 b
 where
 a.udc_session_id=b.udc_session_id
 and a.osvt_channel_id=b.osvt_channel_id
 and a.udc_page_visit_time>b.udc_page_visit_time
 and (a.udc_current_url='http://****/****/trade/cart'
 or a.udc_current_url='http://****/****/mobile/#purchase'
 or a.udc_current_url='http://****/shoppingcart/ShopCarDetail.jhtml'
 or a.udc_current_url='http://****/shoppingcart/clientshopcardetail.jhtml')
) a
where a.num=1;"


#11、购物车页面的访问次数
array_insert[10]="insert into tbm_****_funnel_analyze_visit
(udc_biz_date,
osvt_channel_id,
page_type,				--页面类型(03:购物车页面)
channel_pv,				--页面浏览数
channel_visit_num		--访问次数
)
select 
'${UDC_BIZ_DATE}',
a.osvt_channel_id,
'03',
count(1),
count(distinct(a.udc_session_id))
from 
tbm_****_funnel_shoppingcart_01 a, tbm_****_funnel_homepage_02 b
where 
a.udc_session_id=b.udc_session_id
and a.osvt_channel_id=b.osvt_channel_id
and	b.udc_page_visit_time>=a.udc_page_visit_time	--按照session_id最早出现的时间戳筛选记录
and (b.udc_current_url='http://****/****/trade/cart'
 or b.udc_current_url='http://****/****/mobile/#purchase'
 or b.udc_current_url='http://****/shoppingcart/ShopCarDetail.jhtml'
 or b.udc_current_url='http://****/shoppingcart/clientshopcardetail.jhtml')
group by a.osvt_channel_id;"


#12、购物车页面的访客数
array_insert[11]="insert into tbm_****_funnel_analyze_visitor_num
(udc_biz_date,
osvt_channel_id,
page_type,
channel_visitor
)
select 
'${UDC_BIZ_DATE}',
a.osvt_channel_id,
'03',
count(distinct a.udc_visitor_sequence)
from
tbm_****_funnel_analyze_visitor a, tbm_****_funnel_shoppingcart_01 b
where 
a.osvt_channel_id=b.osvt_channel_id
and a.udc_session_id=b.udc_session_id
group by a.osvt_channel_id;"


#13、商品详情页面的流失次数
array_insert[12]="insert into tbm_****_funnel_analyze_loss
(udc_biz_date,
osvt_channel_id,
page_type,				--页面类型(01:着陆页面)
udc_loss_num		
)
select
'${UDC_BIZ_DATE}',
sub1.osvt_channel_id,
'02',
coalesce(sub1.channel_visit_num,0)-coalesce(sub2.channel_visit_num,0)	--	流失次数
from
(select osvt_channel_id,channel_visit_num from tbm_****_funnel_analyze_visit where page_type='02') sub1	--着陆页面的访问次数
left join
(select osvt_channel_id,channel_visit_num from tbm_****_funnel_analyze_visit where page_type='03') sub2  --商品详情页面的访问次数
on sub1.osvt_channel_id=sub2.osvt_channel_id;"


#14、到达下定订单页面的session_id以及时间戳						
array_insert[13]="insert into tbm_****_funnel_unitaddorder_01			
(udc_biz_date,		
udc_session_id,
osvt_channel_id,
udc_page_visit_time)
select
'${UDC_BIZ_DATE}',
a.udc_session_id,
a.osvt_channel_id,
a.udc_page_visit_time		--每个session_id的首次访问时间戳
from 
(select 
 a.udc_session_id,
 a.osvt_channel_id,
 a.udc_page_visit_time,			
 rank() over(partition by a.udc_session_id,a.osvt_channel_id order by a.udc_page_visit_time) num
 from
 tbm_****_funnel_homepage_02 a, tbm_****_funnel_shoppingcart_01 b
 where
 a.udc_session_id=b.udc_session_id
 and a.osvt_channel_id=b.osvt_channel_id
 and a.udc_page_visit_time>b.udc_page_visit_time
 and (a.udc_current_url like 'http://****/****/view/trade/order-submit%'
 or a.udc_current_url='http://****/****/mobile/views/productDetail/confirm.html'
 or a.udc_current_url='http://****/member/unitAddOrder.jhtml')
) a
where a.num=1;"


#15、下定订单页面的访问次数
array_insert[14]="insert into tbm_****_funnel_analyze_visit
(udc_biz_date,
osvt_channel_id,
page_type,				--页面类型(04:下定订单页面)
channel_pv,				--页面浏览数
channel_visit_num		--访问次数
)
select 
'${UDC_BIZ_DATE}',
a.osvt_channel_id,
'04',
count(1),
count(distinct(a.udc_session_id))
from 
tbm_****_funnel_unitaddorder_01 a, tbm_****_funnel_homepage_02 b
where 
a.udc_session_id=b.udc_session_id
and a.osvt_channel_id=b.osvt_channel_id
and	b.udc_page_visit_time>=a.udc_page_visit_time	--按照session_id最早出现的时间戳筛选记录
and (b.udc_current_url like 'http://****/****/view/trade/order-submit%'
or b.udc_current_url='http://****/****/mobile/views/productDetail/confirm.html'
or b.udc_current_url='http://****/member/unitAddOrder.jhtml')
group by a.osvt_channel_id;"


#16、下定订单页面的访客数
array_insert[15]="insert into tbm_****_funnel_analyze_visitor_num
(udc_biz_date,
osvt_channel_id,
page_type,
channel_visitor
)
select 
'${UDC_BIZ_DATE}',
a.osvt_channel_id,
'04',
count(distinct a.udc_visitor_sequence)
from
tbm_****_funnel_analyze_visitor a, tbm_****_funnel_unitaddorder_01 b
where 
a.osvt_channel_id=b.osvt_channel_id
and a.udc_session_id=b.udc_session_id
group by a.osvt_channel_id;"


#17、购物车的流失次数
array_insert[16]="insert into tbm_****_funnel_analyze_loss
(udc_biz_date,
osvt_channel_id,
page_type,				--页面类型(01:着陆页面)
udc_loss_num		
)
select
'${UDC_BIZ_DATE}',
sub1.osvt_channel_id,
'03',
coalesce(sub1.channel_visit_num,0)-coalesce(sub2.channel_visit_num,0)	--	流失次数
from
(select osvt_channel_id,channel_visit_num from tbm_****_funnel_analyze_visit where page_type='03') sub1	--着陆页面的访问次数
left join
(select osvt_channel_id,channel_visit_num from tbm_****_funnel_analyze_visit where page_type='04') sub2  --商品详情页面的访问次数
on sub1.osvt_channel_id=sub2.osvt_channel_id;"


#18、商品详情页到达率
array_insert[17]="insert into tbm_****_funnel_analyze_sub
(udc_biz_date,
osvt_channel_id,
page_type,				--页面类型(02:商品详情页面)
channel_comp_details,
channel_order_percent
)
select 
'${UDC_BIZ_DATE}',
sub1.osvt_channel_id,
'02',
coalesce(round(sub2.channel_visit_num*100/sub1.channel_visit_num,2)||'%','0.00%'),		--商品详情页到达率
''
from 
(select osvt_channel_id,channel_visit_num from tbm_****_funnel_analyze_visit where page_type='01') sub1	left join  	--着陆页总访问次数
(select osvt_channel_id,channel_visit_num from tbm_****_funnel_analyze_visit where page_type='02') sub2				--商品详情页总访问次数
on sub1.osvt_channel_id=sub2.osvt_channel_id;"


#19、购物车到达率
array_insert[18]="insert into tbm_****_funnel_analyze_sub
(udc_biz_date,
osvt_channel_id,
page_type,				--页面类型(03:购物车页面)
channel_comp_details,
channel_order_percent
)
select 
'${UDC_BIZ_DATE}',
sub1.osvt_channel_id,
'03',
coalesce(round(sub2.channel_visit_num*100/sub1.channel_visit_num,2)||'%','0.00%'),		--购物车到达率
''
from 
(select osvt_channel_id,channel_visit_num from tbm_****_funnel_analyze_visit where page_type='01') sub1	left join 	--着陆页总访问次数
(select osvt_channel_id,channel_visit_num from tbm_****_funnel_analyze_visit where page_type='03') sub2 			--购物车页总访问次数
on sub1.osvt_channel_id=sub2.osvt_channel_id;"


#20、下单转化率
array_insert[19]="insert into tbm_****_funnel_analyze_sub
(udc_biz_date,
osvt_channel_id,
page_type,				--页面类型(04:下定订单页面)
channel_comp_details,
channel_order_percent
)
select 
'${UDC_BIZ_DATE}',
sub1.osvt_channel_id,
'04',
'',
coalesce(round(sub2.channel_visit_num*100/sub1.channel_visit_num,2)||'%','0.00%')		--下单转化率
from 
(select osvt_channel_id,channel_visit_num from tbm_****_funnel_analyze_visit where page_type='01') sub1	left join 	--着陆页总访问次数
(select osvt_channel_id,channel_visit_num from tbm_****_funnel_analyze_visit where page_type='04') sub2				--下定订单页总访问次数
on sub1.osvt_channel_id=sub2.osvt_channel_id;"





#21、 数据插入到最终表
array_insert[20]="insert into tbl_****_funnel_analyze_detail
(channel_page,			--页面名称
 channel_visit_num,		--访问次数
 channel_visitor, 		--访客数
 channel_pv, 			--页面浏览数
 channel_loss_time, 	--流失访次
 channel_loss_percent, 	--流失率
 channel_comp_details, 	--到达率(商品详情/购物车)
 channel_order_percent,	--转化率(下单) 
 channel_act_flag,  	--分类（PC或者TP或者APP）
 udc_biz_date			--业务日期
 )
 select	
  a.page_type,																--页面名称								
  coalesce(to_char(a.channel_visit_num,'FM999,999,999,999,999'),'0'),		--访问次数
  coalesce(to_char(c.channel_visitor,'FM999,999,999,999,999'),'0'),			--访客数
  coalesce(to_char(a.channel_pv,'FM999,999,999,999,999'),'0'),				--页面浏览数
  coalesce(to_char(b.udc_loss_num,'FM999,999,999,999,999'),'0'),			--流失访次
  coalesce(round(b.udc_loss_num*100/a.channel_visit_num,2)||'%','0.00%'),	--流失率
  coalesce(d.channel_comp_details,''),										--到达率
  coalesce(d.channel_order_percent,''),										--转化率
  a.osvt_channel_id,														--渠道id
 '${EBDA_YEAR}-${EBDA_MON}-${EBDA_DAY}'||' 00:00:00'						--日期
 from 
 tbm_****_funnel_analyze_visit a
 left join tbm_****_funnel_analyze_loss b on a.osvt_channel_id=b.osvt_channel_id and a.page_type=b.page_type
 left join tbm_****_funnel_analyze_visitor_num c on a.osvt_channel_id=c.osvt_channel_id and a.page_type=c.page_type
 left join tbm_****_funnel_analyze_sub d on a.osvt_channel_id=d.osvt_channel_id and a.page_type=d.page_type;" 





#V2
##着陆页面的流失次数
#array_insert[3]="insert into tbm_****_funnel_analyze_loss
#(udc_biz_date,
#osvt_channel_id,
#page_type,				--页面类型(01:着陆页面)
#udc_loss_num		
#)
#select
#'${UDC_BIZ_DATE}',
#a.osvt_channel_id,
#'01',
#count(distinct(a.udc_session_id))
#from 
#(select
# a.osvt_channel_id,
# a.udc_session_id,
# a.udc_current_url,
# lead(a.udc_current_url) over (partition by a.udc_session_id,a.osvt_channel_id order by a.udc_page_visit_time) udc_next_url
# from 
# tbm_****_funnel_homepage_02 a, tbm_****_funnel_homepage_01 b
# where a.osvt_channel_id=b.osvt_channel_id
# and a.udc_session_id=b.udc_session_id
# and a.udc_page_visit_time>=b.udc_page_visit_time
#) a
#where a.udc_next_url is null
#and a.udc_current_url ='http://****'
#group by a.osvt_channel_id;"













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
