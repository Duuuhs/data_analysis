EBDA_DIR=/home/ap/dip/appjob/shelljob/dzyh
#EBDA_DIR="/home/hadoop/workspace_EBDA"
EBDA_jobSh=${EBDA_DIR}/****
EBDA_BIN=${EBDA_jobSh}/platform
EBDA_LOG=${EBDA_DIR}/logs
EBDA_etc=${EBDA_DIR}/ebda_etc/ebda.env

source ${EBDA_etc}


echo "【EBDA_INFO】Step1. Write TableName to ${EBDA_jobSh}/ecp/analysis/all_table_name.list"
${EBDA_BIN}/ebda_psql_exec.sh -c "select tablename from pg_tables where tableowner='dzyh_app';" > ${EBDA_jobSh}/ecp/analysis/all_table_name.list

echo "【EBDA_INFO】Step2. DataClear Started"
${EBDA_BIN}/ebda_psql_exec.sh -c "truncate table dzyh_app.tbm_gp_tables_date;"

echo "【EBDA_INFO】Step3. Query All Table Data Quantities And Disk Size"
for line in `cat ${EBDA_jobSh}/ecp/analysis/all_table_name.list`

do
	#表数据量与表占物理磁盘大小一块儿插入数据库后数据会有问题,目前不知道原因,所以分两步执行
	${EBDA_BIN}/ebda_psql_exec.sh -c "insert into tbm_gp_tables_date
	(table_name,table_disk_size)
	select 
	'${line}',														
	pg_size_pretty(pg_total_relation_size('dzyh_app.${line}'))		--表占物理磁盘大小
	;"	
	${EBDA_BIN}/ebda_psql_exec.sh -c "update tbm_gp_tables_date
	set table_num = (select count(1) from ${line})
	where table_name = '${line}'								--表数据量															
	;"	
	fi

done

echo "【EBDA_INFO】Step4. Delete Invalid Data"
${EBDA_BIN}/ebda_psql_exec.sh -c "delete from tbm_gp_tables_date where table_disk_size is null and table_num is null;"

echo "【EBDA_INFO】Job:"${EBDA_JOBNAME}"###FINISH!#########Date"`date +"%Y-%m-%d %H:%M:%S"`
echo "【EBDA_INFO】Job: ${EBDA_JOBNAME} run times:$SECONDS s"
exit 0
