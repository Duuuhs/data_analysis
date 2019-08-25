#!/bin/bash

##################################
##FUNTION: psql executor
##基本参数提示(固定的)
##@author ebda.zh
##################################
#GREENPLUM DBS
EBDA_DIR="/home/ap/dip/appjob/shelljob/****"
#EBDA_DIR="/home/hadoop/workspace_EBDA"
FILE_etc=${EBDA_DIR}/ebda_etc/ebda.env

. ${FILE_etc}

export EBDA_DBNM=${EBDA_DBNM}
export EBDA_DBUSR=${EBDA_DBUSR}
export EBDA_DBPWD=${EBDA_DBPWD}
export EBDA_DBSCHM=${EBDA_DBSCHM}
export EBDA_DBHOST=${EBDA_DBHOST}
export EBDA_DBPORT=${EBDA_DBPORT}

##################################
##方法：如果程序执行出错，exit 1(固定的)
##################################
issucc()
{
    if [ $? -ne 0 ]; then
        echo "last command execute failed!!!"
        exit 1
    fi
}

##################################
##执行过程：读取参数并执行
##      -c "SQL"
##      -f  sqlFIle.sql
##
##################################
echo "#######################################"
echo "#######################################"
export PGPASSWORD=****_app
while [ -n "$1" ]
do
case "$1" in
        -c)
                if [ $# -ne 2 ]; then
                        echo "Input parameter error!! ebda_psql_exec_sql.sh -c sql_string"
                        exit 1;
                fi
               echo "psql -d ${EBDA_DBNM} -h ${EBDA_DBHOST} -U ${EBDA_DBUSR} -p ${EBDA_DBPORT} -c"
                psql -d ${EBDA_DBNM} -h ${EBDA_DBHOST} -U ${EBDA_DBUSR} -p ${EBDA_DBPORT} -c "${2}"
                issucc;
                exit 0
                shift 2
                ;;
        -t)
                if [ $# -ne 2 ]; then
                        echo "Input parameter error!! ebda_psql_exec_sql.sh -c sql_string"
                        exit 1;
                fi

                psql -d ${EBDA_DBNM} -h ${EBDA_DBHOST} -U ${EBDA_DBUSR} -p ${EBDA_DBPORT} -v AUTOCOMMIT='off' -v ON_ERROR_STOP='on' -c "${2}"
                issucc;
                exit 0
                shift 2
                ;;
        -e)
                if [ $# -ne 4 ]; then
                        echo "Input parameter error!! ebda_psql_exec_sql.sh -e SCHEMA  TABLE_NAME  COLUMN_NAME"
                        exit 1;
                fi

                EBDA_SCHEMA=${2}
                EBDA_TABLENAME=${3}
                EBDA_COLUMNNAME=${4}

psql -h ${EBDA_DBHOST} -U ${EBDA_DBUSR} ${EBDA_DBNM}<<EOF
\timing off
set client_encoding='utf-8';
\copy ${EBDA_SCHEMA}.${EBDA_TABLENAME}(${EBDA_COLUMNNAME}) to '${EBDA_TABLENAME}.txt' CSV
\q
EOF

echo "load data
infile '${EBDA_TABLENAME}.txt' discardfile '${EBDA_TABLENAME}.dis'
append
into table ${EBDA_TABLENAME}
fields terminated by ','
optionally enclosed by '\"'
(${EBDA_COLUMNNAME})">"${EBDA_TABLENAME}.ctl"

                shift 4
                ;;
        -f)
                if [ $# -ne 2 ]; then
                        echo "Input parameter error!! ebda_psql_exec_sql.sh -f sql_file.sql"
                        exit 1;
                fi
                psql -d ${EBDA_DBNM} -h ${EBDA_DBHOST} -U ${EBDA_DBUSR} -p ${EBDA_DBPORT} -f "${2}"
                issucc;
                exit 0
                echo "runing the sql file!!!"

                shift 2
                ;;
        *)
                echo "Unkonw Argument: "$1""
                ;;
esac
shift
done
