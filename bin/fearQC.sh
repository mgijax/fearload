#!/bin/sh
#
#  fearQC.sh
###########################################################################
#
#  Purpose:
#
#      This script is a wrapper around the process that does sanity
#      checks for the FeaR load
#
#  Usage:
#
#      fearQC.sh  filename  
#
#      where
#          filename = path to the input file
#
#  Env Vars:
#
#      See the configuration file
#
#  Inputs:
#	FeaR file
#
#  Outputs:
#
#      - sanity report for the input file.
#
#      - Log file (${QC_LOGFILE})
#
#  Exit Codes:
#
#      0:  Successful completion
#      1:  Fatal initialization error occurred
#      3:  Fatal sanity errors
#
#  Assumes:  Nothing
#
#  Implementation:
#
#      This script will perform following steps:
#
#      ) Validate the arguments to the script.
#      ) Validate & source the configuration files to establish the environment.
#      ) Verify that the input file exists.
#      ) Initialize the log and report files.
#      ) Call fearQC.sh to generate the sanity/QC report.
#
#  Notes:  None
#
###########################################################################
#
#  Modification History:
#
#  Date        SE   Change Description
#  ----------  ---  -------------------------------------------------------
#
#  03/11/2014  sc  Initial development
#
###########################################################################
CURRENTDIR=`pwd`
BINDIR=`dirname $0`

CONFIG=`cd ${BINDIR}/..; pwd`/fearload.config
USAGE='Usage: fearQC.sh  filename'

# this is a sanity check only run, set LIVE_RUN accordingly
LIVE_RUN=0; export LIVE_RUN

#
# Make sure an input file was passed to the script. If the optional "live"
# argument is given, that means that the output files are located in the
# /data/loads/... directory, not in the current directory.
#
if [ $# -eq 1 ]
then
    INPUT_FILE=$1
elif [ $# -eq 2 -a "$2" = "live" ]
then
    INPUT_FILE=$1
    LIVE_RUN=1
else
    echo ${USAGE}; exit 1
fi

#
# Make sure the configuration file exists and source it.
#
if [ -f ${CONFIG} ]
then
    . ${CONFIG}
else
    echo "Missing configuration file: ${CONFIG}"
    exit 1
fi

#
# If this is not a "live" run, the output, log and report files should reside
# in the current directory, so override the default settings.
#
if [ ${LIVE_RUN} -eq 0 ]
then
	SANITY_RPT=${CURRENTDIR}/`basename ${SANITY_RPT}`
	QC_RPT=${CURRENTDIR}/`basename ${QC_RPT}`
	WARNING_RPT=${CURRENTDIR}/`basename ${WARNING_RPT}`
	DELETE_RPT=${CURRENTDIR}/`basename ${DELETE_RPT}`
	DELETE_SQL=${CURRENTDIR}/`basename ${DELETE_SQL}`
	QC_LOGFILE=${CURRENTDIR}/`basename ${QC_LOGFILE}`

fi

#
# Initialize the log file.
#
LOG=${QC_LOGFILE}
rm -rf ${LOG}
touch ${LOG}

#
# FUNCTION: Check for lines with missing columns in input file and
#           write the line numbers to the sanity report.
#
checkColumns ()
{
    FILE=$1         # The input file to check
    REPORT=$2       # The sanity report to write to
    NUM_COLUMNS=$3  # The number of columns expected in each input record
    NUM_COLUMNS=$3  # The number of columns expected in each input record
    echo "\nLines With Missing Columns or Data" >> ${REPORT}
    echo "-----------------------------------" >> ${REPORT}
    ${FEARLOAD}/bin/checkColumns.py ${FILE} ${NUM_COLUMNS} > ${TMP_FILE}
    cat ${TMP_FILE} >> ${REPORT}
    if [ `cat ${TMP_FILE} | wc -l` -eq 0 ]
    then
        return 0
    else
        return 1
    fi
}

#
# Initialize the report files to make sure the current user can write to them.
#
RPT_LIST="${SANITY_RPT}"

for i in ${RPT_LIST}
do
    rm -f $i; >$i
done

#
# Create a temporary file and make sure it is removed when this script
# terminates.
#
TMP_FILE=/tmp/`basename $0`.$$
trap "rm -f ${TMP_FILE}" 0 1 2 15
#
# FUNCTION: Check an input file to make sure it has a minimum number of lines.
#
checkLineCount ()
{
    FILE=$1        # The input file to check
    REPORT=$2      # The sanity report to write to
    NUM_LINES=$3   # The minimum number of lines expected in the input file

    COUNT=`cat ${FILE} | wc -l | sed 's/ //g'`
    if [ ${COUNT} -lt ${NUM_LINES} ]
    then
        return 1
    else
        return 0
    fi
}

#
# FUNCTION: Check for duplicate lines in an input file and write the lines
#           to the sanity report.
#
checkDupLines ()
{
    FILE=$1    # The input file to check
    REPORT=$2  # The sanity report to write to

    echo "\n\nDuplicate Lines" >> ${REPORT}
    echo "---------------" >> ${REPORT}
    sort ${FILE} | uniq -d > ${TMP_FILE}
    cat ${TMP_FILE} >> ${REPORT}
    if [ `cat ${TMP_FILE} | wc -l` -eq 0 ]
    then
        return 0
    else
        return 1
    fi
}

echo "" >> ${LOG}
date >> ${LOG}
echo "Run sanity checks on the input file" >> ${LOG}
SANITY_ERROR=0

#
# Make sure the input files exist (regular file or symbolic link).
#
if [ "`ls -L ${INPUT_FILE} 2>/dev/null`" = "" ]
then
    echo "\nInput file does not exist: ${INPUT_FILE}\n\n" | tee -a ${LOG}
    exit 1
fi

checkLineCount ${INPUT_FILE} ${SANITY_RPT} ${MIN_LINES}
if [ $? -ne 0 ]
then
    echo "\nInput file has no data: ${INPUT_FILE}\n\n" | tee -a ${LOG}
    exit 1
fi

checkColumns ${INPUT_FILE} ${SANITY_RPT} ${NUM_COLUMNS}
if [ $? -ne 0 ]
then
    SANITY_ERROR=1
fi

checkDupLines ${INPUT_FILE} ${SANITY_RPT}
if [ $? -ne 0 ]
then
    SANITY_ERROR=1
fi

if [ ${SANITY_ERROR} -ne 0 ]
then
    if [ ${LIVE_RUN} -eq 0 ]
    then
	echo "Sanity errors detected. See ${SANITY_RPT}" | tee -a ${LOG}
    fi
    exit 1
fi

#
# Create temp tables for the input data.
#
#
# Append the current user ID to the name of the temp table that needs to be
# created. This allows multiple people to run the QC checks at the same time
# without sharing the same table.
#
MGI_ID_TEMP_TABLE=${MGI_ID_TEMP_TABLE}_${USER}

echo "" >> ${LOG}
date >> ${LOG}
echo "Create temp tables for the input data" >> ${LOG}
cat - <<EOSQL | isql -S${MGD_DBSERVER} -D${MGD_DBNAME} -U${MGI_PUBLICUSER} -P`cat ${MGI_PUBPASSWORDFILE}` -e  >> ${LOG}

use tempdb
go

create table ${MGI_ID_TEMP_TABLE} (
    mgiID1 int not null,
    mgiID1TypeKey int not null,
    mgiID2 int not null,
    mgiID2TypeKey int not null,
)
go

grant all on  ${MGI_ID_TEMP_TABLE} to public
go

quit
EOSQL

date >> ${LOG}

#
# Generate the QC reports.
#
echo "" >> ${LOG}
date >> ${LOG}
echo "Generate the QC reports" >> ${LOG}
{ ${FEARLOAD}/bin/fearQC.py ${INPUT_FILE} 2>&1; echo $? > ${TMP_FILE}; } >> ${LOG}

if [ `cat ${TMP_FILE}` -eq 1 ]
then
    echo "An error occurred while generating the QC reports"
    echo "See log file (${LOG})"
    RC=2
elif [ `cat ${TMP_FILE}` -eq 2 ]
then
    if [ ${LIVE_RUN} -eq 0 ]
    then
	echo "QC errors detected. See ${QC_RPT} " | tee -a ${LOG}
    fi
    RC=3
else
    if [ ${LIVE_RUN} -eq 0 ]
    then
	echo "No QC errors detected"
    fi
    RC=0
fi

if [ -f ${WARNING_RPT} -a ${LIVE_RUN} -eq 0 ]
then
    cat ${WARNING_RPT}
    rm ${WARNING_RPT}
fi

#
# Drop the temp tables.
#
echo "" >> ${LOG}
date >> ${LOG}
echo "Drop the temp tables" >> ${LOG}
cat - <<EOSQL | isql -S${MGD_DBSERVER} -D${MGD_DBNAME} -U${MGI_PUBLICUSER} -P`cat ${MGI_PUBPASSWORDFILE}` -e  >> ${LOG}

use tempdb
go

drop table ${MGI_ID_TEMP_TABLE}
go

quit
EOSQL

echo "" >> ${LOG}
date >> ${LOG}
echo "Finished running QC checks on the input file" >> ${LOG}

exit ${RC}
