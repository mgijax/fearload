#!/bin/sh
#
#  fearQC.sh
###########################################################################
#
#  Purpose:
#
#      This script does sanity checks and is a wrapper around the process 
#	that does QC checks for the Feature Relationship (FeaR) load
#
#  Usage:
#
#      fearQC.sh  filename  
#
#      where
#          filename = full path to the input file
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
#      - QC report for the input file 	
#      - Log file (${QC_LOGFILE})
#
#  Exit Codes:
#
#      0:  Successful completion
#      1:  Sanity error (prompt to view sanity report)
#      2:  Unexpected error occured running fearQC.py (prompt to view log)
#      3:  QC errors (prompt to view qc report)
#
#  Assumes:  Nothing
#
#  Implementation:
#
#      This script will perform following steps:
#
#      1) Validate the arguments to the script
#      2) Validate & source the configuration files to establish the environment
#      3) Verify that the input file exists
#      4) Update path to sanity/QC reports if this is not a 'live' run 
#	     i.e. curators running the scripts 
#      5) Initialize the log file
#      6) creates table in tempdb for the input file
#      7) Call fearQC.py to generate the QC report
#      8) drops the tempdb table
#
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
USAGE='Usage: fearQC.sh  ["live"] 1-n files OR 1 directory'

# set LIVE_RUN  to sanity/QC check only as the default
LIVE_RUN=0; export LIVE_RUN
numDir=0
numFile=0
#
# Make sure correct arguments are passed to the script. If the optional "live"
# argument is given, that means that the output files are located in the
# /data/loads/... directory, not in the current directory.
#
argCt=$#
echo "argCt: $argCt"
if [ $argCt -lt 1 ]
then
    echo 'No arguments provided on command line'
    echo $USAGE
    exit 1
fi

files=''
dir=''
arg1=$1

# is 'live' first arg?
if [ "$arg1" = "live" ]
then
    LIVE_RUN=1; export LIVE_RUN
    shift
fi

# iterate thru args - minus 'live' if it was first arg
args=$*
for i in $args
do
    echo "arg: $i"
    echo "numDir: $numDir"
    # only one directory expected
    if [ -d $i ]
    then
	echo "$i is directory"
	if [ $numDir -gt 0 ]
	then
	    echo 'Multiple directories on command line'
	    echo $USAGE
	    exit 1
	else
	    numDir=`expr $numDir + 1`
	    dir=$i
	    echo "we have dir: $dir"
	fi
    elif [ -f $i ] # one or more files allowed
    then
	echo "$i is file"
	numFile=`expr $numFile + 1`
        files="$files $i"
    else
        echo "'$i' is not a directory or file"
        echo  $USAGE
        exit 1
    fi
done

# DEBUG
echo "LIVE_RUN: $LIVE_RUN"
echo "numFile: $numFile"
echo "files: $files"
echo "numDir: $numDir"
echo "dir: $dir"

# check for improper arguments
if [ $numDir -eq 1 -a $numFile -ne 0 ]
then
    echo 'Mix of directories and files on the command line'
    echo $USAGE
    exit 1
elif [ $numDir -eq 0 -a $numFile -eq 0 ]
then
    echo 'Live run and no files or directory on the command line'
    echo $USAGE
    exit 1
fi

# if we have a directory get the file listing
echo "getting the files from $dir"
if [ $numDir -eq 1 ]
then
    for f in `ls $dir`
    do
	fullPath="$dir/$f"
	echo "fullPath: $fullPath"
	if [ -d $fullPath ]
	then
	    echo "subdirectories of $dir not allowed"
	    echo $USAGE
	    exit 1
	fi
	files="$files $fullPath"
    done
    echo "files from dir: $files"
else
    echo "files from CL: $files"
fi

# file(s) to process 
INPUT_FILES=$files

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
	SANITY_RPT=${CURRENTDIR}/`basename ${SANITY_RPT}`.${USER}
	QC_RPT=${CURRENTDIR}/`basename ${QC_RPT}`.${USER}
	WARNING_RPT=${CURRENTDIR}/`basename ${WARNING_RPT}`
	DELETE_RPT=${CURRENTDIR}/`basename ${DELETE_RPT}`
	DELETE_SQL=${CURRENTDIR}/`basename ${DELETE_SQL}`
	QC_LOGFILE=${CURRENTDIR}/`basename ${QC_LOGFILE}`
	INPUT_FILE_QC_INT=${CURRENTDIR}/`basename ${INPUT_FILE_QC_INT}`

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
# FUNCTION: Check for duplicate lines in the input file
#           write the line numbers and lines to the sanity report.
#
checkDupLines ()
{
    FILE=$1         # The input file to check
    REPORT=$2       # The sanity report to write to
    echo "\nDuplicate Lines" >> ${REPORT}
    echo "-----------------------------------" >> ${REPORT}
    ${FEARLOAD}/bin/checkDupLines.py ${FILE} > ${TMP_FILE}
    cat ${TMP_FILE} >> ${REPORT}
    if [ `cat ${TMP_FILE} | wc -l` -eq 0 ]
    then
        return 0
    else
        return 1
    fi
}

#
# Initialize the report file(s) to make sure the current user can write to them.
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

echo "" >> ${LOG}
date >> ${LOG}
echo "Run sanity checks on the input file" >> ${LOG}
SANITY_ERROR=0

# catenate all input files to INPUT_FILE_QC_INT
if [ -f ${INPUT_FILE_QC_INT} ]
then
    rm  ${INPUT_FILE_QC_INT}
fi

cat ${INPUT_FILES} >  ${INPUT_FILE_QC_INT}

# check the line count
checkLineCount ${INPUT_FILE_QC_INT} ${SANITY_RPT} ${MIN_LINES}
if [ $? -ne 0 ]
then
    echo "\nInput file has no data: ${INPUT_FILE_QC_INT}\n\n" | tee -a ${LOG}
    exit 1
fi

# check the number of columns
checkColumns ${INPUT_FILE_QC_INT} ${SANITY_RPT} ${NUM_COLUMNS}
if [ $? -ne 0 ]
then
    SANITY_ERROR=1
fi

# check for duplicate lines
checkDupLines ${INPUT_FILE_QC_INT} ${SANITY_RPT}
if [ $? -ne 0 ]
then
    SANITY_ERROR=1
fi

# report if we find sanity errors
if [ ${SANITY_ERROR} -ne 0 ]
then
    if [ ${LIVE_RUN} -eq 0 ]
    then
	echo "Sanity errors detected in input file ${INPUT_FILE_QC_INT}. See ${SANITY_RPT}" | tee -a ${LOG}
    fi
    exit 1
fi

#
# Create temp tables for the input data.
#

#
# Append the current user ID to the name of the temp table being
# created. This allows multiple people to run the QC checks at the same time
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
{ ${FEARLOAD}/bin/fearQC.py ${INPUT_FILE_QC_INT} 2>&1; echo $? > ${TMP_FILE}; } >> ${LOG}

if [ `cat ${TMP_FILE}` -eq 1 ]
then
    echo "An error occurred while generating the QC reports on  ${INPUT_FILE_QC_INT}"
    echo "See log file (${LOG})"
    RC=2
elif [ `cat ${TMP_FILE}` -eq 2 ]
then
    if [ ${LIVE_RUN} -eq 0 ]
    then
	echo "QC errors detected in ${INPUT_FILE_QC_INT}. See ${QC_RPT} " | tee -a ${LOG}
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
