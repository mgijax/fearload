#
#  checkColumns.py
###########################################################################
#
#  Purpose:
#
#       This script checks that there are the correct number of columns
#	in a file
#
#  Usage:
#
#      checkColumns.py  filename numColumns	
#
#      where:
#          filename = path to the input file
#
#  Env Vars:
#
#      The following environment variables are set by the configuration
#      files that are sourced by the wrapper script:
#
#  Inputs:
#
#  Outputs:
#
#  Exit Codes:
#
#      0:  Successful completion
#      1:  An exception occurred
#      2:  Discrepancy errors detected in the input files
#  Implementation:
#
#  Notes:  None
#
###########################################################################


import string
import sys

USAGE = 'Usage: checkColumns.py  inputFile numColumns'
TAB = '\t'

inputFile = None
fpInput = None
numColumns = None
errors = 0

#
# Purpose: Validate the arguments to the script.
# Returns: Nothing
# Assumes: Nothing
# Effects: Sets global variables.
# Throws: Nothing
#
def checkArgs ():
    global inputFile, numColumns

    if len(sys.argv) != 3:
        print(USAGE)
        sys.exit(1)

    inputFile = sys.argv[1]
    numColumns = int(sys.argv[2])
    return

#
# Purpose: Open the file for reading
# Returns: Nothing
# Assumes: Nothing
# Effects: Sets global variables.
# Throws: Nothing
#
def openFile ():
    global fpInput

    try:
        fpInput = open(inputFile, 'r')
    except:
        print('Cannot open input file: ' + inputFile)
        sys.exit(1)
    return

#
# Purpose: check the file for proper number of columns
# Returns: Nothing
# Assumes: Nothing
# Effects: Sets global variables.
# Throws: Nothing
#
def checkColumns ():
    global errors
    lineNum = 1
    header = fpInput.readline()
    for line in fpInput.readlines():
        colError = 0
        lineNum = lineNum + 1
        columns = list(map(str.strip, str.split(line, TAB)))
        # remove newline from last column
        last = columns[-1].strip()
        columns[-1] = last
        nc = len(columns) 
        if nc < numColumns:
            errors = errors + 1
            colError = colError + 1
        ### start code for missing data in req columns
        # If errors then wrong number of columns exists; so continue to next
        if colError > 0:
            print('Missing Column(s): %s' % (columns))
            continue
        # default
        bad = 1

        # strand is optional
        if columns[0] != '' and columns[1] != '' and columns[2] != '' and columns[4] != '' and columns[6] != '' and columns[9] != '' and columns[10] != '' and columns[11] != '':
            bad = 0
        if bad == 1:
            print('Missing Data in required column: %s' % (columns))
        ### end code for missing data in req columns
    return

#
# Purpose: Close the files.
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing
#
def closeFile():
    global fpInput

    fpInput.close()
    return

checkArgs()
openFile()
checkColumns()
closeFile()
if errors > 0:
    sys.exit(1)
sys.exit(0)
