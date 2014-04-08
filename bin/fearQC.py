#!/usr/local/bin/python
#
#  fearQC.py
###########################################################################
#
#  Purpose:
#
#	This script will generate a QC report for a feature relationship
#	    input file
#
#  Usage:
#
#      fearQC.py  filename
#
#      where:
#          filename = path to the input file
#
#  Inputs:
#
#  Outputs:
#
#      - QC report (${QC_RPT})
#
#  Exit Codes:
#
#      0:  Successful completion
#      1:  An exception occurred
#      2:  Non-fatal discrepancy errors detected in the input files
#      3:  Fatal discrepancy errors detected in the input files
#
#  Assumes:
#
#  Implementation:
#
#      This script will perform following steps:
#
#      1) Validate the arguments to the script.
#      2) Perform initialization steps.
#      3) Open the input/output files.
#      4) Generate the QC reports.
#      5) Close the input/output files.
#
#  Notes:  None
#
###########################################################################

import sys
import os
import string
import re
import mgi_utils
import db
import Set
#
#  CONSTANTS
#
TAB = '\t'
CRT = '\n'

USAGE = 'Usage: fearQC.py  inputFile'

#
#  GLOBALS
#

# Report file names
qcRptFile = os.environ['QC_RPT']

# bcp file for MGI ID temp table
idBcpFile= os.environ['MGI_ID_BCP']
idTempTable = os.environ['MGI_ID_TEMP_TABLE']

# 1 if QC errors
hasQcErrors = 0

# category lookup {name:Category object, ...}
categoryDict = {}

# relationship term lookup {term:key, ...}
relationshipDict = {}

# qualifier term lookup {term:key, ...}
qualifierDict = {}

# evidence term lookup {term:key, ...}
evidenceDict = {}

# reference ID (JNum) lookup {term:key, ...}
jNumDict = {}

# marker lookup {mgiID:key, ...)
markerDict = {}

# MGI_User lookup {userLogin:key, ...}
userDict = {}

#
# Purpose: Validate the arguments to the script.
# Returns: Nothing
# Assumes: Nothing
# Effects: sets global variable
# Throws: Nothing
#
def checkArgs ():
    global inputFile

    if len(sys.argv) != 2:
        print USAGE
        sys.exit(1)

    inputFile = sys.argv[1]

    return


#
# Purpose: Perform initialization steps.
# Returns: Nothing
# Assumes: Nothing
# Effects: Sets global variables.
# Throws: Nothing
#
def init ():
    # Purpose: create lookups, open files
    # Returns: Nothing
    # Assumes: Nothing
    # Effects: Sets global variables, exits if a file cant be opened,
    #  creates files in the file system, creates connection to a database

    global nextPropertyKey, categoryDict, relationshipDict
    global qualifierDict, evidenceDict, jNumDict, userDict
    global passwordFile

    openFiles()

    #
    # create database connection
    #
    #user = os.environ['MGD_DBUSER']
    #passwordFile = os.environ['MGD_DBPASSWORDFILE']
    user = os.environ['MGI_PUBLICUSER']
    passwordFile = os.environ['MGI_PUBPASSWORDFILE']

    #db.useOneConnection(1)
    db.set_sqlUser(user)
    db.set_sqlPasswordFromFile(passwordFile)
    
    #
    # create lookups
    #

    # FeaR Category Lookup
    #print 'category lookup %s' % mgi_utils.date()
    results = db.sql('''select name, _Category_key, _RelationshipVocab_key, _RelationshipDAG_key, _MGIType_key_1, _MGIType_key_2
	from MGI_Relationship_Category''', 'auto')
    for r in results:
        categoryDict[r['name'].lower()] = r

    # FeaR vocab lookup
    #print 'FeaR vocab lookup %s' % mgi_utils.date()
    results = db.sql('''select a.accid, a._Object_key, t.isObsolete, dn._DAG_key, vd._Vocab_key
        from ACC_Accession a, VOC_Term t, DAG_Node dn, VOC_VocabDAG vd
        where a._MGIType_key = 13
        and a._LogicalDB_key = 171
        and a.preferred = 1
        and a.private = 0
        and a._Object_key = t._Term_key
        and t._Term_key = dn._Object_key
	and dn._DAG_key between 44 and 47
	and dn._DAG_key = vd._DAG_Key''', 'auto')
    for r in results:
        relationshipDict[r['accid'].lower()] = r

    # FeaR qualifier lookup
    #print 'qualifier lookup %s' % mgi_utils.date()
    results = db.sql('''select _Term_key, term
        from VOC_Term
        where _Vocab_key = 94
        and isObsolete = 0''', 'auto')
    for r in results:
        qualifierDict[r['term'].lower()] = r['_Term_key']

    # FeaR evidence lookup
    #print 'evidence lookup %s' % mgi_utils.date()
    results = db.sql('''select _Term_key, abbreviation
        from VOC_Term
        where _Vocab_key = 95
        and isObsolete = 0''', 'auto')
    for r in results:
        evidenceDict[r['abbreviation'].lower()] = r['_Term_key']

    # Reference lookup
    #print 'reference lookup %s' % mgi_utils.date()
    results = db.sql('''select a.accid, a._Object_key
        from ACC_Accession a
        where a._MGIType_key = 1
        and a._LogicalDB_key = 1
        and a.preferred = 1
        and a.private = 0
        and a.prefixPart = 'J:' ''', 'auto')
    for r in results:
        jNumDict[r['accid'].lower()] = r['_Object_key']

    # marker lookup
    #print 'marker lookup %s' % mgi_utils.date()
    results = db.sql('''select a.accid, a._Object_key
        from ACC_Accession a
        where a._MGIType_key = 2
        and a._LogicalDB_key = 1
        and a.preferred = 1
        and a.private = 0''', 'auto')
    for r in results:
        markerDict[r['accid'].lower()] = r['_Object_key']

    # active status (not data load or inactive)
    #print 'creator lookup %s' % mgi_utils.date()
    results = db.sql('''select login, _User_key
        from MGI_User
        where _UserStatus_key = 316350''', 'auto')
    for r in results:
        userDict[r['login'].lower()] = r['_User_key']

    # for MGI ID verification
    loadTempTables()
    #db.useOneConnection(0)
    return


#
# Purpose: Open the files.
# Returns: Nothing
# Assumes: Nothing
# Effects: Sets global variables.
# Throws: Nothing
#
def openFiles ():
    global fpInput, fpQcRpt, fpIDBCP

    #
    # Open the input file.
    #
    try:
        fpInput = open(inputFile, 'r')
    except:
        print 'Cannot open input file: %s' % inputFile
        sys.exit(1)

    try:
        fpQcRpt = open(qcRptFile, 'w')
    except:
        print 'Cannot open report file: %s' % qcRptFile
        sys.exit(1)

    try:
        fpIDBCP = open(idBcpFile, 'w')
    except:
        print 'Cannot open report file: %s' % idBcpFile
        sys.exit(1)

    return

#
# Purpose: qc input file MGI IDs
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing
#

def qcMarkerIds():
    global hasQcErrors
    #
    # Find any MGI IDs from the relationship file that:
    # 1) Do not exist in the database.
    # 2) Exist for a non-marker object.
    # 3) Exist for a marker, but the status is not "offical" or "interim".
    #

    cmds = '''select tmp.mgiID1, null "name", null "status"
		from tempdb..%s tmp
		where not exists(select 1
		from ACC_Accession a
		where a.accID = tmp.mgiID1)
		union
	  	select tmp.mgiID1, t.name, null "status"
		from tempdb..%s tmp, ACC_Accession a1, ACC_MGIType t
		where a1.accID = tmp.mgiID1 
		and a1._LogicalDB_key = 1
		and a1._MGIType_key != 2
		and not exists (select 1 
			from ACC_Accession a2
			where a2.accID = tmp.mgiID1
			and a2._LogicalDB_key = 1
			and a2._MGIType_key = 2)
			and a1._MGIType_key = t._MGIType_key
		union
		select tmp.mgiID1, t.name, ms.status
		from tempdb..%s tmp,ACC_Accession a, ACC_MGIType t,
			MRK_Marker m, MRK_Status ms
		where a.accID = tmp.mgiID1 
		and a._LogicalDB_key = 1
		and a._MGIType_key = 2
		and a._MGIType_key = t._MGIType_key
                and a._Object_key = m._Marker_key
                and m._Marker_Status_key not in (1,3)
                and m._Marker_Status_key = ms._Marker_Status_key 
                order by tmp.mgiID1''' % (idTempTable, idTempTable, idTempTable)
    #print cmds
    results1 = db.sql(cmds, 'auto')

    cmds = '''select tmp.mgiID2, null "name", null "status"
                from tempdb..%s tmp
                where not exists(select 1
                from ACC_Accession a
                where a.accID = tmp.mgiID2)
                union
                select tmp.mgiID2, t.name, null "status"
                from tempdb..%s tmp, ACC_Accession a1, ACC_MGIType t
                where a1.accID = tmp.mgiID2
                and a1._LogicalDB_key = 1
                and a1._MGIType_key != 2
                and not exists (select 1
                        from ACC_Accession a2
                        where a2.accID = tmp.mgiID2
                        and a2._LogicalDB_key = 1
                        and a2._MGIType_key = 2)
                        and a1._MGIType_key = t._MGIType_key
                union
                select tmp.mgiID2, t.name, ms.status
                from tempdb..%s tmp,ACC_Accession a, ACC_MGIType t,
                        MRK_Marker m, MRK_Status ms
                where a.accID = tmp.mgiID2
                and a._LogicalDB_key = 1
                and a._MGIType_key = 2
                and a._MGIType_key = t._MGIType_key
                and a._Object_key = m._Marker_key
                and m._Marker_Status_key not in (1,3)
                and m._Marker_Status_key = ms._Marker_Status_key
                order by tmp.mgiID2''' % (idTempTable, idTempTable, idTempTable)
    #print cmds
    results2 = db.sql(cmds, 'auto')

    if len(results1) >0 or len(results2) >0:
	hasQcErrors = 1
	fpQcRpt.write(string.center('Invalid Markers',80) + CRT)
	fpQcRpt.write('%-12s  %-20s  %-20s  %-30s%s' %
	     ('MGI ID','Object Type',
	      'Marker Status','Reason',CRT))
	fpQcRpt.write(12*'-' + '  ' + 20*'-' + '  ' + \
	      20*'-' + '  ' + 30*'-' + CRT)


    #
    # Write MGI ID1 records to the report.
    #
    for r in results1:
        #print r
        mgiID = r['mgiID1']
        objectType = r['name']
        markerStatus = r['status']

        if objectType == None:
            objectType = ''
        if markerStatus == None:
            markerStatus = ''

        if objectType == '':
            reason = 'MGI ID1 does not exist'
        elif markerStatus == '':
            reason = 'MGI ID1 exists for non-marker'
        else:
            reason = 'MGI ID1 marker status is invalid'

        fpQcRpt.write('%-12s  %-20s  %-20s  %-30s%s' %
            (mgiID, objectType, markerStatus, reason, CRT))

    #
    # Write MGI ID2 records to the report.
    #
    for r in results2:
        #print r
        mgiID = r['mgiID2']
        objectType = r['name']
        markerStatus = r['status']

        if objectType == None:
            objectType = ''
        if markerStatus == None:
            markerStatus = ''

        if objectType == '':
            reason = 'MGI ID2 does not exist'
        elif markerStatus == '':
            reason = 'MGI ID2 exists for non-marker'
        else:
            reason = 'MGI ID2 marker status is invalid'

	fpQcRpt.write('%-12s  %-20s  %-20s  %-30s%s' %
            (mgiID, objectType, markerStatus, reason, CRT))
	
#
# Purpose: run the QC checks
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing
#
def runQcChecks ():

    global hasQcErrors
    #* col2 - category - verify in db
    #* col3 - objId1 - id exists, is primary and correct type (from category)
    # col5 - relationshipId - id exists, not obsolete, correct vocab/dag (from category)
    #* col7 - objId2 - id exists, is primary and correct type (from category)
    #* col9 - qual abbrev - if specified, abbrev exists
    #* col10 - evid code - code exists
    #* col11 - jNum - verify it exists
    #* col12 - creator login - verify exists
    # no dups between file and db - 'uniqueness' key (non-fatal)
    # cluster member checks: these defered til another US
    ## chr for mrkId1 and mrkId2 match
    ## mrkId1/2 both interim or official status
    ## mrkId1/2 both mouse
    ## if both mrk's have coords, check that mrk2 coords within mrk1
    
    #
    # parse the input file 
    #
    
    # QC error lists
    categoryList = []
    qualifierList = []
    evidenceList = []
    jNumList = []
    userList = []
    relIdList = []
    obsRelIdList = []
    relVocabList = []
    relDagList = []
    line = fpInput.readline()  # discard header
    line = fpInput.readline()
    lineCt = 2
    while line:
	(action, cat, obj1Id, obj2sym, relId, relName, obj2Id, obj2sym, qual, evid, jNum, creator, prop, note) = map(string.strip, string.split(line, TAB))
	#print 'qual: "%s"' % qual
	if not categoryDict.has_key(cat.lower()):
	    hasQcErrors = 1
	    categoryList.append('%-12s  %-20s%s' % (lineCt, cat, CRT))
	    line = fpInput.readline()
	    lineCt += 1
            continue
        else:
	    cDict = categoryDict[cat.lower()]
	if qual == '':
	    qual = 'Not Specified'
	if not qualifierDict.has_key(qual.lower()):
	    hasQcErrors = 1
	    qualifierList.append('%-12s  %-20s' % (lineCt, qual))
	if not evidenceDict.has_key(evid.lower()):
	    hasQcErrors = 1
	    evidenceList.append('%-12s  %-20s' % (lineCt, evid))
	if not jNumDict.has_key(jNum.lower()):
	    hasQcErrors = 1
	    jNumList.append('%-12s  %-20s' % (lineCt, jNum))
	if not userDict.has_key(creator.lower()):
	    hasQcErrors = 1
	    userList.append('%-12s  %-20s' % (lineCt, creator))
	if not relationshipDict.has_key(relId.lower()):
	    hasQcErrors = 1
	    relIdList.append('%-12s  %-20s' % (lineCt, relId))
	else:
	    relDict = relationshipDict[relId.lower()]
	    if relDict['isObsolete'] != 0:
		hasQcErrors = 1
		obsRelIdList.append('%-12s  %-20s' % (lineCt, relId))
	    #print 'Incoming vocab key: %s Category vocab key: %s' % (relDict['_Vocab_key'], cDict['_RelationshipVocab_key'])
	    if relDict['_Vocab_key'] != cDict['_RelationshipVocab_key']:
		hasQcErrors = 1
		relVocabList.append('%-12s  %-20s' % (lineCt, relId))
	    if relDict['_DAG_key'] != cDict['_RelationshipDAG_key']:
		hasQcErrors = 1
		relDagList.append('%-12s  %-20s' % (lineCt, relId))
	line = fpInput.readline()
	lineCt += 1

    qcMarkerIds()
    if len(categoryList):
	fpQcRpt.write(CRT + CRT + string.center('Invalid Categories',60) + CRT)
        fpQcRpt.write('%-12s  %-20s%s' %
             ('Line#','Category', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + CRT)
	fpQcRpt.write(string.join(categoryList, CRT))
	
    if len(qualifierList):
	fpQcRpt.write(CRT + CRT + string.center('Invalid Qualifiers',60) + CRT)
        fpQcRpt.write('%-12s  %-20s%s' %
             ('Line#','Qualifier', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(string.join(qualifierList, CRT))

    if len(evidenceList):
	fpQcRpt.write(CRT + CRT + string.center('Invalid Evidence Codes',60) + CRT)
        fpQcRpt.write('%-12s  %-20s%s' %
             ('Line#','Evidence Code', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(string.join(evidenceList, CRT))

    if len(jNumList):
	fpQcRpt.write(CRT + CRT + string.center('Invalid J Numbers',60) + CRT)
        fpQcRpt.write('%-12s  %-20s%s' %
             ('Line#','J Number', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(string.join(jNumList, CRT))

    if len(userList):
	fpQcRpt.write(CRT + CRT + string.center('Invalid User Login',60) + CRT)
        fpQcRpt.write('%-12s  %-20s%s' %
             ('Line#','User Login', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(string.join(userList, CRT))

    if len(relIdList):
	fpQcRpt.write(CRT + CRT + string.center('Invalid Relationship IDs',60) + CRT)
        fpQcRpt.write('%-12s  %-20s%s' %
             ('Line#','Relationship ID', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(string.join(relIdList, CRT))

    if len(obsRelIdList):
	fpQcRpt.write(CRT + CRT + string.center('Obsolete Relationship IDs',60) + CRT)
        fpQcRpt.write('%-12s  %-20s%s' %
             ('Line#','Relationship ID', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(string.join(obsRelIdList, CRT))

    if len(relVocabList):
	fpQcRpt.write(CRT + CRT + string.center('Relationship Vocab not the same as Category Vocab',60) + CRT)
        fpQcRpt.write('%-12s  %-20s%s' %
             ('Line#','Relationship ID', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(string.join(relVocabList, CRT))

    if len(relDagList):
	fpQcRpt.write(CRT + CRT + string.center('Relationship DAG not the same as Category DAG',60) + CRT)
        fpQcRpt.write('%-12s  %-20s%s' %
             ('Line#','Relationship ID', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(string.join(relDagList, CRT))


#
# Purpose: Close the files.
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing
#
def closeFiles ():
    global fpInput, fpQcRpt
    fpInput.close()
    fpQcRpt.close()
    return

def loadTempTables ():

    print 'Create a bcp file from relationship input file'
    sys.stdout.flush()

    #
    # Open the input file.
    #
    try:
        fp = open(inputFile, 'r')
    except:
        print 'Cannot open input file: %s' % inputFile
        sys.exit(1)
    
    #
    # Read each record from the relationship input file
    # and write them to a bcp file.
    #
    junk = fp.readline() # header
    line = fp.readline()
    while line:
	(action, cat, obj1Id, obj2sym, relId, relName, obj2Id, obj2sym, qual, evid, jNum, creator, prop, note) = string.split(line, TAB)

	fpIDBCP.write('%s%s%s%s%s%s' % (obj1Id, TAB, obj2Id, TAB, cat, CRT))
        line = fp.readline()

    #
    # Close the bcp file.
    #
    fp.close()
    fpIDBCP.close()

    #
    # Load the temp tables with the input data.
    #
    #print 'Load the relationship data into the temp table: %s' % idTempTable
    sys.stdout.flush()
    bcpCmd = 'cat %s | bcp tempdb..%s in %s -c -t"%s" -S%s -U%s' % (passwordFile, idTempTable, idBcpFile, TAB, db.get_sqlServer(), db.get_sqlUser())
    rc = os.system(bcpCmd)
    if rc <> 0:
        closeFiles()
        sys.exit(1)

    return

#
# Main
#
checkArgs()
init()
runQcChecks()
closeFiles()

if hasQcErrors == 1 : 
    sys.exit(2)
#elif nonfatalCount > 0:
#    sys.exit(3)
else:
    sys.exit(0)
