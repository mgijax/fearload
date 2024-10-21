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
#      - input file as parameter - see USAGE
#
#  Outputs:
#
#      - QC report (${QC_RPT})
#      - Warning report (${WARNING_RPT})
#      - Delete report (${DELETE_RPT})
#      - Delete SQL file (${DELETE_SQL})
#      - temp table BCP file (${MGI_ID_BCP})
#
#  Exit Codes:
#
#      0:  Successful completion
#      1:  An exception occurred
#      2:  Fatal QC errors detected and written to report
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
#
#  Modification History:
#
#  Date        SE   Change Description
#  ----------  ---  -------------------------------------------------------
#
# 11/15/2019   sc  TR13068 - organizer/participant chromosome check should allow
#			     X to match XY and Y to match XY in both directions
#
# 04/27/2017   sc  TR12291 - exclude decreased_translational_product_level (RV:0001555)
#                            from chromosome mismatch check for mutation_involves
#
#  03/11/2014  sc  Initial development
#
###########################################################################

import sys
import os
import mgi_utils
import db
import time

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
warnRptFile = os.environ['WARNING_RPT']

# report all relationships that will be deleted
deleteRptFile = os.environ['DELETE_RPT']

# sql file for doing database deletes
deleteSQL = os.environ['DELETE_SQL']

# bcp file for MGI ID temp table
idBcpFile= os.environ['MGI_ID_BCP']
idTempTable = os.environ['MGI_ID_TEMP_TABLE']

# 1 if any QC errors in the input file
hasFatalErrors = 0
hasWarnErrors = 0

# category lookup {name:query result set, ...} from the database
categoryDict = {}

# relationship term lookup {term:resultSet, ...} from the database
relationshipDict = {}

# qualifier term lookup {term:key, ...} from the database
qualifierDict = {}

# evidence term lookup {term:key, ...} from the database
evidenceDict = {}

# reference ID (JNum) lookup {term:key, ...} from the database
jNumDict = {}

# EntrezGene ID non-mouse marker symbol lookup {entrezGeneID:symbol, ...} 
# from the database
egSymbolDict = {}

# MGI_User lookup {userLogin:key, ...} from the database
userDict = {}

# list of valid properties from the database
validPropDict = {}

# proper MGI ID prefix in lowercase
mgiPrefix = 'mgi:'

# columns 1-numNonPropCol may NOT include properties
numNonPropCol=13

# number of header columns
numHeaderColumns = None

#  list of rows with errors by attribute
actionList = []
categoryList = []
qualifierList = []
evidenceList = []
jNumList = []
userList = []
relIdList = []
obsRelIdList = []
relVocabList = []
relDagList = []
badPropList = []
badPropValueList = []
missingPropColumnList = []

# improperly formated organizer or participant MGI IDs
# {badId:type, ...} where type is organizer or participant
badIdDict = {}

#
# data structures for deletes
#

# from input file {MGI ID:alleleKey, ...}
alleleDict = {}

# markers from from input file (MGI ID: markerKey, ...}
markerDict = {}

# list of deletes for the delete report
deleteRptList = []

# list of deletes not found in the database
deleteNotInDbList = []

# for bcp
bcpin = '%s/bin/bcpin.csh' % os.environ['PG_DBUTILS']
server = os.environ['MGD_DBSERVER']
database = os.environ['MGD_DBNAME']

#
# Purpose: Validate the arguments to the script.
# Returns: Nothing
# Assumes: Nothing
# Effects: sets global variable, exits if incorrect # of args
# Throws: Nothing
#
def checkArgs ():
    global inputFile

    if len(sys.argv) != 2:
        print(USAGE)
        sys.exit(1)

    inputFile = sys.argv[1]
    #print 'inputFile: %s' % inputFile
    return

# end checkArgs() -------------------------------

# Purpose: create lookups, open files
# Returns: Nothing
# Assumes: Nothing
# Effects: Sets global variables, exits if a file can't be opened,
#  creates files in the file system, creates connection to a database

def init ():
    global categoryDict, relationshipDict
    global qualifierDict, evidenceDict, jNumDict, userDict
    global validPropDict, passwordFile, egSymbolDict

    # open input/output files
    openFiles()
    db.useOneConnection(1)

    #
    # create lookups
    #

    # FeaR Category Lookup
    results = db.sql('''
        select name, _Category_key, _RelationshipVocab_key, _RelationshipDAG_key, _MGIType_key_1, _MGIType_key_2
        from MGI_Relationship_Category
        ''', 'auto')
    for r in results:
        categoryDict[r['name'].lower()] = r

    # FeaR vocab lookup
    #print 'FeaR vocab lookup %s' % mgi_utils.date()
    results = db.sql('''
        select a.accID, a._Object_key, t.term, t.isObsolete, dn._DAG_key, vd._Vocab_key
        from ACC_Accession a, VOC_Term t, DAG_Node dn, VOC_VocabDAG vd
        where a._MGIType_key = 13
        and a._LogicalDB_key = 171
        and a.preferred = 1
        and a.private = 0
        and a._Object_key = t._Term_key
        and t._Term_key = dn._Object_key
        and dn._DAG_key in (44,45,46,47,54)
        and dn._DAG_key = vd._DAG_Key
        ''', 'auto')
    for r in results:
        relationshipDict[r['accID'].lower()] = r

    # FeaR qualifier lookup
    #print 'qualifier lookup %s' % mgi_utils.date()
    results = db.sql('''
        select _Term_key, term
        from VOC_Term
        where _Vocab_key = 94
        and isObsolete = 0
        ''', 'auto')
    for r in results:
        qualifierDict[r['term'].lower()] = r['_Term_key']
    
    # FeaR evidence lookup
    #print 'evidence lookup %s' % mgi_utils.date()
    results = db.sql('''
        select _Term_key, abbreviation
        from VOC_Term
        where _Vocab_key = 95
        and isObsolete = 0
        ''', 'auto')
    for r in results:
        evidenceDict[r['abbreviation'].lower()] = r['_Term_key']

    # Reference lookup
    #print 'reference lookup %s' % mgi_utils.date()
    results = db.sql('''
        select a.accID, a._Object_key
        from ACC_Accession a
        where a._MGIType_key = 1
        and a._LogicalDB_key = 1
        and a.preferred = 1
        and a.private = 0
        and a.prefixPart = 'J:' 
        ''', 'auto')
    for r in results:
        jNumDict[r['accID'].lower()] = r['_Object_key']

    #EntrezGene id to symbol lookup
    results = db.sql('''
        select a.accID, m.symbol
        from ACC_Accession a, MRK_Marker m
        where a._LogicalDB_key = 55
        and a._MGIType_key = 2
        and a.preferred = 1
        and a._Object_key = m._Marker_key
        and m._Organism_key != 1
        ''', 'auto')
    for r in results:
        egSymbolDict[r['accID']] = r['symbol']

    # Creator lookup
    #print 'creator lookup %s' % mgi_utils.date()
    results = db.sql('''
        select login, _User_key
        from MGI_User
        where _UserStatus_key = 316350
        ''', 'auto')
    for r in results:
        userDict[r['login'].lower()] = r['_User_key']

    # Properties lookup
    results = db.sql('''
        select _Term_key, term
        from VOC_Term 
        where _Vocab_key = 97
        ''', 'auto')
    for r in results:
        validPropDict[r['term'].lower()] = r['_Term_key']
    #print 'validPropDict: %s' % validPropDict

    #
    # load temp table from input file for MGI ID verification
    # 
    loadTempTables()
    print('Done loading temp tables')

    # load allele and marker lookups from temp table
    loadTempTableLookups()

    return

# end init() -------------------------------

# Purpose: load lookups from temp table for delete processing
# Returns: Nothing
# Assumes: temp table loaded with input file data
# Effects: queries a database, modifies global variables
#
def loadTempTableLookups(): 
    global alleleDict, markerDict

    # load org=allele, part= marker from temp table
    results = db.sql('''
            select distinct tmp.mgiID1, 
                a1._Object_key as _Allele_key, aa.symbol as alleleSymbol, 
                tmp.mgiID2, a2._Object_key as _Marker_key, 
                m.symbol as markerSymbol
            from %s tmp, ACC_Accession a1, ACC_Accession a2, ALL_Allele aa, MRK_Marker m
            where tmp.mgiID1TypeKey = 11
            and tmp.mgiID2TypeKey = 2
            and tmp.mgiID1 = a1.numericPart
            and a1._MGIType_key = 11
            and a1.preferred = 1
            and a1._LogicalDB_key = 1
            and a1._Object_key = aa._Allele_key
            and tmp.mgiID2 = a2.numericPart
            and a2._MGIType_key = 2
            and a2.preferred = 1
            and a2._LogicalDB_key = 1
            and a2._Object_key = m._Marker_key
            ''' % idTempTable, 'auto')

    # load alleleDict and markerDict from query results
    for r in results:
        alleleID = 'mgi:%s' % r['mgiID1']
        #print '%s %s' % (alleleID, r['_Allele_key'])
        markerID = 'mgi:%s' % r['mgiID2']
        #print '%s %s' % (markerID, r['_Marker_key'])
        if alleleID not in alleleDict:
            alleleDict[alleleID] = [r['_Allele_key'], r['alleleSymbol']]
        if markerID not in markerDict:
            markerDict[markerID] = [r['_Marker_key'], r['markerSymbol']]
    #print alleleDict

    # load org=marker, part=marker from temp table
    results = db.sql('''
            select distinct tmp.mgiID1, 
                a1._Object_key as _Marker_key_1, m1.symbol as symbol1, 
                tmp.mgiID2, a2._Object_key as _Marker_key_2, 
                m2.symbol as symbol2
            from %s tmp, ACC_Accession a1, ACC_Accession a2, MRK_Marker m1, MRK_Marker m2
            where tmp.mgiID1TypeKey = 2
            and tmp.mgiID2TypeKey = 2
            and tmp.mgiID1 = a1.numericPart
            and a1._MGIType_key = 2
            and a1.preferred = 1
            and a1._LogicalDB_key = 1
            and a1._Object_key = m1._Marker_key
            and tmp.mgiID2 = a2.numericPart
            and a2._MGIType_key = 2
            and a2.preferred = 1
            and a2._LogicalDB_key = 1
            and a2._Object_key = m2._Marker_key
            ''' % idTempTable, 'auto')

    # load markerDict from query results
    for r in results:
        markerID1 = 'mgi:%s' % r['mgiID1']
        markerID2 = 'mgi:%s' % r['mgiID2']

        if markerID1 not in markerDict:
            markerDict[markerID1] = [r['_Marker_key_1'], r['symbol1']]
        if markerID2 not in markerDict:
            markerDict[markerID2] = [r['_Marker_key_2'], r['symbol2']]

    return

# end loadTempTableLookups() -------------------------------

#
# Purpose: Open input and output files.
# Returns: Nothing
# Assumes: Nothing
# Effects: Sets global variables.
# Throws: Nothing
#
def openFiles ():
    global fpInput, fpQcRpt, fpIDBCP, fpWarnRpt, fpDeleteRpt, fpDeleteSQL

    #
    # Open the input file
    #
    try:
        fpInput = open(inputFile, 'r')
    except:
        print('Cannot open input file: %s' % inputFile)
        sys.exit(1)

    #
    # Open QC report file
    #
    try:
        fpQcRpt = open(qcRptFile, 'w')
    except:
        print('Cannot open report file: %s' % qcRptFile)
        sys.exit(1)

    #
    # Open tempdb BCP file
    #
    try:
        fpIDBCP = open(idBcpFile, 'w')
    except:
        print('Cannot open temp table bcp file: %s' % idBcpFile)
        sys.exit(1)

    #
    # Open the warning report
    #
    try:
        fpWarnRpt = open(warnRptFile, 'w')
    except:
        print('Cannot open warning report file: %s' % warnRptFile)
        sys.exit(1)

    #
    # Open the delete report
    #
    try:
        fpDeleteRpt = open(deleteRptFile, 'w')
    except:
        print('Cannot open delete report file: %s' % deleteRptFile)
        sys.exit(1)

    #
    # Open the delete SQL file
    #
    try:
        fpDeleteSQL = open(deleteSQL, 'w')
    except:
        print('Cannot open delete SQL file: %s' % deleteSQL)
        sys.exit(1)

    return

# end openFiles() -------------------------------

#
# Purpose: qc input  file for allele/marker relationships
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing
#

def qcOrgAllelePartMarker():
    global hasFatalErrors, hasWarnErrors

    # Find any MGI IDs from the relationship who's Organizer is  allele
    # and Participant is marker and:

    # Organizer does not exist in the database
    #cmds = '''select tmp.mgiID1, null name, null "status"
    #cmds = '''select tmp.mgiID1, name=null, "status"=null
    cmds = '''select tmp.mgiID1, null as name, null as status
                from %s tmp
                where tmp.mgiID1TypeKey = 11
                and tmp.mgiID2TypeKey = 2
                and tmp.mgiID1 != 0
                and not exists(select 1
                from ACC_Accession a
                where a.numericPart = tmp.mgiID1
                and a.prefixPart = 'MGI:')
                order by tmp.mgiID1''' % idTempTable
    print('running sql for results1a %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time())))
    sys.stdout.flush()
    results1a = db.sql(cmds, 'auto')

    # Organizer exists for a non-allele object.
    cmds = '''select tmp.mgiID1, t.name, null as status
                from %s tmp, ACC_Accession a1, ACC_MGIType t
                where a1.numericPart = tmp.mgiID1
                and a1.prefixPart = 'MGI:'
                and tmp.mgiID1TypeKey = 11
                and tmp.mgiID2TypeKey = 2
                and a1._LogicalDB_key = 1
                and tmp.mgiID1 > 0
                and a1._MGIType_key != 11
                and a1._MGIType_key = t._MGIType_key
                and not exists (select 1
                        from ACC_Accession a2
                        where a2.numericPart = tmp.mgiID1
                        and a2.prefixPart = 'MGI:'
                        and a2._LogicalDB_key = 1
                        and a2._MGIType_key = 11)
                order by tmp.mgiID1''' % idTempTable
    print('running sql for results1b %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time())))
    sys.stdout.flush()
    results1b = db.sql(cmds, 'auto')

    # Organizer has invalid status
    cmds = '''select tmp.mgiID1, t.name, vt.term as status
                from %s tmp,ACC_Accession a, ACC_MGIType t,
                        ALL_Allele aa, VOC_Term vt
                where a.numericPart = tmp.mgiID1
                and a.prefixPart = 'MGI:'
                and tmp.mgiID1TypeKey = 11
                and tmp.mgiID2TypeKey = 2
                and a._LogicalDB_key = 1
                and tmp.mgiID1 > 0
                and a._MGIType_key = 11
                and a._MGIType_key = t._MGIType_key
                and a._Object_key = aa._Allele_key
                and aa._Allele_Status_key not in (847114, 3983021)
                and aa._Allele_Status_key = vt._Term_key
                order by tmp.mgiID1''' % idTempTable

    #print cmds
    print('running sql for results1c %s' % time.strftime("%H.%M.%S.%m.%d.%y", \
        time.localtime(time.time())))
    sys.stdout.flush()
    results1c = db.sql(cmds, 'auto')

    # Participant MGI ID does not exist in the database
    cmds = '''select tmp.mgiID2, null as name, null as status
                from %s tmp
                where tmp.mgiID1TypeKey = 11
                and tmp.mgiID2TypeKey = 2
                and tmp.mgiID2 > 0
                and not exists(select 1
                from ACC_Accession a
                where a.numericPart = tmp.mgiID2
                and a.prefixPart = 'MGI:')
                order by tmp.mgiID2''' % idTempTable
    print('running sql for results2a %s ' % time.strftime("%H.%M.%S.%m.%d.%y", \
        time.localtime(time.time())))
    sys.stdout.flush()
    results2a = db.sql(cmds, 'auto')

    # Participant MGI ID exists in the database for non-marker object
    cmds = '''select tmp.mgiID2, t.name, null as status
                from %s tmp, ACC_Accession a1, ACC_MGIType t
                where a1.numericPart = tmp.mgiID2
                and a1.prefixPart = 'MGI:'
                and tmp.mgiID1TypeKey = 11
                and tmp.mgiID2TypeKey = 2
                and tmp.mgiID2 > 0
                and a1._LogicalDB_key = 1
                and a1._MGIType_key != 2
                and a1._MGIType_key = t._MGIType_key
                and not exists (select 1
                        from ACC_Accession a2
                        where a2.numericPart = tmp.mgiID2
                        and a2.prefixPart = 'MGI:'
                        and a2._LogicalDB_key = 1
                        and a2._MGIType_key = 2)
                order by tmp.mgiID2''' % idTempTable
    print('running sql for results2b %s ' % time.strftime("%H.%M.%S.%m.%d.%y", \
        time.localtime(time.time())))
    sys.stdout.flush()
    results2b = db.sql(cmds, 'auto')

    # Participant has invalid status         
    cmds = '''select tmp.mgiID2, t.name, ms.status
                from %s tmp,ACC_Accession a, ACC_MGIType t,
                        MRK_Marker m, MRK_Status ms
                where a.numericPart = tmp.mgiID2
                and a.prefixPart = 'MGI:'
                and tmp.mgiID1TypeKey = 11
                and tmp.mgiID2TypeKey = 2
                and tmp.mgiID2 > 0
                and a._MGIType_key = 2
                and a._LogicalDB_key = 1
                and a._MGIType_key = t._MGIType_key
                and a._Object_key = m._Marker_key
                and m._Marker_Status_key != 1
                and m._Marker_Status_key = ms._Marker_Status_key
                order by tmp.mgiID2''' % idTempTable
    print('running sql for results2c %s ' % time.strftime("%H.%M.%S.%m.%d.%y", \
        time.localtime(time.time())))
    sys.stdout.flush()
    results2c = db.sql(cmds, 'auto')

    # Organizer ID is secondary
    cmds = '''select tmp.mgiID1,
                       aa.symbol,
                       a2.accID
                from %s tmp,
                     ACC_Accession a1,
                     ACC_Accession a2,
                     ALL_Allele aa
                where tmp.mgiID1 = a1.numericPart
                      and a1.prefixPart = 'MGI:'
                      and tmp.mgiID1TypeKey = 11
                      and tmp.mgiID2TypeKey = 2
                      and a1._MGIType_key = 11
                      and a1._LogicalDB_key = 1
                      and a1.preferred = 0
                      and a1._Object_key = a2._Object_key
                      and a2.prefixPart = 'MGI:'
                      and a2._MGIType_key = 11
                      and a2._LogicalDB_key = 1
                      and a2.preferred = 1
                      and a2._Object_key = aa._Allele_key
                order by tmp.mgiID1''' % idTempTable

 
    print('running sql for results3 %s' % time.strftime("%H.%M.%S.%m.%d.%y", \
        time.localtime(time.time())))
    sys.stdout.flush()
    results3 = db.sql(cmds, 'auto')

    # Participant  ID is secondary
    cmds = '''select tmp.mgiID2,
                       m.symbol,
                       a2.accID
                from %s tmp,
                     ACC_Accession a1,
                     ACC_Accession a2,
                     MRK_Marker m
                where tmp.mgiID2 = a1.numericPart
                      and a1.prefixPart = 'MGI:'
                      and tmp.mgiID1TypeKey = 11
                      and tmp.mgiID2TypeKey = 2
                      and a1._MGIType_key =  2
                      and a1._LogicalDB_key = 1
                      and a1.preferred = 0
                      and a1._Object_key = a2._Object_key
                      and a2.prefixPart = 'MGI:'
                      and a2._MGIType_key = 2
                      and a2._LogicalDB_key = 1
                      and a2.preferred = 1
                      and a2._Object_key = m._Marker_key
                order by tmp.mgiID2''' % idTempTable

    print('running sql for results4 %s' % time.strftime("%H.%M.%S.%m.%d.%y", \
        time.localtime(time.time())))
    sys.stdout.flush()
    results4 = db.sql(cmds, 'auto')
    
    # Organizer and Participant ID do not match
    db.sql('''select * 
                into temp nonExpComp
                from %s tmp
                where category != 'expresses_component' ''' % idTempTable, None)
    db.sql('''create index idxMgiID1 on nonExpComp (mgiID1)''', None)
    db.sql('''create index idxMgiID2 on nonExpComp (mgiID2)''', None)

    # exclude RV:0001555 'decreased_translational_product_level' as chromosome
    # check does not apply
    cmds = '''select distinct tmp.mgiID1 as org, tmp.mgiID2 as part, 
                tmp.category, mo.chromosome as oChr, mp.chromosome as pChr
                from nonExpComp tmp, ALL_Allele a, MRK_Marker mo, MRK_Marker mp, ACC_Accession ao, ACC_Accession ap
                where tmp.mgiID1TypeKey = 11
                and tmp.mgiID2TypeKey = 2
                and tmp.mgiID1 > 0
                and tmp.relID != 'RV:0001555'
                and tmp.mgiID1 = ao.numericPart
                and ao.prefixPart = 'MGI:'
                and ao._MGIType_key =  11
                and ao.preferred = 1
                and ao._Object_key = a._Allele_key
                and a._Marker_key = mo._Marker_key
                and tmp.mgiID2 > 0
                and tmp.mgiID2 = ap.numericPart
                and ap.prefixPart = 'MGI:'
                and ap._MGIType_key =  2
                and ap.preferred = 1
                and ap._Object_key = mp._Marker_key
                and mo.chromosome != mp.chromosome'''
    print('running sql for results5 %s' % time.strftime("%H.%M.%S.%m.%d.%y", \
        time.localtime(time.time())))
    sys.stdout.flush()
    results5 = db.sql(cmds, 'auto')
                
    print('writing OrgAllelePartMarker reports %s' % time.strftime("%H.%M.%S.%m.%d.%y" , time.localtime(time.time())))
    sys.stdout.flush()
    if len(results1a) >0 or len(results1b) >0 or len(results1c) >0  or \
        len(results2a) >0 or len(results2b) >0 or len(results2c):
        hasFatalErrors = 1
        fpQcRpt.write(CRT + CRT + str.center('Invalid Allele/Marker ' + 'Relationships',80) + CRT)
        fpQcRpt.write('%-12s  %-20s  %-20s  %-30s%s' % ('MGI ID','Object Type', 'Status','Reason',CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + '  ' + 20*'-' + '  ' + 30*'-' + CRT)

    #
    # Write MGI ID1 records to the report.
    #
    errorList = []
    for r in results1a:
        organizer = 'MGI:%s' % r['mgiID1']
        objectType = r['name']
        alleleStatus = r['status']

        if objectType == None:
            objectType = ''
        if alleleStatus == None:
            alleleStatus = ''

        reason = 'Organizer does not exist'
        errorList.append('%-12s  %-20s  %-20s  %-30s' % (organizer, objectType, alleleStatus, reason))

    for r in results1b:
        organizer = 'MGI:%s' % r['mgiID1']
        objectType = r['name']
        alleleStatus = r['status']

        if objectType == None:
            objectType = ''
        if alleleStatus == None:
            alleleStatus = ''
        
        reason = 'Organizer exists for non-allele'
        errorList.append('%-12s  %-20s  %-20s  %-30s' % (organizer, objectType, alleleStatus, reason))

    for r in results1c:
        organizer = 'MGI:%s' % r['mgiID1']
        objectType = r['name']
        alleleStatus = r['status']

        if objectType == None:
            objectType = ''
        if alleleStatus == None:
            alleleStatus = ''

        reason = 'Organizer allele status is invalid'
        errorList.append('%-12s  %-20s  %-20s  %-30s' % (organizer, objectType, alleleStatus, reason))

    #
    # Write MGI ID2 records to the report.
    #
    for r in results2a:
        organizer = 'MGI:%s' % r['mgiID2']
        objectType = r['name']
        alleleStatus = r['status']

        if objectType == None:
            objectType = ''
        if alleleStatus == None:
            alleleStatus = ''

        reason = 'Participant does not exist'
        errorList.append('%-12s  %-20s  %-20s  %-30s' % (organizer, objectType, alleleStatus, reason))

    for r in results2b:
        organizer = 'MGI:%s' % r['mgiID2']
        objectType = r['name']
        alleleStatus = r['status']

        if objectType == None:
            objectType = ''
        if alleleStatus == None:
            alleleStatus = ''

        reason = 'Participant exists for non-marker'
        errorList.append('%-12s  %-20s  %-20s  %-30s' % (organizer, objectType, alleleStatus, reason))

    for r in results2c:
        organizer = 'MGI:%s' % r['mgiID2']
        objectType = r['name']
        alleleStatus = r['status']

        if objectType == None:
            objectType = ''
        if alleleStatus == None:
            alleleStatus = ''

        reason = 'Participant marker status is invalid'
        errorList.append('%-12s  %-20s  %-20s  %-30s' % (organizer, objectType, alleleStatus, reason))

    s = set(errorList)
    errorList = list(s)
    fpQcRpt.write(CRT.join(errorList))

    if len(results3) >0 or len(results4) >0:
        hasFatalErrors = 1
        fpQcRpt.write(CRT + CRT + str.center('Secondary MGI IDs used in ' + 'Allele/Marker Relationships',80) + CRT)
        fpQcRpt.write('%-12s  %-20s  %-20s  %-28s%s' % ('2ndary MGI ID','Symbol', 'Primary MGI ID','Organizer or Participant?',CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + '  ' + 20*'-' + '  ' + 28*'-' + CRT)

        # report Organizer discrepancies
        for r in results3:
            sMgiID = 'MGI:%s' % r['mgiID1']
            symbol = r['symbol']
            pMgiID = r['accID']
            which = 'Organizer'
            fpQcRpt.write('%-12s  %-20s  %-20s  %-28s%s' % (sMgiID, symbol, pMgiID, which,  CRT))

        # report Participant discrepancies
        for r in results4:
            sMgiID = 'MGI:%s' % r['mgiID2']
            symbol = r['symbol']
            pMgiID = r['accID']
            which = 'Participant'
            fpQcRpt.write('%-12s  %-20s  %-20s  %-28s%s' % (sMgiID, symbol, pMgiID, which,  CRT))

    if len(results5):
        rptList = []
        for r in results5:
            oChr =  r['oChr']
            pChr = r['pChr']
            rptList.append('%-20s  %-20s  %-20s  %-20s' % ('MGI:%s' % r['org'], oChr, 'MGI:%s' % r['part'], pChr))

        if len(rptList):
            # report Chromosome mismatch between Organizer and Participant
            hasWarnErrors = 1
            fpWarnRpt.write(CRT + CRT + str.center('Mismatched chromosome in ' + 'Allele/Marker Relationships',80) + CRT)
            fpWarnRpt.write('%-20s  %-20s  %-20s  %-20s%s' % ('Organizer MGI ID','Organizer chromosome', 'Participant MGI ID', 'Participant chromosome', CRT))
            fpWarnRpt.write(20*'-' + '  ' + 20*'-' + '  ' + 20*'-' + '  ' + 20*'-' + CRT)
            fpWarnRpt.write(CRT.join(rptList) + CRT)

    return

# end qcOrgAllelePartMarker() -------------------------------

#
# Purpose: qc input  file for marker/marker relationships
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing
#

def qcOrgMarkerPartMarker():
    global hasFatalErrors

    # Find any MGI IDs from the relationship who's Organizer and
    # Participant are both markers and:
    # 1) Does not exist in the database.
    # 2) Exist for a non-marker object.
    # 3) Exist for a marker, but the status is not "official"
    # 4) Are secondary

    cmds = '''
        (select tmp.mgiID1, null as name, null as status
                from %s tmp
                where tmp.mgiID1TypeKey = 2
                and tmp.mgiID2TypeKey = 2
                and tmp.mgiID1 > 0
                and not exists(select 1
                from ACC_Accession a
                where a.numericPart = tmp.mgiID1
                and a.prefixPart = 'MGI:'))
                union
                (select tmp.mgiID1, t.name, null as status
                from %s tmp, ACC_Accession a1, ACC_MGIType t
                where a1.numericPart = tmp.mgiID1 
                and a1.prefixPart = 'MGI:'
                and tmp.mgiID1TypeKey = 2
                and tmp.mgiID2TypeKey = 2
                and a1._LogicalDB_key = 1
                and tmp.mgiID1 > 0
                and a1._MGIType_key != 2
                and a1._MGIType_key = t._MGIType_key
                and not exists (select 1 
                        from ACC_Accession a2
                        where a2.numericPart = tmp.mgiID1
                        and a2.prefixPart = 'MGI:'
                        and a2._LogicalDB_key = 1
                        and a2._MGIType_key = 2))
                union
                (select tmp.mgiID1, t.name, ms.status
                from %s tmp, ACC_Accession a, ACC_MGIType t,
                        MRK_Marker m, MRK_Status ms
                where a.numericPart = tmp.mgiID1 
                and a.prefixPart = 'MGI:'
                and tmp.mgiID1TypeKey = 2
                and tmp.mgiID2TypeKey = 2
                and tmp.mgiID1 > 0
                and a._LogicalDB_key = 1
                and a._MGIType_key = 2
                and a._MGIType_key = t._MGIType_key
                and a._Object_key = m._Marker_key
                and m._Marker_Status_key != 1
                and m._Marker_Status_key = ms._Marker_Status_key
                order by tmp.mgiID1)
                ''' % (idTempTable, idTempTable, idTempTable)
    #print cmds
    print('running sql for results1 %s' % time.strftime("%H.%M.%S.%m.%d.%y", \
        time.localtime(time.time())))
    sys.stdout.flush()
    results1 = db.sql(cmds, 'auto')

    cmds = '''
        (select tmp.mgiID2, null as name, null as status
                from %s tmp
                where tmp.mgiID1TypeKey = 2
                and tmp.mgiID2TypeKey = 2
                and tmp.mgiID2 > 0
                and not exists(select 1
                from ACC_Accession a
                where a.numericPart = tmp.mgiID2
                and a.prefixPart = 'MGI:'))
                union
                (select tmp.mgiID2, t.name, null as status
                from %s tmp, ACC_Accession a1, ACC_MGIType t
                where a1.numericPart = tmp.mgiID2
                and a1.prefixPart = 'MGI:'
                and tmp.mgiID1TypeKey = 2
                and tmp.mgiID2TypeKey = 2
                and tmp.mgiID2 > 0
                and a1._LogicalDB_key = 1
                and a1._MGIType_key != 2
                and a1._MGIType_key = t._MGIType_key
                and not exists (select 1
                        from ACC_Accession a2
                        where a2.numericPart = tmp.mgiID2
                        and a2.prefixPart = 'MGI:'
                        and a2._LogicalDB_key = 1
                        and a2._MGIType_key = 2))
                union
                (select tmp.mgiID2, t.name, ms.status
                from %s tmp,ACC_Accession a, ACC_MGIType t,
                        MRK_Marker m, MRK_Status ms
                where a.numericPart = tmp.mgiID2
                and a.prefixPart = 'MGI:'
                and tmp.mgiID1TypeKey = 2
                and tmp.mgiID2TypeKey = 2
                and tmp.mgiID2 > 0
                and a._MGIType_key = 2
                and a._LogicalDB_key = 1
                and a._MGIType_key = t._MGIType_key
                and a._Object_key = m._Marker_key
                and m._Marker_Status_key != 1
                and m._Marker_Status_key = ms._Marker_Status_key
                order by tmp.mgiID2)
                ''' % (idTempTable, idTempTable, idTempTable)
    #print cmds
    print('running sql for results2 %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time())))
    sys.stdout.flush()
    results2 = db.sql(cmds, 'auto')
 
    cmds = '''
                select tmp.mgiID1, m.symbol, a2.accID
                from %s tmp, ACC_Accession a1, ACC_Accession a2, MRK_Marker m
                where tmp.mgiID1 = a1.numericPart
                      and a1.prefixPart = 'MGI:'
                      and tmp.mgiID1TypeKey = 2
                      and tmp.mgiID2TypeKey = 2
                      and a1._MGIType_key = 2
                      and a1._LogicalDB_key = 1
                      and a1.preferred = 0
                      and a1._Object_key = a2._Object_key
                      and a2.prefixPart = 'MGI:'
                      and a2._MGIType_key = 2
                      and a2._LogicalDB_key = 1
                      and a2.preferred = 1
                      and a2._Object_key = m._Marker_key
                order by tmp.mgiID1
                ''' % idTempTable
    print('running sql for results3 %s' % time.strftime("%H.%M.%S.%m.%d.%y", \
        time.localtime(time.time())))
    sys.stdout.flush()
    results3 = db.sql(cmds, 'auto')

    cmds = '''
                select tmp.mgiID2, m.symbol, a2.accID
                from %s tmp, ACC_Accession a1, ACC_Accession a2, MRK_Marker m
                where tmp.mgiID2 = a1.numericPart
                      and a1.prefixPart = 'MGI:'
                      and tmp.mgiID1TypeKey = 2
                      and tmp.mgiID2TypeKey = 2
                      and a1._MGIType_key =  2
                      and a1._LogicalDB_key = 1
                      and a1.preferred = 0
                      and a1._Object_key = a2._Object_key
                      and a2.prefixPart = 'MGI:'
                      and a2._MGIType_key =  2
                      and a2._LogicalDB_key = 1
                      and a2.preferred = 1
                      and a2._Object_key = m._Marker_key
                order by tmp.mgiID2
                ''' % idTempTable

    print('running sql for results4 %s' % time.strftime("%H.%M.%S.%m.%d.%y" , \
        time.localtime(time.time())))
    sys.stdout.flush()
    results4 = db.sql(cmds, 'auto')
 
    print('writing OrgMarkerPartMarker reports  %s' % time.strftime("%H.%M.%S.%m.%d.%y" , time.localtime(time.time())))
    sys.stdout.flush()
    if len(results1) >0 or len(results2) >0:
        hasFatalErrors = 1
        fpQcRpt.write(CRT + CRT + str.center('Invalid Marker/Marker ' + 'Relationships',80) + CRT)
        fpQcRpt.write('%-12s  %-20s  %-20s  %-30s%s' % ('MGI ID','Object Type', 'Status','Reason',CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + '  ' + 20*'-' + '  ' + 30*'-' + CRT)
 
    #
    # Write MGI ID1 records to the report.
    #
    for r in results1:
        organizer = 'MGI:%s' % r['mgiID1']
        objectType = r['name']
        markerStatus = r['status']

        if objectType == None:
            objectType = ''
        if markerStatus == None:
            markerStatus = ''

        if objectType == '':
            reason = 'Organizer does not exist'
        elif markerStatus == '':
            reason = 'Organizer exists for non-marker'
        else:
            reason = 'Organizer	marker status is invalid'

        fpQcRpt.write('%-12s  %-20s  %-20s  %-30s%s' % (organizer, objectType, markerStatus, reason, CRT))

    #
    # Write MGI ID2 records to the report.
    #
    for r in results2:
        participant = 'MGI:%s' % r['mgiID2']
        objectType = r['name']
        markerStatus = r['status']

        if objectType == None:
            objectType = ''
        if markerStatus == None:
            markerStatus = ''

        if objectType == '':
            reason = 'Participant does not exist'
        elif markerStatus == '':
            reason = 'Participant exists for non-marker'
        else:
            reason = 'Participant marker status is invalid'

        fpQcRpt.write('%-12s  %-20s  %-20s  %-30s%s' % (participant, objectType, markerStatus, reason, CRT))

    if len(results3) >0 or len(results4) >0:
        hasFatalErrors = 1
        fpQcRpt.write(CRT + CRT + str.center('Secondary MGI IDs used in ' + 'Marker/Marker Relationships',80) + CRT)
        fpQcRpt.write('%-12s  %-20s  %-20s  %-28s%s' % ('2ndary MGI ID','Symbol', 'Primary MGI ID','Organizer or Participant?',CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + '  ' + 20*'-' + '  ' + 28*'-' + CRT)

        # report Organizer discrepancies
        for r in results3:
            sMgiID = 'MGI:%s' % r['mgiID1']
            symbol = r['symbol']
            pMgiID = r['accID']
            which = 'Organizer'
            fpQcRpt.write('%-12s  %-20s  %-20s  %-28s%s' % (sMgiID, symbol, pMgiID, which,  CRT))

        # report Participant discrepancies
        for r in results4:
            sMgiID = 'MGI:%s' % r['mgiID2']
            symbol = r['symbol']
            pMgiID = r['accID']
            which = 'Participant'
            fpQcRpt.write('%-12s  %-20s  %-20s  %-28s%s' % (sMgiID, symbol, pMgiID, which,  CRT))
    
    return

# end qcOrgMarkerPartMarker() -------------------------------

#
# Purpose: writes bad MGI IDs to the qc report
# Returns: Nothing
# Assumes: badIdDict has been created; see loadTempTables function
# Effects: writes to report in the file system
# Throws: Nothing
#

def qcInvalidMgiPrefix ():
    global hasFatalErrors

    # IDs w/o proper MGI ID prefix
    print('processing bad MGI IDs %s' % time.strftime("%H.%M.%S.%m.%d.%y" , time.localtime(time.time())))
    sys.stdout.flush()
    badIdList = []
    for id in list(badIdDict.keys()):
        badIdList.append('%-12s  %-20s' % (id, badIdDict[id]))

    #
    # Write bad MGI IDs to report
    #
    if len(badIdList):
        hasFatalErrors = 1
        fpQcRpt.write(CRT + CRT + str.center('Invalid MGI IDs',40) + CRT)
        fpQcRpt.write('%-15s  %-25s%s' % ('MGI ID','Organizer or Participant?', CRT))
        fpQcRpt.write(15*'-' + '  ' + 25*'-' + CRT)
        fpQcRpt.write(CRT.join(badIdList))

    return
 
# end qcInvalidMgiPrefix() -------------------------------

#
# Purpose: QC check the header row property columns
# Returns: 2 if bad Property column format or name
# Assumes: Nothing
# Effects: exits if bad property column format or name
# Throws: Nothing
#
def qcHeader(header):
    global propIndexDict, badPropList

    # all comparisons in lower case
    headerTokens = str.split(header.lower(), TAB)
    if headerTokens[0] != 'action':
        fpQcRpt.write('!!!!No Header Line in File!!!!')
        fpQcRpt.close()
        sys.exit(2)

    # total number of columns in the file
    numColumns = len(headerTokens)

    # col 1-13 - no property columns
    # col 14-N - property columns (parsed) or curator notes columns (ignored)
    # example property header: 'Property:score' or 'Property:data_source'
    colCt = 0
    for h in headerTokens:
        #print 'header: %s' % h
        colCt += 1

        if str.find(h, ':'):
            # remove leading/trailing WS e.g. ' Property : score ' -->
            # ['Property', 'score']
            tokens = list(map(str.strip, str.split(h, ':')))

            # property column header must have 'Property:' prefix
            if tokens[0] == 'property':

                # property column header must have one value
                if len(tokens) != 2:
                    badPropList.append('%-12s  %-20s  %-30s' % (lineCt, h, 'Property header with invalid format' ))

                # columns 1-13 may not be property columns
                elif colCt <= numNonPropCol:
                    badPropList.append('%-12s  %-20s  %-30s' % (lineCt, h, 'Property header in column 1-13' ))

                else:
                    value = tokens[1]

                    # property name must be in the controlled vocab
                    if value not in list(validPropDict.keys()):
                        badPropList.append('%-12s  %-20s  %-30s' % (lineCt, h.strip(), 'Invalid property value' ))
                    else:
                        propIndexDict[colCt-14] = [value, False]

    # if there are bad property column header(s) report them
    if len(badPropList):
        fpQcRpt.write(CRT + CRT + str.center('Invalid Properties',60) + CRT)
        fpQcRpt.write('%-12s  %-20s  %-20s%s' % ('Line#','Property Header', 'Reason', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(CRT.join(badPropList))
        fpQcRpt.close()
        sys.exit(2)

    return

# end qcHeader() -------------------------------

# Purpose: QC a delete line
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing
#
def processDelete(cDict, relDict, cat, obj1Id, obj2Id, relId, qual, evid, jNum, line, lineCt):
    global deleteRptList, deleteNotInDbList

    # resolve uniqueness key (UK) attributes to database keys
    catKey = cDict['_Category_key']
    orgMGITypeKey = cDict['_MGIType_key_1']
    if orgMGITypeKey == 11:
        #print 'allele'
        orgKey = alleleDict[obj1Id][0]
    else:
        #print 'marker'
        orgKey = markerDict[obj1Id][0]

    rvKey = relDict['_Object_key']
    partMGITypeKey = cDict['_MGIType_key_2']
    partKey = markerDict[obj2Id][0]
    qualKey = qualifierDict[qual]
    evidKey = evidenceDict[evid]
    refKey = jNumDict[jNum]

    # query for relationships with the uniqueness key 
    # sc 6/17 - this works and returns same number (27153789) in sybase 
    # and postgres when all the r.* = %s are removed. This is the count 
    # in the test databases for the MGI_Relationship_Property table; as
    # expected
    cmd = '''
        select r._Relationship_key, r._Category_key,
            r._Object_key_1,
            r._RelationshipTerm_key, r._Object_key_2,
            r._Qualifier_key, r._Evidence_key, r._Refs_key,
            t.term as propName, rp.value, n.note
        from MGI_Relationship r
        LEFT OUTER JOIN MGI_Relationship_Property rp on (
            r._Relationship_key = rp._Relationship_key
        )
        LEFT OUTER JOIN VOC_Term t on (
            rp._PropertyName_key = t._Term_key
        )
        LEFT OUTER JOIN MGI_Note n on (
            r._Relationship_key = n._Object_key
            and n._MGIType_key = 40
        )
        where r._Category_key = %s
        and r._Object_key_1 = %s
        and r._RelationshipTerm_key = %s
        and r._Object_key_2 = %s
        and r._Qualifier_key = %s
        and r._Evidence_key = %s
        and r._Refs_key = %s
        ''' % (catKey, orgKey, rvKey, partKey, qualKey, evidKey, refKey) #, 'auto')
    #print 'command'
    #print cmd
    results = db.sql(cmd, 'auto')

    #print 'results'
    #print results

    # organize rows by relationships key, may be multi properties/notes/
    # per relationship
    #  delRelDict = {relKey:r, ...}
    delRelDict = {}
    for r in results:
        relKey = r['_Relationship_key']
        if relKey not in delRelDict:
            delRelDict[relKey] = []
        delRelDict[relKey].append(r)
    # if UK not found in database, write to qc.rpt
    if not len(delRelDict):
        #print 'delete not in database'
        deleteNotInDbList.append('%-12s   %-68s' % (lineCt, str.strip(line)))
    else:
        # if delete in database write to delete.rpt and delete.sql
        #print 'delete in database'

        # get the result set for each relationship defined by the UK
        for rKey in delRelDict:
            rList = delRelDict[rKey]
            propList = []
            noteList = []
            for r in rList:
                # get the list of propertyName:propertyValue pairs and save
                prop = ''
                propName = r['propName']
                if propName != None:
                    value = r['value']
                    prop = '%s:"%s"' % (propName, value)
                    if prop not in propList:
                        propList.append(prop)
                #print 'prop: %s' % prop
                # get the list of notes and save
                note = r['note']
                #print 'note: %s' % note
                if note != None:
                    note = str.strip(note)
                    if note not in noteList:
                        #print 'appending note: "%s"' % note
                        noteList.append(str.strip(note))
                #print  noteList

            # get the organizer and participant symbols
            if catKey in (1001, 1002):  # marker/marker relationships
                obj1Symbol = markerDict[obj1Id][1]
                obj2Symbol = markerDict[obj2Id][1]
            else: # allele/marker relationships
                obj1Symbol = alleleDict[obj1Id][1]
                obj2Symbol = markerDict[obj2Id][1]

            # get the relationship term
            relTerm = relationshipDict[relId]['term']

            # create a report line and write it to the delete report
            rptLine = "%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s" \
                % (cat, TAB, obj1Id, TAB, obj1Symbol, TAB, relId, TAB, \
                relTerm, TAB, obj2Id, TAB, obj2Symbol, TAB, qual, TAB, \
                evid, TAB, jNum, TAB, TAB.join(propList), TAB, \
                ''.join(noteList))
            deleteRptList.append(rptLine)

            # creat a delete sql line and write it to the delete sql file 
            sqlLine  = 'delete from MGI_Relationship where _Relationship_key = %s;%s' % (rKey, CRT)
            fpDeleteSQL.write(sqlLine)

    return

# end processDelete() -------------------------------

#
# Purpose: run all QC checks
# Returns: Nothing
# Assumes: Nothing
# Effects: writes reports to the file system
# Throws: Nothing
#
def runQcChecks ():
    global hasFatalErrors, hasWarnErrors, deleteRptList, propIndexDict
    global badPropList, actionList, categoryList, qualifierList
    global evidenceList, jNumList, userList, relIdList, obsRelIdList
    global relVocabList, relDagList, badPropList, badPropValueList
    global missingPropColumnList
    global lineCt

    #
    # Expected columns; those not listed are for curator use
    #
    # col1 - action 
    # col2 - category
    # col3 - objId1 - Organizer
    # col5 - relationshipId 
    # col7 - objId2 - Participant
    # col9 - qual abbrev 
    # col10 - evid code 
    # col11 - jNum 
    # col12 - creator login 
    # col13 - notes
    # col14-N - properties columns
    # integer index of valid property columns mapped to list where
    # list[0] is the property name, list[1] is True if at least one row for that
    # property has data
    # {14:['score', True], ...}
    propIndexDict = {}

    # list of property columns with no data
    emptyPropColumnList = []

    # current line number we are parsing
    lineCt = 0
    
    #
    # Process the header for properties
    #

    header = fpInput.readline()  
    lineCt = 1
    print('Running qcHeader() %s' % time.strftime("%H.%M.%S.%m.%d.%y",time.localtime(time.time())))
    qcHeader(header)

    #
    # do the organizer/participant ID checks - these functions use temp table
    # and write any errors to the directly to the report
    #
    print('Running qcInvalidMgiPrefix() %s' % time.strftime("%H.%M.%S.%m.%d.%y",time.localtime(time.time())))
    qcInvalidMgiPrefix()

    print('Running qcOrgAllelePartMarker() %s' % time.strftime("%H.%M.%S.%m.%d.%y",time.localtime(time.time())))
    qcOrgAllelePartMarker()

    print('Running qcOrgMarkerPartMarker() %s' % time.strftime("%H.%M.%S.%m.%d.%y",time.localtime(time.time())))
    qcOrgMarkerPartMarker()
   
    #
    # Iterate through the input file to do the remaining QC checks
    #
    line = fpInput.readline()
    #print 'line: %s' % line
    lineCt += 1
    while line:

        # get the first 13 lines - these are fixed columns
        (action, cat, obj1Id, obj2sym, relId, relName, obj2Id, obj2sym, 
            qual, evid, jNum, creator, note) = list(map( \
              str.lower, list(map(str.strip, str.split( \
                line, TAB)))[:13]))

        remainingTokens = list(map(str.strip, str.split(line, TAB)[13:]))
        if len(remainingTokens) + numNonPropCol < numHeaderColumns:
            hasFatalErrors = 1
            missingPropColumnList.append('%-12s  %-20s' % (lineCt,line))
            line = fpInput.readline()
            lineCt += 1
            continue

        if action != 'add' and action != 'delete':
            hasFatalErrors = 1
            actionList.append('%-12s  %-20s' % (lineCt, action))

        # is the category value valid?
        if cat not in categoryDict:
            hasFatalErrors = 1
            categoryList.append('%-12s  %-20s' % (lineCt, cat))
            # if we don't know the category, we can't do all the QC checks
            # so continue to next line
            line = fpInput.readline()
            lineCt += 1
            continue
        else:
            cDict = categoryDict[cat]

        # default value when qual column empty is 'Not Specified'
        if qual == '':
            qual = 'not specified'

        # is the qualifier value valid?
        if qual not in qualifierDict:
            hasFatalErrors = 1
            qualifierList.append('%-12s  %-20s' % (lineCt, qual))

        # is the evidence value valid?
        if evid not in evidenceDict:
            hasFatalErrors = 1
            evidenceList.append('%-12s  %-20s' % (lineCt, evid))

        # is the J Number valid?
        if jNum not in jNumDict:
            hasFatalErrors = 1
            jNumList.append('%-12s  %-20s' % (lineCt, jNum))

        # is the user login valid?
        if creator not in userDict:
            hasFatalErrors = 1
            userList.append('%-12s  %-20s' % (lineCt, creator))

        # is the relationship ID valid?
        if relId not in relationshipDict:
            hasFatalErrors = 1
            relIdList.append('%-12s  %-20s' % (lineCt, relId))
        else:
            relDict = relationshipDict[relId]
        
            # is the relationship term obsolete?	
            if relDict['isObsolete'] != 0:
                hasFatalErrors = 1
                obsRelIdList.append('%-12s  %-20s' % (lineCt, relId))

            # is the relationship vocab different than the category vocab?
            # NOTE: since we are only using one vocab at this time, this
            # can never happen, leaving the code in for the future
            if relDict['_Vocab_key'] != cDict['_RelationshipVocab_key']:
                hasFatalErrors = 1
                relVocabList.append('%-12s  %-20s' % (lineCt, relId))

            # is the relationship DAG different than the category DAG?
            print(relId, relDict['_DAG_key'], cDict['_RelationshipDAG_key'])
            if relDict['_DAG_key'] != cDict['_RelationshipDAG_key']:
                hasFatalErrors = 1
                relDagList.append('%-12s  %-20s' % (lineCt, relId))
        
        # process a delete only if no fatal errors
        if action == 'delete' and not hasFatalErrors:
            processDelete(cDict, relDict, cat, obj1Id, obj2Id, relId, qual, evid, jNum, line, lineCt)

        # We only check properties for action=add i.e. not for deletes
        if action == 'add':
            # we will use this dict to compare the symbol in the file
            # to the symbol for the gene ID in the the database
            geneSymbol = ''
            egID = ''
            hasWarnErrors = 0

            for i in list(propIndexDict.keys()):
                # check for data in each property column
                propertyValue = remainingTokens[i]
                propertyName = propIndexDict[i][0]

                if propertyValue != '':
                    propIndexDict[i][1] = True

                # QC the 'score' property
                if propertyName == 'score' and propertyValue != '':
                    #print 'property is score, value is %s' % propertyValue
                    #print str.find(propertyValue, '+') == 0
                    if str.find(propertyValue, '+') == 0 or \
                          str.find(propertyValue, '-') == 0:
                        propertyValue = propertyValue[1:]
                        #print propertyValue
                    try:
                        propertyValueFloat = float(propertyValue)
                    except:
                        #print 'invalid score: %s' % propertyValue
                        hasFatalErrors = 1
                        badPropValueList.append('%-12s   %-20s  %-20s' % (lineCt, propertyName, propertyValue))

        line = fpInput.readline()
        lineCt += 1

    #
    # Check for no data in property columns - 
    #     we don't check properties for deletes
    #
    if action == 'add':	
        for i in list(propIndexDict.keys()):
            if propIndexDict[i][1] == False:
                emptyPropColumnList.append (propIndexDict[i][0])
        if emptyPropColumnList:
            fpWarnRpt.write('\nProperty Columns with no Data: %s' % CRT)
            for p in emptyPropColumnList:
                fpWarnRpt.write('    %s%s' % (p, CRT))
    #
    # Now write any errors to the report
    #
    writeReport()

    return

# end runQcChecks() -------------------------------

#
# Purpose: writes out errors to the qc report
# Returns: Nothing
# Assumes: Nothing
# Effects: writes report to the file system
# Throws: Nothing
#

def writeReport():
    global hasFatalErrors
    #
    # Now write any errors to the report
    #
    if len(actionList):
        fpQcRpt.write(CRT + CRT + str.center('Invalid Action Values',60) + CRT)
        fpQcRpt.write('%-12s  %-20s%s' % ('Line#','Action', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(CRT.join(actionList))

    if len(categoryList):
        fpQcRpt.write(CRT + CRT + str.center('Invalid Categories',60) + CRT)
        fpQcRpt.write('%-12s  %-20s%s' % ('Line#','Category', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(CRT.join(categoryList))
        
    if len(qualifierList):
        fpQcRpt.write(CRT + CRT + str.center('Invalid Qualifiers',60) + CRT)
        fpQcRpt.write('%-12s  %-20s%s' % ('Line#','Qualifier', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(CRT.join(qualifierList))

    if len(evidenceList):
        fpQcRpt.write(CRT + CRT + str.center('Invalid Evidence Codes',60) + CRT)
        fpQcRpt.write('%-12s  %-20s%s' % ('Line#','Evidence Code', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(CRT.join(evidenceList))

    if len(jNumList):
        fpQcRpt.write(CRT + CRT + str.center('Invalid J Numbers',60) + CRT)
        fpQcRpt.write('%-12s  %-20s%s' % ('Line#','J Number', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(CRT.join(jNumList))

    if len(userList):
        fpQcRpt.write(CRT + CRT + str.center('Invalid User Login',60) + CRT)
        fpQcRpt.write('%-12s  %-20s%s' % ('Line#','User Login', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(CRT.join(userList))

    if len(relIdList):
        fpQcRpt.write(CRT + CRT + str.center('Invalid Relationship IDs',60) + CRT)
        fpQcRpt.write('%-12s  %-20s%s' % ('Line#','Relationship ID', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(CRT.join(relIdList))

    if len(obsRelIdList):
        fpQcRpt.write(CRT + CRT + str.center('Obsolete Relationship IDs',60) + CRT)
        fpQcRpt.write('%-12s  %-20s%s' % ('Line#','Relationship ID', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(CRT.join(obsRelIdList))

    if len(relVocabList):
        fpQcRpt.write(CRT + CRT + str.center('Relationship Vocab not ' + 'the  same as Category Vocab',60) + CRT)
        fpQcRpt.write('%-12s  %-20s%s' % ('Line#','Relationship ID', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(CRT.join(relVocabList))

    if len(relDagList):
        fpQcRpt.write(CRT + CRT + str.center('Relationship DAG not the ' + 'same as Category DAG',60) + CRT)
        fpQcRpt.write('%-12s  %-20s%s' % ('Line#','Relationship ID', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(CRT.join(relDagList))

    if len(badPropValueList):
        fpQcRpt.write(CRT + CRT + str.center('Invalid Property Values',60)+ CRT)
        fpQcRpt.write('%-12s  %-20s  %-20s%s' % ('Line#','Property', 'Value', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(CRT.join(badPropValueList))

    if len(missingPropColumnList):
        fpQcRpt.write(CRT + CRT + str.center('Lines with Missing Property Columns',60)+ CRT)
        fpQcRpt.write('%-12s  %-20s%s' % ('Line#','Line', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(CRT.join(missingPropColumnList))

    if len(deleteNotInDbList):
        hasFatalErrors = 1
        fpQcRpt.write(CRT + CRT + str.center('Deletes not in Database',60)+ CRT)
        fpQcRpt.write('%-12s  %-68s %s' % ('Line#','Line', CRT))
        fpQcRpt.write(12*'-' + '  ' + 68*'-' + CRT)
        fpQcRpt.write(CRT.join( deleteNotInDbList))

    # if no fatal errors found write all deletes to informational delete report
    if len(deleteRptList) and not hasFatalErrors:
        fpWarnRpt.write('\nProcessing the specified input file will delete ' + \
            '%s relationship records from the database. See %s for details %s' % (len(deleteRptList), deleteRptFile, CRT))
        fpDeleteRpt.write(CRT + CRT + str.center('The following ' + 'relationships will be deleted from the database',60) + CRT)
        fpDeleteRpt.write(80*'-' + CRT)
        fpDeleteRpt.write(CRT.join( deleteRptList))

    return

# end writeReport() -------------------------------

#
# Purpose: Close the files.
# Returns: Nothing
# Assumes: Nothing
# Effects: Modifies global variables
# Throws: Nothing
#
def closeFiles ():
    global fpInput, fpQcRpt, fpWarnRpt, fpDeleteRpt, fpDeleteSQL
    fpInput.close()
    fpQcRpt.close()
    fpWarnRpt.close()
    fpDeleteRpt.close()
    fpDeleteSQL.close()
    return

# end closeFiles) -------------------------------

#
# Purpose: Load temp table with input file data
# Returns: Nothing
# Assumes: Connection to db has been established
# Effects: Modifies global variables
# Throws: Nothing
#
def loadTempTables ():
    global badIdDict, numHeaderColumns

    print('Create a bcp file from relationship input file')
    sys.stdout.flush()

    #
    # Open the input file.
    #
    try:
        fp = open(inputFile, 'r')
    except:
        print('Cannot open input file: %s' % inputFile)
        sys.exit(1)
    
    #
    # Read each record from the relationship input file
    # and write them to a bcp file.
    #
    junk = fp.readline() # header
    numHeaderColumns = len(str.split(junk, TAB))
    line = fp.readline()
    #print 'line: %s' % line
    while line:
        (action, cat, obj1Id, obj2sym, relId, relName, obj2Id, obj2sym, qual, evid, jNum, creator, note) = list(map(str.strip, str.split(line, TAB)))[:13]
        if cat not in categoryDict:
            print('FATAL ERROR Category: %s does not exist' % cat)
            sys.exit(1)
        #print 'category: %s' % cat
        # we are loading just the numeric part of the MGI ID for efficiency
        # if and Org ID or a Part ID is improperly formatted we load it as
        # 0 so we can QC the good ID
        badIdOrg = 0
        badIdPart = 0
        obj1IdInt = 0
        obj2IdInt = 0

        #
        # Process the Organizer ID	
        #
        # ID must have ':', suffix must exist, prefix must be 'MGI'
        if obj1Id.find(':') == -1 or len(obj1Id.split(':')[1]) == 0 or obj1Id.split(':')[0] != 'MGI':
            badIdDict[obj1Id] = 'Organizer'
            badIdOrg = 1
            #print 'badId Organizer: %s' % obj1Id
        else:
            obj1IdInt = obj1Id.split(':')[1]
            # suffix must be integer
            try:
                int(obj1IdInt)
            except:	# if suffix not integer assign it 0  so we can load 
                        # something into db, we'll filter it out later
                badIdDict[obj1Id] = 'Organizer'
                badIdOrg = 1
                obj1IdInt = 0

        #
        # Process the Participant ID
        #
        # ID must have ':', suffix must exist, prefix must be 'MGI'
        if obj2Id.find(':') == -1 or len(obj2Id.split(':')[1]) == 0 or obj2Id.split(':')[0] != 'MGI':
            badIdDict[obj2Id] = 'Participant'
            badIdPart = 1
            #print 'badId Participant %s' % obj2Id
        else:
            obj2IdInt = obj2Id.split(':')[1]
            # suffix must be integer
            try:
                int(obj2IdInt)
            except:	# if suffix not integer assign it 0 so we can load 
                        # something into db we'll filter it out later
                badIdDict[obj2Id] = 'Organizer'		
                badIdPart = 1
                obj2IdInt = 0
        #
        # if we have at least one good ID, load into temp table
        #   bad id will be zero
        #
        if not (badIdOrg and badIdPart):
            # get the MGI Types
            obj1IdTypeKey = categoryDict[cat]['_MGIType_key_1']
            obj2IdTypeKey = categoryDict[cat]['_MGIType_key_2']
            #print 'writing to bcp file: %s%s%s%s%s%s%s%s%s%s%s%s' % (obj1IdInt, TAB,  obj1IdTypeKey, TAB, obj2IdInt, TAB, obj2IdTypeKey, TAB, relId, TAB, cat, CRT)
            fpIDBCP.write('%s%s%s%s%s%s%s%s%s%s%s%s' % (obj1IdInt, TAB, obj1IdTypeKey, TAB, obj2IdInt, TAB, obj2IdTypeKey, TAB, relId, TAB, cat, CRT))

        line = fp.readline()

    #
    # Close the bcp file.
    #
    fp.close()
    fpIDBCP.close()

    #
    # Load the temp table with the input data.
    #
    #print 'Load the relationship data into the temp table: %s' % idTempTable
    sys.stdout.flush()
    bcpCmd = '%s %s %s %s ./ %s "\\t" "\\n" mgd' % (bcpin, server, database, idTempTable, idBcpFile)

    #print 'bcpCmd: %s' % bcpCmd
    rc = os.system(bcpCmd)
    if rc != 0:
        closeFiles()
        sys.exit(1)

#    db.sql('''create index idx1 on %s (mgiID1)''' % idTempTable, None)
#    db.sql('''create index idx2 on %s (mgiID1TypeKey)'''  % idTempTable, None)
#    db.sql('''create index idx3 on %s (mgiID2)''' % idTempTable, None)
#    db.sql('''create index idx4 on %s (mgiID2TypeKey)''' % idTempTable, None)

    return

# end loadTempTables() -------------------------------

#
# Main
#
print('checkArgs(): %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time())))
sys.stdout.flush()
checkArgs()

print('init(): %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time())))
sys.stdout.flush()
init()

print('runQcChecks(): %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time())))
sys.stdout.flush()
runQcChecks()

print('closeFiles(): %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time())))
sys.stdout.flush()
closeFiles()

db.useOneConnection(0)
print('done: %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time())))

if hasFatalErrors == 1 : 
    sys.exit(2)
else:
    sys.exit(0)

