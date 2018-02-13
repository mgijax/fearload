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
# 04/27/2017   sc  TR12291 - exclude decreased_translational_product_level (RV:0001555)
#                            from chromosome mismatch check for mutation_involves
#
#  03/11/2014  sc  Initial development
#
###########################################################################

import sys
import os
import string
import re
import mgi_utils
import db
import time
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
# bad expresses component property values
badECPropValueList = []

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

# list of expressed component input lines duplicated in the database
exprCompDupList = []

# expresses component lookup from the database
exprCompList = []

# template for expressed component relationships in the database
exprCompTemplate = '%s|%s|%s|%s|%s|%s|%s|%s'

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
        print USAGE
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
    global validPropDict, passwordFile, egSymbolDict, exprCompList

    # open input/output files
    openFiles()

    db.useOneConnection(1)

    #
    # create lookups
    #

    # FeaR Category Lookup
    results = db.sql('''select name, _Category_key, _RelationshipVocab_key, 
	    _RelationshipDAG_key, _MGIType_key_1, _MGIType_key_2
	from MGI_Relationship_Category''', 'auto')

    for r in results:
        categoryDict[r['name'].lower()] = r

    # FeaR vocab lookup
    #print 'FeaR vocab lookup %s' % mgi_utils.date()
    results = db.sql('''select a.accID, a._Object_key, t.term, t.isObsolete, 
	    dn._DAG_key, vd._Vocab_key
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
        relationshipDict[r['accID'].lower()] = r

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
    results = db.sql('''select a.accID, a._Object_key
        from ACC_Accession a
        where a._MGIType_key = 1
        and a._LogicalDB_key = 1
        and a.preferred = 1
        and a.private = 0
        and a.prefixPart = 'J:' ''', 'auto')
    for r in results:
        jNumDict[r['accID'].lower()] = r['_Object_key']

    #EntrezGene id to symbol lookup
    results = db.sql('''select a.accID, m.symbol
	from ACC_Accession a, MRK_Marker m
	where a._LogicalDB_key = 55
	and a._MGIType_key = 2
	and a.preferred = 1
	and a._Object_key = m._Marker_key
	and m._Organism_key != 1''', 'auto')
    for r in results:
	egSymbolDict[r['accID']] = r['symbol']

    # Creator lookup
    #print 'creator lookup %s' % mgi_utils.date()
    results = db.sql('''select login, _User_key
        from MGI_User
        where _UserStatus_key = 316350''', 'auto')
    for r in results:
        userDict[r['login'].lower()] = r['_User_key']

    # Properties lookup
    results = db.sql('''select _Term_key, term
        from VOC_Term 
        where _Vocab_key = 97''', 'auto')
    for r in results:
        validPropDict[r['term'].lower()] = r['_Term_key']
    #print 'validPropDict: %s' % validPropDict

    # Expresses component lookup
    # this returns 3836 on sybase and on postgres
    db.sql('''select r.*, a.accid as relID, v1.term as qual,
            v2.abbreviation as evid, v3.term as propName, p.value as propValue
        into temp EC
        from ACC_Accession a, VOC_Term v1, VOC_Term v2, MGI_Relationship r
        LEFT OUTER JOIN MGI_Relationship_Property p on (
            r._Relationship_key = p._Relationship_key)
        LEFT OUTER JOIN  VOC_Term v3 on (
            p._PropertyName_key = v3._Term_key)
        where r._Qualifier_key = v1._Term_key
        and r._Evidence_key = v2._Term_key
        and r._Category_key = 1004
        and r._RelationshipTerm_key = a._Object_key
        and a._MGIType_key = 13
        and a._LogicalDB_key = 171
	and preferred = 1''', None)
    
    db.sql('create index idxObjKey1 on EC(_Object_key_1)', None)
    db.sql('create index idxObjKey2 on EC(_Object_key_2)', None)

    results = db.sql('''select distinct a1.accID as alleleID, a2.accID as markerID, 
	    e.relID, e.qual, e.evid, e.propName, e.propValue, e._Relationship_key
	from EC e, ACC_Accession a1, ACC_Accession a2
	where e._Object_key_1 = a1._Object_key
	and a1._MGIType_key = 11
	and a1.prefixPart = 'MGI:'
	and a1._LogicalDB_key = 1
	and a1.preferred = 1
	and e._Object_key_2 = a2._Object_key
	and a2._MGIType_key = 2
	and a2.prefixPart = 'MGI:'
	and a2._LogicalDB_key = 1
	and a2.preferred = 1
	order by _Relationship_key''', 'auto')
    #print 'Num results: %s' % len(results)
    resDict = {}
    # organize by relationship; multi properties per relationship
    for r in results:
	key = r['_Relationship_key']
	if key not in resDict:
	    resDict[key] = []
	resDict[key].append(r)
    # not create expr comp lookup list $a
    for key in resDict.keys():
	#print key
	alleleID = resDict[key][0]['alleleID'].lower()
	relID =  resDict[key][0]['relID'].lower()
	markerID = resDict[key][0]['markerID'].lower()
	qual = resDict[key][0]['qual'].lower()
	evid = resDict[key][0]['evid'].lower()
	orgPropVal = ''
	symbolPropVal = ''
	geneIDPropVal = ''
	for r in resDict[key]:
	    if r['propName'] == None:
		continue
	    elif r['propName'] == 'Non-mouse_Organism':
		orgPropVal =  r['propValue'].lower()
	    elif r['propName'] == 'Non-mouse_Gene_Symbol':
		symbolPropVal =  r['propValue']
	    elif r['propName'] == 'Non-mouse_NCBI_Gene_ID':
		geneIDPropVal =  r['propValue']
	    else:
		print 'unrecognized property name'
        exprCompList.append(exprCompTemplate % (alleleID, relID, markerID, qual, evid, orgPropVal, symbolPropVal, geneIDPropVal))
    #print exprCompList

    #
    # load temp table from input file for MGI ID verification
    # 
    loadTempTables()
    print 'Done loading temp tables'

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
    results = db.sql('''select distinct tmp.mgiID1, 
		a1._Object_key as _Allele_key, aa.symbol as alleleSymbol, 
		tmp.mgiID2, a2._Object_key as _Marker_key, 
		m.symbol as markerSymbol
	    from %s tmp, ACC_Accession a1, ACC_Accession a2,
		ALL_Allele aa, MRK_Marker m
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
	    and a2._Object_key = m._Marker_key''' % idTempTable, 'auto')

    # load alleleDict and markerDict from query results
    for r in results:
	alleleID = 'mgi:%s' % r['mgiID1']
	#print '%s %s' % (alleleID, r['_Allele_key'])
	markerID = 'mgi:%s' % r['mgiID2']
	#print '%s %s' % (markerID, r['_Marker_key'])
	if not alleleDict.has_key(alleleID):
	    alleleDict[alleleID] = [r['_Allele_key'], r['alleleSymbol']]
	if not markerDict.has_key(markerID):
	    markerDict[markerID] = [r['_Marker_key'], r['markerSymbol']]
    #print alleleDict
    # load org=marker, part=marker from temp table
    results = db.sql('''select distinct tmp.mgiID1, 
		a1._Object_key as _Marker_key_1, m1.symbol as symbol1, 
		tmp.mgiID2, a2._Object_key as _Marker_key_2, 
		m2.symbol as symbol2
	    from %s tmp, ACC_Accession a1, ACC_Accession a2,
		MRK_Marker m1, MRK_Marker m2
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
	    and a2._Object_key = m2._Marker_key''' % \
		idTempTable, 'auto')
    # load markerDict from query results
    for r in results:
	markerID1 = 'mgi:%s' % r['mgiID1']
	markerID2 = 'mgi:%s' % r['mgiID2']

	if not markerDict.has_key(markerID1):
	    markerDict[markerID1] = [r['_Marker_key_1'], r['symbol1']]
	if not markerDict.has_key(markerID2):
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
        print 'Cannot open input file: %s' % inputFile
        sys.exit(1)

    #
    # Open QC report file
    #
    try:
        fpQcRpt = open(qcRptFile, 'w')
    except:
        print 'Cannot open report file: %s' % qcRptFile
        sys.exit(1)

    #
    # Open tempdb BCP file
    #
    try:
        fpIDBCP = open(idBcpFile, 'w')
    except:
        print 'Cannot open temp table bcp file: %s' % idBcpFile
        sys.exit(1)

    #
    # Open the warning report
    #
    try:
	fpWarnRpt = open(warnRptFile, 'w')
    except:
	print 'Cannot open warning report file: %s' % warnRptFile
        sys.exit(1)

    #
    # Open the delete report
    #
    try:
        fpDeleteRpt = open(deleteRptFile, 'w')
    except:
        print 'Cannot open delete report file: %s' % deleteRptFile
        sys.exit(1)

    #
    # Open the delete SQL file
    #
    try:
        fpDeleteSQL = open(deleteSQL, 'w')
    except:
        print 'Cannot open delete SQL file: %s' % deleteSQL
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
    global hasFatalErrors

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
    print 'running sql for results1a %s' % time.strftime("%H.%M.%S.%m.%d.%y", \
	time.localtime(time.time()))
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
    print 'running sql for results1b %s' % time.strftime("%H.%M.%S.%m.%d.%y", \
	time.localtime(time.time()))
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
    print 'running sql for results1c %s' % time.strftime("%H.%M.%S.%m.%d.%y", \
	time.localtime(time.time()))
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
    print 'running sql for results2a %s ' % time.strftime("%H.%M.%S.%m.%d.%y", \
	time.localtime(time.time()))
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
    print 'running sql for results2b %s ' % time.strftime("%H.%M.%S.%m.%d.%y", \
	time.localtime(time.time()))
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
    print 'running sql for results2c %s ' % time.strftime("%H.%M.%S.%m.%d.%y", \
	time.localtime(time.time()))
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

 
    print 'running sql for results3 %s' % time.strftime("%H.%M.%S.%m.%d.%y", \
	time.localtime(time.time()))
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

    print 'running sql for results4 %s' % time.strftime("%H.%M.%S.%m.%d.%y", \
	time.localtime(time.time()))
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
		    mo.chromosome as oChr, mp.chromosome as pChr
		from nonExpComp tmp,
		ALL_Allele a, MRK_Marker mo, MRK_Marker mp, ACC_Accession ao, 
		    ACC_Accession ap
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
    print 'running sql for results5 %s' % time.strftime("%H.%M.%S.%m.%d.%y", \
	time.localtime(time.time()))
    sys.stdout.flush()
    results5 = db.sql(cmds, 'auto')
		
	
    print 'writing OrgAllelePartMarker reports %s' % time.strftime( \
	"%H.%M.%S.%m.%d.%y" , time.localtime(time.time()))
    sys.stdout.flush()
    if len(results1a) >0 or len(results1b) >0 or len(results1c) >0  or \
	len(results2a) >0 or len(results2b) >0 or len(results2c):
        hasFatalErrors = 1
        fpQcRpt.write(CRT + CRT + string.center('Invalid Allele/Marker ' + \
	    'Relationships',80) + CRT)
        fpQcRpt.write('%-12s  %-20s  %-20s  %-30s%s' %
             ('MGI ID','Object Type',
              'Status','Reason',CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + '  ' + \
              20*'-' + '  ' + 30*'-' + CRT)

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

	errorList.append('%-12s  %-20s  %-20s  %-30s' %
            (organizer, objectType, alleleStatus, reason))
    for r in results1b:
	organizer = 'MGI:%s' % r['mgiID1']
        objectType = r['name']
        alleleStatus = r['status']

        if objectType == None:
            objectType = ''
        if alleleStatus == None:
            alleleStatus = ''
	
	reason = 'Organizer exists for non-allele'

 	errorList.append('%-12s  %-20s  %-20s  %-30s' %
            (organizer, objectType, alleleStatus, reason))

    for r in results1c:
	organizer = 'MGI:%s' % r['mgiID1']
        objectType = r['name']
        alleleStatus = r['status']

        if objectType == None:
            objectType = ''
        if alleleStatus == None:
            alleleStatus = ''

	reason = 'Organizer allele status is invalid'

        errorList.append('%-12s  %-20s  %-20s  %-30s' %
            (organizer, objectType, alleleStatus, reason))

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

        errorList.append('%-12s  %-20s  %-20s  %-30s' %
            (organizer, objectType, alleleStatus, reason))

    for r in results2b:
        organizer = 'MGI:%s' % r['mgiID2']
        objectType = r['name']
        alleleStatus = r['status']

        if objectType == None:
            objectType = ''
        if alleleStatus == None:
            alleleStatus = ''

        reason = 'Participant exists for non-marker'

        errorList.append('%-12s  %-20s  %-20s  %-30s' %
            (organizer, objectType, alleleStatus, reason))

    for r in results2c:
        organizer = 'MGI:%s' % r['mgiID2']
        objectType = r['name']
        alleleStatus = r['status']

        if objectType == None:
            objectType = ''
        if alleleStatus == None:
            alleleStatus = ''

        reason = 'Participant marker status is invalid'

        errorList.append('%-12s  %-20s  %-20s  %-30s' %
            (organizer, objectType, alleleStatus, reason))
    s = set(errorList)
    errorList = list(s)
    fpQcRpt.write(string.join(errorList, CRT))

    if len(results3) >0 or len(results4) >0:
        hasFatalErrors = 1
        fpQcRpt.write(CRT + CRT + string.center('Secondary MGI IDs used in ' + \
	    'Allele/Marker Relationships',80) + CRT)
        fpQcRpt.write('%-12s  %-20s  %-20s  %-28s%s' %
             ('2ndary MGI ID','Symbol',
              'Primary MGI ID','Organizer or Participant?',CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + '  ' + \
              20*'-' + '  ' + 28*'-' + CRT)

        # report Organizer discrepancies
        for r in results3:
            sMgiID = 'MGI:%s' % r['mgiID1']
            symbol = r['symbol']
            pMgiID = r['accID']
            which = 'Organizer'
            fpQcRpt.write('%-12s  %-20s  %-20s  %-28s%s' %
                (sMgiID, symbol, pMgiID, which,  CRT))
        # report Participant discrepancies
        for r in results4:
            sMgiID = 'MGI:%s' % r['mgiID2']
            symbol = r['symbol']
            pMgiID = r['accID']
            which = 'Participant'
            fpQcRpt.write('%-12s  %-20s  %-20s  %-28s%s' %
                (sMgiID, symbol, pMgiID, which,  CRT))

    if len(results5) > 0:
	hasFatalErrors = 1
	fpQcRpt.write(CRT + CRT + string.center('Mismatched chromosome in ' + \
	    'Allele/Marker Relationships',80) + CRT)
	fpQcRpt.write('%-20s  %-20s  %-20s  %-20s%s' %
             ('Organizer MGI ID','Organizer chromosome', 
		'Participant MGI ID', 'Participant chromosome', CRT))
        fpQcRpt.write(20*'-' + '  ' + 20*'-' + '  ' + \
              20*'-' + '  ' + 20*'-' + CRT)
	# report Chromosome mismatch between Organizer and Participant
	for r in results5:
	    fpQcRpt.write('%-20s  %-20s  %-20s  %-20s%s' %
	    ('MGI:%s' % r['org'], r['oChr'], 'MGI:%s' % r['part'], \
		r['pChr'],  CRT))

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
    cmds = '''(select tmp.mgiID1, null as name, null as status
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
                order by tmp.mgiID1)''' % (idTempTable, idTempTable, idTempTable)
    #print cmds
    print 'running sql for results1 %s' % time.strftime("%H.%M.%S.%m.%d.%y", \
	time.localtime(time.time()))
    sys.stdout.flush()
    results1 = db.sql(cmds, 'auto')

    cmds = '''(select tmp.mgiID2, null as name, null as status
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
                order by tmp.mgiID2)''' % (idTempTable, idTempTable, idTempTable)
    #print cmds
    print 'running sql for results2 %s' % time.strftime("%H.%M.%S.%m.%d.%y", \
	time.localtime(time.time()))
    sys.stdout.flush()
    results2 = db.sql(cmds, 'auto')
 
    cmds = '''select tmp.mgiID1,
                       m.symbol,
                       a2.accID
                from %s tmp,
                     ACC_Accession a1,
                     ACC_Accession a2,
                     MRK_Marker m
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
                order by tmp.mgiID1''' % idTempTable
    print 'running sql for results3 %s' % time.strftime("%H.%M.%S.%m.%d.%y", \
	time.localtime(time.time()))
    sys.stdout.flush()
    results3 = db.sql(cmds, 'auto')

    cmds = '''select tmp.mgiID2,
                       m.symbol,
                       a2.accID
                from %s tmp,
                     ACC_Accession a1,
                     ACC_Accession a2,
                     MRK_Marker m
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
                order by tmp.mgiID2''' % idTempTable

    print 'running sql for results4 %s' % time.strftime("%H.%M.%S.%m.%d.%y" , \
	time.localtime(time.time()))
    sys.stdout.flush()
    results4 = db.sql(cmds, 'auto')
 
    print 'writing OrgMarkerPartMarker reports  %s' % time.strftime( \
	"%H.%M.%S.%m.%d.%y" , time.localtime(time.time()))
    sys.stdout.flush()
    if len(results1) >0 or len(results2) >0:
        hasFatalErrors = 1
        fpQcRpt.write(CRT + CRT + string.center('Invalid Marker/Marker ' + \
	    'Relationships',80) + CRT)
        fpQcRpt.write('%-12s  %-20s  %-20s  %-30s%s' %
             ('MGI ID','Object Type',
              'Status','Reason',CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + '  ' + \
              20*'-' + '  ' + 30*'-' + CRT)
 
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

        fpQcRpt.write('%-12s  %-20s  %-20s  %-30s%s' %
            (organizer, objectType, markerStatus, reason, CRT))

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

	fpQcRpt.write('%-12s  %-20s  %-20s  %-30s%s' %
            (participant, objectType, markerStatus, reason, CRT))

    if len(results3) >0 or len(results4) >0:
        hasFatalErrors = 1
        fpQcRpt.write(CRT + CRT + string.center('Secondary MGI IDs used in ' + \
	'Marker/Marker Relationships',80) + CRT)
        fpQcRpt.write('%-12s  %-20s  %-20s  %-28s%s' %
             ('2ndary MGI ID','Symbol',
              'Primary MGI ID','Organizer or Participant?',CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + '  ' + \
              20*'-' + '  ' + 28*'-' + CRT)

	# report Organizer discrepancies
	for r in results3:
	    sMgiID = 'MGI:%s' % r['mgiID1']
	    symbol = r['symbol']
	    pMgiID = r['accID']
	    which = 'Organizer'
	    fpQcRpt.write('%-12s  %-20s  %-20s  %-28s%s' %
		(sMgiID, symbol, pMgiID, which,  CRT))
	# report Participant discrepancies
	for r in results4:
	    sMgiID = 'MGI:%s' % r['mgiID2']
	    symbol = r['symbol']
	    pMgiID = r['accID']
	    which = 'Participant'
	    fpQcRpt.write('%-12s  %-20s  %-20s  %-28s%s' %
		(sMgiID, symbol, pMgiID, which,  CRT))
    
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
    print 'processing bad MGI IDs %s' % time.strftime("%H.%M.%S.%m.%d.%y" , \
	time.localtime(time.time()))
    sys.stdout.flush()
    badIdList = []
    for id in badIdDict.keys():
	badIdList.append('%-12s  %-20s' % (id, badIdDict[id]))

    #
    # Write bad MGI IDs to report
    #
    if len(badIdList):
	hasFatalErrors = 1
        fpQcRpt.write(CRT + CRT + string.center('Invalid MGI IDs',40) + CRT)
        fpQcRpt.write('%-15s  %-25s%s' %
             ('MGI ID','Organizer or Participant?', CRT))
        fpQcRpt.write(15*'-' + '  ' + 25*'-' + CRT)
        fpQcRpt.write(string.join(badIdList, CRT))

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
    headerTokens = string.split(header.lower(), TAB)
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
        if string.find(h, ':'):
            # remove leading/trailing WS e.g. ' Property : score ' -->
            # ['Property', 'score']
            tokens = map(string.strip, string.split(h, ':'))
	    # property column header must have 'Property:' prefix
            if tokens[0] == 'property':
		# property column header must have one value
                if len(tokens) != 2:
                    badPropList.append('%-12s  %-20s  %-30s' %
                        (lineCt, h, 'Property header with invalid format' ))
		# columns 1-13 may not be property columns
                elif colCt <= numNonPropCol:
                    badPropList.append('%-12s  %-20s  %-30s' %
                        (lineCt, h, 'Property header in column 1-13' ))
                else:
                    value = tokens[1]
		    # property name must be in the controlled vocab
                    if value not in validPropDict.keys():
                        badPropList.append('%-12s  %-20s  %-30s' %
                        (lineCt, h.strip(), 'Invalid property value' ))
                    else:
                        propIndexDict[colCt-14] = [value, False]

    # if there are bad property column header(s) report them
    if len(badPropList):
        fpQcRpt.write(CRT + CRT + string.center('Invalid Properties',60) + CRT)
        fpQcRpt.write('%-12s  %-20s  %-20s%s' %
             ('Line#','Property Header', 'Reason', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(string.join(badPropList, CRT))
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
def processDelete(cDict, relDict, cat, obj1Id, obj2Id, relId, qual, \
	evid, jNum, line, lineCt):
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
    cmd = '''select r._Relationship_key, r._Category_key,
	    r._Object_key_1,
	    r._RelationshipTerm_key, r._Object_key_2,
	    r._Qualifier_key, r._Evidence_key, r._Refs_key,
	    t.term as propName, rp.value, nc.note
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
	LEFT OUTER JOIN MGI_NoteChunk nc on (
	    n._Note_key = nc._Note_key
 	)
	where r._Category_key = %s
	and r._Object_key_1 = %s
	and r._RelationshipTerm_key = %s
	and r._Object_key_2 = %s
	and r._Qualifier_key = %s
	and r._Evidence_key = %s
	and r._Refs_key = %s''' % (catKey, orgKey, rvKey, \
	    partKey, qualKey, evidKey, refKey) #, 'auto')
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
	if not delRelDict.has_key(relKey):
	    delRelDict[relKey] = []
	delRelDict[relKey].append(r)
    # if UK not found in database, write to qc.rpt
    if not len(delRelDict):
	#print 'delete not in database'
	deleteNotInDbList.append('%-12s   %-68s' % (lineCt, \
	    string.strip(line)))
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
		    note = string.strip(note)
		    if note not in noteList:
			#print 'appending note: "%s"' % note
			noteList.append(string.strip(note))
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
		evid, TAB, jNum, TAB, string.join(propList, TAB), TAB, \
		string.join(noteList, ''))
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
    global hasFatalErrors, deleteRptList, propIndexDict
    global badPropList, actionList, categoryList, qualifierList
    global evidenceList, jNumList, userList, relIdList, obsRelIdList
    global relVocabList, relDagList, badPropList, badPropValueList
    global missingPropColumnList
    global badECPropValueList, exprCompDupList, lineCt

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
    print 'Running qcHeader() %s' % time.strftime("%H.%M.%S.%m.%d.%y" , \
	time.localtime(time.time()))
    qcHeader(header)

    #
    # do the organizer/participant ID checks - these functions use temp table
    # and write any errors to the directly to the report
    #
    print 'Running qcInvalidMgiPrefix() %s' % time.strftime( \
	"%H.%M.%S.%m.%d.%y" , time.localtime(time.time()))
    qcInvalidMgiPrefix()

    print 'Running qcOrgAllelePartMarker() %s' % time.strftime( \
	"%H.%M.%S.%m.%d.%y" , time.localtime(time.time()))
    qcOrgAllelePartMarker()

    print 'Running qcOrgMarkerPartMarker() %s' % time.strftime( \
	"%H.%M.%S.%m.%d.%y" , time.localtime(time.time()))
    qcOrgMarkerPartMarker()
   
    #
    # Iterate through the input file to do the remaining QC checks
    #
    line = fpInput.readline()
    #print 'line: %s' % line
    lineCt += 1
    while line:

	# get the first 13 lines - these are fixed columns
	(action, cat, obj1Id, obj2sym, relId, relName, obj2Id, obj2sym, \
            qual, evid, jNum, creator, note) = map( \
              string.lower, map(string.strip, string.split( \
                line, TAB))[:13])

        remainingTokens = map(string.strip, string.split(line, TAB)[13:])
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
	if not categoryDict.has_key(cat):
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
	if not qualifierDict.has_key(qual):
	    hasFatalErrors = 1
	    qualifierList.append('%-12s  %-20s' % (lineCt, qual))

	# is the evidence value valid?
	if not evidenceDict.has_key(evid):
	    hasFatalErrors = 1
	    evidenceList.append('%-12s  %-20s' % (lineCt, evid))

	# is the J Number valid?
	if not jNumDict.has_key(jNum):
	    hasFatalErrors = 1
	    jNumList.append('%-12s  %-20s' % (lineCt, jNum))

	# is the user login valid?
	if not userDict.has_key(creator):
	    hasFatalErrors = 1
	    userList.append('%-12s  %-20s' % (lineCt, creator))

	# is the relationship ID valid?
	if not relationshipDict.has_key(relId):
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
	    if relDict['_DAG_key'] != cDict['_RelationshipDAG_key']:
		hasFatalErrors = 1
		relDagList.append('%-12s  %-20s' % (lineCt, relId))
	
	# process a delete only if no fatal errors
	if action == 'delete' and not hasFatalErrors:
	    processDelete(cDict, relDict, cat, obj1Id, obj2Id, relId, qual, \
		evid, jNum, line, lineCt)

	# We only check properties for action=add i.e. not for deletes
	if action == 'add':
	    # we will use this dict to compare the symbol in the file
	    # to the symbol for the gene ID in the the database
	    # if cat == 'expresses_component' and relId == 'rv:0000211':
	    geneSymbol = ''
	    egID = ''
	    hasWarnErrors = 0

	    # strictly for expresses component dup checking
	    orgPropVal = ''
	    symbolPropVal = ''
	    geneIDPropVal = ''

	    for i in propIndexDict.keys():
		# check for data in each property column
                propertyValue = remainingTokens[i]
                propertyName = propIndexDict[i][0]

		# strictly for expressed component dup checking
		if propertyName == 'non-mouse_organism':
		    orgPropVal = propertyValue.lower()
		elif propertyName == 'non-mouse_gene_symbol':
		    symbolPropVal = propertyValue
		elif propertyName == 'non-mouse_ncbi_gene_id':
		    geneIDPropVal = propertyValue
		#print '%s %s %s' % (orgPropVal, symbolPropVal, geneIDPropVal)

		if propertyValue != '':
		    propIndexDict[i][1] = True
		#
		# Property checks for category expresses component
		#

		# If relationship term = "expresses_an_orthologous gene" 
		# RV:0000211 then the properties Non-mouse_Organism and 
		# Non-mouse_Gene_Symbol are required to have a value 
		# (any text value).
		# if the property is non-mouse_ncbi_gene_id report warning if
		# 1) there is no value
		# 2) the value is not in the database
		# 3) the gene symbol associated with the value in the file not 
		#    same as the gene symbol in the database (case sensitive)
		if cat == 'expresses_component' and relId == 'rv:0000211':
		    if propertyName == 'non-mouse_organism' and \
			propertyValue == '':
			hasFatalErrors = 1
			reason = '"expresses_an_orthologous_gene" ' + \
			    'relationship with no "Non-mouse_Organism" ' + \
			    'property value'
			toReport = '%-12s   %-68s ' % (lineCt, reason)
			#if toReport not in badECPropValueList:
			badECPropValueList.append(toReport)
		    elif propertyName == 'non-mouse_gene_symbol' and \
			propertyValue == '':
                        hasFatalErrors = 1
                        reason = '"expresses_an_orthologous_gene" ' + \
			    'relationship with no "Non-mouse_Gene_Symbol" ' + \
			    'property value'
                        toReport = '%-12s   %-68s ' % (lineCt, reason)
                        #if toReport not in badECPropValueList:
                        badECPropValueList.append(toReport)
		    elif propertyName == 'non-mouse_ncbi_gene_id' and \
                        propertyValue == '':
			reason = 'Relationship term is ' + \
			    '"expresses_an_orthologous_gene" and ' + \
			    '"Non-mouse_NCBI_Gene_ID" not present.'
			fpWarnRpt.write('%-5s    %-75s%s' % \
			    (lineCt, reason, CRT))
			hasWarnErrors = 1
		    elif  propertyName == 'non-mouse_ncbi_gene_id':
		 	egID = propertyValue
		    elif propertyName == 'non-mouse_gene_symbol':
			geneSymbol = propertyValue
		    #print 'egID: %s geneSymbol: %s' % (egID, geneSymbol)
		# If relationship term = "expresses_mouse gene"
                # RV:0000210 then the properties Non-mouse_Organism and
                # Non-mouse_Gene_Symbol and Non-mouse_NCBI_Gene_ID
		# should have no value
                # (any text value).
		elif cat == 'expresses_component' and relId == 'rv:0000210':
		    if  propertyName == 'non-mouse_organism' and \
                        propertyValue != '':
                        hasFatalErrors = 1
			reason = '"expresses_mouse_gene" relationship with ' + \
			    'a "Non-mouse_Organism" property value'
                        toReport = '%-12s   %-68s ' % (lineCt, reason)
                        #if toReport not in badECPropValueList:
                        badECPropValueList.append(toReport)
			#print reason
		    elif  propertyName == 'non-mouse_gene_symbol' and \
                        propertyValue != '':
                        hasFatalErrors = 1
                        reason = '"expresses_mouse_gene" relationship with ' + \
				'a "Non-mouse_Gene_Symbol" property value'
                        toReport = '%-12s   %-68s ' % (lineCt, reason)
                        #if toReport not in badECPropValueList:
                        badECPropValueList.append(toReport)
			#print reason
		    elif propertyName == 'non-mouse_ncbi_gene_id' and \
			propertyValue != '':
                        hasFatalErrors = 1
                        reason = '"expresses_mouse_gene" relationship with ' + \
			    'a "Non-mouse_NCBI_Gene_ID" property value'
                        toReport = '%-12s   %-68s ' % (lineCt, reason)
                        #if toReport not in badECPropValueList:
                        badECPropValueList.append(toReport)
			#print reason
		# QC the 'score' property
                if propertyName == 'score' and propertyValue != '':
                    #print 'property is score, value is %s' % propertyValue
                    #print string.find(propertyValue, '+') == 0
                    if string.find(propertyValue, '+') == 0 or \
                          string.find(propertyValue, '-') == 0:
                        propertyValue = propertyValue[1:]
                        #print propertyValue
                    try:
                        propertyValueFloat = float(propertyValue)
                    except:
                        #print 'invalid score: %s' % propertyValue
                        hasFatalErrors = 1
                        badPropValueList.append('%-12s   %-20s  %-20s' % \
                            (lineCt, propertyName, propertyValue))

	    # After we've checked all properties, if 
	    # the category is expresses_component and
	    # the relID is rv:0000211 and
	    # we don't have a fatal or warning error check 
	    # check the geneSymbol in the file against the gene symbol 
	    # for the egID in the database (case insensitive)
	    if cat == 'expresses_component' and relId == 'rv:0000211' and egID != '' \
		    and not hasFatalErrors and not hasWarnErrors:
		if not egSymbolDict.has_key(egID):
		    reason = 'Non-mouse_NCBI_Gene_ID: %s not in MGI ' % \
			egID  + 'accession table'
		    fpWarnRpt.write('%-5s    %-75s%s' % (lineCt, reason, CRT))
		    #print reason
		else:
		    dbSymbol = egSymbolDict[egID]
		    if geneSymbol != dbSymbol:
			reason = 'Non-mouse_NCBI_Gene_ID: %s associated symbol: %s not equal to the non-mouse gene symbol in file: %s' % \
			    (egID, dbSymbol, geneSymbol)
			#print reason
			fpWarnRpt.write('%-5s    %-75s%s' % (lineCt, reason, CRT))


	    # after we've checked all properties, 
	    # if expresses component -  check for dups in the database for 
	    # expresses component HDP-2 US186
	    if cat == 'expresses_component':
		key = exprCompTemplate % (obj1Id, relId, obj2Id, qual, evid, orgPropVal, symbolPropVal, geneIDPropVal)
		if key in exprCompList:
		    hasFatalErrors = 1
		    reason = line
		    toReport = '%-12s   %-68s ' % (lineCt, reason)
		    exprCompDupList.append(toReport)

	line = fpInput.readline()
	lineCt += 1

    #
    # Check for no data in property columns - 
    #     we don't check properties for deletes
    #
    if action == 'add':	
	for i in propIndexDict.keys():
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
	fpQcRpt.write(CRT + CRT + string.center('Invalid Action Values',60) + \
	    CRT)
        fpQcRpt.write('%-12s  %-20s%s' %
             ('Line#','Action', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(string.join(actionList, CRT))

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
	fpQcRpt.write(CRT + CRT + string.center('Invalid Evidence Codes',60) + \
	    CRT)
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
	fpQcRpt.write(CRT + CRT + string.center('Invalid Relationship IDs',60) \
          + CRT)
        fpQcRpt.write('%-12s  %-20s%s' %
             ('Line#','Relationship ID', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(string.join(relIdList, CRT))

    if len(obsRelIdList):
	fpQcRpt.write(CRT + CRT + string.center('Obsolete Relationship IDs',60)\
 	   + CRT)
        fpQcRpt.write('%-12s  %-20s%s' %
             ('Line#','Relationship ID', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(string.join(obsRelIdList, CRT))

    if len(relVocabList):
	fpQcRpt.write(CRT + CRT + string.center('Relationship Vocab not ' + \
	    'the  same as Category Vocab',60) + CRT)
        fpQcRpt.write('%-12s  %-20s%s' %
             ('Line#','Relationship ID', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(string.join(relVocabList, CRT))

    if len(relDagList):
	fpQcRpt.write(CRT + CRT + string.center('Relationship DAG not the ' + \
	    'same as Category DAG',60) + CRT)
        fpQcRpt.write('%-12s  %-20s%s' %
             ('Line#','Relationship ID', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(string.join(relDagList, CRT))
    if len(badPropValueList):
	fpQcRpt.write(CRT + CRT + string.center('Invalid Property Values',60)+\
	    CRT)
        fpQcRpt.write('%-12s  %-20s  %-20s%s' %
             ('Line#','Property', 'Value', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(string.join(badPropValueList, CRT))
    if len(missingPropColumnList):
	fpQcRpt.write(CRT + CRT + string.center('Lines with Missing Property Columns',60)+\
            CRT)
        fpQcRpt.write('%-12s  %-20s%s' %
             ('Line#','Line', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(string.join(missingPropColumnList, CRT))

    if len(badECPropValueList):
        fpQcRpt.write(CRT + CRT + string.center('Invalid Expresses Component Property Values',60)+\
            CRT)
        fpQcRpt.write('%-12s  %-68s %s' % ('Line#','Reason', CRT))
	fpQcRpt.write(12*'-' + '  ' + 68*'-' + CRT)
        fpQcRpt.write(string.join(badECPropValueList, CRT))

    if len(deleteNotInDbList):
	hasFatalErrors = 1
	fpQcRpt.write(CRT + CRT + string.center('Deletes not in Database',60)+\
	    CRT)
        fpQcRpt.write('%-12s  %-68s %s' % ('Line#','Line', CRT))
        fpQcRpt.write(12*'-' + '  ' + 68*'-' + CRT)
        fpQcRpt.write(string.join( deleteNotInDbList, CRT))

    if len(exprCompDupList):
        fpQcRpt.write(CRT + CRT + string.center('Expressed Component lines Duplicated in the Database',60)+\
            CRT)
        fpQcRpt.write('%-12s  %-68s %s' % ('Line#','Line', CRT))
        fpQcRpt.write(12*'-' + '  ' + 68*'-' + CRT)
        fpQcRpt.write(string.join( exprCompDupList, CRT))

    # if no fatal errors found write all deletes to informational delete report
    if len(deleteRptList) and not hasFatalErrors:
	fpWarnRpt.write('\nProcessing the specified input file will delete ' + \
	    '%s relationship records from the database. See %s for details %s' \
	    % (len(deleteRptList), deleteRptFile, CRT))

	fpDeleteRpt.write(CRT + CRT + string.center('The following ' + \
	    'relationships will be deleted from the database',60) + CRT)
        fpDeleteRpt.write(80*'-' + CRT)
        fpDeleteRpt.write(string.join( deleteRptList, CRT))
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
    numHeaderColumns = len(string.split(junk, TAB))
    line = fp.readline()
    #print 'line: %s' % line
    while line:
	(action, cat, obj1Id, obj2sym, relId, relName, obj2Id, obj2sym, qual, \
	    evid, jNum, creator, note) = map(string.strip, string.split( \
		line, TAB))[:13]
	if not categoryDict.has_key(cat):
            print 'FATAL ERROR Category: %s does not exist' % cat
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
	if obj1Id.find(':') == -1 or len(obj1Id.split(':')[1]) == 0 or \
	        obj1Id.split(':')[0] != 'MGI':
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
	if obj2Id.find(':') == -1 or len(obj2Id.split(':')[1]) == 0 or \
		obj2Id.split(':')[0] != 'MGI':
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

	    fpIDBCP.write('%s%s%s%s%s%s%s%s%s%s%s%s' % (obj1IdInt, TAB, \
		obj1IdTypeKey, TAB, obj2IdInt, TAB, obj2IdTypeKey, TAB, relId, TAB, \
		cat, CRT))
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
## NEW
    bcpCmd = '%s %s %s %s ./ %s "\\t" "\\n" mgd' % (bcpin, server, database, idTempTable, idBcpFile)

    #print 'bcpCmd: %s' % bcpCmd
    rc = os.system(bcpCmd)
    if rc <> 0:
        closeFiles()
        sys.exit(1)

#    db.sql('''create index idx1 on %s (mgiID1)''' % idTempTable, None)
#    db.sql('''create index idx2 on %s (mgiID1TypeKey)'''  % \
#	idTempTable, None)
#    db.sql('''create index idx3 on %s (mgiID2)''' % idTempTable, None)
#    db.sql('''create index idx4 on %s (mgiID2TypeKey)''' % \
#	idTempTable, None)

    return

# end loadTempTables() -------------------------------

#
# Main
#
print 'checkArgs(): %s' % time.strftime("%H.%M.%S.%m.%d.%y", \
    time.localtime(time.time()))
sys.stdout.flush()
checkArgs()

print 'init(): %s' % time.strftime("%H.%M.%S.%m.%d.%y", \
    time.localtime(time.time()))
sys.stdout.flush()
init()

print 'runQcChecks(): %s' % time.strftime("%H.%M.%S.%m.%d.%y", \
    time.localtime(time.time()))
sys.stdout.flush()
runQcChecks()

print 'closeFiles(): %s' % time.strftime("%H.%M.%S.%m.%d.%y", 
    time.localtime(time.time()))
sys.stdout.flush()
closeFiles()

db.useOneConnection(0)
print 'done: %s' % time.strftime("%H.%M.%S.%m.%d.%y", 
    time.localtime(time.time()))


if hasFatalErrors == 1 : 
    sys.exit(2)
else:
    sys.exit(0)
