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
#      2:  Fatal discrepancy errors detected and written to report
#      3:  Warning errors written to the command line
#      4:  Non-fatal discrepancy errors detected and written to report
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

# bcp file for MGI ID temp table
idBcpFile= os.environ['MGI_ID_BCP']
idTempTable = os.environ['MGI_ID_TEMP_TABLE']

# 1 if QC errors
hasFatalErrors = 0

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

# MGI_User lookup {userLogin:key, ...}
userDict = {}

# list of valid properties
propList = []

# proper MGI ID prefix in lowercase
mgiPrefix = 'mgi:'

# columns 1-numNonPropCol may NOT include properties
numNonPropCol=13
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
    global propList, passwordFile

    openFiles()

    #
    # create database connection
    #
    #user = os.environ['MGD_DBUSER']
    #passwordFile = os.environ['MGD_DBPASSWORDFILE']
    user = os.environ['MGI_PUBLICUSER']
    passwordFile = os.environ['MGI_PUBPASSWORDFILE']

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
	#print r['name'].lower()
        categoryDict[r['name'].lower()] = r

    # FeaR vocab lookup
    #print 'FeaR vocab lookup %s' % mgi_utils.date()
    results = db.sql('''select a.accID, a._Object_key, t.isObsolete, dn._DAG_key, vd._Vocab_key
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

    # Creator lookup
    #print 'creator lookup %s' % mgi_utils.date()
    results = db.sql('''select login, _User_key
        from MGI_User
        where _UserStatus_key = 316350''', 'auto')
    for r in results:
        userDict[r['login'].lower()] = r['_User_key']

    # Properties lookup
    results = db.sql('''select t.term
        from VOC_Term t
        where _Vocab_key = 97''', 'auto')
    for r in results:
        propList.append(r['term'].lower())
    #print 'propList: %s' % propList

    # for MGI ID verification
    loadTempTables()
    print 'Done loading temp tables'
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
    # 1) Does not exist in the database.
    # 2a) Organizer exists for a non-allele object.
    # 2b) Participant exists for a non-marker object.
    # 3a) Organizer exists for a allele, but the status is not 
    #      "approved" or "autoload".
    # 3b) Participant exists for a marker, but the status is not
    #      "official" or "interim"
    # 4) Are secondary

    # Organizer MGI ID does not exist in the database
    # union
    # Organizer MGI ID exists in the database for non-allele object(s)
    # union
    # Organizer  has invalid status

    db.useOneConnection(1)
    cmds = '''select tmp.mgiID1, null "name", null "status"
                from tempdb..%s tmp
                where tmp.mgiID1TypeKey = 11
		and tmp.mgiID2TypeKey = 2
                and not exists(select 1
                from ACC_Accession a
                where a.accID = tmp.mgiID1)
                union
                select tmp.mgiID1, t.name, null "status"
                from tempdb..%s tmp, ACC_Accession a1, ACC_MGIType t
                where a1.accID = tmp.mgiID1
                and tmp.mgiID1TypeKey = 11
		and tmp.mgiID2TypeKey = 2
                and a1._LogicalDB_key = 1
                and a1._MGIType_key != tmp.mgiID1TypeKey
                and a1._MGIType_key = t._MGIType_key
                and not exists (select 1
                        from ACC_Accession a2
                        where a2.accID = tmp.mgiID1
                        and a2._LogicalDB_key = 1
                        and a2._MGIType_key = 11)
                union
                select tmp.mgiID1, t.name, vt.term as status
                from tempdb..%s tmp,ACC_Accession a, ACC_MGIType t,
                        ALL_Allele aa, VOC_Term vt
                where a.accID = tmp.mgiID1
                and tmp.mgiID1TypeKey = 11
		and tmp.mgiID2TypeKey = 2
                and a._LogicalDB_key = 1
                and a._MGIType_key = tmp.mgiID1TypeKey
                and a._MGIType_key = t._MGIType_key
                and a._Object_key = aa._Allele_key
                and aa._Allele_Status_key not in (847114, 3983021)
                and aa._Allele_Status_key = vt._Term_key
                order by tmp.mgiID1''' % (idTempTable, idTempTable, idTempTable)
    #print cmds
    print 'running sql for results1 %s' % time.strftime("%H.%M.%S.%m.%d.%y" , time.localtime(time.time()))
    results1 = db.sql(cmds, 'auto')

    # Participant MGI ID does not exist in the database
    # union
    # Participant MGI ID exists in the database for non-marker object(s)
    # union
    # Participant  has invalid status

    cmds = '''select tmp.mgiID2, null "name", null "status"
                from tempdb..%s tmp
                where tmp.mgiID1TypeKey = 11
		and tmp.mgiID2TypeKey = 2
                and not exists(select 1
                from ACC_Accession a
                where a.accID = tmp.mgiID2)
                union
                select tmp.mgiID2, t.name, null "status"
                from tempdb..%s tmp, ACC_Accession a1, ACC_MGIType t
                where a1.accID = tmp.mgiID2
                and tmp.mgiID1TypeKey = 11
		and tmp.mgiID2TypeKey = 2
                and a1._LogicalDB_key = 1
                and a1._MGIType_key != tmp.mgiID2TypeKey
                and a1._MGIType_key = t._MGIType_key
                and not exists (select 1
                        from ACC_Accession a2
                        where a2.accID = tmp.mgiID2
                        and a2._LogicalDB_key = 1
                        and a2._MGIType_key = 2)
                union
                select tmp.mgiID2, t.name, ms.status
                from tempdb..%s tmp,ACC_Accession a, ACC_MGIType t,
                        MRK_Marker m, MRK_Status ms
                where a.accID = tmp.mgiID2
                and tmp.mgiID1TypeKey = 11
		and tmp.mgiID2TypeKey = 2
                and a._MGIType_key = tmp.mgiID2TypeKey
                and a._LogicalDB_key = 1
                and a._MGIType_key = t._MGIType_key
                and a._Object_key = m._Marker_key
                and m._Marker_Status_key not in (1,3)
                and m._Marker_Status_key = ms._Marker_Status_key
                order by tmp.mgiID2''' % (idTempTable, idTempTable, idTempTable)
    #print cmds
    print 'running sql for results2 %s ' % time.strftime("%H.%M.%S.%m.%d.%y" , time.localtime(time.time()))
    results2 = db.sql(cmds, 'auto')

    # Organizer ID is secondary
    db.sql('''select tmp.mgiID1, tmp.mgiID1TypeKey, a1._Object_key,  aa.symbol
		into #org
                  from tempdb..%s tmp,
                       ACC_Accession a1,
                       ALL_Allele aa
                  where tmp.mgiID1 = a1.accID
                        and tmp.mgiID1TypeKey = 11
                        and tmp.mgiID2TypeKey = 2
                        and a1._MGIType_key = tmp.mgiID1TypeKey
                        and a1._LogicalDB_key = 1
                        and a1.preferred = 0
                        and a1._Object_key = aa._Allele_key''' % idTempTable, None)
    db.sql('''create index idx1 on #org(_Object_key)''', None)
    cmds = '''select tmp.*, a2.accID
                 from #org tmp,
                       ACC_Accession a2
                  where tmp._Object_key = a2._Object_key
                        and a2._MGIType_key = tmp.mgiID1TypeKey
                        and a2._LogicalDB_key = 1
                        and a2.preferred = 1
			order by tmp.mgiID1''' 
 
    print 'running sql for results3 %s' % time.strftime("%H.%M.%S.%m.%d.%y" , time.localtime(time.time()))
    results3 = db.sql(cmds, 'auto')

    # Participant ID is secondary
    db.sql('''select tmp.mgiID2, tmp.mgiID2TypeKey, a1._Object_key,  aa.symbol
                into #part
                  from tempdb..%s tmp,
                       ACC_Accession a1,
                       ALL_Allele aa
                  where tmp.mgiID2 = a1.accID
                        and tmp.mgiID1TypeKey = 11
                        and tmp.mgiID2TypeKey = 2
                        and a1._MGIType_key = tmp.mgiID2TypeKey
                        and a1._LogicalDB_key = 1
                        and a1.preferred = 0
                        and a1._Object_key = aa._Allele_key''' % idTempTable, None)
    db.sql('''Create index idx1 on #part(_Object_key)''', None)
    cmds = '''select tmp.*, a2.accID
                 from #part tmp,
                       ACC_Accession a2
                  where tmp._Object_key = a2._Object_key
                        and a2._MGIType_key = tmp.mgiID2TypeKey
                        and a2._LogicalDB_key = 1
                        and a2.preferred = 1
                        order by tmp.mgiID2''' 

    print 'running sql for results4 %s' % time.strftime("%H.%M.%S.%m.%d.%y" , time.localtime(time.time()))
    results4 = db.sql(cmds, 'auto')
    
    cmds = '''select distinct tmp.mgiID1 as org, tmp.mgiID2 as part, 
		    mo.chromosome as oChr, mp.chromosome as pChr
		from tempdb..%s tmp,
		ALL_Allele a, MRK_Marker mo, MRK_Marker mp, ACC_Accession ao, ACC_Accession ap
		where tmp.mgiID1TypeKey = 11
		and tmp.mgiID2TypeKey = 2
		and tmp.mgiID1 = ao.accID
		and ao._MGIType_key =  tmp.mgiID1TypeKey
		and ao.preferred = 1
		and ao._Object_key = a._Allele_key
		and a._Marker_key = mo._Marker_key
		and tmp.mgiID2 = ap.accID
		and ap._MGIType_key =  tmp.mgiID2TypeKey
                and ap.preferred = 1
		and ap._Object_key = mp._Marker_key
		and mo.chromosome != mp.chromosome''' % idTempTable
    print 'running sql for results5 %s' % time.strftime("%H.%M.%S.%m.%d.%y" , time.localtime(time.time()))
    results5 = db.sql(cmds, 'auto')
		
	
    print 'writing OrgAllelePartMarker reports %s' % time.strftime("%H.%M.%S.%m.%d.%y" , time.localtime(time.time()))
    if len(results1) >0 or len(results2) >0:
        hasFatalErrors = 1
        fpQcRpt.write(CRT + CRT + string.center('Invalid Allele/Marker Relationships',80) + CRT)
        fpQcRpt.write('%-12s  %-20s  %-20s  %-30s%s' %
             ('MGI ID','Object Type',
              'Allele Status','Reason',CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + '  ' + \
              20*'-' + '  ' + 30*'-' + CRT)

    #
    # Write MGI ID1 records to the report.
    #
    for r in results1:
        organizer = r['mgiID1']
        objectType = r['name']
        alleleStatus = r['status']

        if objectType == None:
            objectType = ''
        if alleleStatus == None:
            alleleStatus = ''

        if objectType == '':
            reason = 'Organizer does not exist'
        elif alleleStatus == '':
            reason = 'Organizer exists for non-allele'
        else:
            reason = 'Organizer allele status is invalid'

        fpQcRpt.write('%-12s  %-20s  %-20s  %-30s%s' %
            (organizer, objectType, alleleStatus, reason, CRT))

    #
    # Write MGI ID2 records to the report.
    #
    for r in results2:
        participant = r['mgiID2']
        objectType = r['name']
        alleleStatus = r['status']

        if objectType == None:
            objectType = ''
        if alleleStatus == None:
            alleleStatus = ''

        if objectType == '':
            reason = 'Participant does not exist'
        elif alleleStatus == '':
            reason = 'Participant exists for non-marker'
        else:
            reason = 'Participant marker status is invalid'

        fpQcRpt.write('%-12s  %-20s  %-20s  %-30s%s' %
            (participant, objectType, alleleStatus, reason, CRT))

    if len(results3) >0 or len(results4) >0:
        hasFatalErrors = 1
        fpQcRpt.write(CRT + CRT + string.center('Secondary MGI IDs used in Allele/Marker Relationships',80) + CRT)
        fpQcRpt.write('%-12s  %-20s  %-20s  %-28s%s' %
             ('2ndary MGI ID','Symbol',
              'Primary MGI ID','Organizer or Participant?',CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + '  ' + \
              20*'-' + '  ' + 28*'-' + CRT)

        # report Organizer discrepancies
        for r in results3:
            sMgiID = r['mgiID1']
            symbol = r['symbol']
            pMgiID = r['accID']
            which = 'Organizer'
            fpQcRpt.write('%-12s  %-20s  %-20s  %-28s%s' %
                (sMgiID, symbol, pMgiID, which,  CRT))
        # report Participant discrepancies
        for r in results4:
            sMgiID = r['mgiID2']
            symbol = r['symbol']
            pMgiID = r['accID']
            which = 'Participant'
            fpQcRpt.write('%-12s  %-20s  %-20s  %-28s%s' %
                (sMgiID, symbol, pMgiID, which,  CRT))

    if len(results5) > 0:
	hasFatalErrors = 1
	fpQcRpt.write(CRT + CRT + string.center('Mismatched chromosome in Allele/Marker Relationships',80) + CRT)
	fpQcRpt.write('%-20s  %-20s  %-20s  %-20s%s' %
             ('Organizer MGI ID','Organizer chromosome', 
		'Participant MGI ID', 'Participant chromosome', CRT))
        fpQcRpt.write(20*'-' + '  ' + 20*'-' + '  ' + \
              20*'-' + '  ' + 20*'-' + CRT)
	# report Chromosome mismatch between Organizer and Participant
	for r in results5:
	    fpQcRpt.write('%-20s  %-20s  %-20s  %-20s%s' %
	    (r['org'], r['oChr'], r['part'], r['pChr'],  CRT))

    db.useOneConnection(0)
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
    # 3) Exist for a marker, but the status is not "official" or "interim".
    # 4) Are secondary
    db.useOneConnection(1)
    cmds = '''select tmp.mgiID1, null "name", null "status"
		from tempdb..%s tmp
		where tmp.mgiID1TypeKey = 2
		and tmp.mgiID2TypeKey = 2
		and not exists(select 1
		from ACC_Accession a
		where a.accID = tmp.mgiID1)
		union
	  	select tmp.mgiID1, t.name, null "status"
		from tempdb..%s tmp, ACC_Accession a1, ACC_MGIType t
		where a1.accID = tmp.mgiID1 
		and tmp.mgiID1TypeKey = 2
		and tmp.mgiID2TypeKey = 2
		and a1._LogicalDB_key = 1
		and a1._MGIType_key != tmp.mgiID1TypeKey
		and a1._MGIType_key = t._MGIType_key
		and not exists (select 1 
			from ACC_Accession a2
			where a2.accID = tmp.mgiID1
			and a2._LogicalDB_key = 1
			and a2._MGIType_key = 2)
		union
		select tmp.mgiID1, t.name, ms.status
		from tempdb..%s tmp,ACC_Accession a, ACC_MGIType t,
			MRK_Marker m, MRK_Status ms
		where a.accID = tmp.mgiID1 
		and tmp.mgiID1TypeKey = 2
		and tmp.mgiID2TypeKey = 2
		and a._LogicalDB_key = 1
		and a._MGIType_key = tmp.mgiID1TypeKey
		and a._MGIType_key = t._MGIType_key
                and a._Object_key = m._Marker_key
                and m._Marker_Status_key not in (1,3)
                and m._Marker_Status_key = ms._Marker_Status_key 
                order by tmp.mgiID1''' % (idTempTable, idTempTable, idTempTable)
    #print cmds
    print 'running sql for results1 %s' % time.strftime("%H.%M.%S.%m.%d.%y" , time.localtime(time.time()))
    results1 = db.sql(cmds, 'auto')

    cmds = '''select tmp.mgiID2, null "name", null "status"
                from tempdb..%s tmp
		where tmp.mgiID1TypeKey = 2
		and tmp.mgiID2TypeKey = 2
                and not exists(select 1
                from ACC_Accession a
                where a.accID = tmp.mgiID2)
                union
                select tmp.mgiID2, t.name, null "status"
                from tempdb..%s tmp, ACC_Accession a1, ACC_MGIType t
                where a1.accID = tmp.mgiID2
		and tmp.mgiID1TypeKey = 2
		and tmp.mgiID2TypeKey = 2
                and a1._LogicalDB_key = 1
                and a1._MGIType_key != tmp.mgiID2TypeKey
		and a1._MGIType_key = t._MGIType_key
                and not exists (select 1
                        from ACC_Accession a2
                        where a2.accID = tmp.mgiID2
                        and a2._LogicalDB_key = 1
                        and a2._MGIType_key = 2)
                union
                select tmp.mgiID2, t.name, ms.status
                from tempdb..%s tmp,ACC_Accession a, ACC_MGIType t,
                        MRK_Marker m, MRK_Status ms
                where a.accID = tmp.mgiID2
		and tmp.mgiID1TypeKey = 2
		and tmp.mgiID2TypeKey = 2
		and a._MGIType_key = tmp.mgiID2TypeKey
                and a._LogicalDB_key = 1
                and a._MGIType_key = t._MGIType_key
                and a._Object_key = m._Marker_key
                and m._Marker_Status_key not in (1,3)
                and m._Marker_Status_key = ms._Marker_Status_key
                order by tmp.mgiID2''' % (idTempTable, idTempTable, idTempTable)
    #print cmds
    print 'running sql for results2 %s' % time.strftime("%H.%M.%S.%m.%d.%y" , time.localtime(time.time()))
    results2 = db.sql(cmds, 'auto')
  
    db.sql('''select tmp.mgiID1, tmp.mgiID1TypeKey, a1._Object_key,  m.symbol
                into #org
                from tempdb..%s tmp,
                   ACC_Accession a1,
                   MRK_Marker m
                where tmp.mgiID1 = a1.accID
                    and tmp.mgiID1TypeKey = 2
                    and tmp.mgiID2TypeKey = 2
                    and a1._MGIType_key = tmp.mgiID1TypeKey
                    and a1._LogicalDB_key = 1
                    and a1.preferred = 0
                    and a1._Object_key = m._Marker_key''' % idTempTable, None)
    db.sql('''Create index idx1 on #org(_Object_key)''', None)
    cmds = '''select tmp.*, a2.accID
                 from #org tmp,
                       ACC_Accession a2
                  where tmp._Object_key = a2._Object_key
                        and a2._MGIType_key = tmp.mgiID1TypeKey
                        and a2._LogicalDB_key = 1
                        and a2.preferred = 1
		  order by tmp.mgiID1''' 
    print 'running sql for results3 %s' % time.strftime("%H.%M.%S.%m.%d.%y" , time.localtime(time.time()))
    results3 = db.sql(cmds, 'auto')

    db.sql('''select tmp.mgiID2, tmp.mgiID2TypeKey, a1._Object_key,  m.symbol
                into #part
                from tempdb..%s tmp,
                   ACC_Accession a1,
                   MRK_Marker m
                where tmp.mgiID2 = a1.accID
                    and tmp.mgiID1TypeKey = 2
                    and tmp.mgiID2TypeKey = 2
                    and a1._MGIType_key = tmp.mgiID2TypeKey
                    and a1._LogicalDB_key = 1
                    and a1.preferred = 0
                    and a1._Object_key = m._Marker_key''' % idTempTable, None)
    db.sql('''create index idx1 on #part(_Object_key)''', None)
    cmds = '''select tmp.*, a2.accID
                 from #part tmp,
                       ACC_Accession a2
                  where tmp._Object_key = a2._Object_key
                        and a2._MGIType_key = tmp.mgiID2TypeKey
                        and a2._LogicalDB_key = 1
                        and a2.preferred = 0
                  order by tmp.mgiID2'''

    print 'running sql for results4 %s' % time.strftime("%H.%M.%S.%m.%d.%y" , time.localtime(time.time()))
    results4 = db.sql(cmds, 'auto')
 
    print 'writing OrgMarkerPartMarker reports  %s' % time.strftime("%H.%M.%S.%m.%d.%y" , time.localtime(time.time()))
    if len(results1) >0 or len(results2) >0:
        hasFatalErrors = 1
        fpQcRpt.write(CRT + CRT + string.center('Invalid Marker/Marker Relationships',80) + CRT)
        fpQcRpt.write('%-12s  %-20s  %-20s  %-30s%s' %
             ('MGI ID','Object Type',
              'Marker Status','Reason',CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + '  ' + \
              20*'-' + '  ' + 30*'-' + CRT)
 
    #
    # Write MGI ID1 records to the report.
    #
    for r in results1:
        organizer = r['mgiID1']
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
        participant = r['mgiID2']
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
        fpQcRpt.write(CRT + CRT + string.center('Secondary MGI IDs used in Marker/Marker Relationships',80) + CRT)
        fpQcRpt.write('%-12s  %-20s  %-20s  %-28s%s' %
             ('2ndary MGI ID','Symbol',
              'Primary MGI ID','Organizer or Participant?',CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + '  ' + \
              20*'-' + '  ' + 28*'-' + CRT)

	# report Organizer discrepancies
	for r in results3:
	    sMgiID = r['mgiID1']
	    symbol = r['symbol']
	    pMgiID = r['accID']
	    which = 'Organizer'
	    fpQcRpt.write('%-12s  %-20s  %-20s  %-28s%s' %
		(sMgiID, symbol, pMgiID, which,  CRT))
	# report Participant discrepancies
	for r in results4:
	    sMgiID = r['mgiID2']
	    symbol = r['symbol']
	    pMgiID = r['accID']
	    which = 'Participant'
	    fpQcRpt.write('%-12s  %-20s  %-20s  %-28s%s' %
		(sMgiID, symbol, pMgiID, which,  CRT))
    db.useOneConnection(0)
def qcInvalidMgiPrefix ():
    global hasFatalErrors
    # IDs w/o proper MGI ID prefix
    badIdList = []
    print 'querying qcInvalidMgiPrefix %s' % time.strftime("%H.%M.%S.%m.%d.%y" , time.localtime(time.time()))
    results1 = db.sql('''select mgiID1 as organizer, mgiID2 as participant
                from tempdb..%s tmp''' % idTempTable, 'auto')
    print 'done querying qcInvalidMgiPrefix %s' % time.strftime("%H.%M.%S.%m.%d.%y" , time.localtime(time.time()))
    for r in results1:
	if string.find(r['organizer'].lower(), mgiPrefix ) == -1:
	    #print 'organizer not mgi id'
	    badIdList.append('%-12s  %-20s' % (r['organizer'], 'Organizer'))
	if string.find(r['participant'].lower(), mgiPrefix ) == -1:
	    #print 'participant not mgi id'
	    badIdList.append('%-12s  %-20s' % (r['participant'], 'Participant'))

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

#
# Purpose: run the QC checks
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing
#
def runQcChecks ():

    global hasFatalErrors
    #* col2 - category - verify in db
    #* col3 - objId1 - id exists, is primary and correct type (from category)
    # col5 - relationshipId - id exists, not obsolete, correct vocab/dag (from category)
    #* col7 - objId2 - id exists, is primary and correct type (from category)
    #* col9 - qual abbrev - if specified, abbrev exists
    #* col10 - evid code - code exists
    #* col11 - jNum - verify it exists
    #* col12 - creator login - verify exists
    #* col13 - notes
    #* col14-N - properties columns, may be mixed with non-property columns
    #*    which will be ignored
    
    #
    # parse the input file 
    #
    
    # QC error lists
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

    # integer index of valid property columns mapped to list where
    # list[0] is column header, list[1] is True of at least one row for that
    # property has data
    # {14:['score', True], ...}
    propIndexDict = {}
    emptyPropColumnList = []
    lineCt = 0

    #
    # Process the header for properties
    #
    header = fpInput.readline()  
    lineCt = 1

    # all comparisons in lower case
    headerTokens = string.split(header.lower(), TAB)

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
	    if tokens[0] == 'property':
		if len(tokens) != 2:
		    #print 'Property column with improper format'
		    badPropList.append('%-12s  %-20s  %-30s' %
                        (lineCt, h, 'Property header with invalid format' ))
		elif colCt <= numNonPropCol:
		    #print 'Property found in columns 1-13'
		    badPropList.append('%-12s  %-20s  %-30s' %
			(lineCt, h, 'Property header in column 1-13' ))
		else:
		    value = tokens[1]
		    if value not in propList:
			badPropList.append('%-12s  %-20s  %-30s' %
                        (lineCt, h.strip(), 'Invalid property value' ))
		    else:
			propIndexDict[colCt-14] = [value, False]
    if len(badPropList):
	fpQcRpt.write(CRT + CRT + string.center('Invalid Properties',60) + CRT)
        fpQcRpt.write('%-12s  %-20s  %-20s%s' %
             ('Line#','Property Header', 'Reason', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(string.join(badPropList, CRT))
        fpQcRpt.close()
	sys.exit(2)

    line = fpInput.readline()
    lineCt += 1
    while line:
	# get the first 13 lines - these are fixed columns
	(action, cat, obj1Id, obj2sym, relId, relName, obj2Id, obj2sym, qual, evid, jNum, creator, note) = map(string.lower, map(string.strip, string.split(line, TAB))[:13])
        remainingTokens = map(string.lower, map(string.strip, string.split(line
, TAB))[13:])
	#print remainingTokens
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
	    #print 'Incoming vocab key: %s Category vocab key: %s' % (relDict['_Vocab_key'], cDict['_RelationshipVocab_key'])
	    #print 'Rel vocab key: %s, cat vocab key %s' % (relDict['_Vocab_key'], cDict['_RelationshipVocab_key'])

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
	for i in propIndexDict.keys():
	    #print '%s: %s' % (i, propIndexDict[i][0])
	    propertyValue = remainingTokens[i]
	    propertyName = propIndexDict[i][0]
	    if propertyValue != '':
		propIndexDict[i][1] = True
	    if propertyName == 'score' and propertyValue != '':
		#print 'property is score, value is %s' % propertyValue
		#print string.find(propertyValue, '+') == 0 
		if string.find(propertyValue, '+') == 0 or string.find(propertyValue, '-') == 0:
		    propertyValue = propertyValue[1:]
		    #print propertyValue
		try:
		    propertyValueFloat = float(propertyValue)
		except:
		    #print 'invalid score: %s' % propertyValue
		    hasFatalErrors = 1
		    badPropValueList.append('%-12s   %-20s  %-20s' % (lineCt, propertyName, propertyValue))
		#print 'propertyValueFloat: %s' % propertyValueFloat
	
		    

	line = fpInput.readline()
	lineCt += 1
	
    # check for no data in a property column and write out to intermediate
    # file. This is a warning report
    #print 'propertyIndexDict'
    #print propIndexDict
    for i in propIndexDict.keys():
	if propIndexDict[i][1] == False:
	    emptyPropColumnList.append (propIndexDict[i][0])
    if emptyPropColumnList:
	fpWarn = open(os.environ['WARNING_RPT'], 'w')
	fpWarn.write('\nProperty Columns with no Data: %s' % CRT)
	for p in emptyPropColumnList:
	    fpWarn.write('    %s%s' % (p, CRT))
	fpWarn.close()

    # do the allele and marker organizer checks - these functions write 
    # any errors to the report
    print 'Running qcInvalidMgiPrefix() %s' % time.strftime("%H.%M.%S.%m.%d.%y" , time.localtime(time.time()))
    qcInvalidMgiPrefix()
    print 'Running qcOrgAllelePartMarker() %s' % time.strftime("%H.%M.%S.%m.%d.%y" , time.localtime(time.time()))
    qcOrgAllelePartMarker()
    print 'Running qcOrgMarkerPartMarker() %s' % time.strftime("%H.%M.%S.%m.%d.%y" , time.localtime(time.time()))
    qcOrgMarkerPartMarker()

    # write remaining errors to the report
    if len(actionList):
	fpQcRpt.write(CRT + CRT + string.center('Invalid Action Values',60) + CRT)
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
    if len(badPropValueList):
	fpQcRpt.write(CRT + CRT + string.center('Invalid Property Values',60) + CRT)
        fpQcRpt.write('%-12s  %-20s  %-20s%s' %
             ('Line#','Property', 'Value', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(string.join(badPropValueList, CRT))

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
	(action, cat, obj1Id, obj2sym, relId, relName, obj2Id, obj2sym, qual, evid, jNum, creator, note) = map(string.strip, string.split(line, TAB))[:13]
	if not categoryDict.has_key(cat):
	    print 'FATAL ERROR Category: %s does not exist' % cat
	    sys.exit(1)
	    
	obj1IdTypeKey = categoryDict[cat]['_MGIType_key_1']
	#print 'obj1IdTypeKey: %s' % obj1IdTypeKey
	obj2IdTypeKey = categoryDict[cat]['_MGIType_key_2']
	fpIDBCP.write('%s%s%s%s%s%s%s%s%s%s' % (obj1Id, TAB, obj1IdTypeKey, TAB, obj2Id, TAB, obj2IdTypeKey, TAB, cat, CRT))
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
print 'checkArgs(): %s' % time.strftime("%H.%M.%S.%m.%d.%y" , time.localtime(time.time()))
checkArgs()

print 'init(): %s' % time.strftime("%H.%M.%S.%m.%d.%y" , time.localtime(time.time()))
init()

print 'runQcChecks(): %s' % time.strftime("%H.%M.%S.%m.%d.%y" , time.localtime(time.time()))

runQcChecks()

print 'closeFiles(): %s' % time.strftime("%H.%M.%S.%m.%d.%y" , time.localtime(time.time()))

closeFiles()

print 'done: %s' % time.strftime("%H.%M.%S.%m.%d.%y" , time.localtime(time.time()))


if hasFatalErrors == 1 : 
    sys.exit(2)
else:
    sys.exit(0)
