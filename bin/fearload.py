#!/usr/local/bin/python
#
#  fearload.py
###########################################################################
#
#  Purpose:
#
#      Validate input and create feature relationships bcp file
#
#  Usage:
#
#      fearload.py 
#
#  Inputs:
#
#	1. load-ready FeaR file tab-delimited in the following format
#	    1. Action
#	    2. Category
#	    3. Object1 ID
#	    4. Object1 symbol (Optional)
#	    5. Relationship ID
#	    6. Relationship Name (Optional)
#	    7. Object2 ID
#	    8. Object2 symbol (Optional)
#	    9. Qualifier (Optional)
#	    10. Evidence code
#	    11. J: Number
#	    12. Curator login
#	    13. Notes (Optional)
#	    14. Properties in key=value pairs (Optional)
#
#	2. Configuration - see fearload.config
#
#  Outputs:
#
#       1. MGI_Relationship.bcp
#	2. MGI_Relationship_Property.bcp
#	3. MGI_Note 
#	4. MGI_NoteChunk 
#
#  Exit Codes:
#
#      0:  Successful completion
#      1:  An exception occurred
#
#  Assumes:
#
#      1) Sanity/QC checks have been run and all errors fixed
#	
#  Implementation:
#
#      This script will perform following steps:
#
#      1) Validate the arguments to the script.
#      2) Perform initialization steps.
#      3) Open the input/output files.
#      4) Run the sanity/QC  checks
#      5) Run the load if sanity/QC checks pass
#      6) Close the input/output files.
#      7) Note: this script ignores deletes as these have already
#	    been written to an SQL file by fearQC.py
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

import sys
import os
import string
import db
import mgi_utils

db.setAutoTranslate(False)
db.setAutoTranslateBE(False)

#
#  CONSTANTS
#
TAB = '\t'
CRT = '\n'
DATE = mgi_utils.date("%m/%d/%Y")
USAGE='fearload.py'

#
#  GLOBALS
#

# input file
inFile = os.environ['INPUT_FILE_DEFAULT']

# output bcp files
relationshipFile =   os.environ['RELATIONSHIP_BCP']
propertyFile = os.environ['PROPERTY_BCP']
noteFile = os.environ['NOTE_BCP']
noteChunkFile = os.environ['NOTECHUNK_BCP']

# file descriptors
fpInFile = ''
fpRelationshipFile = ''
fpPropertyFile = ''
fpNoteFile = ''
fpNoteChunkFile = ''

# database primary keys, will be set to the next available from the db
nextRelationshipKey = 1000	# MGI_Relationship._Relationship_key
nextPropertyKey = 1000		# MGI_Relationship_Property._Property_key
nextNoteKey = 1000		# MGI_Note._Note_key

# relationship mgiType 
relationshipMgiTypeKey = 40

# relationship note type key
relationshipNoteTypeKey =  1042

# category lookup {name:result set, ...}
categoryDict = {}

# relationship term lookup {term:key, ...}
relationshipDict = {}

# qualifier term lookup {term:key, ...}
qualifierDict = {}

# default when qualifier is blank in the input file
defaultQual = 'not specified'

# evidence term lookup {termAbbrev:key, ...}
evidenceDict = {}

# reference ID (JNum) lookup {jNum:refsKey, ...}
jNumDict = {}

# marker lookup {mgiID:key, ...)
markerDict = {}

# allele lookup {mgiID:key, ...)
alleleDict = {}

# MGI_User lookup {userLogin:key, ...}
userDict = {}

# property lookup (propName:key, ...)
propertyDict = {}

def checkArgs ():
    # Purpose: Validate the arguments to the script.
    # Returns: Nothing
    # Assumes: Nothing
    # Effects: exits if unexpected args found on the command line
    # Throws: Nothing

    if len(sys.argv) != 1:
        print USAGE
        sys.exit(1)
    return

# end checkArgs() -------------------------------

def init():
    # Purpose: create lookups, open files, create db connection, gets max
    #	keys from the db
    # Returns: Nothing
    # Assumes: Nothing
    # Effects: Sets global variables, exits if a file can't be opened,
    #  creates files in the file system, creates connection to a database

    global nextRelationshipKey, nextPropertyKey, nextNoteKey
    global categoryDict, relationshipDict, propertyDict
    global qualifierDict, evidenceDict, jNumDict, userDict, markerDict
    global alleleDict

    #
    # Open input and output files
    #
    openFiles()

    #
    # create database connection
    #
    user = os.environ['MGD_DBUSER']
    passwordFileName = os.environ['MGD_DBPASSWORDFILE']
    db.useOneConnection(1)
    db.set_sqlUser(user)
    db.set_sqlPasswordFromFile(passwordFileName)

    #
    # get next MGI_Relationship and MGI_Relationship_Property keys
    #
    results = db.sql('''select max(_Relationship_key) + 1 as nextKey
	    from MGI_Relationship''', 'auto')
    if results[0]['nextKey'] is None:
	nextRelationshipKey = 1000
    else:
	nextRelationshipKey = results[0]['nextKey']

    results = db.sql('''select max(_RelationshipProperty_key) + 1 as nextKey
            from MGI_Relationship_Property''', 'auto')
    if results[0]['nextKey'] is None:
        nextPropertyKey = 1000
    else:
        nextPropertyKey = results[0]['nextKey']

    #
    # get next MGI_Note key
    #
    results = db.sql('''select max(_Note_key) + 1 as nextKey
	from MGI_Note''', 'auto')
        
    nextNoteKey = results[0]['nextKey']

    #
    # create lookups
    #

    # FeaR Category Lookup
    results = db.sql('''select * from MGI_Relationship_Category''', 'auto')
    for r in results:
	name = r['name'].lower()
  	cat = Category()
	cat.key = r['_Category_key']
	cat.name = name
	cat.mgiTypeKey1 = r['_MGIType_key_1']
	cat.mgiTypeKey2 = r['_MGIType_key_2']
	categoryDict[name] = cat

    # FeaR vocab lookup
    results = db.sql('''select a.accid, a._Object_key
	from ACC_Accession a, VOC_Term t
	where a._MGIType_key = 13 
	and a._LogicalDB_key = 171
	and a.preferred = 1
	and a.private = 0
	and a._Object_key = t._Term_key
	and t.isObsolete = 0''', 'auto')
    for r in results:
	relationshipDict[r['accid'].lower()] = r['_Object_key']

    # FeaR qualifier lookup
    results = db.sql('''select _Term_key, term
        from VOC_Term
        where _Vocab_key = 94
        and isObsolete = 0''', 'auto')
    for r in results:
        qualifierDict[r['term'].lower()] = r['_Term_key']

    # FeaR evidence lookup
    results = db.sql('''select _Term_key, abbreviation
        from VOC_Term
        where _Vocab_key = 95
        and isObsolete = 0''', 'auto')
    for r in results:
        evidenceDict[r['abbreviation'].lower()] = r['_Term_key']

    # Reference lookup
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
    results = db.sql('''select a.accid, a._Object_key
        from ACC_Accession a
        where a._MGIType_key = 2
        and a._LogicalDB_key = 1
        and a.preferred = 1
        and a.private = 0''', 'auto')
    for r in results:
        markerDict[r['accid'].lower()] = r['_Object_key']

    # allele lookup
    results = db.sql('''select a.accid, a._Object_key
        from ACC_Accession a
        where a._MGIType_key = 11
        and a._LogicalDB_key = 1
        and a.preferred = 1
        and a.private = 0''', 'auto')
    for r in results:
        alleleDict[r['accid'].lower()] = r['_Object_key']

    # active status (not data load or inactive)
    results = db.sql('''select login, _User_key
	from MGI_User
	where _UserStatus_key = 316350''', 'auto')
    for r in results:
	userDict[r['login'].lower()] = r['_User_key']

    # property term lookup
    results = db.sql('''select term, _Term_key
        from VOC_Term
        where _Vocab_key = 97''', 'auto')
    for r in results:
        propertyDict[r['term'].lower()] = r['_Term_key']

    db.useOneConnection(0)
    
    return

# end init() -------------------------------

def openFiles ():
    # Purpose: Open input/output files.
    # Returns: Nothing
    # Assumes: Nothing
    # Effects: Sets global variables, exits if a file can't be opened, 
    #  creates files in the file system

    global fpInFile, fpRelationshipFile, fpPropertyFile
    global fpNoteFile, fpNoteChunkFile

    try:
        fpInFile = open(inFile, 'r')
    except:
        print 'Cannot open Feature relationships input file: %s' % inFile
        sys.exit(1)

    try:
        fpRelationshipFile = open(relationshipFile, 'w')
    except:
        print 'Cannot open Feature relationships bcp file: %s' % relationshipFile
        sys.exit(1)

    try:
        fpPropertyFile = open(propertyFile, 'w')
    except:
        print 'Cannot open Feature relationships property bcp file: %s' % propertyFile
        sys.exit(1)

    try:
        fpNoteFile = open(noteFile, 'w')
    except:
        print 'Cannot open Feature relationships Note bcp file: %s' % noteFile
        sys.exit(1)

    try:
        fpNoteChunkFile = open(noteChunkFile, 'w')
    except:
        print 'Cannot open Feature relationships Note Chunk bcp file: %s' % \
	    noteChunkFile
        sys.exit(1)

    return

# end openFiles() -------------------------------


def closeFiles ():
    # Purpose: Close all file descriptors
    # Returns: Nothing
    # Assumes: all file descriptors were initialized
    # Effects: Nothing
    # Throws: Nothing

    global fpInFile, fpRelationshipFile, fpPropertyFile
    global fpNoteFile, fpNoteChunkFile

    fpInFile.close()
    fpRelationshipFile.close()
    fpPropertyFile.close()
    fpNoteFile.close()
    fpNoteChunkFile.close()

    return

# end closeFiles() -------------------------------

def createFiles( ): 
    # Purpose: parses feature relationship file, does verification
    #  creates bcp files
    # Returns: Nothing
    # Assumes: file descriptors have been initialized
    # Effects: sets global variables, writes to the file system
    # Throws: Nothing

    global nextRelationshipKey, nextNoteKey, nextPropertyKey

    # remove the header line
    header = fpInFile.readline()

    #
    # find properties columns in the header and map their column number to 
    # their property name.
    # example property header: 'Property:score' or 'Property:data_source

    # {colNum:propNameKey, ...}
    inputPropDict = {}

    # get all property tokens
    propTokens = map(string.strip, string.split(header, TAB))[13:]

    # iterate thru property column headers and load dictionary with the position
    # key and property name value
    colCt = 0
    for p in propTokens:
	colCt += 1
	if string.find(p, ':'):
	    tokens = map(string.strip, string.split(p, ':'))
            if tokens[0].lower() == 'property' and len(tokens) == 2:
		propName = tokens[1].lower()
		# assume QC script has verified the propName
		inputPropDict[colCt] = propertyDict[propName]

    #
    # Iterate throught the input file
    #
    line = fpInFile.readline()
    while line:

 	# get the first 12 lines - these are fixed columns; map to lower case
	# moved note (13) to 'remaining columns' as we don't want in lower case
	(action, cat, obj1Id, obj2sym, relId, relName, obj2Id, obj2sym, qual, evid, jNum, creator) = map(string.lower, map(string.strip, string.split(line, TAB))[:12])

	# get notes column (13) and any property columns (14+)
	remainingColumns = map(string.strip, string.split(line, TAB))[12:]

        # get notes column
	note = remainingColumns[0]

	# get properties columns
	remainingColumns = remainingColumns[1:]

	# skip deletes as they have been processed by the QC script 
	# - if any were found, and passed QC, they were written to an 
	# sql file for execution by the wrapper fearload.sh
	if action == 'delete':
	    line = fpInFile.readline()
	    continue

	# get the category key
	if categoryDict.has_key(cat):
	    c = categoryDict[cat]
	    catKey = c.key
	else:
	    print 'category (%s) not found in line ' % (cat, line)
	    continue

	# get the organizer key, determining if allele or marker
	if c.mgiTypeKey1 == 2:
	    if markerDict.has_key(obj1Id):
		objKey1 = markerDict[obj1Id]
	    else:
		print 'Organizer marker ID (%s) not found in line %s' % (obj1Id, line)
		continue
	elif c.mgiTypeKey1 == 11:
	    if alleleDict.has_key(obj1Id):
                objKey1 = alleleDict[obj1Id]
            else:
                print 'Organizer Allele ID (%s) on line %s not found' % (obj1Id, line)
                continue
	else:
	    print 'Organizer mgiType not supported in line %s' % (obj1Id, line)

   	# get the participant key
	if c.mgiTypeKey2 == 2:
	    if markerDict.has_key(obj2Id):
		objKey2 = markerDict[obj2Id]
	    else:
		print 'Participant marker ID (%s) not found in line %s' % (obj2Id, line)
		continue
	# currently no allele participant, but coded for it anyway
	elif c.mgiTypeKey2 == 11:
            if alleleDict.has_key(obj2Id):
                objKey1 = alleleDict[obj2Id]
            else:
                print 'Participant allele ID (%s) not found in line %s' % (obj1Id, line)
                continue
	else:
	    print 'Participant mgiType not supported in line %s' % (obj2Id, line)

	# get the relationship term key
	if relationshipDict.has_key(relId):
	    relKey = relationshipDict[relId]
	else:
	    print 'relationship id (%s) not found in line %s' % (relId, line)
	    continue

	# get the qualifier term key; empty qualifier gets default value
	if qual == '':
	    qual = defaultQual
	if qualifierDict.has_key(qual):
	    qualKey = qualifierDict[qual]
	else:
	    print 'qualifier (%s) not found in line %s' % (qual, line)
	    continue

	# get the evidence term key
	if evidenceDict.has_key(evid):
	    evidKey = evidenceDict[evid]
	else:
	    print 'evidence (%s) not found in line %s' % (evid, line)
	    continue

	# get the reference key
	if jNumDict.has_key(jNum):
	    refsKey = jNumDict[jNum]
	else:
	    print 'jNum (%s) not found in line %s' % (jNum, line)
	    continue

	# get the user key
	if userDict.has_key(creator):
	    userKey = userDict[creator]
	else:
	    print 'User (%s) not found in line %s' % (creator, line)
	    continue

	#
	# create bcp lines
	#

	# MGI_Relationship
	fpRelationshipFile.write('%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s' % \
	    (nextRelationshipKey, TAB, catKey, TAB, objKey1, TAB, objKey2, TAB, relKey, TAB, qualKey, TAB, evidKey, TAB, refsKey, TAB, userKey, TAB, userKey, TAB, DATE, TAB, DATE, CRT))

  	# MGI_Note
	if len(note) > 0:
	    fpNoteFile.write('%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s' % \
		(nextNoteKey, TAB, nextRelationshipKey, TAB, relationshipMgiTypeKey, TAB, relationshipNoteTypeKey, TAB, userKey, TAB, userKey, TAB, DATE, TAB, DATE, CRT))

	    # MGI_NoteChunk
	    seqNum = 1
	    if  len(note) > 0:
		fpNoteChunkFile.write('%s%s%s%s%s%s%s%s%s%s%s%s%s%s'% \
                    (nextNoteKey, TAB, seqNum, TAB, note, TAB, userKey, TAB, userKey, TAB, DATE, TAB, DATE, CRT))

	# MGI_Relationship_Property
	seqNum = 0
	for i in inputPropDict.keys():
	    seqNum += 1
	    propValue = remainingColumns[i-1]
	    propNameKey = inputPropDict[i]

	    #  no prop specified for this relationship, continue
	    if propValue == '':
		continue
	    # if property is 'score' convert the value to a float
	    elif propNameKey == 11588491: 	# score
		if string.find(propValue, '+')  == 0:
		    propValue = propValue[1:]
		propValue = float(propValue)	# convert score to float

	    fpPropertyFile.write('%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s' % (nextPropertyKey, TAB, nextRelationshipKey, TAB, propNameKey, TAB, propValue, TAB, seqNum, TAB, userKey, TAB, userKey, TAB, DATE, TAB, DATE, CRT ) )
	    nextPropertyKey += 1
	nextRelationshipKey += 1
	nextNoteKey += 1
	line = fpInFile.readline()
    
    return

# end createFiles() -------------------------------------

class Category:
    # Is: data object for category info (MGI_Relationship_Category)
    # Has: a set of category attributes
    # Does: provides direct access to its attributes
    #       
    def __init__ (self):
	# Purpose: constructor
	# Returns: nothing
	# Assumes: nothing
	# Effects: nothing
	# Throws: nothing
	self.key = None
	self.name = None
	self.relationshipVocabKey = None
	self.mgiTypeKey1 = None
	self.mgiTypeKey2 = None

# end class Category -----------------------------------------

#####################
#
# Main
#
#####################

# check the arguments to this script
checkArgs()

# this function will exit(1) if errors opening files
init()

# validate data and create load bcp files
createFiles()

# close all output files
closeFiles()

sys.exit(0)
