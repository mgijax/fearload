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
#	    13. Properties in key=value pairs (Optional)
#	    14. Notes (Optional)
#
#	2. Configuration - see fearload.config
#	    1. 
#	    2. 
#
#  Outputs:
#
#       1. MGI_Relationship.bcp
#	2. MGI_Relationship_Property.bcp
#	3. MGI_Note (TBD)
#	4. MGI_NoteChunk (TBD)
#
#  Exit Codes:
#
#      0:  Successful completion
#      1:  An exception occurred
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
#      4) Generate the sanity reports.
#      5) Run the load if sanity checks pass
#      6) Close the input/output files.
#
#  Notes:  None
#
###########################################################################

import sys
import os
import string
import Set

import db
import mgi_utils
import vocloadlib

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
relationshipFile =   os.environ['RELATIONSHIP_BCP']
propertyFile = os.environ['PROPERTY_BCP']
noteFile = os.environ['NOTE_BCP']
noteChunkFile = os.environ['NOTECHUNK_BCP']
relVocabKey = os.environ['RELATIONSHIP_VOCAB_KEY']
qualVocabKey = os.environ['QUALIFIER_VOCAB_KEY']
evidVocabKey = os.environ['EVIDENCE_VOCAB_KEY']
relationshipNoteTypeKey =  os.environ['RELATIONSHIP_NOTE_KEY']

# file descriptors
fpInFile = ''
fpRelationshipFile = ''
fpPropertyFile = ''
fpNoteFile = ''
fpNoteChunkFile = ''

# database primary keys, the next one available
nextRelationshipKey = 1000	# MGI_Relationship._Relationship_key
nextPropertyKey = 1000		# MGI_Relationship_Property._Property_key
nextNoteKey = 1000		# MGI_Note._Note_key

# relationship mgiType 
relationshipMgiTypeKey = 40

# category lookup {name:Category object, ...}
categoryDict = {}

# relationship term lookup {term:key, ...}
relationshipDict = {}

# qualifier term lookup {term:key, ...}
qualifierDict = {}
# default when qualifier is blank in the input file
defaultQual = 'Not Specified'

# evidence term lookup {term:key, ...}
evidenceDict = {}

# reference ID (JNum) lookup {term:key, ...}
jNumDict = {}

# marker lookup {mgiID:key, ...)
markerDict = {}

# MGI_User lookup {userLogin:key, ...}
userDict = {}

# property lookup (propName:key, ...)
propertyDict = {}

def checkArgs ():
    # Purpose: Validate the arguments to the script.
    # Returns: Nothing
    # Assumes: Nothing
    # Effects: exits if args found on the command line
    # Throws: Nothing

    if len(sys.argv) != 1:
        print USAGE
        sys.exit(1)
    return

# end checkArgs() -------------------------------

def init():
    # Purpose: create lookups, open files
    # Returns: Nothing
    # Assumes: Nothing
    # Effects: Sets global variables, exits if a file cant be opened,
    #  creates files in the file system, creates connection to a database

    global nextRelationshipKey, nextPropertyKey, nextNoteKey
    global categoryDict, relationshipDict, propertyDict
    global qualifierDict, evidenceDict, jNumDict, userDict, markerDict
    
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
	name = r['name']
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
	relationshipDict[r['accid']] = r['_Object_key']

    # FeaR qualifier lookup
    results = db.sql('''select _Term_key, term
        from VOC_Term
        where _Vocab_key = 94
        and isObsolete = 0''', 'auto')
    for r in results:
        qualifierDict[r['term']] = r['_Term_key']

    # FeaR evidence lookup
    results = db.sql('''select _Term_key, abbreviation
        from VOC_Term
        where _Vocab_key = 95
        and isObsolete = 0''', 'auto')
    for r in results:
        evidenceDict[r['abbreviation']] = r['_Term_key']

    # Reference lookup
    results = db.sql('''select a.accid, a._Object_key
        from ACC_Accession a
        where a._MGIType_key = 1
        and a._LogicalDB_key = 1
        and a.preferred = 1
        and a.private = 0
	and a.prefixPart = 'J:' ''', 'auto')
    for r in results:
        jNumDict[r['accid']] = r['_Object_key']

    # marker lookup
    results = db.sql('''select a.accid, a._Object_key
        from ACC_Accession a
        where a._MGIType_key = 2
        and a._LogicalDB_key = 1
        and a.preferred = 1
        and a.private = 0''', 'auto')
    for r in results:
        markerDict[r['accid']] = r['_Object_key']

    # active status (not data load or inactive)
    results = db.sql('''select login, _User_key
	from MGI_User
	where _UserStatus_key = 316350''', 'auto')
    for r in results:
	userDict[r['login']] = r['_User_key']

    # property term lookup

    # FeaR property term lookup
    results = db.sql('''select term, _Term_key
        from VOC_Term
        where _Vocab_key = 97''', 'auto')
    for r in results:
        propertyDict[r['term'].lower()] = r['_Term_key']

    db.useOneConnection(0)
# end init()

def openFiles ():
    # Purpose: Open input/output files.
    # Returns: Nothing
    # Assumes: Nothing
    # Effects: Sets global variables, exits if a file cant be opened, 
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

    # find properties columns and map their column number to their property
    # name {colNum:propNameKey, ...}
    inputPropDict = {}

    # example property header: 'Property:score' or 'Property:data_source
    propTokens = map(string.strip, string.split(header, TAB))[13:]
    colCt = 0
    for p in propTokens:
	colCt += 1
	if string.find(p, ':'):
	    tokens = map(string.strip, string.split(p, ':'))
            if tokens[0].lower() == 'property' and len(tokens) == 2:
		propName = tokens[1].lower()
		# assume QC script has verified
		inputPropDict[colCt] = propertyDict[propName]
    #print inputPropDict

    line = fpInFile.readline()
    while line:
	# added quick cleanup to remove leading & trailing spaces from fields

	(action, cat, obj1Id, obj2sym, relId, relName, obj2Id, obj2sym, qual, evid, jNum, creator, note) = map(string.strip, string.split(line, TAB)[:13])
	remainingTokens = map(string.strip, string.split(line, TAB))[13:]

	#print '%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s' % (action, cat, obj1Id, obj2sym, relId, relName, obj2Id, obj2sym, qual, evid, jNum, creator, note)

	# added more info to line-by-line debugging

	if categoryDict.has_key(cat):
	    c = categoryDict[cat]
	    catKey = c.key
	else:
	    print 'category (%s) not found in line ' % (cat, line)
	    continue
	if markerDict.has_key(obj1Id):
	    objKey1 = markerDict[obj1Id]
	else:
	    print 'marker1 (%s) not found in line %s' % (obj1Id, line)
	    continue
	if markerDict.has_key(obj2Id):
	    objKey2 = markerDict[obj2Id]
	else:
	    print 'marker2 (%s) not found in line %s' % (obj2Id, line)
	    continue
	if relationshipDict.has_key(relId):
	    relKey = relationshipDict[relId]
	else:
	    print 'relationship id (%s) not found in line %s' % (relId, line)
	    continue
	if qual == '':
	    qual = defaultQual
	if qualifierDict.has_key(qual):
	    qualKey = qualifierDict[qual]
	else:
	    print 'qualifier (%s) not found in line %s' % (qual, line)
	    continue
	if evidenceDict.has_key(evid):
	    evidKey = evidenceDict[evid]
	else:
	    print 'evidence (%s) not found in line %s' % (evid, line)
	    continue
	if jNumDict.has_key(jNum):
	    refsKey = jNumDict[jNum]
	else:
	    print 'jNum (%s) not found in line %s' % (jNum, line)
	    continue
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
	if note != '':
	    fpNoteFile.write('%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s' % \
		(nextNoteKey, TAB, nextRelationshipKey, TAB, relationshipMgiTypeKey, TAB, relationshipNoteTypeKey, TAB, userKey, TAB, userKey, TAB, DATE, TAB, DATE, CRT))

	    # MGI_NoteChunk
	    seqNum = 0
	    for chunk in vocloadlib.splitBySize(note, 255):
		seqNum += 1
		fpNoteChunkFile.write('%s%s%s%s%s%s%s%s%s%s%s%s%s%s'% \
		    (nextNoteKey, TAB, seqNum, TAB, chunk, TAB, userKey, TAB, userKey, TAB, DATE, TAB, DATE, CRT))


	# MGI_Relationship_Property
	seqNum = 0
	for i in inputPropDict.keys():
	    seqNum += 1
	    propValue = remainingTokens[i-1]
	    propNameKey = inputPropDict[i]
	    #  no prop specified for this relationship
	    if propValue == '':
		continue
	    elif propNameKey == 11588491: 	# score
		propValue = float(propValue)	# convert score to float
	    fpPropertyFile.write('%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s' % (nextPropertyKey, TAB, nextRelationshipKey, TAB, propNameKey, TAB, propValue, TAB, seqNum, TAB, userKey, TAB, userKey, TAB, DATE, TAB, DATE, CRT ) )

	nextRelationshipKey += 1
	nextNoteKey += 1
	nextPropertyKey += 1
	line = fpInFile.readline()
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
