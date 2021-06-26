import pandas as pd

##############################
#
# filepath
# separatorOfFile
# columnNames
# testedColumn
# testedString

def getFrameByCondition(filepath, separatorOfFile, columnNames, testedColumn, testedString,outputFilename):
    chunksOfFile = pd.read_csv(filepath_or_buffer=filepath,
                               names=columnNames, sep=separatorOfFile, chunksize=1000)
    czechDBpedia = pd.DataFrame()
    czechDBpediaCount = 0
    for i,chunk in enumerate(chunksOfFile):
        czechDBpedia = pd.concat([czechDBpedia,chunk[chunk[testedColumn].str.contains(testedString)]])

        if czechDBpedia.shape[0] >= 10000:
            print(outputFilename+ str(czechDBpediaCount))
            czechDBpedia.to_csv(path_or_buf='sourceData/' + outputFilename+ str(czechDBpediaCount) + '.bz2',sep='\t',
                                encoding='utf-8',index=None,compression='bz2')
            czechDBpedia = pd.DataFrame()
            czechDBpediaCount += 1
    czechDBpedia.to_csv(path_or_buf='sourceData/' + outputFilename + str(czechDBpediaCount)+ '.bz2', sep='\t',
                        encoding='utf-8', index=None,compression='bz2')

getFrameByCondition(filepath='sourceData/sameas-all-wikis.ttl.bz2', separatorOfFile=' ',
                    columnNames=['subject', 'predicate', 'object', 'None'], testedColumn = 'object',
                    testedString='http://cs.dbpedia.org/', outputFilename='czechDBpedia')
