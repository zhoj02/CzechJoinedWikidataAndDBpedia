import pandas as pd
import click

from wikiDF import wikidataDataFrame, DBpediaDataFrame

def createLastLineFile(nameOfRun, lineToWrite,fileCount,lineCountInFile):
    lastLine = open("lastedRow" + nameOfRun + ".txt", "w")
    lastLine.write(lineToWrite)
    lastLine.write(str(fileCount) + '\n' + str(lineCountInFile))
    lastLine.close()

def getFrames(folder, subject,object):

    wikidataID = (subject.rsplit('/', 1)[-1]).strip('>')
    DBpediaID = object

    wikidataDF = wikidataDataFrame('wd:' + wikidataID).getDataFrame()
    DBpediaDF = DBpediaDataFrame(DBpediaID).getDataFrame()
    wikidataDF.to_csv(path_or_buf=folder + wikidataID, index=None, sep='\t',compression='bz2')
    DBpediaDF.to_csv(path_or_buf=folder + (DBpediaID.rsplit('/', 1)[-1]).strip('>'), index=None, sep='\t',compression='bz2')


@click.command()
@click.option('-again','--startfrombeginning', default=0)
@click.option('-name','--nameofrun')
def main(startfrombeginning: int, nameofrun: str):
    folder = './.data/'

    filesPath = 'sourceData/czechDBpedia'
    filetype = '.bz2'

    fileCount = 0
    completeLog = open("log" + nameofrun + ".txt", "a")

    if not startfrombeginning:
        for i, row in enumerate(open("lastedRow" + nameofrun + ".txt", "r")):
            if i == 1:
                fileCount = int(row)
            if i == 2:
                getToRow = row

    while True:
        print("filecount: "+str(fileCount))
        completeLog.write("Started reading:" + str(fileCount) + ". file" + '\n')
        try:
            fileName = filesPath+str(fileCount).strip('\n')+filetype
            file = pd.read_csv(filepath_or_buffer=fileName,sep='\t',encoding='utf-8')
        except FileNotFoundError:
            print('No other file')
            break
        lineCountInFile = 0
        for index, row in file.iterrows():
            if not startfrombeginning:
                if lineCountInFile < int(getToRow):
                    lineCountInFile += 1
                    continue
            subject = row['subject']
            object = row['object']
            currentLine = subject + '\t' + object
            print(str(lineCountInFile) + '\t' + str(currentLine))
            createLastLineFile(nameofrun, currentLine + '\n', fileCount, lineCountInFile)

            getFrames(folder, subject, object)

            lineCountInFile += 1

        completeLog.write("Finished reading:" + str(fileCount) + ". file" + '\n')
        completeLog.close()
        fileCount += 1
        getToRow = 0

if __name__ == '__main__':
    main()

