import pandas as pd
import click

from joinedEntity import joinedEntity

def createLastLineFile(nameOfRun, lineToWrite,fileCount,lineCountInFile):
    lastLine = open("joinedlastedRow" + nameOfRun + ".txt", "w")
    lastLine.write(lineToWrite)
    lastLine.write(str(fileCount) + '\n' + str(lineCountInFile))
    lastLine.close()

def joinFrames(sourcefolder,outputfolder, subject, object):

    wikidataID = (subject.rsplit('/', 1)[-1]).strip('>')
    DBpediaID = (object.rsplit('/', 1)[-1]).strip('>')

    joinedEntity(sourcefolder,outputfolder, wikidataID, DBpediaID)

@click.command()
@click.option('-again','--startfrombeginning', default=0)
@click.option('-name','--nameofrun')
def main(startfrombeginning: int, nameofrun: str):
    outputfolder = './joinedfiles/'
    sourcefolder = './.data/'
    filesPath = 'sourceData/czechDBpedia'
    filetype = '.bz2'
    fileCount = 0
    completeLog = open("joinedlog" + nameofrun + ".txt", "a")

    if not startfrombeginning:
        for i, row in enumerate(open("joinedlastedRow" + nameofrun + ".txt", "r")):
            if i == 1:
                fileCount = int(row)
            if i == 2:
                getToRow = row

    while True:
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

            joinFrames(sourcefolder,outputfolder, subject, object)

            lineCountInFile += 1

        completeLog.write("Finished reading:" + str(fileCount) + ". file" + '\n')
        fileCount += 1
        getToRow = 0

if __name__ == '__main__':
    main()