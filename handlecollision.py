import pandas as pd
import click
import os


from czechWDtoDBlibrary import createLastLineFile,parametrstartfrombeginning,\
    parametrnameofrun, filesPath,filetype,joinedfiles,origindata
from collide import collideData

htmlStart = """<html>
<head>
<title>{0}</title>

<link rel="stylesheet" href="https://www.w3schools.com/w3css/4/w3.css">"""
htmlStart2 = """
<style>
th,td {
    max-width: 400px;
    table-layout: fixed;
    overflow: hidden;
    }

table {

  overflow: hidden;

} 
body {
  max-width: 1300px;
  margin: 0 auto;
  background: white;
  padding: 10px;
}   

</style>
</head>
<body>"""

htmlEnd = """ 
</body>
</html>
"""

def createJoinedData(subject, object, outputfolder):

    wikidataID = (subject.rsplit('/', 1)[-1]).strip('>')
    DBpediaID = (object.rsplit('/', 1)[-1]).strip('>')
    collideData(wikidataID,DBpediaID, outputfolder)

@click.command()
@click.option(parametrstartfrombeginning[0],parametrstartfrombeginning[1], default=0)
@click.option(parametrnameofrun[0],parametrnameofrun[1])
def main(startfrombeginning: int, nameofrun: str):
    maxResults = 100
    countResults = 0
    fileCount = 0
    completeLog = open("colliedLog" + nameofrun + ".txt", "a")

    if not startfrombeginning:
        for i, row in enumerate(open("collideLastedRow" + nameofrun + ".txt", "r")):
            if i == 1:
                fileCount = int(row)
            if i == 2:
                getToRow = row

    while True:
        if maxFiles > 100:

        completeLog.write("Started reading:" + str(fileCount) + ". file" + '\n')
        try:
            if not os.path.exists("../mainApp/htmlfiles/"+str(fileCount)): os.makedirs("../mainApp/htmlfiles/"+str(fileCount))
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
            createLastLineFile("collide",nameofrun, currentLine + '\n', fileCount, lineCountInFile)

            createJoinedData(subject,object,str("../mainApp/htmlfiles/"+str(fileCount)+"/"))
            lineCountInFile += 1

        completeLog.write("Finished reading:" + str(fileCount) + ". file" + '\n')
        fileCount += 1
        getToRow = 0

if __name__ == '__main__':
    main()