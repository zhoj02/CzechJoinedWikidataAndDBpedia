parametrstartfrombeginning = ['-new','--startfrombeginning']
parametrnameofrun = ['-name','--nameofrun']
filesPath = 'sourceData/czechDBpedia'
filetype = '.bz2'
joinedfiles = './joinedfiles/'
origindata = './.data/'
htmlfiles = './.htmlfiles/'

def createLastLineFile(typeOfRun,nameOfRun, lineToWrite,fileCount,lineCountInFile):
    lastLine = open(typeOfRun + "lastedRow" + nameOfRun + ".txt", "w")
    lastLine.write(lineToWrite)
    lastLine.write(str(fileCount) + '\n' + str(lineCountInFile))
    lastLine.close()