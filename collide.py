import pandas as pd
pd.set_option('display.max_columns', 500)


from czechWDtoDBlibrary import joinedfiles, origindata



#Základní konstrukce html souboru
htmlStart= """<html>
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

class collideData():
    def __init__(self,wikidataID,DBpediaID,outputFolder):

        try:
            self.outputFolder = outputFolder
            self.wikidataID = wikidataID
            self.DBpediaID = DBpediaID
            self.resultEntityName = DBpediaID + wikidataID
            self.sourceEntityName = DBpediaID + wikidataID

            self.wikidataDF = pd.read_csv(filepath_or_buffer=origindata + wikidataID, sep='\t', compression='bz2')
            self.DBpediaDF = pd.read_csv(filepath_or_buffer=origindata + DBpediaID, sep='\t', compression='bz2')

            #Choosing text description of entity
            try:
                self.entityHeader = (self.DBpediaDF[self.DBpediaDF['predicateValue'] == 'http://www.w3.org/2000/01/rdf-schema#label'].iloc[0])['objectValue']
            except:
                self.entityHeader = "Tato entita nemá název."
            try:
                self.descriptionOfEntity = (self.DBpediaDF[self.DBpediaDF['predicateValue'] == 'http://www.w3.org/2000/01/rdf-schema#comment'].iloc[0])['objectValue']
            except:
                self.descriptionOfEntity = "Tanto entita nemá popis."

            self.DBpediaDF = self.DBpediaDF[self.DBpediaDF['predicateValue'].str.startswith('http://cs.dbpedia.org/property/')]

            intersectfile = open(joinedfiles + self.sourceEntityName + 'joined', 'r', encoding='utf-8')
            contents = intersectfile.read()
            intersectfile.close()
            self.storeFrames = []
            self.DBpediajoined = []
            self.Wikidatajoined = []
            try:
                dictionary = eval(contents)
                for vazba in dictionary:
                    self.storeFrames.append(self.createDF(dictionary[vazba]))
                    self.DBpediajoined.append(dictionary[vazba]['origURIs']['dbpedia'])
                    self.Wikidatajoined.append(dictionary[vazba]['origURIs']['wikidata'])
            except NameError:
                pass




            self.createHTMLfile2()
            print(self.DBpediajoined)
            print(self.Wikidatajoined)
        except pd.errors.EmptyDataError:
            print('noFile')



    def createHTMLfile2(self):
        self.wikidataDF['vlastnost'] = self.wikidataDF.apply(self.getAhrefwikidata, axis=1)
        self.wikidataDF['zdroj'] = 'Wikidata'
        self.wikidataDF = self.wikidataDF.rename(columns={'valueLabel': 'hodnota'})
        try:
            self.wikidataDF = self.wikidataDF[['vlastnost', 'hodnota', 'qualifierLabel', 'qualifierValue', 'zdroj']]
        except:
            return -1
        self.wikidataDF = self.wikidataDF[~self.wikidataDF['vlastnost'].isin(self.Wikidatajoined)]

        try:
            self.DBpediaDF['vlastnost'] = self.DBpediaDF.apply(self.getAhrefDBpedia, axis=1)
            self.DBpediaDF['zdroj'] = 'DBpedia'
            self.DBpediaDF = self.DBpediaDF.rename(columns={'objectValue': 'hodnota'})

            self.DBpediaDF = self.DBpediaDF[['vlastnost', 'hodnota', 'zdroj']]
            self.DBpediaDF = self.DBpediaDF[~self.DBpediaDF['vlastnost'].isin(self.DBpediajoined)]

            tableDF = pd.concat([self.wikidataDF, self.DBpediaDF])
        except:
            tableDF = self.wikidataDF


        fileout = open(self.outputFolder + self.resultEntityName + '.html', "w", encoding='utf-8')
        originTables = tableDF.to_html(escape=False,na_rep="",index=False,classes='w3-responsive w3-table-all double-scroll')


        fileout.writelines(htmlStart.format(self.resultEntityName)+htmlStart2)
        fileout.write("<body>")
        fileout.write("<h1 class=\"w3-panel w3-blue\" style=\"width: 100%\">" + str(self.entityHeader) + "</h1>")
        fileout.write("<div class=\"w3-panel\">" + self.descriptionOfEntity +"</div>")


        fileout.writelines(originTables)

        for x in self.storeFrames:
            fileout.write("<br>")
            fileout.write("<h2>{0}</h2>".format(x[2]['dbpedia'].replace('http://cs.dbpedia.org/property/','')))
            fileout.write("<p>URI vlasnosti v české DBpedii: {0} </p>".format(x[2]['dbpedia']))
            fileout.write("<p>URI vlastnosti ve Wikidatech: {0}</p>".format(x[2]['wikidata']))
            fileout.write('<h3>Kolizní tabulka</h3>')
            fileout.writelines(x[0].to_html(escape=False,na_rep="",classes='koliznitabulka w3-responsive w3-table-all '))
            fileout.write("<p>Zvolená hodnota: {0}</p>".format(x[1]))
        fileout.write("</body></html>")
        fileout.close()

    def getAhrefwikidata(self,row):
        try:
            return "<a href=\"{0}\">{1}</a>".format(row['predicateValue'].strip(),row['predicateLabel'].strip())
        except:
            return "Neidentifikováno"

    def getAhrefDBpedia(self,row):
        return "<a href=\"{0}\">{1}</a>".format(row['predicateValue'].strip(),row['predicateValue'].replace('http://cs.dbpedia.org/property/','').replace('_',' '))

    def putToSameAsFile(self, dataFrame):
        pass

    def findLatest(self, dataFrame):
        print('findLatest')
        dataFrame = dataFrame.sort_values('datum')
        valueOfLastRow = dataFrame['value'].iloc[-1]
        return valueOfLastRow

    #textové hodnoty
    def vote(self, dataFrame):
        print('vote')
        result = dataFrame.value.mode().tolist()
        return result[0]

    #číselné hodnoty
    def average(self, dataFrame):
        print('average')
        try:
            dataFrame["value"] = pd.to_numeric(dataFrame["value"], downcast="float")
        except:
            return "Kolize nebyla vyřešena."
        sum = 0
        for i in dataFrame["value"]:
            sum = i + sum
        print(dataFrame['value'].tolist())
        result = sum/len(dataFrame["value"])
        return result

    def pictureChoose(self,dataFrame):
        print('pictureChoose')
        result = dataFrame[dataFrame['value'].str.startswith('http')]
        try:
            return result['value'].tolist()[0]
        except:
            return "Nelze určit obrázek, který reprezentuje danou vlastnost."

    def createDF(self, vazba):
        functionChosen = 0
        vazbaFrame = pd.DataFrame(columns=['value', 'source'])

        for item in vazba['dbpedia']:
            if item.endswith('.svg') and not functionChosen:
                functionChosen = 1
                chosenFuction = self.pictureChoose
            dbpedia = {'value': (item.replace('http://cs.dbpedia.org/resource/','')).replace('_',' ').lower(), 'source': 'dbpedia'}
            vazbaFrame = vazbaFrame.append(dbpedia, ignore_index=True)

        for item in vazba['wikidata']:
            if not len(vazba['wikidata'][item]):
                wikidataitem = {'value': item.lower(), 'source': 'wikidata'}
                vazbaFrame = vazbaFrame.append(wikidataitem, ignore_index=True)
                try:
                    float(item)
                    if not functionChosen:
                        functionChosen = 1
                        chosenFuction = self.average
                except:
                    pass
                if not functionChosen:
                    functionChosen = 1
                    chosenFuction = self.vote
            else:
                wikidataitem = {'value': item.lower(), 'source': 'wikidata'}
                for qualifier in vazba['wikidata'][item]:
                    qualifierName = qualifier[0]
                    qualifierValue = qualifier[1]
                    if qualifierName not in vazbaFrame.columns:
                        vazbaFrame[qualifierName] = ''
                        print('i',isinstance(qualifierValue, float))
                        if 'datum' == qualifierName:
                            chosenFuction = self.findLatest
                    wikidataitem[qualifierName] = qualifierValue

                vazbaFrame = vazbaFrame.append(wikidataitem, ignore_index=True)

        print(vazba)
        print(vazbaFrame)
        if not functionChosen:
            chosenFuction = self.vote
        try:
            chosenValue = chosenFuction(vazbaFrame)
        except UnboundLocalError:
            chosenValue = "Kolize nešla vyřešit."
        print(chosenValue)
        print('------------------------------------')
        return vazbaFrame, chosenValue, vazba['origURIs']


# ).replace('\\xa0', ' ')
