import pandas as pd
import math

rules = {
            'počet obyvatel': ['highestInt', 'datum'],
            'obrazek': ['ignore', 'None']
        }

def getObjectValueWikiData(row):
    if row['objectType'] in ('literal','typed-literal'):
        value = row['objectValue']
    elif row['directObjectType'] == 'literal':
        value = row['directObjectValue']
    elif row['valueLabel'] != '':
        value = row['valueLabel']
    else:
        value = 'No literal value'

    if 'qualifierValue' not in row.keys():
        return [value, -1]
    elif isinstance(row['qualifierValue'],float) and math.isnan(row['qualifierValue']):
        return [value, -1]
    else:
        return [value, (row['qualifierLabel'], row['qualifierValue'])]


def getObjectValueDBPedia(row):
    """if row['objectType'] in ('literal', 'typed-literal'):
        value = row['objectValue']
    elif row['objectType'] == 'uri':
        value = row['objectValue'].rsplit('/',1)[-1]"""
    return (row['objectValue'])

class joinedEntity():
    def __init__(self, sourcefolder, outputfolder, wikidataDatabusID, datatabusDBpediaID):
        self.wikidataID = wikidataDatabusID
        self.DBpediaID = datatabusDBpediaID
        self.sourcefolder = sourcefolder
        self.outputfolder = outputfolder
        try:
            self.wikidataDF = pd.read_csv(filepath_or_buffer=self.sourcefolder + self.wikidataID,sep='\t',
                                          compression='bz2')
            self.DBpediaDF = pd.read_csv(filepath_or_buffer=self.sourcefolder + self.DBpediaID, sep='\t',
                                         compression='bz2')
            self.__intersectDataFrames()
            self.__createJoinedData()
        except pd.errors.EmptyDataError:
            print('noFile')



    def __intersectDataFrames(self):
        self.joinedResult = dict()
        doNotCheck = {
            'DBpedia':['http://dbpedia.org/ontology/wikiPageWikiLink','http://dbpedia.org/ontology/wikiPageWikiLinkText'
                ,'http://cs.dbpedia.org/property/wikiPageUsesTemplate','http://dbpedia.org/ontology/wikiPageLength',
                       'http://dbpedia.org/ontology/wikiPageOutDegree'],
            'wikidata':[]
        }
        toCompareWD = self.wikidataDF[~self.wikidataDF['predicateValue'].isin(doNotCheck['wikidata'])]
        toCompareDBpedia = self.DBpediaDF[~self.DBpediaDF['predicateValue'].isin(doNotCheck['DBpedia'])]
        toCompareDBpedia = toCompareDBpedia[toCompareDBpedia['predicateValue'].str.startswith('http://cs.dbpedia.org/property/')]

        for index, wikidataRow in toCompareWD.iterrows():
            try:
                wikidataAllOfLabelsForPredicate = wikidataRow['predicateAlsoKnowns'].split(',')
            except (KeyError, AttributeError) as e:
                wikidataAllOfLabelsForPredicate = []
            try:
                wikidataAllOfLabelsForPredicate.append(wikidataRow['predicateLabel'])
            except (KeyError, AttributeError) as e:
                continue

            wdPredAllNormalized = []
            for n in wikidataAllOfLabelsForPredicate:
                wdPredAllNormalized.append(n.replace(' ', '').lower())
            wikidataAllOfLabelsForPredicate = wdPredAllNormalized

            for nindex,dbpediaRow in toCompareDBpedia.iterrows():
                dbpediaLabel = dbpediaRow['predicateValue'].replace('http://cs.dbpedia.org/property/','')
                if dbpediaLabel.lower() in wikidataAllOfLabelsForPredicate:
                    if dbpediaLabel not in self.joinedResult.keys():
                        self.joinedResult[dbpediaLabel] = dict()
                        self.joinedResult[dbpediaLabel]["dbpedia"] = dict()
                        self.joinedResult[dbpediaLabel]["wikidata"] = dict()
                        self.joinedResult[dbpediaLabel]["origURIs"] = dict()
                        self.joinedResult[dbpediaLabel]["origURIs"]["dbpedia"] = dbpediaRow['predicateValue']
                        self.joinedResult[dbpediaLabel]["origURIs"]["wikidata"] = wikidataRow['predicateValue']

                    DBpediaValue = getObjectValueDBPedia(dbpediaRow)
                    WikidataValue = getObjectValueWikiData(wikidataRow)

                    #TODO
                    if DBpediaValue not in self.joinedResult[dbpediaLabel]["dbpedia"]:
                        self.joinedResult[dbpediaLabel]["dbpedia"][DBpediaValue] = set()

                    if WikidataValue[0] not in self.joinedResult[dbpediaLabel]["wikidata"]:
                        self.joinedResult[dbpediaLabel]["wikidata"][WikidataValue[0]] = set()
                    if not isinstance(WikidataValue[1],int):
                        self.joinedResult[dbpediaLabel]["wikidata"][WikidataValue[0]].add(WikidataValue[1])


    def __createJoinedData(self):

        f = open(self.outputfolder + self.DBpediaID+self.wikidataID+'joined', "w",encoding='utf-8')
        f.write(str(self.joinedResult))
        f.close()