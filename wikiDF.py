from abc import ABCMeta, abstractmethod
from sparqlDatabase import czechDBpediaEndpoint,wikidataEndpoint

import pandas as pd
colsWikiData = ['predicateValue','predicateType','predicateLabel','predicateAlsoKnowns',
                'objectValue','objectLanguage','objectType',
                'directObjectValue','directObjectType',
                'valueLabel','valueLabelLanguage',
                'qualifierLabel', 'qualifierValue']

colsDBpedia = ['predicateValue','predicateType','objectValue','objectLanguage','objectType']

def getItem(itemInJson, itemName,checkLang = 1):
    errorCode = [-1,itemName]
    if itemName not in itemInJson.keys():
        return errorCode
    else:
        item = itemInJson[itemName]
        if 'value' not in item.keys():
            return errorCode
        else:
            itemValue = item['value']

        if 'xml:lang' in item.keys():
            itemLanguage = item['xml:lang']
            if itemLanguage != 'cs' and checkLang:
                return errorCode
        else:
            itemLanguage = 'None'

        if 'type' in item.keys():
            itemType = item['type']
        else:
            itemType = 'None'

    return [itemName, itemValue, itemLanguage, itemType]

class parentDataFrame():

    @abstractmethod
    def getDataFrame(self):
        pass



class wikidataDataFrame(parentDataFrame):
    def __init__(self,subject):
        rawJSON = wikidataEndpoint.selectEveryThingRawAsJSON(subject)
        self.dataFrame = pd.DataFrame(index=None)
        self.bindings = rawJSON['results']['bindings']

        for itemInJson in self.bindings:
            predicate = getItem(itemInJson,'predicate')
            if predicate[0] == -1 or "/prop/direct/" in predicate[1]:
                continue
            else:
                predicateValue, predicateType = [predicate[1], predicate[3]]

            object = getItem(itemInJson,'object')
            if object[0] == -1:
                continue
            else:
                objectValue, objectLanguage, objectType = object[1:]

            propertyLabel = getItem(itemInJson,'propertyLabel')
            if propertyLabel[0] == -1:
                predicateLabel = ''
            else:
                predicateLabel = propertyLabel[1]

            propertyAltLabel = getItem(itemInJson, 'propertyAltLabel')
            if propertyAltLabel[0] == -1:
                predicateAlsoKnowns = ''
            else:
                predicateAlsoKnowns = propertyAltLabel[1]

            valueJS = getItem(itemInJson,'value')
            if valueJS[0] == -1:
                directObjectValue,directObjectType = ['','']
            else:
                directObjectValue,directObjectType = [valueJS[1],valueJS[3]]

            valueLabelJS = getItem(itemInJson,'valueLabel',0)
            if valueLabelJS[0] == -1:
                valueLabel,valueLabelLanguage = ['','']
            else:
                valueLabel,valueLabelLanguage = [valueLabelJS[1],valueLabelJS[2]]

            qualifierLabelJS = getItem(itemInJson,'qualifierLabel')
            if qualifierLabelJS[0] == -1:
                qualifierLabel = ''
            else:
                qualifierLabel = qualifierLabelJS[1]

            qualifierValueJS = getItem(itemInJson,'qualifierValue')
            if qualifierValueJS[0] == -1:
                qualifierValue = ''
            else:
                qualifierValue = qualifierValueJS[1]


            rowDF = pd.Series([predicateValue, predicateType,predicateLabel,predicateAlsoKnowns,
                               objectValue, objectLanguage, objectType,
                               directObjectValue,directObjectType,
                               valueLabel,valueLabelLanguage,
                               qualifierLabel, qualifierValue])

            rowDF = pd.DataFrame([rowDF])
            self.dataFrame = pd.concat([self.dataFrame, rowDF])



    def getDataFrame(self):
        if not self.dataFrame.empty:
            self.dataFrame.columns = colsWikiData
            """self.dataFrame = self.dataFrame.sort_values(by=['valueLabelLanguage'])
            self.dataFrame = self.dataFrame.drop_duplicates(subset=['predicateValue', 'directObjectValue'],
                                                            keep='first')"""
            return self.dataFrame
        else:
            return self.dataFrame


class DBpediaDataFrame(parentDataFrame):
    def __init__(self,subject):
        rawJSON = czechDBpediaEndpoint.selectEveryThingRawAsJSON(subject)

        self.dataFrame = pd.DataFrame(index=None)
        self.bindings = rawJSON['results']['bindings']

        for itemInJson in self.bindings:
            predicate = getItem(itemInJson,'predicate')
            if predicate == -1:
                continue
            else:
                predicateValue, predicateType = [predicate[1], predicate[3]]

            object = getItem(itemInJson,'object')
            if object == -1:
                continue
            else:
                objectValue, objectLanguage, objectType = object[1:]

            rowDF = pd.Series([predicateValue, predicateType, objectValue, objectLanguage, objectType])
            rowDF = pd.DataFrame([rowDF])
            self.dataFrame = pd.concat([self.dataFrame, rowDF])

    def getDataFrame(self):
        if not self.dataFrame.empty:
            self.dataFrame.columns = colsDBpedia
            return self.dataFrame
        else:
            return self.dataFrame