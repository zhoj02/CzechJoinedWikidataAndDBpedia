from SPARQLWrapper import SPARQLWrapper, JSON
from abc import abstractmethod

class sparqlDatabase():
   #Inits database
   #Set agent in order not to be blocked by the endpoint
   def __init__(self,endpoint):
      self.database = SPARQLWrapper(endpoint,agent='Mozilla/5.0')

   #Obtains raw response in JSON
   def selectEveryThingRawAsJSON(self, subject):
      query = self.buildQuery(subject)
      self.database.setQuery(query)
      self.database.setReturnFormat(JSON)
      self.responseJSON = self.database.queryAndConvert()
      return self.responseJSON

   #Different query for a database type
   @abstractmethod
   def buildQuery(self):
      pass

class sparqlDatabaseWikidata(sparqlDatabase):

    def buildQuery(self,subject):
        filewithQuery = open("WikidataQuery.txt", "r")
        splitInput = str(filewithQuery.read()).split("[subject]")
        query = splitInput[0] + subject + splitInput[1]
        return query

class sparqlDatabaseDBpedia(sparqlDatabase):

   def buildQuery(self,subject):
      query = "SELECT ?predicate ?object ?objectLabel " \
              "WHERE {" + subject + " ?predicate ?object. }"
      return query

czechDBpediaEndpoint = sparqlDatabaseDBpedia('https://cs.dbpedia.org/sparql')
wikidataEndpoint = sparqlDatabaseWikidata('https://query.wikidata.org/sparql')

testResponse = str(wikidataEndpoint.selectEveryThingRawAsJSON('wd:Q22'))

f = open("demofile2.txt", "w",encoding='utf-8')
f.write(testResponse)
f.close()
print("DBpedia: ", czechDBpediaEndpoint.selectEveryThingRawAsJSON('<http://cs.dbpedia.org/resource/Skotsko>'))