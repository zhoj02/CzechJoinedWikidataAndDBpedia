import json
import codecs
json1 = '''{
    "name": "test_2304",
    "priority": 0,
    "tasks": ['''

everyJSON = '''{
    "data_for_solution": {
        "templateName": "Zelenydotazy",
        "variables": {'''

jsonend = "]}"
with codecs.open('2019-09-02.json',"r",'utf-16') as json_file,open('result.json','w') as rfile:
    lines = json_file.read().split(', {')
    print(lines)

    rfile.write(json1)
    for y in lines:
        y = y.split(',')[0].replace("\"query\": ","")
        y = y.strip('"')
        try:
            rfile.write(everyJSON + "\"query\": \"" + y + "\"}}}, \n")
        except:
            pass
    rfile.write(jsonend)
