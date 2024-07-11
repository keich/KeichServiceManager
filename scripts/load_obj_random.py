import requests
import datetime
import re
import datetime
import json
import random

source = "pyrandom"
sourcekey = datetime.datetime.now().strftime("%d.%m.%Y, %H:%M:%S")
api_url = "http://localhost:8080"
session = requests.Session()
session.verify = False

objs = []
rootChildren = []
itemCounter = 2;

def getItemId():
    global itemCounter
    tmp = str(itemCounter)
    itemCounter = itemCounter + 1
    return tmp

for parentCount in range(1000):
    children = []
    parentId = getItemId()
    rootChildren.append(parentId)
    for childCount in range(10):
        childId = getItemId()
        children.append(childId)
        flt = { 'byId': {'equalFields': {'itemId': childId}}}
        name = 'Child' + childId
        obj = {'id': childId, 'source': source, 'sourceKey': sourcekey, 'name': name, 'fields': {'name': name}, 'filters': flt}
        objs.append(obj)
    flt = { 'byId': {'equalFields': {'itemId': parentId}}}
    name = 'Parent' + parentId
    obj = {'id': parentId, 'source': source, 'sourceKey': sourcekey, 'name': name, 'fields': {'name': name}, 'filters': flt, 'children': children}
    objs.append(obj)


obj = {'id':'1','source':source,'sourceKey': sourcekey, 'name': 'ROOT','fields':{'name':'ROOT'},'children': rootChildren}
objs.append(obj)

events = []
eventCounter = 0

def getEventId():
    global eventCounter
    tmp = str(eventCounter)
    eventCounter = eventCounter + 1
    return tmp

def getEventStatusRandom():
    r = random.randint(1, 5)
    if r == 1:
        return 'INDETERMINATE'
    if r == 2:
        return 'INFORMATION'
    if r == 3:
        return 'WARNING'
    if r == 4:
        return 'MAJOR'
    if r == 5:
        return 'CRITICAL'
    return 'CLEAR'

for itemId in range(itemCounter):
    eventId = getEventId()
    status = getEventStatusRandom()
    event = {'id': eventId, 'source': source, 'sourceKey': sourcekey, 'fields': {'itemId': str(itemId),'node': 'Item ' + str(itemId), 'summary': 'Test message for ' + str(itemId) }, 'status': status,'type': 'PROBLEM' }
    events.append(event)


start = datetime.datetime.now()
print(str(start) + " Items Add Start ")
response = session.post(api_url+'/api/v1/item', json=objs)
end = datetime.datetime.now()
print(str(end) + " Items Add End " + str(response.status_code) + " " + str(response.content))
print(str(end) + " Spend time " + str(end - start))

start = datetime.datetime.now()
print(str(start) +" Items Del Old Start")
response = session.delete(api_url+'/api/v1/item?query=bySourceAndSourceKeyNot&source='+ source + '&sourceKey=' + sourcekey)
end = datetime.datetime.now()
print(str(end) +" Items Del Old End: " + str(response.status_code) + " " + str(response.content))
print(str(end) + " Spend time " + str(end - start))

start = datetime.datetime.now()
print(str(start) + " Events Add Start ")
response = session.post(api_url+'/api/v1/event', json=events)
end = datetime.datetime.now()
print(str(end) + " Events Add End " + str(response.status_code) + " " + str(response.content))
print(str(end) + " Spend time " + str(end - start))

start = datetime.datetime.now()
print(str(start) +" Events Del Old Start")
response = session.delete(api_url+'/api/v1/event?query=bySourceAndSourceKeyNot&source='+ source + '&sourceKey=' + sourcekey)
end = datetime.datetime.now()
print(str(end) +" Events Del Old End: " + str(response.status_code) + " " + str(response.content))
print(str(end) + " Spend time " + str(end - start))
