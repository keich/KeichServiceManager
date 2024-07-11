import requests
import datetime

zabbix_url    = 'https://localhost/zabbix/api_jsonrpc.php'
zabbix_token  = 'secret'
zabbix_groups = ['Linux']
zabbix_id     = 'zabbix'
zabbix_name   = 'Zabbix'

api_url       = "http://localhost:8080"

source        = "pyzabbix"
sourcekey     = datetime.datetime.now().strftime("%d.%m.%Y, %H:%M:%S")

session = requests.Session()
session.verify = False

zabbixSession = requests.Session()
zabbixSession.verify = False
zabbixSession.headers.update({"Authorization": "Bearer " + zabbix_token})


severityToStr = {'0': 'INDETERMINATE', '1': 'INFORMATION', '2': 'INFORMATION', '3': 'WARNING','4': 'MAJOR', '5': 'CRITICAL'}

objs = []

def getZabbixGroups(zabbix_url, zabbixSession, zabbix_groups):
    req = {
        "jsonrpc": "2.0",
        "method": "hostgroup.get",
        "params": {
            "filter": {
                "name": zabbix_groups
            }
        },
        "id": 1
    }
    response = zabbixSession.post(zabbix_url, json=req)
    groupIds = []
    for group in response.json()['result']:
        groupIds.append(group['groupid'])
    return groupIds


def getZabbixHostsByGroup(zabbix_url, zabbixSession, zabbix_groupIds):
    req = {
        "jsonrpc": "2.0",
        "method": "host.get",
        "params": {
                "groupids": zabbix_groupIds,
                "output": ['hostid', 'name', 'description'],
                "selectGroups": 'extend'
            },
        "id": 1
    }
    response = zabbixSession.post(zabbix_url, json=req)
    return response.json()['result']

def getZabbixProblems(zabbix_url, zabbixSession, zabbix_groupIds):
    req = {
        "jsonrpc": "2.0",
        "method":'problem.get',
        "params": {
            "output": ["eventId"],
            "groupids": zabbix_groupIds
        },
        "id": 1
    }
    response = zabbixSession.post(zabbix_url, json=req)

    evetnIds = []
    for problem in response.json()['result']:
        evetnIds.append(problem['eventid'])

    req = {
        "jsonrpc": "2.0",
        "method": "event.get",
        "params": {
            "output": "extend",
            "eventids": evetnIds,
            "selectHosts": ["name"]
        },
        "id": 1
    }
    response = zabbixSession.post(zabbix_url, json=req)
    return response.json()['result']

groupIds = getZabbixGroups(zabbix_url, zabbixSession, zabbix_groups)
hosts = getZabbixHostsByGroup(zabbix_url, zabbixSession, groupIds)
problems = getZabbixProblems(zabbix_url, zabbixSession, groupIds)

groups = {}

for host in hosts:
    hostGroups = host['groups']
    hostId = host['hostid']
    for group in hostGroups:
        groupId = group['groupid']
        if groupId in groups:
            groups[groupId]['children'].append(hostId)
        else:
            groups[groupId] = group
            groups[groupId]['children'] = [hostId]
    del host['groups']
    filters = {'byHostId': {'equalFields': {'hostid': hostId}}}
    objs.append({'id': hostId, 'source':source, 'sourceKey': sourcekey, 'name': host['name'], 'fields': host, 'filters': filters })

groupIds = []

for groupId in groups:
    group = groups[groupId]
    groupIds.append(groupId)
    children = group['children']
    del group['children']
    objs.append({'id': groupId, 'source': source, 'sourceKey': sourcekey, 'name': group['name'], 'fields': group, 'children': children})

objs.append({'id': zabbix_id, 'source': source, 'sourceKey': sourcekey, 'name': zabbix_name, 'fields': {'name': zabbix_name,'zabbix_url': zabbix_url}, 'children': groupIds})
objs.append({'id': '1', 'source': source, 'sourceKey': sourcekey, 'name': 'ROOT', 'fields': {'name': 'ROOT'}, 'children': [zabbix_id]})

events = []

for problem in problems:
    eventId = problem['eventid']
    status = severityToStr[problem['severity']]
    hostId = problem['hosts'][0]['hostid']
    hostName = problem['hosts'][0]['name']
    fields = problem
    del fields['urls']
    del fields['hosts']
    fields['summary'] = problem['name']
    fields['hostid'] = hostId
    fields['node'] = hostName
    events.append({'id': eventId, 'source': source, 'sourceKey': sourcekey, 'fields': fields, 'status': status, 'type': 'PROBLEM' })

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
