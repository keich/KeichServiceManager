# About The Project
This system is designed to offer simple event capture.

Key facts:
*  Event object type - store events
*  Item object type - store configuration item(CI)
*  Relation between items
*  Calculate parent CI status based on child CI
*  Different algorithms for calculating CI status
*  API for Prometheus/VictoriaMetrics Alertmanager
*  Items and Events is conflict-free replicated data types
*  Can work as multi node system
*  History CI statuses to Prometheus/VictoriaMetrics
*  History events to OpenSearch(bad idea, TODO sql database)

## Software architecture may be like this
[![Schema][product-schema]]

# Getting Started

1. Build IndexedHashMap project
https://github.com/keich/IndexedHashMap \
Distr jar files will be stored to local mavent repository

2. Build GUI
Build GUI from repository https://github.com/keich/KeichServiceManagerGUI \
Copy files from directory distr to /src/main/resources/static directory of KeichServiceManager project
```sh
cp -R ./distr ../KeichServiceManager/src/main/resources/static
```
4. Clone repository

```sh
   git clone https://github.com/keich/KeichServiceManager.git
```
4. Run for development

```sh
mvn spring-boot:run
```
5. Run for build jar files

```sh
mvn install
```

6. Run jar files

```sh
java -jar KeichServicesManager-0.0.1-exec.jar
```
7. Add to systemd

Copy file for systemd
```sh
cp ./main/systemd/ksm.service to /etc/systemd/system/
```

```sh
systemctl daemon-reload
systemctl status ksm.service
systemctl start ksm.service
```

# Object Types

Object has general fields:
| Name       | Description                                        |
| ---------- | -------------------------------------------------- |
| id         | Object unique id. Sets by source systems. |
| version    | Grow when object is updated. |
| source     | Name of source. Sets by source systems. |
| sourceKey  | Additional string. Sets by source systems. |
| sourceType | Predefined source type. OTHER, ZABBIX, VICTORIAMETRICS, ORACLE, SAP. Sets by source systems.|
| status     | Predefined statuses CLEAR(0), INDETERMINATE(1), INFORMATION(2), WARNING(3), MAJOR(4), CRITICAL(5). Sets by source systems. |
| createdOn  | Creation time |
| updatedOn  | Updated time |
| deletedOn  | Deletion time |
| fromHistory| For replication |
| fields     | Map<String, String> for store any data. Uses for some logic |

## Event
Event object has additional fields
| Name       | Description                                        |
| ---------- | -------------------------------------------------- |
| type       | Predefined NOTSET, PROBLEM, RESOLUTION, INFORMATION. Sets by source systems. |
| node       | Display Name CI |
| summary    | Message of event |
| endsOn     | Time for auto close |

### Example Event JSON
```json
{
  "id": "902725947_1760526300",
  "version": 86892801,
  "source": "Alerts",
  "sourceKey": "AlertsInternal",
  "sourceType": "VICTORIAMETRICS",
  "node": "server.local",
  "summary": "server.local CPU usage at 100% for 30m",
  "type": "PROBLEM",
  "status": "WARNING",
  "fields": {
    "alertgroup": "LinuxServer",
    "alert.generator_url": "http://victoriametrics:8880/vmalert/alert?group_id=23423423&alert_id=902725947",
    "alertname": "CPUUsage",
    "alert.starts_at": "2025-10-15T14:05:00+03:00",
    "alert.ends_at": "2025-10-24T16:12:27.347159646+03:00",
    "annotations.description": "server.local CPU usage at 100% for 30m",
    "annotations.node": "server.local",
    "annotations.alert_id": "902725947",
    "annotations.status": "3"
  },
  "fromHistory": [
    "node1"
  ],
  "createdOn": "2025-10-15T11:07:27.379059445Z",
  "updatedOn": "2025-10-24T12:52:27.369446603Z",
  "deletedOn": null,
  "endsOn": "2025-10-24T13:12:27.347159646Z"
}
```

## Item
Item object has additional fields
| Name        | Description                                        |
| ----------- | -------------------------------------------------- |
| rules       | Rules for calculate status.  Sets by source systems. |
| filters     | Filters for mapping events.  Sets by source systems. |
| childrenIds | List of id children.  Sets by source systems. |
| hasChildren | Auto sets if item has children and they exists. For GUI. |
| name        | Name item |
| eventsStatus | For output events id and its statuses |
| aggStatus | For output. Store last max status for 5 min(sets by param) |
| children  | For output. Store children objects |
| parents | For output. Store parents objects |
| events | For output. Store events objects |

### Example Item JSON

```json
{
  "id": "server-72536",
  "version": 95264177,
  "source": "pydnut",
  "sourceKey": "24.10.2025, 16:00:01",
  "sourceType": "OTHER",
  "status": "WARNING",
  "name": "server.local",
  "fields": {
    "OsName": "Linux Astra",
    "item": "Server",
    "owner": "SomeUser",
  },
  "rules": {

  },
  "filters": {
    "byname": {
      "resultStatus": "INDETERMINATE",
      "usingResultStatus": false,
      "equalFields": {
        "name": "server.local"
      }
    },
  },
  "childrenIds": [
    "nginx-124354"
  ],
  "fromHistory": [
    "node1"
  ],
  "createdOn": "2025-05-30T13:00:10.824233561Z",
  "updatedOn": "2025-10-24T13:00:09.325405906Z",
  "deletedOn": null,
  "aggStatus": "WARNING",
  "children": [],
  "parents": [],
  "hasChildren": true
}
```

<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[product-schema]: images/ksm.drawio.png
