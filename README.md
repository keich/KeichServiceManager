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

<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[product-schema]: images/ksm.drawio.png
