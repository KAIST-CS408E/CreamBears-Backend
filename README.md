# xIS-Backend
* Crawl articles from KAIST Portal.
* Extract texts from images and attached files in the articles.
* Index articles into Elasticsearch.
## How to use
* Production mode
```shell
$ sbt
sbt:xis-elastic> run [index] [type]
```
* Debug mode
```shell
$ sbt
sbt:xis-elastic> run [index] [type] --debug [start] [end]
```
* Type `exit` to finish
* Press the enter key to see runtime information
