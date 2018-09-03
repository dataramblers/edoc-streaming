# edoc-streaming: An application for reconciling Edoc data with Crossref

The application matches Edoc against Crossref records and possibly enriches the
former with a [DOI](https://www.doi.org/) provided by the Crossref data. In order to
achieve acceptable matching results you can perform several runs with varying boost 
and score threshold settings. The resulting "scores" (i.e. the relative ) are written 
along with the newly created records into new Elasticsearch indices.

## Setting up the environment

Before you can start the application, there are several preparatory steps you have to perform:

1. You need a local dump of Crossref data. Unfortunately this step isn't that easy because a)
the Crossref data is quite large and b) you can't just download a dump of the entire data. One
possibility is to use a scrapper script which queries the Crossref API. You can find further 
instructions [here](https://github.com/CrossRef/rest-api-doc). And just remember to be 
[nice](https://github.com/CrossRef/rest-api-doc#etiquette)!
2. Load the dump into an Elasticsearch index. That works best if you use our dedicated 
[script](https://github.com/dataramblers/hackathon17/blob/master/crossref/load-data-crossref.py).
3. You also need a dump of Edoc data. You should contact us for that one...
4. Finally JDK (at least 8.0), the scala compiler as well as [sbt](https://scala-sbt.org)
are required to build the application

## Setup and configuration

0. Follow the steps outlined in the section [above](#setting-up-the-environment)
1. Clone the repository and change into the directory
2. Make the appropriate changes in the [settings file](src/main/resources/application.json).
For further instructions see below
3. Build the application: `sbt assembly` (you need JDK, the scala compiler and sbt for that 
(see section above))
4. Run the application with 
`java -jar target/scala-2.11/edoc-streaming-assembly-0.2-SNAPSHOT.jar`

## Application settings

The settings are provided by a JSON file. The default search path points to 
[this](src/main/resources/application.json), but you can also define an ad-hoc configuration by
giving the path to the file as only argument to the application.

The following settings can be changed in the file:

* `sources.edoc-path`: Path to Edoc dump (gzipped)
* `sources.crossref-index`: Name of Crossref index in Elasticsearch
* `sources.crossref-type`:  Name of Crossref type in Elasticsearch 
(meaning that the whole dump is in one type)
* `elasticsearch.host`: Elasticsearch host name
* `elasticsearch.port`: Elasticsearch port
* `reconciling.<field>.fuzziness.<from, to, step>`: Fuzziness value 
for matching. If you want to iterate over different settings, 
you can provide deviating `from` and `to` values as well as 
adequate intermediary `step`s. `field` can be `title` or `person`
* `reconciling.<field>.boost.<from, to, step>`: Boost value for matching. 
If you want to iterate over different settings, you can provide deviating
`from` and `to` values as well as adequate intermediary `step`s. `field` can
be one of `title`, `person`, `isbn`, `issn` and `date`.
* `output.index-prefix`: Prefix for output indices (there will be several if
 iteration is involved)
* `output.index-counter-offset`: Number which will be appended to the index name
(will be increased for every iteration)
* `output.type`: Name of type for output
