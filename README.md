


<h1 align="center">NextiaBS</h1>

<p align="center">
  <a href="#about">About</a> •
  <a href="#requirements">Requirements</a> •
  <a href="#usage">Usage</a> •
  <a href="#installation">Installation</a> •
  <a href="#extending">Extending</a> •
</p>



## About
This library supports the generation of graph-based schemas for heterogeneous data sources.
  
## Key features   
* Extraction of schamata levaraging on the structure of schemaless data sources 
* Standardization of such extracted schemata into RDFS graph data model

## How it works

We encourage you to read [our paper](http://www.semantic-web-journal.net/system/files/swj3138.pdf) to better understand what NextiaBS is and how can fit your scenarios. 

## Requirements
* Java 11

## Installation
Under construction

## Usage    

The class you use depends on the data source format, however, all of them share the same method `bootstrapSchema()` or `bootstrapSchema(Boolean generateMetadata)`. Therefore you need to provide the required paramters for the bootstrap class and call the method `bootstrapSchema` to generate an schema. We currently support JSON and CSV sources. We plan to add more in the future. 


#### JSON
To bootstrap a JSON file, we need to import the class:

```
import edu.upc.essi.dtim.nextiabs.JSONBootstrap;

```

Then to start the bootstrapping, we create an instance of the class `JSONBootstrap` as follows:

```
JSONBootstrap b = new JSONBootstrap(<Here data source name>, <here the data source id >, <path to the data source>);
```
Using this instance, namely `b`, we call the method `bootstrapSchema()`. This method will return a Jena model containing the schema represented as triples. An example using this method is:

```
Model schema_graph_based = b.bootstrapSchema();
```

#### CSV
To bootstrap a CSV file, we need to import the class:

```
import edu.upc.essi.dtim.nextiabs.CSVBootstrap;

```

Then to start the bootstrapping, we create an instance of the class `CSVBootstrap` as follows:

```
CSVBootstrap b = new CSVBootstrap(<Here data source name>, <here the data source id >, <path to the data source>);
```
Using this instance, namely `b`, we call the method `bootstrapSchema()`. This method will return a Jena model containing the schema represented as triples. An example of the used for this method is:

```
Model schema_graph_based = b.bootstrapSchema();
```

## Extending new metamodels

To extend this library with new supported data sources, you only need to create a class at the package `edu.upc.essi.dtim.nextiabs`. To create this class use the following nomenclature: `<Data source format in uppercase> + Bootstrap`. Moreover, this class should extends `DataSource`class and implements `IBootstrap<Graph>`

To extend this library with new supported data sources, you only need to create a class at the package `edu.upc.essi.dtim.nextiabs`. To create this class use the following nomenclature: `<Data source format in uppercase> + Bootstrap`. Moreover, this class should extends `DataSource`class and implements IBootstrap<Graph>`interface.

The class `DataSouce` provides the required attributes and implemented logic to generate resources for the data source elements via the method `createIRI(String name)`. 






