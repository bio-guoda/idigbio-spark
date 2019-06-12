# idigbio-spark
Generate taxonomic checklists and occurrence collections from biodiversity collections like GBIF, iDigBio.

This library relies on an [apache spark](https://spark.apache.org) and Mesos/HDFS clusters to:

1. generate checklists
2. generate occurrence collection
3. import Darwin Core Archive into [apache parquet](https://parquet.apache.org) data formats

At time of writing (June 2017), this library is used by http://effechecka.org and https://gimmefreshdata.github.io .

