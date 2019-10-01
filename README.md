[![Build Status](https://travis-ci.org/bio-guoda/idigbio-spark.svg?branch=master)](https://travis-ci.org/bio-guoda/idigbio-spark)

# idigbio-spark
Generate taxonomic checklists and occurrence collections from biodiversity collections like GBIF, iDigBio. Converts DwCA tracked by [Preston](https://preston.guoda.bio) into parquet and sequence files to enable parallel processing in a compute cluster.

This library relies on an [apache spark](https://spark.apache.org) and Mesos/HDFS clusters to:

1. generate checklists
2. generate occurrence collection
3. import Darwin Core Archive into [apache parquet](https://parquet.apache.org) data formats

At time of writing (June 2017), this library is used by http://effechecka.org and https://gimmefreshdata.github.io . Note that effechecka and freshdata projects are not longer active.

# Funding

This work is funded in part by grant [NSF OAC 1839201](https://www.nsf.gov/awardsearch/showAward?AWD_ID=1839201&HistoricalAwards=false) from the National Science Foundation.
