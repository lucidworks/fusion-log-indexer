# fusion-log-indexer

This project offers up a number of tools designed to quickly and efficiently get logs into Fusion.  It supports a pluggable
Parsing strategy (with implementations for Grok, DNS, JSON and NoOp) as well as a number of preconfigured Grok patterns similar
 to what is available in Logstash and other engines
 
# Getting Started
 
## Prerequisites

1. Maven (http://maven.apache.org)
1. Java 1.7 or later

## Building
 
After cloning the repository, do the following on the command line:
 
1. mvn package   // -DskipTests if you want to skip the tests

The output JAR file is in the target directory

## Running

1. To see all options:  ```java -jar ./target/fusion-log-indexer-1.0-exe.jar```

### Examples

1. ```java -jar ./target/fusion-log-indexer-1.0-exe.jar -dir ~/projects/content/lucid/lucidfind/logs/  
 -fusion "http://localhost:8764/api/apollo/index-pipelines/my_collection-default/collections/my_collection/index" 
 -fusionUser USER_HERE -fusionPass PASSWORD_HERE -senderThreads 4 -fusionBatchSize 500 --verbose   -lineParserConfig sample-properties/apachelogs-grok-parser.properties
 
# Contributing
 
Please submit a pull request against the master branch with your changes. 
