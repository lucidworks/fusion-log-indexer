# fusion-log-indexer

This project offers up a number of tools designed to quickly and efficiently get logs into Fusion.  It supports a pluggable
Parsing strategy (with implementations for Grok, DNS, JSON and NoOp) as well as a number of preconfigured Grok patterns similar
 to what is available in Logstash and other engines.
 
# Features
 
1. Fast, multithreaded, lightweight client for installation on machines to be monitored
1. Pluggable log parsing logic with support for a variety of formats, including Grok, JSON and DNS
1. Ability to watch directories and automatically index new content
1. Integration with <a href="http://www.lucidworks.com/products/fusion">Lucidworks Fusion</a> 
 
# Getting Started
 
## Prerequisites

1. Maven (http://maven.apache.org)
1. Java 1.7 or later
1. <a href="http://www.lucidworks.com/products/fusion">Lucidworks Fusion</a> 

## Building
 
After cloning the repository, do the following on the command line:
 
1. mvn package   // -DskipTests if you want to skip the tests

The output JAR file is in the target directory

## Running

1. To see all options:  ```java -jar ./target/fusion-log-indexer-1.0-exe.jar```

### Examples

1. Watches and sends in logs from old Lucidworks Search system in 500 at a time to the my_collection collection using the default pipeline:```java -jar ./target/fusion-log-indexer-1.0-exe.jar -dir ~/projects/content/lucid/lucidfind/logs/  
 -fusion "http://localhost:8764/api/apollo/index-pipelines/my_collection-default/collections/my_collection/index" 
 -fusionUser USER_HERE -fusionPass PASSWORD_HERE -senderThreads 4 -fusionBatchSize 500 --verbose   -lineParserConfig sample-properties/lws-grok-parser.properties```
 
1. Nagios example: ```java -jar ./target/fusion-log-indexer-1.0-exe.jar -dir ~/projects/content/nagios/  
  -fusion "http://localhost:8764/api/apollo/index-pipelines/nagios-default/collections/nagios/index" 
  -fusionUser USER -fusionPass PASSWORD  -lineParserConfig sample-properties/nagios-grok-parser.properties``` 
 
# Contributing
 
Please submit a pull request against the master branch with your changes. 


# Grokking Grok

For Grok, we are using https://github.com/thekrakken/java-grok/ implementation, which is a little thin on documentation.  However, there
are some useful tools available for learning and working with Grok.  Additionally, see the ```src/main/resources/patterns``` directory
for examples ranging from Apache logs to MongoDB to Nagios.

Useful Sites:
1. http://grokconstructor.appspot.com/do/match
1. Syntax: http://grokconstructor.appspot.com/RegularExpressionSyntax.txt