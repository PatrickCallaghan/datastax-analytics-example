Analytics Example
========================================================
This demo creates the sstable files and loads them through jmx to a cassandra cluster. It then runs a analytics function over the data from that table. The main reason to do this is to show an alternative to hive which can be used in the short term.

This demo finds the top ten transaction by amount. 

## Running the demo 

To run this code, you need to have your cluster 'cassandra.yaml' and 'log4j-tools.properties' in the 'src/main/resources' directory.

You will need a java 7 runtime along with maven 3 to run this demo. Start DSE 4.0.X or a cassandra 2.0.X instance on your local machine. This demo just runs as a standalone process on the localhost.

This demo uses quite a lot of memory so it is worth setting the MAVEN_OPTS to run maven with more memory

    export MAVEN_OPTS=-Xmx512M


## Schema Setup
Note : This will drop the keyspace "datastax_analytics_demo" and create a new one. All existing data will be lost. 

To create the a single node cluster with replication factor of 1 for standard localhost setup, run the following

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaSetup"

To run the bulk loader, this defaults to 1 million rows but to change this add -DnoOfRows=<number> 

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.bulkloader.Main" 

To run the analytic function, which finds the top 10 transactions by amount 

	mvn clean compile exec:java -Dexec.mainClass="com.datastax.analytics.AnalyticsRunner"
		
To remove the tables and the schema, run the following.

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaTeardown"
	
