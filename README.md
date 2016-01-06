## ScanDataImporter
This is a small tool written to import data output by an HBase shell scan into HBase.
 
### Building

```
$ sbt package
```

### Running
This was written in Scala 2.10, to run:

```
scala -classpath $(hbase classpath):./target/scala-2.10/scandataimporter_2.10-1.0.jar ScanDataImporter <tableName> <scanoutput.txt>
```
