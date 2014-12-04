# First Steps #

Mongo Crossdata connector allows the integration between Crossdata and MongoDB. Crossdata provides a easy and common language as well as the integration with several other databases. More information about Crossdata can be found at [Crossdata](https://github.com/Stratio/crossdata)

Table of Contents
=================

-   [Before you start](#before-you-start)
    -   [Prerequisites](#prerequisites)
    -   [Configuration](#configuration)
-   [Creating the database and collection](#creating-the-database-and-collection)
    -   [Step 1: Create the database](#step-1-create-the-database)
    -   [Step 2: Create the collection](#step-2-create-the-collection-schemas)
    -   [Step 3: Create indexes](#step-3-create-indexes)
        -   [Create a default index](#create-a-default-index)
        -   [Create a hashed index with options](#create-a-custom-index)
-   [Inserting Data](#inserting-data)
    -   [Step 4: Insert into collection students](#step-4-insert-into-collection-students)
-   [Updating Data](#updating-data)
    -   [Step 5: Update collection students](#step-5-Update-collection-students)
-   [Querying Data](#querying-data)
    -   [Step 6: Select From](#step-6-select-from)
        -   [Select all](#select-all)
        -   [Select with primary key](#select-primary-key)
        -   [Select with alias](#select-with-alias)
        -   [Select with limit](#select-with-limit)
        -   [Select with several where clauses](#select-with-several-where-clauses)
        -   [Select with groupby](#select-with-groupby)
-   [Altering schemas](#altering-schemas)
    -   [Step 7: Alter collection](#step-7-alter-collection)
        -   [Add column](#add-column)
        -   [Drop column](#drop-column)
-   [Delete Data and Remove schemas](#delete-data-and-remove-schemas)
    -   [Step 8: Delete](#step-8-delete-data)
    -   [Step 9: Drop Index](#step-9-drop-index)
    -   [Step 10: Drop Collection](#step-10-drop-collection)
    -   [Step 11: Drop Database](#step-11-drop-database)
-   [Where to go from here](#where-to-go-from-here)

Before you start
================

Prerequisites
-------------

-   Basic knowledge of SQL like language.
-   First of all [Stratio Crossdata 0.1.1](https://github.com/Stratio/crossdata) is needed and must be installed. The 
server and the shell must be running.
-   An installation of [MongoDB](http://docs.mongodb.org/manual/installation/). 
-   Build an MongoConnector executable and run it following this [guide](https://github.com/Stratio/stratio-connector-mongodb#build-an-executable-connector-mongo). 

Configuration
-------------


In the Crossdata Shell we need to add the Datastore Manifest.

```
   > add datastore "<path_to_manifest_folder>/MongoDataStore.xml";
```

The output must be:

```
   [INFO|Shell] Response time: 0 seconds    
   [INFO|Shell] OK
```

Now we need to add the ConnectorManifest.

```
   > add connector "<path_to_manifest_folder>/MongoConnector.xml";  
```
The output must be:


```
   [INFO|Shell] Response time: 0 seconds    
   [INFO|Shell] OK
```

At this point we have reported to Crossdata the connector options and operations. Now we configure the 
datastore cluster.

```
> ATTACH CLUSTER mongoCluster ON DATASTORE Mongo WITH OPTIONS {'Hosts': '[Ip1, Ip2,..,Ipn]', 
'Port': '[Port1,Port2,...,Portn]'};
```

The output must be similar to:
```
  Result: QID: 82926b1e-2f72-463f-8164-98969c352d40
  Cluster attached successfully
```

It is possible to add options as the read preference. All options available are described in the MongoDataStore.xml (e.g. 'mongo.readPreference' : 'secondaryPreferred')

Now we must run the connector.

```
  > <path_to_connector_executable>/connector-elasticsearch-core-<version> start
```


The last step is to attach the connector to the cluster created before.

```
  >  ATTACH CONNECTOR mongoconnector TO mongoCluster  WITH OPTIONS {};
```

The output must be:
```
CONNECTOR attached successfully
```

To ensure that the connector is online we can execute the Crossdata Shell command:

```
  > describe connectors;
```

And the output must show a message similar to:

```
Connector: connector.mongoconnector	ONLINE	[]	[datastore.mongo]	akka.tcp://CrossdataServerCluster@127.0.0.1:46646/user/ConnectorActor/
```


Creating the database and collection
===============================

Step 1: Create the database
---------------------------

Now we will create the catalog and the table which we will use later in the next steps.

To create the catalog we must execute.

```
    > CREATE CATALOG highschool;
```
The output must be:

```
CATALOG created successfully;
```

Step 2: Create the collection
--------------------------------

We switch to the database we have just created.

```
  > USE highschool;
```

To create the table we must execute the next command.

```
  > CREATE TABLE students ON CLUSTER mongoCluster (id int PRIMARY KEY, name text, age int, 
enrolled boolean);
```

And the output must show:

```
TABLE created successfully
```
Step 3: Create Indexes
----------------------

### Create a default index


```
  > CREATE DEFAULT INDEX indexname ON catalogt.tabletest (age);
```

The shell shows:

```
```

It is possible to specify some MongoDB index options. (e.g. : CREATE DEFAULT INDEX indexname ON table (id) WITH {'sparse': true, 'unique' : true};

### Create a custom index



Inserting Data
==============

Step 4: Insert into collection students
-------------------------------

At first we must insert some rows in the table created before.
```
  >  INSERT INTO students(id, name,age,enrolled) VALUES (1, 'Jhon', 16,true);
  >  INSERT INTO students(id, name,age,enrolled) VALUES (2, 'Eva',20,true);
  >  INSERT INTO students(id, name,age,enrolled) VALUES (3, 'Lucie',18,true);
  >  INSERT INTO students(id, name,age,enrolled) VALUES (4, 'Cole',16,true);
  >  INSERT INTO students(id, name,age,enrolled) VALUES (5, 'Finn',17,false);
  >  INSERT INTO students(id, name,age,enrolled) VALUES (6, 'Violet',21,false);
  >  INSERT INTO students(id, name,age,enrolled) VALUES (7, 'Beatrice',18,true);
  >  INSERT INTO students(id, name,age,enrolled) VALUES (8, 'Henry',16,false);
  >  INSERT INTO students(id, name,age,enrolled) VALUES (9, 'Tom',17,true);
  >  INSERT INTO students(id, name,age,enrolled) VALUES (10, 'Betty',19,true);
```

For each row the output must be:

```
STORED successfully
```

Updating Data
==============

Step 5: Update collection students
-------------------------------

```
  >  UPDATE students SET name = 'Tommy' WHERE id=9;
```

```
  >  UPDATE students SET age = age + 1 WHERE name=';
```

For each row the output must be:

```
STORED successfully
```

Querying Data
=============

Step 6: Select From
-------------------

Now we execute a set of queries and we will show the expected results.

### Select all

```
 > SELECT * FROM students;
 
 Partial result: true
 ----------------------------------
 | age | id | enrolled | name     | 
 ----------------------------------
 | 16  | 4  | true     | Cole     | 
 | 17  | 9  | true     | Tom      | 
 | 16  | 8  | false    | Henry    | 
 | 18  | 3  | true     | Lucie    | 
 | 20  | 2  | true     | Eva      | 
 | 18  | 7  | true     | Beatrice | 
 | 16  | 1  | true     | Jhon     | 
 | 21  | 6  | false    | Violet   | 
 ----------------------------------
```

### Select with primary key
```
  > SELECT name, enrolled FROM students where id = 1;
  
  Partial result: true
  -------------------
  | name | enrolled | 
  -------------------
  | Jhon | true     | 
  -------------------
```

### Select with alias

```
   >  SELECT name as the_name, enrolled  as is_enrolled FROM students;
   
   Partial result: true
   --------------------------
   | the_name | is_enrolled | 
   --------------------------
   | Cole     | true        | 
   | Tom      | true        | 
   | Lucie    | true        | 
   | Henry    | false       | 
   | Eva      | true        | 
   | Beatrice | true        | 
   | Jhon     | true        | 
   | Violet   | false       | 
   --------------------------
```

### Select with limit

```
  >  SELECT * FROM students LIMIT 3;
  
  Partial result: true
  -------------------------------
  | age | id | enrolled | name  | 
  -------------------------------
  | 16  | 4  | true     | Cole  | 
  | 17  | 9  | true     | Tom   | 
  | 18  | 3  | true     | Lucie | 
  -------------------------------
```
### Select with several where clauses

```
  >  SELECT * FROM students WHERE age > 20 AND enrolled = true;
  
  Partial result: true
  -------------------------------
  | age | id | enrolled | name  | 
  -------------------------------
  | 16  | 4  | true     | Cole  | 
  | 17  | 9  | true     | Tom   | 
  | 18  | 3  | true     | Lucie | 
  -------------------------------
```

### Select with groupby

```
  >  SELECT * FROM students WHERE age > 20 AND enrolled = true;
  
```


Altering Schemas
=============

Step 7: Alter collection
-------------------

### Add column

Now we will alter the table structure.
```
  > ALTER TABLE highschool.students ADD surname TEXT;
```

After the alter operation we can insert the surname field in the table.
```
	> INSERT INTO highschool.students(id, name,age,enrolled,surname) VALUES (10, 'Betty',19,true, 'Smith');
```
And table must contain the row correctly. 
```
  > SELECT * FROM highschool.students;
  
-----------------------------------------
| surname | age | id | enrolled | name  | 
-----------------------------------------
| Smith   | 19  | 10 | true     | Betty | 
-----------------------------------------
```

### Drop column

Now we will alter the table structure.
```
  > ALTER TABLE highschool.students ADD surname TEXT;
```

After the alter operation we can insert the surname field in the table.
```
	> INSERT INTO highschool.students(id, name,age,enrolled,surname) VALUES (10, 'Betty',19,true, 'Smith');
```
And table must contain the row correctly. 
```
  > SELECT * FROM highschool.students;
  
-----------------------------------------
| surname | age | id | enrolled | name  | 
-----------------------------------------
| Smith   | 19  | 10 | true     | Betty | 
-----------------------------------------
```

Delete Data and Remove Schemas
==============================

Step 8: Delete Data
-------------------

For these examples we will execute many delete instructions and we will show the table evolution. 


```
 ----------------------------------
 | age | id | enrolled | name     | 
 ----------------------------------
 | 16  | 4  | true     | Cole     | 
 | 17  | 9  | true     | Tom      | 
 | 16  | 8  | false    | Henry    | 
 | 18  | 3  | true     | Lucie    | 
 | 20  | 2  | true     | Eva      | 
 | 18  | 7  | true     | Beatrice | 
 | 16  | 1  | true     | Jhon     | 
 | 21  | 6  | false    | Violet   | 
 ----------------------------------
 
  >  DELETE FROM highschool.students  WHERE id = 1;
  
  ----------------------------------
  | age | id | enrolled | name     | 
  ----------------------------------
  | 16  | 4  | true     | Cole     | 
  | 17  | 9  | true     | Tom      | 
  | 16  | 8  | false    | Henry    | 
  | 18  | 3  | true     | Lucie    | 
  | 20  | 2  | true     | Eva      | 
  | 18  | 7  | true     | Beatrice | 
  | 21  | 6  | false    | Violet   | 
  ----------------------------------
  
  > DELETE FROM highschool.students  WHERE id < 3;
  
  ----------------------------------
  | age | id | enrolled | name     | 
  ----------------------------------
  | 16  | 4  | true     | Cole     | 
  | 17  | 9  | true     | Tom      | 
  | 16  | 8  | false    | Henry    | 
  | 18  | 3  | true     | Lucie    | 
  | 18  | 7  | true     | Beatrice | 
  | 21  | 6  | false    | Violet   | 
  ----------------------------------
  
  > DELETE FROM highschool.students  WHERE age <= 17;
  
  ----------------------------------
  | age | id | enrolled | name     | 
  ----------------------------------
  | 18  | 3  | true     | Lucie    | 
  | 18  | 7  | true     | Beatrice | 
  | 21  | 6  | false    | Violet   | 
  ----------------------------------
  
  >  DELETE FROM highschool.students  WHERE id > 6;
  
  --------------------------------
  | age | id | enrolled | name   | 
  --------------------------------
  | 18  | 3  | true     | Lucie  | 
  | 21  | 6  | false    | Violet | 
  --------------------------------
  
  > TRUNCATE highschool.students;
```

At this point the table must be empty. The sentence select * from highschool.students returns:

```
OK
Result page: 0
```

Step 9: Drop Index
-------------------

```
DROP INDEX indexname;
```

Step 10: Drop Collection
-------------------

To drop the table we must execute:
```
  >  DROP TABLE if exists students;
TABLE dropped successfully

```

Step 11: Drop database
----------------------

```
  >  DROP CATALOG if exists highschool;
CATALOG dropped successfully

```

Where to go from here
=====================

To learn more about Stratio Crossdata, we recommend to visit the [Crossdata Reference](https://github.com/Stratio/crossdata/blob/0.1.1/_doc/meta-reference.md "Meta Reference").

