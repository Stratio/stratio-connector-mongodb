First Steps
===========

Mongo Crossdata connector allows the integration between Crossdata and
MongoDB. Crossdata provides an easy and common language as well as the
integration with several other databases. More information about
Crossdata can be found at
`Crossdata <https://github.com/Stratio/crossdata>`__

Table of Contents
=================

-  `Before you start <#before-you-start>`__

   -  `Prerequisites <#prerequisites>`__
   -  `Configuration <#configuration>`__

-  `Creating the database and
   collection <#creating-the-database-and-collection>`__

   -  `Step 1: Create the database <#step-1-create-the-database>`__
   -  `Step 2: Create the collection <#step-2-create-the-collection>`__
   -  `Step 3: Create indexes <#step-3-create-indexes>`__

      -  `Create a default index <#create-a-default-index>`__
      -  `Create a custom index <#create-a-custom-index>`__

-  `Inserting Data <#inserting-data>`__

   -  `Step 4: Insert into collection
      students <#step-4-insert-into-collection-students>`__

      -  `Insert if not exists <#insert-if-not-exists>`__

-  `Updating Data <#updating-data>`__

   -  `Step 5: Update collection
      students <#step-5-update-collection-students>`__

-  `Querying Data <#querying-data>`__

   -  `Step 6: Select From <#step-6-select-from>`__

      -  `Select all <#select-all>`__
      -  `Select with primary key <#select-with-primary-key>`__
      -  `Select with alias <#select-with-alias>`__
      -  `Select with limit <#select-with-limit>`__
      -  `Select with several where
         clauses <#select-with-several-where-clauses>`__
      -  `Select with groupby <#select-with-groupby>`__
      -  `Select with orderby <#select-with-orderby>`__

-  `Altering schemas <#altering-schemas>`__

   -  `Step 7: Alter collection <#step-7-alter-collection>`__

      -  `Add column <#add-column>`__
      -  `Drop column <#drop-column>`__

-  `Delete Data and Remove schemas <#delete-data-and-remove-schemas>`__

   -  `Step 8: Delete <#step-8-delete-data>`__
   -  `Step 9: Drop Index <#step-9-drop-index>`__
   -  `Step 10: Drop Collection <#step-10-drop-collection>`__
   -  `Step 11: Drop Database <#step-11-drop-database>`__

-  `Where to go from here <#where-to-go-from-here>`__

Before you start
================

Prerequisites
-------------

-  Basic knowledge of SQL like language.
-  First of all `Stratio Crossdata <https://github.com/Stratio/crossdata>`__ is needed and must be
   installed. The server and the shell must be running.
-  An installation of
   `MongoDB <http://docs.mongodb.org/manual/installation/>`__.
-  Build an MongoConnector executable and run it following this
   `guide <https://github.com/Stratio/stratio-connector-mongodb#build-an-executable-connector-mongo>`__.

Configuration
-------------

In the Crossdata Shell we need to add the Datastore Manifest.

::

       > add datastore "<path_to_manifest_folder>/MongoDataStore.xml";

The output must be:

::

       [INFO|Shell] CrossdataManifest added 
        DATASTORE
        Name: Mongo
        Version: 0.3.0
        Required properties: 
        Property: 
            PropertyName: Hosts
            Description: The list of hosts ips (csv). Example: host1,host2,host3
        Property: 
            PropertyName: Port
            Description: The list of ports (csv).
    Optional properties: 
        Property: 
            PropertyName: mongo.readPreference
            Description: primary, primarypreferred(default), secondary, secondarypreferred or nearest
        Property: 
            PropertyName: mongo.writeConcern
            Description: acknowledged(default), unacknowledged, replica_acknowledged or journaled
        Property: 
            PropertyName: mongo.acceptableLatencyDifference
            Description: the acceptable latency difference(ms)
        Property: 
            PropertyName: mongo.maxConnectionsPerHost
            Description: the maximum number of connections allowed per host
        Property: 
            PropertyName: mongo.maxConnectionIdleTime
            Description: The maximum idle time of a pooled connection(ms). A zero value indicates no limit
        Property: 
            PropertyName: mongo.connectTimeout
            Description: the connection timeout(ms). A zero value indicates no timeout

Now we need to add the ConnectorManifest.

::

       > add connector "<path_to_manifest_folder>/MongoConnector.xml";  

The output must be:

::

       [INFO|Shell] CrossdataManifest added 
        CONNECTOR
        ConnectorName: MongoConnector
       DataStores: 
        DataStoreName: Mongo
        Version: 0.3.0
        Supported operations:
                                .
                                .
                                .

At this point we have reported to Crossdata the connector options and
operations. Now we configure the datastore cluster.

::

    > ATTACH CLUSTER mongoCluster ON DATASTORE Mongo WITH OPTIONS {'Hosts': '[Ip1, Ip2,..,Ipn]', 
    'Port': '[Port1,Port2,...,Portn]'};

The output must be similar to:

::

      Result: QID: 82926b1e-2f72-463f-8164-98969c352d40
      Cluster attached successfully

It is possible to add options like the read preference, write concern,
etc... All options available are described in the MongoDataStore.xml
(e.g. 'mongo.readPreference' : 'secondaryPreferred')

Now we must run the connector.

The last step is to attach the connector to the cluster created before.

::

      >  ATTACH CONNECTOR MongoConnector TO mongoCluster  WITH OPTIONS {};

The output must be:

::

    CONNECTOR attached successfully

To ensure that the connector is online we can execute the Crossdata
Shell command:

::

      > describe connectors;

And the output must show a message similar to:

::

    Connector: connector.mongoconnector ONLINE  []  [datastore.mongo]   akka.tcp://CrossdataServerCluster@127.0.0.1:46646/user/ConnectorActor/

Creating the database and collection
====================================

If schema metadata has not been imported from MongoDB, Crossdata cannot
work with databases previously created. So, before inserting new data it
is possible discover the existing databases and collections with an
asynchronous operation running this command:

::

        > DISCOVER METADATA ON CLUSTER mongoCluster;

The existing schemas will be displayed when the operation finishes.

Step 1: Create the database
---------------------------

Now we will create the catalog and the table which we will use later in
the next steps.

To create the catalog we must execute.

::

        > CREATE CATALOG highschool;

The output must be:

::

    CATALOG created successfully;

Step 2: Create the collection
-----------------------------

We switch to the database we have just created.

::

      > USE highschool;

To create the table we must execute the next command.

::

      > CREATE TABLE students ON CLUSTER mongoCluster (id int PRIMARY KEY, name text, age int, 
    enrolled boolean);

And the output must show:

::

    TABLE created successfully

Step 3: Create Indexes
----------------------

Create a default index
~~~~~~~~~~~~~~~~~~~~~~

::

      > CREATE DEFAULT INDEX indexname ON students (age);

The shell shows:

::

    INDEX created successfully

It is possible to specify some MongoDB index options. e.g. => CREATE
DEFAULT INDEX uniqueindex ON students (id) WITH {'sparse': true,
'unique' : true};

Create a custom index
~~~~~~~~~~~~~~~~~~~~~

::

      > CREATE CUSTOM INDEX hashedindex ON students (name) WITH {'index_type' : 'hashed'};

Inserting Data
==============

Step 4: Insert into collection students
---------------------------------------

At first we must insert some rows in the table created before.

::

      >  INSERT INTO students(id, name,age,enrolled) VALUES (1, 'Jhon', 16, true);
      >  INSERT INTO students(id, name,age,enrolled) VALUES (2, 'Eva', 20, true);
      >  INSERT INTO students(id, name,age,enrolled) VALUES (3, 'Lucie', 18, true);
      >  INSERT INTO students(id, name,age,enrolled) VALUES (4, 'Cole', 16, true);
      >  INSERT INTO students(id, name,age,enrolled) VALUES (5, 'Finn', 17, false);
      >  INSERT INTO students(id, name,age,enrolled) VALUES (6, 'Violet', 21, false);
      >  INSERT INTO students(id, name,age,enrolled) VALUES (7, 'Beatrice', 18, true);
      >  INSERT INTO students(id, name,age,enrolled) VALUES (8, 'Henry', 16, false);
      

For each row the output must be:

::

    STORED successfully

Insert if not exists
~~~~~~~~~~~~~~~~~~~~

::

      >  INSERT INTO students(id, name,age,enrolled) VALUES (8, 'Allan', 16, false) IF NOT EXISTS;
      >  INSERT INTO students(id, name,age,enrolled) VALUES (9, 'Tom', 17, true) IF NOT EXISTS;
      >  INSERT INTO students(id, name,age,enrolled) VALUES (10, 'Betty', 19, true) IF NOT EXISTS;

The first "INSERT IF NOT EXISTS" will not store new values for the
record with primary key = 8 because this record already exists.

Updating Data
=============

Step 5: Update collection students
----------------------------------

::

      >  UPDATE students SET name = 'Tommy' WHERE id=9;

::

      >  UPDATE students SET age = age + 1 WHERE name='Betty';

For each row the output must be:

::

    STORED successfully

Querying Data
=============

Step 6: Select From
-------------------

Now we execute a set of queries and we will show the expected results.

Select all
~~~~~~~~~~

::

     > SELECT * FROM students;
     
      Partial result: true
      ----------------------------------
      | age | name     | id | enrolled | 
      ----------------------------------
      | 16  | Jhon     | 1  | true     | 
      | 20  | Eva      | 2  | true     | 
      | 18  | Lucie    | 3  | true     | 
      | 16  | Cole     | 4  | true     | 
      | 17  | Finn     | 5  | false    | 
      | 21  | Violet   | 6  | false    | 
      | 18  | Beatrice | 7  | true     | 
      | 16  | Henry    | 8  | false    | 
      | 17  | Tommy    | 9  | true     | 
      | 20  | Betty    | 10 | true     | 
      ----------------------------------

Select with primary key
~~~~~~~~~~~~~~~~~~~~~~~

::

      > SELECT name, enrolled FROM students where id = 1;
      
      Partial result: true
      -------------------
      | name | enrolled | 
      -------------------
      | Jhon | true     | 
      -------------------

Select with alias
~~~~~~~~~~~~~~~~~

::

       >  SELECT name as the_name, enrolled  as is_enrolled FROM students;
       
      Partial result: true
      --------------------------
      | the_name | is_enrolled | 
      --------------------------
      | Jhon     | true        | 
      | Eva      | true        | 
      | Lucie    | true        | 
      | Cole     | true        | 
      | Finn     | false       | 
      | Violet   | false       | 
      | Beatrice | true        | 
      | Henry    | false       | 
      | Tommy    | true        | 
      | Betty    | true        | 
    --------------------------

Select with limit
~~~~~~~~~~~~~~~~~

::

      > SELECT * FROM students LIMIT 3;


      Partial result: true
      -------------------------------
      | age | name  | id | enrolled | 
      -------------------------------
      | 16  | Jhon  | 1  | true     | 
      | 20  | Eva   | 2  | true     | 
      | 18  | Lucie | 3  | true     | 
      -------------------------------

Select with several where clauses
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

      >  SELECT * FROM students WHERE age > 19 AND enrolled = true;
      
      Partial result: true
      -------------------------------
      | age | name  | id | enrolled | 
      -------------------------------
      | 20  | Eva   | 2  | true     | 
      | 20  | Betty | 10 | true     | 
      -------------------------------

Select with groupby
~~~~~~~~~~~~~~~~~~~

::

      >  SELECT age FROM students GROUP BY age;

      Partial result: true
      -------
      | age | 
      -------
      | 21  | 
      | 17  | 
      | 18  | 
      | 20  | 
      | 16  | 
      -------
      

Select with orderby
~~~~~~~~~~~~~~~~~~~

::

      >  SELECT * FROM students ORDER BY age;
      
      Partial result: true
      ----------------------------------
      | id | name     | age | enrolled | 
      ----------------------------------
      | 1  | Jhon     | 16  | true     | 
      | 4  | Cole     | 16  | true     | 
      | 8  | Henry    | 16  | false    | 
      | 5  | Finn     | 17  | false    | 
      | 9  | Tommy    | 17  | true     | 
      | 3  | Lucie    | 18  | true     | 
      | 7  | Beatrice | 18  | true     | 
      | 2  | Eva      | 20  | true     | 
      | 10 | Betty    | 20  | true     | 
      | 6  | Violet   | 21  | false    | 
      ----------------------------------

      >  SELECT * FROM students ORDER BY name;
      
      Partial result: true
      ----------------------------------
      | id | name     | age | enrolled | 
      ----------------------------------
      | 7  | Beatrice | 18  | true     | 
      | 10 | Betty    | 20  | true     | 
      | 4  | Cole     | 16  | true     | 
      | 2  | Eva      | 20  | true     | 
      | 5  | Finn     | 17  | false    | 
      | 8  | Henry    | 16  | false    | 
      | 1  | Jhon     | 16  | true     | 
      | 3  | Lucie    | 18  | true     | 
      | 9  | Tommy    | 17  | true     | 
      | 6  | Violet   | 21  | false    | 
      ----------------------------------
      
       >  SELECT * FROM students ORDER BY id DESC;
       
       Partial result: true
      ----------------------------------
      | id | name     | age | enrolled | 
      ----------------------------------
      | 10 | Betty    | 20  | true     | 
      | 9  | Tommy    | 17  | true     | 
      | 8  | Henry    | 16  | false    | 
      | 7  | Beatrice | 18  | true     | 
      | 6  | Violet   | 21  | false    | 
      | 5  | Finn     | 17  | false    | 
      | 4  | Cole     | 16  | true     | 
      | 3  | Lucie    | 18  | true     | 
      | 2  | Eva      | 20  | true     | 
      | 1  | Jhon     | 16  | true     | 
      ----------------------------------
       
      >  SELECT * FROM students ORDER BY age ASC, id DESC;
      
      Partial result: true
      ----------------------------------
      | id | name     | age | enrolled | 
      ----------------------------------
      | 8  | Henry    | 16  | false    | 
      | 4  | Cole     | 16  | true     | 
      | 1  | Jhon     | 16  | true     | 
      | 9  | Tommy    | 17  | true     | 
      | 5  | Finn     | 17  | false    | 
      | 7  | Beatrice | 18  | true     | 
      | 3  | Lucie    | 18  | true     | 
      | 10 | Betty    | 20  | true     | 
      | 2  | Eva      | 20  | true     | 
      | 6  | Violet   | 21  | false    | 
      ----------------------------------
        
      

Altering Schemas
================

Step 7: Alter collection
------------------------

Add column
~~~~~~~~~~

Now we will alter the table structure.

::

      > ALTER TABLE students ADD surname TEXT;
      OK

After the alter operation we can insert the surname field in the table.

::

        > INSERT INTO students(id, name,age,enrolled,surname) VALUES (10, 'Betty',19,true, 'Smith');

And table must contain the row correctly.

::

      > SELECT * FROM students where surname = 'Smith';
      
      -----------------------------------------
      | age | name  | id | surname | enrolled | 
      -----------------------------------------
      | 19  | Betty | 10 | Smith   | true     | 
      -----------------------------------------

Drop column
~~~~~~~~~~~

Now we will alter the table structure:

::

      > ALTER TABLE students DROP surname;
      OK

After the alter operation we can check:

::

      > SELECT * FROM students where name = 'Betty';
      
      -------------------------------
      | age | name  | id | enrolled | 
      -------------------------------
      | 19  | Betty | 10 | true     | 
      -------------------------------

Delete Data and Remove Schemas
==============================

Step 8: Delete Data
-------------------

For these examples we will execute many delete instructions and we will
show the table evolution.

::

      ----------------------------------
      | age | name     | id | enrolled | 
      ----------------------------------
      | 16  | Jhon     | 1  | true     | 
      | 20  | Eva      | 2  | true     | 
      | 18  | Lucie    | 3  | true     | 
      | 16  | Cole     | 4  | true     | 
      | 17  | Finn     | 5  | false    | 
      | 21  | Violet   | 6  | false    | 
      | 18  | Beatrice | 7  | true     | 
      | 16  | Henry    | 8  | false    | 
      | 17  | Tommy    | 9  | true     | 
      | 19  | Betty    | 10 | true     | 
      ----------------------------------

     
      >  DELETE FROM students WHERE id = 1;
      
      ----------------------------------
      | age | name     | id | enrolled | 
      ----------------------------------
      | 20  | Eva      | 2  | true     | 
      | 18  | Lucie    | 3  | true     | 
      | 16  | Cole     | 4  | true     | 
      | 17  | Finn     | 5  | false    | 
      | 21  | Violet   | 6  | false    | 
      | 18  | Beatrice | 7  | true     | 
      | 16  | Henry    | 8  | false    | 
      | 17  | Tommy    | 9  | true     | 
      | 19  | Betty    | 10 | true     | 
      ----------------------------------

      
      > DELETE FROM students  WHERE age <= 17;
      
      ----------------------------------
      | age | name     | id | enrolled | 
      ----------------------------------
      | 20  | Eva      | 2  | true     | 
      | 18  | Lucie    | 3  | true     | 
      | 21  | Violet   | 6  | false    | 
      | 18  | Beatrice | 7  | true     | 
      | 19  | Betty    | 10 | true     | 
      ----------------------------------

      
      >  DELETE FROM students  WHERE id > 6;
      
      --------------------------------
      | age | name   | id | enrolled | 
      --------------------------------
      | 20  | Eva    | 2  | true     | 
      | 18  | Lucie  | 3  | true     | 
      | 21  | Violet | 6  | false    | 
      --------------------------------

      
      > TRUNCATE students;

At this point the table must be empty. The sentence select \* from
highschool.students returns:

::

    OK
    Result page: 0

Step 9: Drop Index
------------------

::

      > DROP INDEX students.indexname;
      INDEX dropped successfully

Step 10: Drop Collection
------------------------

To drop the table we must execute:

::

      >  DROP TABLE students;
      TABLE dropped successfully

Step 11: Drop database
----------------------

::

      >  DROP CATALOG IF EXISTS highschool;
      CATALOG dropped successfully

Where to go from here
=====================

To learn more about Stratio Crossdata, we recommend to visit the
`Crossdata
Reference <https://github.com/Stratio/crossdata/tree/master/_doc/meta-reference.md>`__.

