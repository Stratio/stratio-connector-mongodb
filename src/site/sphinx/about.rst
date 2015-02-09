About
=====

Native connector for Mongo using Crossdata.

Requirements
------------

[MongoDB] (http://www.mongodb.org/downloads) 2.0 version or later
[Crossdata] (https://github.com/Stratio/crossdata) is needed to interact
with this connector.

Compiling Stratio Connector Mongo
---------------------------------

To automatically build execute the following command:

::

       > mvn clean compile install

Build an executable Connector Mongo
-----------------------------------

To generate the executable, execute the following command:

::

       > mvn crossdata-connector:install

The user and the group of the service are set up to root by default. It
could be changed in the following file:

::

       > target/stratio-connector-mongodb-0.3.0/bin/stratio-connector-mongodb-0.3.0

Running the Stratio Connector Mongo
-----------------------------------

To run Mongo Connector execute:

::

       > target/stratio-connector-mongodb-0.3.0/bin/stratio-connector-mongodb-0.3.0 start

To stop the connector execute:

::

       > target/stratio-connector-mongodb-0.3.0/bin/stratio-connector-mongodb-0.3.0 stop

How to use Mongo Connector
--------------------------

A complete tutorial is available `here <_doc/FirstSteps.md>`__. The
basic commands are described below.

1. Start `crossdata-server and then
   crossdata-shell <https://github.com/Stratio/crossdata>`__.
2. Start Mongo Connector as it is explained before.
3. In crossdata-shell:

   Add a data store with this command. We need to specified the XML
   manifest that defines the data store. The XML manifest can be found
   in the path of the Mongo Connector in
   target/stratio-connector-mongodb-0.3.0/conf/MongoDataStore.xml

   ``xdsh:user>  ADD DATASTORE <Absolute path to MongoDB Datastore manifest>;``

   Attach cluster on that data store. The data store name must be the
   same as the defined in the data store manifest.

   ``xdsh:user>  ATTACH CLUSTER <cluster_name> ON DATASTORE <datastore_name> WITH OPTIONS {'Hosts': '[<IPHost_1,IPHost_2,...,IPHost_n>]', 'Port': '[<PortHost_1,PortHost_2,...,PortHost_n>]'};``

   Add the connector manifest. The XML with the manifest can be found in
   the path of the Mongo Connector in
   target/stratio-connector-mongodb-0.3.0/conf/MongoConnector.xml

   ``xdsh:user>  ADD CONNECTOR <Path to MongoDB Connector Manifest>``

   Attach the connector to the previously defined cluster. The connector
   name must match the one defined in the Connector Manifest.

   ::

       ```
           xdsh:user>  ATTACH CONNECTOR <connector name> TO <cluster name> WITH OPTIONS {};
       ```

   At this point, we can start to send queries.

   ::

       ...
           xdsh:user> CREATE CATALOG catalogTest;

           xdsh:user> USE catalogTest;

           xdsh:user> CREATE TABLE tableTest ON CLUSTER <cluster_name> (id int PRIMARY KEY, name text);

           xdsh:user> INSERT INTO tableTest(id, name) VALUES (1, 'stratio');

           xdsh:user> SELECT * FROM tableTest;
       ...

License
=======

Licensed to STRATIO (C) under one or more contributor license
agreements. See the NOTICE file distributed with this work for
additional information regarding copyright ownership. The STRATIO (C)
licenses this file to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

