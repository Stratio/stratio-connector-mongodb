About
=====

The Stratio Connector-MongoDB allows [Stratio Crossdata] (<https://github.com/Stratio/crossdata>) to interact with MongoDB.

Requirements
------------

`MongoDB <http://www.mongodb.org/downloads>`_ 2.0 version or later.
`Crossdata <https://github.com/Stratio/crossdata>`__ is needed to interact with this connector.


Compiling Stratio Connector-MongoDB
-----------------------------------
To automatically build execute the following command:

::

   > mvn clean compile install
   > cd conector-mongodb
   > mvn crossdata-connector:install

  To run Stratio Connector-MongoDB execute:

::

   > target/stratio-connector-mongodb-[version]/bin/stratio-connector-mongodb-[version] start  
   

Build an executable Stratio Connector-MongoDB
---------------------------------------------

To generate the executable, run the following commands:

::

   > mvn package -Ppackage
   
   Now to start/stop the connector:

::

    > service stratio-connector-mongodb start
    > service stratio-connector-mongodb stop



How to use Stratio Connector-MongoDB
------------------------------------

A complete tutorial is available `here <https://github.com/Stratio/stratio-connector-mongodb/blob/master/doc/src/site/sphinx/First_Steps.rst>`__. The basic commands are described below.

1. Start `Stratio Crossdata Server and then Stratio Crossdata Shell <http://docs.stratio.com/crossdata>`__.

 2. Start Stratio Connector-MongoDB as it is explained before.

3. In the Stratio Crossdata Shell:

   Add a datastore. We need to specified the XML
   manifest that defines the data store. The XML manifest can be found
   in the path of the Stratio Connector-MongoDB in
   target/stratio-connector-mongo-core-[VERSION]/conf/MongoDataStore.xml

   ``xdsh:user>  ADD DATASTORE <Absolute path to MongoDatastore manifest>;``

   Attach a cluster on that datastore. The datastore name must be the same
   as the defined in the Datastore manifest.

      ```
         xdsh:user>  ATTACH CLUSTER <cluster_name> ON DATASTORE <datastore_name> WITH OPTIONS {'Hosts': '[<IPHost_1,IPHost_2,...,IPHost_n>]', 'Port': '[<PortHost_1,PortHost_2,...,PortHost_n>]'};
      ```

    Add the connector manifest. The XML with the manifest can be found in the path of the Mongo Connector in target/stratio-connector-mongodb-core-[VERSION]/conf/MongoConnector.xml

       ```
         xdsh:user>  ADD CONNECTOR <Path to MongoDB Connector Manifest>
       ```

    Attach the connector to the previously defined cluster. The connector name must match the one defined in the
    Connector Manifest.

        ```
            xdsh:user>  ATTACH CONNECTOR <connector name> TO <cluster name> WITH OPTIONS {};
        ```

At this point, we can start to send queries in the Stratio Crossdata Shell.

License
=======

Licensed to STRATIO (C) under one or more contributor license agreements.
See the NOTICE file distributed with this work for additional information
regarding copyright ownership.  The STRATIO (C) licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.


