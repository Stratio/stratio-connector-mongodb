About
=====

The Stratio Connector-MongoDB allows `Stratio Crossdata <http://docs.stratio.com/modules/crossdata/0.4/index.html>`_ to interact with MongoDB.

Requirements
------------

`MongoDB <http://www.mongodb.org/downloads>`_ 3.0 version or later.
`Crossdata <http://docs.stratio.com/modules/crossdata/0.4/index.html>`__ is needed to interact with this connector.
rpm is needed to generate executable connector. You can install it as follow:
::

 > sudo apt-get install rpm

Compiling an building an executable Stratio Connector-MongoDB
-------------------------------------------------------------
To automatically build execute the following command:

::

   > mvn clean install
   

Running the Stratio Connector-MongoDB
-------------------------------------

Run the executable:

::

    >  ./connector-mongodb/target/stratio-connector-mongodb/bin/stratio-connector-mongodb
::

Build a redistributable package
-------------------------------

It is possible too, to create a RPM or DEB package, as :

::

   > mvn package -Ppackage


Once the package is created, execute this commands to install:

RPM Package:

::

    > rpm -i target/stratio-connector-mongodb-<version>.rpm



DEB Package:

::

    > dpkg -i target/stratio-connector-mongodb-<version>.deb



Now, to start/stop the connector:

::

    > service connector-mongodb start
    > service connector-mongodb stop


How to use Stratio Connector-MongoDB
------------------------------------

A complete tutorial is available `here <http://docs.stratio.com/modules/stratio-connector-mongodb/0.5/
First_Steps.html>`__. The basic commands are described below.

1. Start `Stratio Crossdata Server and then Stratio Crossdata Shell <http://docs.stratio.com/modules/crossdata/0.4/index.html>`__.

2. Start Stratio Connector-MongoDB as it is explained before.

3. In the Stratio Crossdata Shell:

   Attach a cluster on the datastore. The datastore name must be the same
   as the defined in the Datastore manifest.

::

    xdsh:user>  ATTACH CLUSTER <cluster_name> ON DATASTORE <datastore_name> WITH OPTIONS {'Hosts': '[<IPHost_1,IPHost_2,...,IPHost_n>]', 'Port': '[<PortHost_1,PortHost_2,...,PortHost_n>]'};


   Attach the connector to the previously defined cluster. The connector name must match the one defined in the
    Connector Manifest.

::

    xdsh:user>  ATTACH CONNECTOR <connector name> TO <cluster name> WITH OPTIONS {};

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
