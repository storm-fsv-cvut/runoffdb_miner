# runoffdb_miner
<p>Python framework to access the RunoffDB content via class structure reflecting the entity structure in the DB itself. The miner should provide a convenient way to get the data stored in RunoffDB in arbitrary format.</p>
<p>While the RunoffDB database holds current state of the entities the runoffdb_miner framework provides functionalities to the entites that allow them to be used to do some work.</p>
<p>The RunoffDB is an ongoing project to store experimental data in a unified and comprehensive structure that allows human editing and viewing as well as machine processing of the data.</p>
<p>For more information visit:</br>
website of the project https://runoffdb.fsv.cvut.cz/en</br>
MySQL database repository https://github.com/storm-fsv-cvut/runoffDB</br>
PHP based GUI repository https://github.com/storm-fsv-cvut/runoffDB_UI</br></p>

# disclaimer
<p>This is an early stage development of a one man team ... so far it was just my playground but hopefully others can benefit from it too. And in the long run I hope that some more experienced developer can become a part of the team to correct all my mistakes and point the development in right direction :-)</p>

# code structure
<p>The source code consists of only 3 files so far: one defines the DB entity classes, second is providing the database connection and the last one holds the miner itself.</p>
<p>Being quite a beginner in the development I believe it could be structured much better.</p>

## Miner class
<p>Serves as a toolbox for working with the RunoffDB entity classes and utilize those objects to process their data and to construct structured exports. Contains (growing) number of methods that generate predefined table structures or structured exports of the RunoffDB entries.</p>

## entity classes
<p>RunoffDB entities are represented by object classes that hold attributes and provide methods to work with the attributes and their child entities' attributes when useful.
The class' properties are loaded from the RunoffDB either on initiation of the RunoffDB class instance (the rigid or semi-rigid lists) or on demand (the actual measurements, records and data).
The data of measurements' records are translated to Pandas dataframes available for further processing (e.g. by the Miner instance).</p>

### RunoffDB class
### Run class
### Measurement class
### Record class
### Locality class
### SoilSample class
### Plot class
### Crop class
### Agrotechnology class
### Operation class
### Unit class
### Project class
### Simulator class
### Organization class
### ProtectionMeasure class
### RynType class
### CropType class
### OperationType class
### OperationIntensity class
### Phenomenon class
### RecordType class
### QualityIndex class

## database connection
<p>So far access to the database is limited to local instance of the database (on localhost) for several reasons, especially:
 <ul>
   <li>security - not to expose the institutional servers to any threat caused by leaked credentials</li>
 <li>data integrity - any mess-up with the data damages only the local copy and can be always be rolled-back by getting a fresh dump from the production database</li>
 <li>access issues - this way the local "sandbox" is always available regardless of working internet connection</li>
</ul>

