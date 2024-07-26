# runoffdb_miner
<p>Python framework to access the RunoffDB content via class structure reflecting the entity structure in the DB itself. The miner should provide a convenient way to get the data stored in RunoffDB in arbitrary format.</p>
<p>The RunoffDB is an ongoing project to store experimental data in a unified and comprehensive structure that allows human editing and viewing as well as machine processing of the data.</p>
<p>For more information visit:</br>
website of the project https://runoffdb.fsv.cvut.cz/en</br>
MySQL database repository https://github.com/storm-fsv-cvut/runoffDB</br>
PHP based GUI repository https://github.com/storm-fsv-cvut/runoffDB_UI</br></p>

# disclaimer
<p>This is an early stage development ... so far it was just my playground but hopefully others can benefit from it too. And in the long run I hope that some more experienced developer can become a part of the team to correct all my mistakes and point the development in right direction :-)</p>


# entities
<p>The entities (represented by object classes) are defined to represent record structure in the RunoffDB MySQL database.
The class' properties are loaded from the RunoffDB and methods of the classes allow for access to the properties and all related entities and their properties.
On the level of recorded data of measurements are translated to Pandas dataframes available for further processing.</p>

# database connection
<p>So far access to the database is limited to local instance of the database (on localhost) for several reasons, especially:
 <ul>
   <li>security - not to expose the institutional servers to any threat caused by leaked credentials</li>
 <li>data integrity - any mess-up with the data damages only the local copy and can be always be rolled-back by getting a fresh dump from the production database</li>
 <li>access issues - this way the local "sandbox" is always available regardless of working internet connection</li>
</ul>

# miner
<p>Serves as a toolbox for loading data from the database and establish the entities (objects) structure so it can be worked with.</p>
