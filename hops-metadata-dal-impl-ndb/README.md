Hops Metadata DAL NDB Implementation
===

Hops Database abstraction layer for storing the hops metadata in MySQL Cluster

How to build
===

```
mvn clean install -DskipTests
```

If you get an error that LIBNDBPATH is not set (or not correct), go to the [Hops](https://github.com/hopshadoop/hops) folder, and then the /target/lib folder. Copy the complete path (find it with pwd), and add it to your .bashrc file:

```
export LIBNDBPATH=<your path here, e.g. /home/user/hops/hops/target/lib>
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$LIBNDBPATH
```

And reload bashrc with:

```
source ~/.bashrc
```

Development Notes
===
Updates to the schema should be done in the schema/update-schema_XXX.sql corresponding to the version you are working on.

# License

Hops-Metadata-dal-impl-ndb is released under an [GPL 2.0 license](LICENSE.txt).

OsX Developer Help
===
Add to bash profile:
export LIBRARY_PATH=<your path here> #location of the ndbclient.dylib
