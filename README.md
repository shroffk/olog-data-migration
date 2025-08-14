# olog-data-migration
migration of data to the new Phoebus Olog service


### Build

`mvn clean install`

### Run

`java -jar target/olog-data-migration-2.0.0-spring-boot.jar`

example:

We can use properties files to configure the URL and other cobnfiguration details on where to retrive logs and where to 
write them.

`java -Dold_olog.properties=old_olog.properties -Dspring.config.location=olog.properties -jar olog-data-migration-2.0.0-spring-boot.jar`

## Data Migration

The goal of this project is to assist in moving data for any logbook to the new Phoebus Olog service.

In order to move data from any logbook service to Phoebus Olog you will need to provide an implementation for [LogRetrieval](https://github.com/shroffk/olog-data-migration/blob/master/src/main/java/org/phoebus/olog/LogRetrieval.java) Interface. Your implementation can describe how to fetch the old log entires and how they should be mapped onto the new Phoebus Olog entry structure ( you can preserve the create date, add properties, etc...) 

An example implementation for moving data for the olog Olog is avaiable [here](https://github.com/shroffk/olog-data-migration/blob/master/src/main/java/org/phoebus/old/olog/OldOlogLogRetrieval.java).
