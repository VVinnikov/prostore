## Dtm migration service

### To run with default params:
java -jar dtm-migration-2.0.0-SNAPSHOT.jar

### To run with your own params:
java -jar -Ddatasource.database=serviceDb -Ddatasource.host=localhost -Ddatasource.port=3306 -Ddatasource.user=user
-Ddatasource.password=pwd -Dliquibase.command=update dtm-migration-2.0.0-SNAPSHOT.jar