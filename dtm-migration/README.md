## Dtm migration service

### To run with default params:
java -jar dtm-migration-2.0.0-SNAPSHOT.jar

### To run with your own params:
java -jar -Ddatasource.service.options.database=serviceDb -Ddatasource.service.options.host=localhost -Ddatasource.service.options.port=3306
-Ddatasource.service.options.user=user -Ddatasource.service.options.password=pwd -Dliquibase.command=update dtm-migration-2.0.0-SNAPSHOT.jar