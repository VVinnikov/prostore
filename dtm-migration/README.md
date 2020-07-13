## Dtm migration service

### To run with default params:
java -jar dtm-migration-2.0.0-SNAPSHOT.jar

### To run with your own params:
java -jar -Ddatabase=serviceDb -Dhost=localhost -Dport=3306 -Dusername=user -Dpassword=pwd
-Dliquibase.command=update dtm-migration-2.0.0-SNAPSHOT.jar