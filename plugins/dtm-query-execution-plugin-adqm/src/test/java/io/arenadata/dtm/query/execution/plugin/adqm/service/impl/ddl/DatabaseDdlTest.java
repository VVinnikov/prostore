package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.ddl;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.calcite.core.extension.eddl.SqlCreateDatabase;
import io.arenadata.dtm.query.execution.plugin.adqm.configuration.properties.DdlProperties;
import io.arenadata.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adqm.service.mock.MockDatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.DdlExecutor;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertTrue;
//FixMe Test
class DatabaseDdlTest {
//    private static final DdlProperties ddlProperties = new DdlProperties();
//
//    @BeforeAll
//    public static void setup() {
//        ddlProperties.setCluster("test_cluster");
//    }
//
//    @Test
//    public void testCreateDatabase() {
//        SqlParserPos pos = new SqlParserPos(1, 1);
//        SqlCreateDatabase createDatabase = new SqlCreateDatabase(pos, true,
//                new SqlIdentifier("testdb", pos));
//        DdlRequestContext context = new DdlRequestContext(null, createDatabase);
//
//        DatabaseExecutor executor = new MockDatabaseExecutor(
//                Collections.singletonList(t -> t.equalsIgnoreCase("CREATE DATABASE IF NOT EXISTS dev__testdb on cluster test_cluster")));
//
//        DdlExecutor<Void> databaseDdlService = new CreateDatabaseExecutor(executor, ddlProperties);
//
//        databaseDdlService.execute(context, "CREATE").onComplete(ar -> assertTrue(ar.succeeded()));
//    }
//
//    @Test
//    public void testDropDatabase() {
//        final String datamartName = "testdb";
//        DdlRequestContext context = new DdlRequestContext(new DdlRequest(new QueryRequest()));
//        context.setDatamartName(datamartName);
//
//        DatabaseExecutor executor = new MockDatabaseExecutor(
//                Collections.singletonList(t -> t.equalsIgnoreCase("drop database if exists dev__testdb on cluster test_cluster")));
//
//        DdlExecutor<Void> databaseDdlService = new DropDatabaseExecutor(executor, ddlProperties);
//
//        databaseDdlService.execute(context, "DROP").onComplete(ar -> assertTrue(ar.succeeded()));
//    }
}
