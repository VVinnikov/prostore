package io.arenadata.dtm.query.execution.plugin.adp.ddl;

import io.arenadata.dtm.query.execution.plugin.adp.base.dto.metadata.AdpTableEntity;
import io.arenadata.dtm.query.execution.plugin.adp.base.dto.metadata.AdpTables;
import io.arenadata.dtm.query.execution.plugin.adp.base.factory.metadata.AdpTableEntitiesFactory;
import io.arenadata.dtm.query.execution.plugin.adp.db.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adp.ddl.factory.AdpCreateTableQueriesFactory;
import io.arenadata.dtm.query.execution.plugin.adp.ddl.service.CreateTableExecutor;
import io.arenadata.dtm.query.execution.plugin.adp.ddl.service.DropTableExecutor;
import io.arenadata.dtm.query.execution.plugin.api.factory.CreateTableQueriesFactory;
import io.arenadata.dtm.query.execution.plugin.api.factory.TableEntitiesFactory;
import io.vertx.core.Future;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.arenadata.dtm.query.execution.plugin.adp.util.TestUtils.*;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

class CreateTableExecutorTest {
    private final DatabaseExecutor databaseExecutor = mock(DatabaseExecutor.class);
    private final DropTableExecutor dropTableExecutor = mock(DropTableExecutor.class);
    private final TableEntitiesFactory<AdpTables<AdpTableEntity>> tableEntitiesFactory = new AdpTableEntitiesFactory();
    private final CreateTableQueriesFactory<AdpTables<String>> queriesFactory = new AdpCreateTableQueriesFactory(tableEntitiesFactory);
    private final CreateTableExecutor createTableExecutor = new CreateTableExecutor(databaseExecutor, dropTableExecutor, queriesFactory);

    @BeforeEach
    void setUp() {
        when(dropTableExecutor.execute(any())).thenReturn(Future.succeededFuture());
        when(databaseExecutor.executeUpdate(anyString())).thenReturn(Future.succeededFuture());
    }

    @Test
    void testCreateTable() {
        val expectedSql =
                "CREATE TABLE datamart.table_actual (id int8, double_col float8, float_col float4, varchar_col varchar(36), boolean_col bool, int_col int8, bigint_col int8, timestamp_col timestamp(6), time_col time(6), date_col date, uuid_col varchar(36), char_col varchar(10), int32_col int4, sys_from int8, sys_to int8, sys_op int4, constraint pk_datamart_table_actual primary key (id, sys_from)); " +
                "CREATE TABLE datamart.table_history (id int8, double_col float8, float_col float4, varchar_col varchar(36), boolean_col bool, int_col int8, bigint_col int8, timestamp_col timestamp(6), time_col time(6), date_col date, uuid_col varchar(36), char_col varchar(10), int32_col int4, sys_from int8, sys_to int8, sys_op int4, constraint pk_datamart_table_history primary key (id, sys_from)); " +
                "CREATE TABLE datamart.table_staging (id int8, double_col float8, float_col float4, varchar_col varchar(36), boolean_col bool, int_col int8, bigint_col int8, timestamp_col timestamp(6), time_col time(6), date_col date, uuid_col varchar(36), char_col varchar(10), int32_col int4, sys_from int8, sys_to int8, sys_op int4)";
        val request= createDdlRequest(createAllTypesEntity());

        createTableExecutor.execute(request)
                .onComplete(ar -> assertTrue(ar.succeeded()));

        verify(databaseExecutor).executeUpdate(argThat(input -> input.equals(expectedSql)));
        verify(dropTableExecutor).execute(any());
    }
}
