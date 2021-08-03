package io.arenadata.dtm.query.execution.plugin.adp.ddl;

import io.arenadata.dtm.query.execution.plugin.adp.db.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adp.ddl.service.CreateSchemaExecutor;
import io.vertx.core.Future;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.arenadata.dtm.query.execution.plugin.adp.util.TestUtils.SCHEMA;
import static io.arenadata.dtm.query.execution.plugin.adp.util.TestUtils.createDdlRequest;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class CreateSchemaExecutorTest {

    private final DatabaseExecutor databaseExecutor = mock(DatabaseExecutor.class);
    private final CreateSchemaExecutor createSchemaExecutor = new CreateSchemaExecutor(databaseExecutor);

    @BeforeEach
    void setUp() {
        when(databaseExecutor.executeUpdate(anyString())).thenReturn(Future.succeededFuture());
    }

    @Test
    void testCreateSchema() {
        val expectedSql = "CREATE SCHEMA IF NOT EXISTS " + SCHEMA;

        createSchemaExecutor.execute(createDdlRequest())
                .onComplete(ar -> assertTrue(ar.succeeded()));

        verify(databaseExecutor).executeUpdate(argThat(input -> input.equals(expectedSql)));
    }
}
