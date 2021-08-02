package io.arenadata.dtm.query.execution.plugin.adp.ddl;

import io.arenadata.dtm.query.execution.plugin.adp.db.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adp.ddl.service.DropTableExecutor;
import io.vertx.core.Future;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.arenadata.dtm.query.execution.plugin.adp.util.TestUtils.*;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

class DropTableExecutorTest {
    private final DatabaseExecutor databaseExecutor = mock(DatabaseExecutor.class);
    private final DropTableExecutor dropTableExecutor = new DropTableExecutor(databaseExecutor);

    @BeforeEach
    void setUp() {
        when(databaseExecutor.executeUpdate(anyString())).thenReturn(Future.succeededFuture());
    }

    @Test
    void test() {
        val expectedSql = "DROP TABLE IF EXISTS <dtm>.<tbl>_actual; DROP TABLE IF EXISTS <dtm>.<tbl>_history; DROP TABLE IF EXISTS <dtm>.<tbl>_staging; "
                .replace("<dtm>", SCHEMA)
                .replace("<tbl>", TABLE);
        val request= createDdlRequest();
        request.getEntity().setSchema(SCHEMA);
        request.getEntity().setName(TABLE);
        dropTableExecutor.execute(request)
                .onComplete(ar -> assertTrue(ar.succeeded()));

        verify(databaseExecutor).executeUpdate(argThat(input -> input.equals(expectedSql)));
    }
}
