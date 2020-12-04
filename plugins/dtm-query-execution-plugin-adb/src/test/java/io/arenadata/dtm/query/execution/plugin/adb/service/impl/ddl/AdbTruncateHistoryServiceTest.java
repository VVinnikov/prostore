package io.arenadata.dtm.query.execution.plugin.adb.service.impl.ddl;

import io.arenadata.dtm.query.execution.plugin.adb.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.query.AdbQueryExecutor;
import io.arenadata.dtm.query.execution.plugin.api.dto.TruncateHistoryParams;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.TruncateHistoryService;
import io.vertx.core.Future;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class AdbTruncateHistoryServiceTest {
    private static final String SCHEMA = "schema";
    private static final String TABLE = "table";

    private final DatabaseExecutor adbQueryExecutor = mock(AdbQueryExecutor.class);
    private final TruncateHistoryService adbTruncateHistoryService = new AdbTruncateHistoryService(adbQueryExecutor);

    @BeforeEach
    void setUp() {
        when(adbQueryExecutor.execute(anyString())).thenReturn(Future.succeededFuture());
    }

    @Test
    void test() {
        List<String> expectedList = Arrays.asList(
                "DELETE FROM schema.table_actual",
                "DELETE FROM schema.table_history"
        );
        test(null, null, expectedList);
    }

    @Test
    void testWithConditions() {
        String conditions = "id > 2";
        List<String> expectedList = Arrays.asList(
                String.format("DELETE FROM schema.table_actual WHERE %s", conditions),
                String.format("DELETE FROM schema.table_history WHERE %s", conditions));
        test(null, conditions, expectedList);
    }

    @Test
    void testWithSysCn() {
        Long sysCn = 1L;
        String expected = String.format("DELETE FROM schema.table_history WHERE sys_to < %s", sysCn);
        test(sysCn, null, Collections.singletonList(expected));
    }


    @Test
    void testWithConditionsAndSysCn() {
        String conditions = "id > 2";
        Long sysCn = 1L;
        String expected = String.format("DELETE FROM schema.table_history WHERE %s AND sys_to < %s", conditions, sysCn);
        test(sysCn, conditions, Collections.singletonList(expected));
    }

    private void test(Long sysCn, String conditions, List<String> expectedList) {
        TruncateHistoryParams params = new TruncateHistoryParams(null, null, sysCn, SCHEMA, TABLE, null, conditions);
        adbTruncateHistoryService.truncateHistory(params);
        expectedList.forEach(expected -> verify(adbQueryExecutor, times(1)).execute(expected));
        verify(adbQueryExecutor, times(expectedList.size())).execute(anyString());
    }
}
