package io.arenadata.dtm.query.execution.plugin.adb.service.impl.ddl;

import io.arenadata.dtm.common.plugin.sql.PreparedStatementRequest;
import io.arenadata.dtm.query.execution.plugin.adb.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.query.AdbQueryExecutor;
import io.arenadata.dtm.query.execution.plugin.api.dto.TruncateHistoryParams;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.TruncateHistoryService;
import io.vertx.core.Future;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

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
        doNothing().when(adbQueryExecutor).executeInTransaction(any(), any());
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
        test(sysCn, null, expected);
    }


    @Test
    void testWithConditionsAndSysCn() {
        String conditions = "id > 2";
        Long sysCn = 1L;
        String expected = String.format("DELETE FROM schema.table_history WHERE %s AND sys_to < %s", conditions, sysCn);
        test(sysCn, conditions, expected);
    }

    private void test(Long sysCn, String conditions, List<String> list) {
        adbTruncateHistoryService.truncateHistory(getParams(sysCn, conditions));
        Class<ArrayList<PreparedStatementRequest>> listClass =
                (Class<ArrayList<PreparedStatementRequest>>) (Class) ArrayList.class;
        ArgumentCaptor<ArrayList<PreparedStatementRequest>> argument = ArgumentCaptor.forClass(listClass);
        verify(adbQueryExecutor).executeInTransaction(argThat(input -> input.stream()
                        .map(PreparedStatementRequest::getSql)
                        .collect(Collectors.toList())
                        .equals(list)),
                any());
    }

    private void test(Long sysCn, String conditions, String expected) {
        adbTruncateHistoryService.truncateHistory(getParams(sysCn, conditions));
        verify(adbQueryExecutor, times(1)).execute(expected);
    }

    private TruncateHistoryParams getParams(Long sysCn, String conditions) {
        return new TruncateHistoryParams(null, null, sysCn, SCHEMA, TABLE, null, conditions);
    }
}
