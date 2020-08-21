package ru.ibs.dtm.query.execution.core.service.impl;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import ru.ibs.dtm.common.reader.QueryRequest;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
class SemicolonRemoverImplTest {

    public static final String EXPECTED_SQL = "ALTER VIEW db4.view_f AS SELECT *, ';' as t from information_schema.tables t1 where t1 = ';'";

    @Test
    void removeWith() {
        String sql = "ALTER VIEW db4.view_f AS SELECT *, ';' as t from information_schema.tables t1 where t1 = ';';\n ";
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setSql(sql);
        String actualSql = new SemicolonRemoverImpl().remove(queryRequest).getSql();
        log.info(actualSql);
        assertEquals(EXPECTED_SQL, actualSql);
    }

}
