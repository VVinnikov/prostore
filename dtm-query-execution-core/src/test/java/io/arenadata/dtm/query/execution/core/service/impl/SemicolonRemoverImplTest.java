package io.arenadata.dtm.query.execution.core.service.impl;

import io.arenadata.dtm.common.reader.QueryRequest;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

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
