/*
 * Copyright © 2021 ProStore
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.arenadata.dtm.query.execution.core.service.impl;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.core.service.query.impl.QuerySemicolonRemoverImpl;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
class QuerySemicolonRemoverImplTest {

    public static final String EXPECTED_SQL = "ALTER VIEW db4.view_f AS SELECT *, ';' as t from information_schema.tables t1 where t1 = ';'";

    @Test
    void removeWith() {
        String sql = "ALTER VIEW db4.view_f AS SELECT *, ';' as t from information_schema.tables t1 where t1 = ';';\n ";
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setSql(sql);
        String actualSql = new QuerySemicolonRemoverImpl().remove(queryRequest).getSql();
        log.info(actualSql);
        assertEquals(EXPECTED_SQL, actualSql);
    }

}
