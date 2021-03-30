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
package io.arenadata.dtm.query.execution.plugin.adb.service.impl.ddl;

import io.arenadata.dtm.common.plugin.sql.PreparedStatementRequest;
import io.arenadata.dtm.query.execution.plugin.adb.factory.TruncateHistoryDeleteQueriesFactory;
import io.arenadata.dtm.query.execution.plugin.adb.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.api.dto.TruncateHistoryRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.TruncateHistoryService;
import io.vertx.core.Future;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.stream.Collectors;

@Service("adbTruncateHistoryService")
public class AdbTruncateHistoryService implements TruncateHistoryService {
    private final DatabaseExecutor adbQueryExecutor;
    private final TruncateHistoryDeleteQueriesFactory queriesFactory;

    @Autowired
    public AdbTruncateHistoryService(DatabaseExecutor adbQueryExecutor,
                                     TruncateHistoryDeleteQueriesFactory queriesFactory) {
        this.adbQueryExecutor = adbQueryExecutor;
        this.queriesFactory = queriesFactory;
    }

    @Override
    public Future<Void> truncateHistory(TruncateHistoryRequest request) {
        return request.getSysCn().isPresent() ? executeWithSysCn(request) : execute(request);
    }

    private Future<Void> execute(TruncateHistoryRequest request) {
        val queries = queriesFactory.create(request);
        return adbQueryExecutor.executeInTransaction(queries.stream()
                .map(PreparedStatementRequest::onlySql)
                .collect(Collectors.toList()));
    }

    private Future<Void> executeWithSysCn(TruncateHistoryRequest request) {
        return adbQueryExecutor.execute(queriesFactory.createWithSysCn(request))
                .compose(result -> Future.succeededFuture());
    }
}
