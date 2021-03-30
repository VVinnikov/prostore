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
package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.ddl;

import io.arenadata.dtm.query.execution.plugin.adqm.factory.AdqmTruncateHistoryQueriesFactory;
import io.arenadata.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.api.dto.TruncateHistoryRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.TruncateHistoryService;
import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service("adqmTruncateHistoryService")
public class AdqmTruncateHistoryService implements TruncateHistoryService {

    private final DatabaseExecutor adqmQueryExecutor;
    private final AdqmTruncateHistoryQueriesFactory queriesFactory;

    @Autowired
    public AdqmTruncateHistoryService(DatabaseExecutor adqmQueryExecutor,
                                      AdqmTruncateHistoryQueriesFactory queriesFactory) {
        this.adqmQueryExecutor = adqmQueryExecutor;
        this.queriesFactory = queriesFactory;
    }

    @Override
    public Future<Void> truncateHistory(TruncateHistoryRequest request) {
        return adqmQueryExecutor.execute(queriesFactory.insertIntoActualQuery(request))
                .compose(result -> adqmQueryExecutor.execute(queriesFactory.flushQuery(request)))
                .compose(result -> adqmQueryExecutor.execute(queriesFactory.optimizeQuery(request)))
                .compose(result -> Future.succeededFuture());
    }
}
