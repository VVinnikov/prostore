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

import io.arenadata.dtm.query.execution.plugin.adb.dto.AdbTables;
import io.arenadata.dtm.query.execution.plugin.adb.factory.DdlSqlFactory;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.query.AdbQueryExecutor;
import io.arenadata.dtm.query.execution.plugin.api.factory.CreateTableQueriesFactory;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.DdlExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.DdlService;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class CreateTableExecutor implements DdlExecutor<Void> {

    private final AdbQueryExecutor adbQueryExecutor;
    private final DdlSqlFactory sqlFactory;
    private final DropTableExecutor dropTableExecutor;
    private final CreateTableQueriesFactory<AdbTables<String>> createTableQueriesFactory;

    @Autowired
    public CreateTableExecutor(AdbQueryExecutor adbQueryExecutor,
                               DdlSqlFactory sqlFactory,
                               DropTableExecutor dropTableExecutor,
                               CreateTableQueriesFactory<AdbTables<String>> createTableQueriesFactory) {
        this.adbQueryExecutor = adbQueryExecutor;
        this.sqlFactory = sqlFactory;
        this.dropTableExecutor = dropTableExecutor;
        this.createTableQueriesFactory = createTableQueriesFactory;
    }

    @Override
    public Future<Void> execute(DdlRequest request) {
        return dropTableExecutor.execute(request)
                .compose(v -> createTableWithIndexes(request));
    }

    private Future<Void> createTableWithIndexes(DdlRequest request) {
        return Future.future(promise -> {
            AdbTables<String> createTableQueries = createTableQueriesFactory.create(request.getEntity(), request.getEnvName());
            String createTablesSql = String.join("; ", createTableQueries.getActual(),
                    createTableQueries.getHistory(), createTableQueries.getStaging());
            String createIndexesSql = sqlFactory.createSecondaryIndexSqlQuery(request.getEntity().getSchema(),
                    request.getEntity().getName());
            adbQueryExecutor.executeUpdate(createTablesSql)
                    .compose(v -> adbQueryExecutor.executeUpdate(createIndexesSql))
                    .onComplete(promise);
        });
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.CREATE_TABLE;
    }

    @Override
    @Autowired
    public void register(@Qualifier("adbDdlService") DdlService<Void> service) {
        service.addExecutor(this);
    }
}
