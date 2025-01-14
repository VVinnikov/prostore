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
package io.arenadata.dtm.query.execution.core.dml.service.impl;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.extension.dml.DmlType;
import io.arenadata.dtm.query.execution.core.dml.dto.DmlRequestContext;
import io.arenadata.dtm.query.execution.core.dml.service.DmlExecutor;
import io.arenadata.dtm.query.execution.core.dml.service.DmlService;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.EnumMap;
import java.util.Map;

@Slf4j
@Service("coreDmlService")
public class DmlServiceImpl implements DmlService<QueryResult> {
    private final Map<DmlType, DmlExecutor<QueryResult>> executorMap;

    public DmlServiceImpl() {
        this.executorMap = new EnumMap<>(DmlType.class);
    }

    @Override
    public Future<QueryResult> execute(DmlRequestContext context) {
        return getExecutor(context).execute(context);
    }

    private DmlExecutor<QueryResult> getExecutor(DmlRequestContext context) {
        final DmlExecutor<QueryResult> dmlExecutor = executorMap.get(context.getType());

        if (dmlExecutor != null) {
            return dmlExecutor;
        }

        throw new DtmException(
                String.format("Couldn't find dml executor for query kind %s",
                        context.getSqlNode().getKind()));
    }

    @Override
    public SqlProcessingType getSqlProcessingType() {
        return SqlProcessingType.DML;
    }

    @Override
    public void addExecutor(DmlExecutor<QueryResult> executor) {
        executorMap.put(executor.getType(), executor);
    }
}
