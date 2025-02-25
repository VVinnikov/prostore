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
package io.arenadata.dtm.query.execution.core.edml.mppr.service.impl;

import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.calcite.core.extension.dml.SqlDataSourceTypeGetter;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.execution.core.base.service.column.CheckColumnTypesService;
import io.arenadata.dtm.query.execution.core.dml.service.ColumnMetadataService;
import io.arenadata.dtm.query.execution.core.edml.configuration.EdmlProperties;
import io.arenadata.dtm.query.execution.core.edml.dto.EdmlRequestContext;
import io.arenadata.dtm.query.execution.core.edml.mppr.factory.MpprKafkaRequestFactory;
import io.arenadata.dtm.query.execution.core.edml.mppr.service.EdmlDownloadExecutor;
import io.arenadata.dtm.query.execution.core.plugin.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.plugin.api.mppr.MpprRequest;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class DownloadKafkaExecutor implements EdmlDownloadExecutor {

    private final EdmlProperties edmlProperties;
    private final QueryParserService queryParserService;
    private final CheckColumnTypesService checkColumnTypesService;
    private final MpprKafkaRequestFactory mpprKafkaRequestFactory;
    private final ColumnMetadataService columnMetadataService;
    private final DataSourcePluginService pluginService;

    @Autowired
    public DownloadKafkaExecutor(EdmlProperties edmlProperties,
                                 @Qualifier("coreCalciteDMLQueryParserService") QueryParserService coreCalciteDMLQueryParserService,
                                 CheckColumnTypesService checkColumnTypesService,
                                 MpprKafkaRequestFactory mpprKafkaRequestFactory,
                                 ColumnMetadataService columnMetadataService,
                                 DataSourcePluginService pluginService) {
        this.edmlProperties = edmlProperties;
        this.queryParserService = coreCalciteDMLQueryParserService;
        this.checkColumnTypesService = checkColumnTypesService;
        this.mpprKafkaRequestFactory = mpprKafkaRequestFactory;
        this.columnMetadataService = columnMetadataService;
        this.pluginService = pluginService;
    }

    @Override
    public Future<QueryResult> execute(EdmlRequestContext context) {
        val actualDatasourceType = getActualDatasourceType(context.getSqlNode());
        if (!checkDestinationType(context, actualDatasourceType)) {
            return Future.failedFuture(new DtmException(
                    String.format("Queried entity is missing for the specified DATASOURCE_TYPE %s", actualDatasourceType)));
        }
        //TODO add checking for column names, and throw new ColumnNotExistsException if will be error
        return queryParserService.parse(new QueryParserRequest(context.getDmlSubQuery(), context.getLogicalSchema()))
                .map(parserResponse -> {
                    if (!checkColumnTypesService.check(context.getDestinationEntity().getFields(), parserResponse.getRelNode())) {
                        throw getFailCheckColumnsException(context);
                    }
                    return parserResponse.getRelNode();
                })
                .compose(relNode -> initColumnMetadata(relNode, context))
                .compose(mpprKafkaRequest -> pluginService.mppr(actualDatasourceType, context.getMetrics(), mpprKafkaRequest));
    }

    private SourceType getActualDatasourceType(SqlNode sqlInsert) {
        if (!(sqlInsert instanceof SqlInsert)) {
            return edmlProperties.getSourceType();
        }

        SqlNode source = ((SqlInsert) sqlInsert).getSource();
        if (source instanceof SqlDataSourceTypeGetter) {
            SqlDataSourceTypeGetter sqlNodeWithDatasource = (SqlDataSourceTypeGetter) source;
            if (sqlNodeWithDatasource.getDatasourceType() != null) {
                return SourceType.valueOfAvailable(sqlNodeWithDatasource.getDatasourceType().getNlsString().getValue());
            }
        }

        return edmlProperties.getSourceType();
    }

    private boolean checkDestinationType(EdmlRequestContext context, SourceType actualSourceType) {
        return context.getLogicalSchema().stream()
                .flatMap(datamart -> datamart.getEntities().stream())
                .allMatch(entity -> entity.getDestination().contains(actualSourceType));
    }

    private DtmException getFailCheckColumnsException(EdmlRequestContext context) {
        return new DtmException(String.format(CheckColumnTypesService.FAIL_CHECK_COLUMNS_PATTERN,
                context.getDestinationEntity().getName()));
    }

    private Future<MpprRequest> initColumnMetadata(RelRoot relNode, EdmlRequestContext context) {
        return columnMetadataService.getColumnMetadata(relNode)
                .compose(columnMetadata -> mpprKafkaRequestFactory.create(context, columnMetadata));
    }

    @Override
    public ExternalTableLocationType getDownloadType() {
        return ExternalTableLocationType.KAFKA;
    }
}
