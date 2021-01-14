package io.arenadata.dtm.query.execution.core.service.dml.impl;

import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.reader.InformationSchemaView;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.QuerySourceRequest;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.execution.core.service.dml.InformationSchemaExecutor;
import io.arenadata.dtm.query.execution.core.service.hsql.HSQLClient;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.stream.Collectors;

@Service
public class InformationSchemaExecutorImpl implements InformationSchemaExecutor {

    private final SqlDialect coreSqlDialect;
    private final QueryParserService parserService;
    private final HSQLClient client;

    @Autowired
    public InformationSchemaExecutorImpl(HSQLClient client,
                                         @Qualifier("coreSqlDialect") SqlDialect coreSqlDialect,
                                         @Qualifier("coreCalciteDMLQueryParserService") QueryParserService parserService) {
        this.client = client;
        this.coreSqlDialect = coreSqlDialect;
        this.parserService = parserService;
    }

    @Override
    public Future<QueryResult> execute(QuerySourceRequest request) {
        return executeInternal(request);
    }

    private Future<QueryResult> executeInternal(QuerySourceRequest request) {
        return Future.future(promise -> {
            getEnrichmentQuerySql(request)
                    .onSuccess(query -> client.getQueryResult(query)
                            .onSuccess(resultSet -> {
                                val result = resultSet.getRows().stream()
                                        .map(JsonObject::getMap)
                                        .collect(Collectors.toList());
                                promise.complete(
                                        QueryResult.builder()
                                                .requestId(request.getQueryRequest().getRequestId())
                                                .metadata(request.getMetadata())
                                                .result(result)
                                                .build());
                            })
                            .onFailure(promise::fail))
                    .onFailure(promise::fail);
        });
    }

    private Future<String> getEnrichmentQuerySql(QuerySourceRequest request) {
        return Future.future(p -> {
                    toUpperCase(request);
                    val parserRequest = new QueryParserRequest(request.getQuery(), request.getLogicalSchema());
                    parserService.parse(parserRequest)
                            .map(response -> {
                                val enrichmentNode = response.getSqlNode();
                                return enrichmentNode.toSqlString(coreSqlDialect).getSql()
                                        .replace(InformationSchemaView.SCHEMA_NAME.toLowerCase(),
                                                InformationSchemaView.DTM_SCHEMA_NAME.toLowerCase());
                            })
                            .onComplete(p);
                }
        );
    }

    private void toUpperCase(QuerySourceRequest request) {
        request.getMetadata()
                .forEach(c -> c.setName(c.getName().toUpperCase()));
    }
}
