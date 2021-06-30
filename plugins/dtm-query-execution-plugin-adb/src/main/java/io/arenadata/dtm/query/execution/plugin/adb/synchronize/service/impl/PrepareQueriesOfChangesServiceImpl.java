package io.arenadata.dtm.query.execution.plugin.adb.synchronize.service.impl;

import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.delta.DeltaType;
import io.arenadata.dtm.common.delta.SelectOnInterval;
import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.dto.QueryParserResponse;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.query.calcite.core.node.SqlSelectTree;
import io.arenadata.dtm.query.calcite.core.node.SqlTreeNode;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.calcite.core.util.SqlNodeUtil;
import io.arenadata.dtm.query.execution.plugin.adb.enrichment.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.adb.enrichment.service.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.adb.synchronize.service.PrepareQueriesOfChangesService;
import io.arenadata.dtm.query.execution.plugin.adb.synchronize.service.PrepareRequestOfChangesRequest;
import io.arenadata.dtm.query.execution.plugin.adb.synchronize.service.PrepareRequestOfChangesResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDateLiteral;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.String.format;

@Service
public class PrepareQueriesOfChangesServiceImpl implements PrepareQueriesOfChangesService {
    private static final String COLUMN_SELECT = "SELECT.OTHER$";
    private static final int SYS_OP_MODIFIED = 0;
    private static final int SYS_OP_DELETED = 1;

    private final QueryParserService parserService;
    private final SqlDialect sqlDialect;
    private final QueryEnrichmentService queryEnrichmentService;

    public PrepareQueriesOfChangesServiceImpl(@Qualifier("adbCalciteDMLQueryParserService") QueryParserService parserService,
                                              @Qualifier("adbSqlDialect") SqlDialect sqlDialect,
                                              QueryEnrichmentService queryEnrichmentService) {
        this.parserService = parserService;
        this.sqlDialect = sqlDialect;
        this.queryEnrichmentService = queryEnrichmentService;
    }

    @Override
    public Future<PrepareRequestOfChangesResult> prepare(PrepareRequestOfChangesRequest request) {
        return parserService.parse(new QueryParserRequest(request.getViewQuery(), request.getDatamarts()))
                .compose(this::replaceTimeBasedColumns)
                .compose(sqlNode -> prepareQueriesOfChanges(sqlNode, request));
    }

    private Future<PrepareRequestOfChangesResult> prepareQueriesOfChanges(SqlNode sqlNode, PrepareRequestOfChangesRequest request) {
        return Future.future(promise -> {
            SqlSelectTree sqlNodeTree = new SqlSelectTree(sqlNode);
            List<SqlTreeNode> allTableAndSnapshots = sqlNodeTree.findAllTableAndSnapshots();

            if (allTableAndSnapshots.isEmpty()) {
                throw new DtmException("No tables in query");
            }


            List<Future> futures = new ArrayList<>(2);
            if (allTableAndSnapshots.size() > 1) {
                futures.add(prepareMultipleRecordsQuery(sqlNode, request, request.getDeltaNumToBe(), request.getDeltaNumToBe() - 1, SYS_OP_MODIFIED));
                futures.add(prepareMultipleRecordsQuery(sqlNode, request, request.getDeltaNumToBe() - 1, request.getDeltaNumToBe(), SYS_OP_DELETED));
            } else {
                futures.add(enrichQueryWithDelta(sqlNode, request, request.getDeltaNumToBe(), DeltaType.STARTED_IN, SYS_OP_MODIFIED));
                futures.add(enrichQueryWithDelta(sqlNode, request, request.getDeltaNumToBe(), DeltaType.FINISHED_IN, SYS_OP_DELETED));
            }

            CompositeFuture.join(futures)
                    .onSuccess(event -> {
                        List<String> result = event.list();
                        promise.complete(new PrepareRequestOfChangesResult(result.get(0), result.get(1)));
                    })
                    .onFailure(promise::fail);
        });
    }

    private Future<String> prepareMultipleRecordsQuery(SqlNode sqlNode, PrepareRequestOfChangesRequest request, long deltaNumCurrent, long deltaNumPrevious, int sysOp) {
        return Future.future(promise -> {
            Future<String> currentStateQuery = enrichQueryWithDelta(sqlNode, request, deltaNumCurrent, DeltaType.NUM, sysOp);
            Future<String> previousStateQuery = enrichQueryWithDelta(sqlNode, request, deltaNumPrevious, DeltaType.NUM, sysOp);
            List<Future> futures = Arrays.asList(currentStateQuery, previousStateQuery);
            CompositeFuture.join(futures)
                    .onSuccess(event -> {
                        List<String> result = event.list();
                        promise.complete(result.get(0) + " EXCEPT " + result.get(1));
                    })
                    .onFailure(promise::fail);
        });
    }

    private Future<String> enrichQueryWithDelta(SqlNode originalSqlNode, PrepareRequestOfChangesRequest request, long deltaNumToBe, DeltaType deltaType, int sysOp) {
        return Future.future(promise -> {
            SqlNode sqlNode = SqlNodeUtil.copy(originalSqlNode);
            SqlSelectTree sqlNodesTree = new SqlSelectTree(sqlNode);
            List<DeltaInformation> deltaInformations = new ArrayList<>();
            sqlNodesTree.findAllTableAndSnapshots().forEach(sqlTreeNode -> {
                DeltaInformation deltaInformation = addDeltaToTableQuery(sqlNodesTree, sqlTreeNode, deltaType, deltaNumToBe);
                deltaInformations.add(deltaInformation);
            });

            addSystemColumns(sqlNodesTree, sysOp);

            queryEnrichmentService.enrich(new EnrichQueryRequest(deltaInformations, request.getDatamarts(), request.getEnvName(), sqlNode))
                    .onComplete(promise);
        });
    }

    private void addSystemColumns(SqlSelectTree sqlNodesTree, int sysOp) {
        List<SqlTreeNode> nodesByPathRegex = sqlNodesTree.findNodesByPathRegex(COLUMN_SELECT);
        if (nodesByPathRegex.size() != 1) {
            throw new DtmException(format("Expected one node contain columns, got: %s", nodesByPathRegex.size()));
        }

        SqlNodeList node = nodesByPathRegex.get(0).getNode();
        node.add(SqlLiteral.createExactNumeric(Integer.toString(sysOp), SqlParserPos.ZERO));
    }

    private DeltaInformation addDeltaToTableQuery(SqlSelectTree sqlNodesTree, SqlTreeNode sqlTreeNode, DeltaType deltaType, long deltaNum) {
        SelectOnInterval builderInterval = null;
        Long builderDeltaNum = null;
        if (deltaType == DeltaType.FINISHED_IN || deltaType == DeltaType.STARTED_IN) {
            builderInterval = new SelectOnInterval(deltaNum, deltaNum);
        } else {
            builderDeltaNum = deltaNum;
        }

        SqlTreeNode tableTreeNode = sqlTreeNode;

        String alias = "";
        if (sqlTreeNode.getNode().getKind() == SqlKind.AS) {
            List<SqlTreeNode> asNodes = sqlNodesTree.findNodesByParent(sqlTreeNode);
            tableTreeNode = asNodes.get(0);
            alias = ((SqlIdentifier) asNodes.get(1).getNode()).names.get(0);
        }

        SqlIdentifier tableSqlNode = tableTreeNode.getNode();
        SqlParserPos parserPos = tableSqlNode.getParserPosition();

        DeltaInformation.DeltaInformationBuilder latestUncommittedDelta = DeltaInformation.builder()
                .pos(parserPos)
                .schemaName(tableSqlNode.names.get(0))
                .tableName(tableSqlNode.names.get(1))
                .tableAlias(alias)
                .type(deltaType)
                .selectOnInterval(builderInterval)
                .selectOnNum(builderDeltaNum)
                .isLatestUncommittedDelta(false);

        return latestUncommittedDelta
                .build();
    }

    private Future<SqlNode> replaceTimeBasedColumns(QueryParserResponse parserResponse) {
        return Future.future(promise -> {
            SqlNode sqlNode = parserResponse.getSqlNode();
            SqlSelectTree sqlNodeTree = new SqlSelectTree(sqlNode);
            List<SqlTreeNode> columnsNode = sqlNodeTree.findNodesByPathRegex(COLUMN_SELECT);
            if (columnsNode.isEmpty()) {
                throw new DtmException(format("Query has no columns: %s", sqlNode.toSqlString(sqlDialect).toString()));
            }

            List<SqlTreeNode> columnsNodes = sqlNodeTree.findNodesByParent(columnsNode.get(0));
            List<SqlTypeName> columnsTypes = parserResponse.getRelNode().rel
                    .getRowType()
                    .getFieldList()
                    .stream()
                    .map(RelDataTypeField::getType)
                    .map(RelDataType::getSqlTypeName)
                    .collect(Collectors.toList());

            for (int i = 0; i < columnsNodes.size(); i++) {
                SqlTypeName columnType = columnsTypes.get(i);
                if (isNotTimeType(columnType)) {
                    continue;
                }

                SqlTreeNode columnNode = columnsNodes.get(i);

                if (columnNode.getNode().getKind() == SqlKind.AS) {
                    columnNode = sqlNodeTree.findNodesByParent(columnNode).get(0);
                }

                columnNode.getSqlNodeSetter().accept(surroundWith(columnType, columnNode.getNode()));
            }

            promise.complete(sqlNode);
        });
    }

    private SqlNode surroundWith(SqlTypeName columnType, SqlNode nodeToSurround) {
        SqlParserPos parserPosition = nodeToSurround.getParserPosition();
        switch (columnType) {
            case DATE: {
                SqlDateLiteral date = SqlDateLiteral.createDate(new DateString(1970, 1, 1), parserPosition);
                return new SqlBasicCall(SqlStdOperatorTable.MINUS_DATE, new SqlNode[]{nodeToSurround, date}, parserPosition);
            }
            case TIME:
            case TIMESTAMP: {
                SqlIntervalQualifier epoch = new SqlIntervalQualifier(TimeUnit.EPOCH, TimeUnit.EPOCH, parserPosition);
                SqlNode extract = new SqlBasicCall(SqlStdOperatorTable.EXTRACT, new SqlNode[]{epoch, nodeToSurround}, parserPosition);
                SqlNode multiply = new SqlBasicCall(SqlStdOperatorTable.MULTIPLY, new SqlNode[]{extract, SqlNumericLiteral.createExactNumeric("1000000", parserPosition)}, parserPosition);
                SqlDataTypeSpec bigintType = new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.BIGINT, parserPosition), parserPosition);
                return new SqlBasicCall(SqlStdOperatorTable.CAST, new SqlNode[]{multiply, bigintType}, parserPosition);
            }
            default:
                throw new IllegalArgumentException("Invalid type to surround");
        }
    }

    private boolean isNotTimeType(SqlTypeName columnType) {
        switch (columnType) {
            case TIMESTAMP:
            case TIME:
            case DATE:
                return false;
            default:
                return true;
        }
    }
}
