package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.enrichment;

import io.arenadata.dtm.common.calcite.CalciteContext;
import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.query.calcite.core.node.SqlSelectTree;
import io.arenadata.dtm.query.execution.plugin.adqm.calcite.rel2sql.AdqmNullNotCastableRelToSqlConverter;
import io.arenadata.dtm.query.execution.plugin.adqm.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.adqm.dto.QueryGeneratorContext;
import io.arenadata.dtm.query.execution.plugin.adqm.service.AdqmQueryJoinConditionsCheckService;
import io.arenadata.dtm.query.execution.plugin.adqm.service.QueryExtendService;
import io.arenadata.dtm.query.execution.plugin.adqm.service.QueryGenerator;
import io.arenadata.dtm.query.execution.plugin.api.exception.DataSourceException;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import lombok.var;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.util.Util;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;

@Slf4j
@Service("adqmQueryGenerator")
public class AdqmQueryGeneratorImpl implements QueryGenerator {
    public static final String ALIAS_PATTERN = ".*SELECT.OTHER(\\[\\d+\\]|)(.AS(\\[\\d+\\]|)|).IDENTIFIER";
    private final QueryExtendService queryExtendService;
    private final SqlDialect sqlDialect;
    private final AdqmQueryJoinConditionsCheckService joinConditionsCheckService;

    public AdqmQueryGeneratorImpl(@Qualifier("adqmCalciteDmlQueryExtendService") QueryExtendService queryExtendService,
                                  @Qualifier("adqmSqlDialect") SqlDialect sqlDialect,
                                  AdqmQueryJoinConditionsCheckService joinConditionsCheckService) {
        this.queryExtendService = queryExtendService;
        this.sqlDialect = sqlDialect;
        this.joinConditionsCheckService = joinConditionsCheckService;
    }

    @Override
    public Future<String> mutateQuery(RelRoot relNode,
                                      List<DeltaInformation> deltaInformations,
                                      CalciteContext calciteContext,
                                      EnrichQueryRequest enrichQueryRequest) {
        return Future.future(promise -> {
            val generatorContext = getContext(relNode,
                    deltaInformations,
                    calciteContext,
                    enrichQueryRequest);

            if (!joinConditionsCheckService.isJoinConditionsCorrect(enrichQueryRequest)) {
                promise.fail(new DataSourceException("Clickhouse’s global join is restricted"));
            }
            try {
                var extendedQuery = queryExtendService.extendQuery(generatorContext);
                val sqlNodeResult = new AdqmNullNotCastableRelToSqlConverter(sqlDialect)
                        .visitChild(0, extendedQuery)
                        .asStatement();
                val sqlTree = new SqlSelectTree(sqlNodeResult);
                addFinalOperatorTopUnionTables(sqlTree);
                replaceDollarSuffixInAlias(sqlTree);
                val queryResult = Util.toLinux(sqlNodeResult.toSqlString(sqlDialect).getSql())
                        .replaceAll("\n", " ");
                log.debug("sql = " + queryResult);
                promise.complete(queryResult);
            } catch (Exception exception) {
                promise.fail(new DataSourceException("Error in converting relation node", exception));
            }
        });
    }

    private void addFinalOperatorTopUnionTables(SqlSelectTree tree) {
        tree.findAllTableAndSnapshots()
                .stream()
                .filter(n -> !n.getKindPath().contains("UNION[1]"))
                .filter(n -> !n.getKindPath().contains("SCALAR_QUERY"))
                .forEach(node -> {
                    SqlIdentifier identifier = node.getNode();
                    val names = Arrays.asList(
                            identifier.names.get(0),
                            identifier.names.get(1) + " FINAL"
                    );
                    node.getSqlNodeSetter().accept(new SqlIdentifier(names, identifier.getParserPosition()));
                });
    }

    private void replaceDollarSuffixInAlias(SqlSelectTree tree) {
        tree.findNodesByPathRegex(ALIAS_PATTERN).stream()
                .filter(n -> {
                    val alias = n.tryGetTableName();
                    return alias.isPresent() && alias.get().contains("$");
                })
                .forEach(sqlTreeNode -> {
                    SqlIdentifier identifier = sqlTreeNode.getNode();
                    val preparedAlias = identifier.getSimple().replace("$", "__");
                    sqlTreeNode.getSqlNodeSetter().accept(new SqlIdentifier(preparedAlias, identifier.getParserPosition()));
                });
    }

    private QueryGeneratorContext getContext(RelRoot relNode,
                                             List<DeltaInformation> deltaInformations,
                                             CalciteContext calciteContext,
                                             EnrichQueryRequest enrichQueryRequest) {
        return QueryGeneratorContext.builder()
                .deltaIterator(deltaInformations.iterator())
                .relBuilder(calciteContext.getRelBuilder())
                .enrichQueryRequest(enrichQueryRequest)
                .relNode(relNode)
                .build();
    }
}
