package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.enrichment;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.calcite.CalciteContext;
import ru.ibs.dtm.common.delta.DeltaInformation;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.calcite.core.node.SqlSelectTree;
import ru.ibs.dtm.query.calcite.core.rel2sql.NullNotCastableRelToSqlConverter;
import ru.ibs.dtm.query.execution.plugin.adqm.dto.QueryGeneratorContext;
import ru.ibs.dtm.query.execution.plugin.adqm.service.QueryExtendService;
import ru.ibs.dtm.query.execution.plugin.adqm.service.QueryGenerator;

import java.util.Arrays;
import java.util.List;

@Service("adqmQueryGenerator")
@Slf4j
public class AdqmQueryGeneratorImpl implements QueryGenerator {
    private final QueryExtendService queryExtendService;
    private final SqlDialect sqlDialect;

    public AdqmQueryGeneratorImpl(@Qualifier("adqmCalciteDmlQueryExtendService") QueryExtendService queryExtendService,
                                  @Qualifier("adqmSqlDialect") SqlDialect sqlDialect) {
        this.queryExtendService = queryExtendService;
        this.sqlDialect = sqlDialect;
    }

    @Override
    public void mutateQuery(RelRoot relNode,
                            List<DeltaInformation> deltaInformations,
                            CalciteContext calciteContext,
                            QueryRequest queryRequest,
                            Handler<AsyncResult<String>> handler) {
        try {
            val generatorContext = getContext(relNode, deltaInformations, calciteContext, queryRequest);
            val extendedQuery = queryExtendService.extendQuery(generatorContext);
            RelNode planAfter = calciteContext.getPlanner().transform(0, extendedQuery.getTraitSet().replace(EnumerableConvention.INSTANCE), extendedQuery);
            SqlNode sqlNodeResult = new NullNotCastableRelToSqlConverter(sqlDialect).visitChild(0, planAfter).asStatement();
            SqlSelectTree tree = new SqlSelectTree(sqlNodeResult);
            toFinalTables(tree);
            renameDollarSuffixInAlias(tree);
            SqlPrettyWriter writer = new SqlPrettyWriter(sqlDialect);
//            writer.format()
//            String queryResult = Util.toLinux(sqlNodeResult.toSqlString(sqlDialect).getSql());
            String queryResult = writer.format(sqlNodeResult);
            log.debug("sql = " + queryResult);
            handler.handle(Future.succeededFuture(queryResult));
        } catch (Exception e) {
            log.error("Request conversion execution error", e);
            handler.handle(Future.failedFuture(e));
        }
    }

    private void toFinalTables(SqlSelectTree tree) {
        tree.findAllTableAndSnapshots()
            .stream()
            .filter(n -> n.getKindPath().startsWith("UNION."))
            .filter(n -> !n.getKindPath().contains("UNION[1]"))
            .filter(n -> !n.getKindPath().contains("SCALAR_QUERY"))
            .forEach(n -> {
                SqlIdentifier identifier = n.getNode();
                val names = Arrays.asList(
                    identifier.names.get(0),
                    identifier.names.get(1) + " FINAL"
                );
                n.getSqlNodeSetter().accept(new SqlIdentifier(names, identifier.getParserPosition()));
            });
    }

    private void renameDollarSuffixInAlias(SqlSelectTree tree) {
        tree.findNodesByPathRegex(".*AS(\\[\\d+\\]|).IDENTIFIER").stream()
            .filter(n -> {
                val alias = n.tryGetTableName();
                return alias.isPresent() && alias.get().contains("$");
            })
            .forEach(n -> {
                SqlIdentifier identifier = n.getNode();
                val preparedAlias = identifier.getSimple().replaceAll("\\$", "__");
                n.getSqlNodeSetter().accept(new SqlIdentifier(preparedAlias, identifier.getParserPosition()));
            });
    }

    private QueryGeneratorContext getContext(RelRoot relNode,
                                             List<DeltaInformation> deltaInformations,
                                             CalciteContext calciteContext,
                                             QueryRequest queryRequest) {
        return new QueryGeneratorContext(
            deltaInformations.iterator(),
            queryRequest,
            calciteContext.getRelBuilder(),
            true,
            relNode);
    }
}
