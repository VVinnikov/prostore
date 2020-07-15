package ru.ibs.dtm.query.execution.plugin.adg.service.impl.enrichment;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.query.execution.plugin.adg.dto.QueryGeneratorContext;
import ru.ibs.dtm.query.execution.plugin.adg.factory.AdgHelperTableNamesFactory;
import ru.ibs.dtm.query.execution.plugin.adg.service.QueryExtendService;

import static ru.ibs.dtm.query.execution.plugin.adg.constants.ColumnFields.SYS_FROM_FIELD;
import static ru.ibs.dtm.query.execution.plugin.adg.constants.ColumnFields.SYS_TO_FIELD;


@Slf4j
@Service("adgCalciteDmlQueryExtendService")
public class AdgCalciteDmlQueryExtendServiceImpl implements QueryExtendService {
    private final AdgHelperTableNamesFactory helperTableNamesFactory;

    public AdgCalciteDmlQueryExtendServiceImpl(AdgHelperTableNamesFactory helperTableNamesFactory) {
        this.helperTableNamesFactory = helperTableNamesFactory;
    }

    public RelNode extendQuery(QueryGeneratorContext context) {
        context.getRelBuilder().clear();
        RelNode relNode = iterateTree(context, context.getRelNode().rel);
        context.getRelBuilder().clear();
        return relNode;
    }

    RelNode iterateTree(QueryGeneratorContext context, RelNode node) {
        List<RelNode> newInput = new ArrayList<>();
        if (node.getInputs() == null || node.getInputs().isEmpty()) {
            if (node instanceof TableScan) {
                context.getRelBuilder().push(insertModifiedTableScan(context, node, context.getDeltaIterator().next().getDeltaNum()));
            }
            return context.getRelBuilder().build();
        }
        node.getInputs().forEach(input -> {
            newInput.add(iterateTree(context, input));
        });
        context.getRelBuilder().push(node.copy(node.getTraitSet(), newInput));
        return context.getRelBuilder().build();
    }

    RelNode insertModifiedTableScan(QueryGeneratorContext context, RelNode tableScan, Long selectOnDelta) {
        val relBuilder = RelBuilder.proto(tableScan.getCluster().getPlanner().getContext())
                .create(tableScan.getCluster(), context.getRelBuilder().getRelOptSchema());

        val rexBuilder = relBuilder.getCluster().getRexBuilder();
        List<RexNode> rexNodes = new ArrayList<>();
        IntStream.range(0, tableScan.getTable().getRowType().getFieldList().size()).forEach(index ->
                {
                    rexNodes.add(rexBuilder.makeInputRef(tableScan, index));
                }
        );

        if (selectOnDelta == null) {
            log.warn("Параметр selectOn = null использовано значение 0");
            selectOnDelta = 0L;
        }
        val qualifiedName = tableScan.getTable().getQualifiedName();
        val tableName = qualifiedName.get(qualifiedName.size() > 1 ? 1 : 0);
        val queryRequest = context.getQueryRequest();
        val tableNames = helperTableNamesFactory.create(queryRequest.getSystemName(),
                queryRequest.getDatamartMnemonic(),
                tableName);
        val topRelNode = relBuilder.scan(tableNames.getHistory()).filter(
                relBuilder.call(SqlStdOperatorTable.AND,
                        relBuilder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                                relBuilder.field(SYS_FROM_FIELD),
                                relBuilder.literal(selectOnDelta)),
                        relBuilder.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                                relBuilder.field(SYS_TO_FIELD),
                                relBuilder.literal(selectOnDelta))
                )
        ).project(rexNodes).build();
        val bottomRelNode = relBuilder.scan(tableNames.getActual()).filter(
                relBuilder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                        relBuilder.field(SYS_FROM_FIELD),
                        relBuilder.literal(selectOnDelta))).project(rexNodes).build();

        return relBuilder.push(topRelNode).push(bottomRelNode).union(true).build();
    }

}
