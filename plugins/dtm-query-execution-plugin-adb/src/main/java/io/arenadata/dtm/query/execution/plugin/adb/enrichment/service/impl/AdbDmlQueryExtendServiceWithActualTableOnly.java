package io.arenadata.dtm.query.execution.plugin.adb.enrichment.service.impl;

import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.delta.DeltaType;
import io.arenadata.dtm.query.execution.plugin.adb.enrichment.dto.QueryGeneratorContext;
import io.arenadata.dtm.query.execution.plugin.adb.enrichment.service.QueryExtendService;
import io.arenadata.dtm.query.execution.plugin.api.exception.DataSourceException;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import lombok.var;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.arenadata.dtm.query.execution.plugin.adb.base.factory.Constants.*;

@Slf4j
public class AdbDmlQueryExtendServiceWithActualTableOnly implements QueryExtendService {

    public static final String TABLE_PREFIX = "_";
    public static final long SYS_TO_MAX_VALUE = 9223372036854775807L;

    @Override
    public RelNode extendQuery(QueryGeneratorContext context) {
        context.getRelBuilder().clear();
        Map<LogicalProject, TableFilter> filterMap = new HashMap<>();
        var relNode = iterateTree(context, context.getRelNode().rel);
        Map<TableScan, DeltaInformation> deltasMap = new HashMap<>();
        initTableFilterMap(filterMap, relNode, deltasMap, context.getDeltaIterator());
        relNode = addSysFieldConfitions(context, filterMap, relNode, deltasMap);
        context.getRelBuilder().clear();
        return relNode;
    }

    RelNode iterateTree(QueryGeneratorContext context, RelNode node) {
        val relBuilder = context.getRelBuilder();
        val newInput = new ArrayList<RelNode>();
        if (node.getInputs() == null || node.getInputs().isEmpty()) {
            if (node instanceof TableScan) {
                if (!context.getDeltaIterator().hasNext()) {
                    throw new DataSourceException("No parameters defined to enrich the request");
                }
                RelNode tableScan = insertModifiedTableScan(relBuilder, node);
                relBuilder.push(tableScan);
            } else {
                relBuilder.push(node);
            }
            return relBuilder.build();
        }
        node.getInputs().forEach(input -> newInput.add(iterateTree(context, input)));
        relBuilder.push(node.copy(node.getTraitSet(), newInput));
        return relBuilder.build();
    }

    private void initTableFilterMap(Map<LogicalProject, TableFilter> filterMap,
                                    RelNode relNode,
                                    Map<TableScan, DeltaInformation> deltasMap,
                                    Iterator<DeltaInformation> deltaIt) {
        AtomicInteger index = new AtomicInteger(0);
        relNode.accept(new RelHomogeneousShuttle() {

            @Override
            public RelNode visit(TableScan scan) {
                deltasMap.put(scan, deltaIt.next());
                return super.visit(scan);
            }

            @Override
            public RelNode visit(LogicalProject project) {
                TableFilter tableFilter = new TableFilter();
                tableFilter.setIndex(index.get());
                index.incrementAndGet();
                project.accept(new RelHomogeneousShuttle() {
                    @Override
                    public RelNode visit(LogicalFilter filter) {
                        if (tableFilter.getFilter() == null) {
                            tableFilter.setFilter(filter);
                        }
                        return super.visit(filter);
                    }

                    @Override
                    public RelNode visit(TableScan scan) {
                        tableFilter.getTables().add(scan);
                        return super.visit(scan);
                    }
                });
                filterMap.put(project, tableFilter);
                return super.visit(project);
            }
        });

        List<TableFilter> tableFilters = filterMap.values().stream()
                .sorted((i1, i2) -> Integer.compare(i2.getIndex(), i1.getIndex()))
                .collect(Collectors.toList());

        Iterator<TableFilter> it = tableFilters.iterator();
        Set<TableScan> ts = new HashSet<>(it.next().getTables());

        while (it.hasNext()) {
            TableFilter currentFilter = it.next();
            currentFilter.getTables().removeAll(ts);
            ts.addAll(currentFilter.getTables());
        }
    }

    private RelNode addSysFieldConfitions(QueryGeneratorContext context,
                                          Map<LogicalProject, TableFilter> filterMap,
                                          RelNode relNode,
                                          Map<TableScan, DeltaInformation> deltasMap) {
        return relNode.accept(new RelHomogeneousShuttle() {

            @Override
            public RelNode visit(LogicalProject project) {
                TableFilter tableFilter = filterMap.remove(project);
                if (tableFilter.getFilter() != null) {
                    return super.visit(project.accept(new RelHomogeneousShuttle() {
                        @Override
                        public RelNode visit(LogicalFilter filter) {
                            if (filter.equals(tableFilter.getFilter())) {
                                RelBuilder builder = context.getRelBuilder()
                                        .push(tableFilter.getFilter().getInput());
                                RexNode[] resFilterNodes = new RexNode[tableFilter.getTables().size() + 1];
                                RexNode[] rexNodes = createRexNodeDeltaFilters(deltasMap,
                                        builder,
                                        tableFilter);
                                System.arraycopy(rexNodes, 0, resFilterNodes, 1, rexNodes.length);
                                resFilterNodes[0] = filter.getCondition();
                                RelNode relWithFilter = builder
                                        .filter(resFilterNodes)
                                        .build();
                                tableFilter.setFilter(null);
                                return relWithFilter;
                            } else {
                                return filter;
                            }
                        }
                    }));
                } else {
                    RelBuilder builder = context.getRelBuilder().push(project.getInput());
                    RexNode[] rexNodes = createRexNodeDeltaFilters(deltasMap,
                            builder,
                            tableFilter);
                    return builder.filter(rexNodes).project(project.getChildExps()).build();
                }
            }
        });
    }

    RelNode insertModifiedTableScan(RelBuilder parentBuilder, RelNode tableScan) {
        val relBuilder = RelBuilder
                .proto(tableScan.getCluster().getPlanner().getContext())
                .create(tableScan.getCluster(), parentBuilder.getRelOptSchema());
        val qualifiedName = tableScan.getTable().getQualifiedName();
        val mutableQualifiedName = new ArrayList<String>(qualifiedName);

        val rexBuilder = relBuilder.getCluster().getRexBuilder();
        val rexNodes = new ArrayList<RexNode>();
        IntStream.range(0, tableScan.getTable().getRowType().getFieldList().size()).forEach(index ->
                rexNodes.add(rexBuilder.makeInputRef(tableScan, index))
        );
        val name = new StringBuilder(mutableQualifiedName.get(mutableQualifiedName.size() - 1));

        initActualTableName(mutableQualifiedName, name);
        return relBuilder.push(createActualNode(relBuilder, mutableQualifiedName)).build();
    }


    private void initActualTableName(ArrayList<String> mutableQualifiedName, StringBuilder name) {
        mutableQualifiedName.set(mutableQualifiedName.size() - 1, name + TABLE_PREFIX + ACTUAL_TABLE);
    }

    private RexNode[] createRexNodeDeltaFilters(Map<TableScan, DeltaInformation> deltas,
                                                RelBuilder relBuilder,
                                                TableFilter filter) {
        RexNode[] rexNodes = new RexNode[filter.getTables().size()];
        for (int i = 0; i < filter.getTables().size(); i++) {
            DeltaInformation deltaInformation = deltas.get(filter.getTables().get(i));
            rexNodes[i] = getRexNode(relBuilder, i, deltaInformation);
        }
        return rexNodes;
    }

    private RexNode getRexNode(RelBuilder relBuilder, int i, DeltaInformation deltaInformation) {
        switch (deltaInformation.getType()) {
            case STARTED_IN:
                return createDeltaStartedInRexNodes(relBuilder, i, deltaInformation);
            case FINISHED_IN:
                return createDeltaFinishedInRexNodes(relBuilder, i, deltaInformation);
            case DATETIME:
            case NUM:
                return createDeltaNumRexNodes(relBuilder, i, deltaInformation);
            default:
                throw new DataSourceException(String.format("Incorrect delta type %s, expected values: %s!",
                        deltaInformation.getType(),
                        Arrays.toString(DeltaType.values())));
        }
    }

    private RexNode createDeltaNumRexNodes(RelBuilder relBuilder, int index, DeltaInformation deltaInformation) {
        //(sys_from <= <sys_cn> AND COALESCE(sys_to, 9223372036854775807) >= <sys_cn>)
        return relBuilder.call(SqlStdOperatorTable.AND,
                relBuilder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                        relBuilder.field(getSysConditionField(index, SYS_FROM_ATTR)),
                        relBuilder.literal(deltaInformation.getSelectOnNum())),
                relBuilder.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                        relBuilder.call(SqlStdOperatorTable.COALESCE,
                                relBuilder.field(getSysConditionField(index, SYS_TO_ATTR)),
                                relBuilder.literal(SYS_TO_MAX_VALUE)),
                        relBuilder.literal(deltaInformation.getSelectOnNum())));
    }

    private RexNode createDeltaFinishedInRexNodes(RelBuilder relBuilder, int index, DeltaInformation deltaInformation) {
        //AND (COALESCE(sys_to, 9223372036854775807) >= <sys_cn1> - 1 AND COALESCE(sys_to, 9223372036854775807) <= <sys_cn2> - 1 AND sys_op = 1)
        return relBuilder.call(SqlStdOperatorTable.AND,
                relBuilder.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                        relBuilder.call(SqlStdOperatorTable.COALESCE,
                                relBuilder.field(getSysConditionField(index, SYS_TO_ATTR)),
                                relBuilder.literal(SYS_TO_MAX_VALUE)),
                        relBuilder.literal(deltaInformation.getSelectOnInterval().getSelectOnFrom() - 1)),
                relBuilder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                        relBuilder.call(SqlStdOperatorTable.COALESCE,
                                relBuilder.field(getSysConditionField(index, SYS_TO_ATTR)),
                                relBuilder.literal(SYS_TO_MAX_VALUE)),
                        relBuilder.literal(deltaInformation.getSelectOnInterval().getSelectOnTo() - 1)),
                relBuilder.call(SqlStdOperatorTable.EQUALS,
                        relBuilder.field(getSysConditionField(index, SYS_OP_ATTR)),
                        relBuilder.literal(1))
        );
    }

    private RexNode createDeltaStartedInRexNodes(RelBuilder relBuilder, int index, DeltaInformation deltaInformation) {
        //AND (sys_from >= <sys_cn1> AND sys_from <= <sys_cn2>)
        return relBuilder.call(SqlStdOperatorTable.AND,
                relBuilder.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                        relBuilder.field(getSysConditionField(index, SYS_FROM_ATTR)),
                        relBuilder.literal(deltaInformation.getSelectOnInterval().getSelectOnFrom())),
                relBuilder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                        relBuilder.field(getSysConditionField(index, SYS_FROM_ATTR)),
                        relBuilder.literal(deltaInformation.getSelectOnInterval().getSelectOnTo()))
        );
    }

    private String getSysConditionField(int index, String fieldName) {
        return index == 0 ? fieldName : fieldName + (index - 1);
    }

    private RelNode createActualNode(RelBuilder relBuilder,
                                     List<String> mutableQualifiedName) {
        return relBuilder.scan(mutableQualifiedName).build();
    }

    @Data
    private class TableFilter {
        private int index;
        private LogicalFilter filter;
        private List<TableScan> tables = new ArrayList<>();
    }

}
