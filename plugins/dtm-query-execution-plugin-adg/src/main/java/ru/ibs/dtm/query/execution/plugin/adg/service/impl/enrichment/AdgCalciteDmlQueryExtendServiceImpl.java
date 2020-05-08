package ru.ibs.dtm.query.execution.plugin.adg.service.impl.enrichment;

import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.query.execution.plugin.adg.service.QueryExtendService;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.IntStream;

import static ru.ibs.dtm.query.execution.plugin.adg.constants.ColumnFields.*;


@Service("adgCalciteDmlQueryExtendService")
@Slf4j
public class AdgCalciteDmlQueryExtendServiceImpl implements QueryExtendService {
  private LinkedList<Object> options = new LinkedList<>();
  private RelBuilder relBuilder;

  public void setRequestBuilder(RelBuilder relBuilder, boolean clearOptions) {
    this.relBuilder = relBuilder;
    if (clearOptions) {
      options = new LinkedList<>();
    }
  }

  public void addOption(Object option) {
    options.add(option);
  }

  public RelNode extendQuery(RelNode queryTree) {
    if (options.isEmpty()) {
      throw new RuntimeException("Не определены параметры для обогащения запроса");
    }
    relBuilder.clear();
    RelNode relNode = iterateTree(queryTree);
    relBuilder.clear();
    return relNode;
  }

  RelNode iterateTree(RelNode node) {
    List<RelNode> newInput = new ArrayList();
    if (node.getInputs() == null || node.getInputs().isEmpty()) {
      if (node instanceof TableScan) {
        relBuilder.push(insertModifiedTableScan(node, (Long) options.getFirst()));
        removeOption();
      }
      return relBuilder.build();
    }
    node.getInputs().forEach(input -> {
      newInput.add(iterateTree(input));
    });
    relBuilder.push(node.copy(node.getTraitSet(), newInput));
    return relBuilder.build();
  }

  private void removeOption() {
    options.removeFirst();
  }

  RelNode insertModifiedTableScan(RelNode tableScan, Long selectOnDelta) {
    RelBuilder relBuilder = RelBuilder.proto(tableScan.getCluster().getPlanner().getContext()).create(tableScan.getCluster(), this.relBuilder.getRelOptSchema());

    RexBuilder rexBuilder = relBuilder.getCluster().getRexBuilder();
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

    String physicalActualTableName = createPhysicalTableName(tableScan.getTable().getQualifiedName(), ACTUAL_POSTFIX);
    String physicalHistoryTableName = createPhysicalTableName(tableScan.getTable().getQualifiedName(), HISTORY_POSTFIX);

    RelNode topRelNode = relBuilder.scan(physicalHistoryTableName).filter(
      relBuilder.call(SqlStdOperatorTable.AND,
        relBuilder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
          relBuilder.field(SYS_FROM_FIELD),
          relBuilder.literal(selectOnDelta)),
        relBuilder.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
          relBuilder.field(SYS_TO_FIELD),
          relBuilder.literal(selectOnDelta))
      )
    ).project(rexNodes).build();
    RelNode bottomRelNode = relBuilder.scan(physicalActualTableName).filter(
      relBuilder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
        relBuilder.field(SYS_FROM_FIELD),
        relBuilder.literal(selectOnDelta))).project(rexNodes).build();

    RelNode subQueryNode = relBuilder.push(topRelNode).push(bottomRelNode).union(true).build();
    return subQueryNode;
  }

  private String createPhysicalTableName(List<String> qualifiedName, String postfix) {
    List<String> mutableQualifiedName = new ArrayList<>(qualifiedName);
    //формирует имя физической таблицы, к примеру DOC_ACTUAL или OBJ_HISTORY
    //TODO будет переделываться на <имя среды>_<имя схемы>__<имя таблицы>_<суффикс>
    return mutableQualifiedName.get(mutableQualifiedName.size() > 1 ? 1 : 0) + postfix;
  }

}
