package ru.ibs.dtm.query.execution.plugin.adb.service.impl.enrichment;

import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.query.execution.plugin.adb.service.QueryExtendService;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static ru.ibs.dtm.query.execution.plugin.adb.factory.impl.MetadataSqlFactoryImpl.*;

@Service
@Slf4j
public class AdbCalciteDMLQueryExtendServiceImpl implements QueryExtendService {
	private List<Object> options = new ArrayList<>();
	private RelBuilder relBuilder;

	public void setRequestBuilder(RelBuilder relBuilder, boolean clearOptions) {
		this.relBuilder = relBuilder;
		if (clearOptions) {
			options = new ArrayList<>();
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
		List<RelNode> newInput = new ArrayList<>();
		if (node.getInputs() == null || node.getInputs().isEmpty()) {
			if (node instanceof TableScan) {
				relBuilder.push(insertModifiedTableScan(node, (Long) options.get(0)));
			}
			return relBuilder.build();
		}
		node.getInputs().forEach(input -> newInput.add(iterateTree(input)));
		relBuilder.push(node.copy(node.getTraitSet(), newInput));
		return relBuilder.build();
	}

	RelNode insertModifiedTableScan(RelNode tableScan, Long selectOnDelta) {
		RelBuilder relBuilder = RelBuilder.proto(tableScan.getCluster().getPlanner().getContext()).create(tableScan.getCluster(), this.relBuilder.getRelOptSchema());
		List<String> qualifiedName = tableScan.getTable().getQualifiedName();
		List<String> mutableQualifiedName = new ArrayList<>(qualifiedName);

		RexBuilder rexBuilder = relBuilder.getCluster().getRexBuilder();
		List<RexNode> rexNodes = new ArrayList<>();
		IntStream.range(0, tableScan.getTable().getRowType().getFieldList().size()).forEach(index ->
                rexNodes.add(rexBuilder.makeInputRef(tableScan, index))
		);
		StringBuilder name = new StringBuilder(mutableQualifiedName.get(mutableQualifiedName.size() - 1));
		mutableQualifiedName.set(mutableQualifiedName.size() - 1, name + "_" + HISTORY_TABLE);
		RelNode topRelNode = relBuilder.scan(mutableQualifiedName).filter(
				relBuilder.call(SqlStdOperatorTable.AND,
						relBuilder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
								relBuilder.field(SYS_FROM_ATTR),
								relBuilder.literal(selectOnDelta)),
						relBuilder.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
								relBuilder.field(SYS_TO_ATTR),
								relBuilder.literal(selectOnDelta))
				)
		).project(rexNodes).build();

		mutableQualifiedName.set(mutableQualifiedName.size() - 1, name + "_" + ACTUAL_TABLE);
		RelNode bottomRelNode = relBuilder.scan(mutableQualifiedName).filter(
				relBuilder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
						relBuilder.field(SYS_FROM_ATTR),
						relBuilder.literal(selectOnDelta))).project(rexNodes).build();

		return relBuilder.push(topRelNode).push(bottomRelNode).union(true).build();

	}

}
