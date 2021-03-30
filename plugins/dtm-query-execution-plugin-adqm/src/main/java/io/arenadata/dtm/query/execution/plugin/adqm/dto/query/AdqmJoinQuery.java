package io.arenadata.dtm.query.execution.plugin.adqm.dto.query;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rex.RexCall;

@Data
@AllArgsConstructor
public class AdqmJoinQuery {
    private RelNode left;
    private RelNode right;
    private JoinInfo joinInfo;
    private RexCall condition;
}
