package io.arenadata.dtm.query.execution.core.dml.service.view;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.calcite.core.node.SqlSelectTree;
import io.arenadata.dtm.query.calcite.core.node.SqlTreeNode;
import lombok.Builder;
import lombok.Data;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSnapshot;

@Data
@Builder
public class ViewReplaceContext {

    private ViewReplacerService viewReplacerService;
    private SqlNode viewQueryNode;
    private String datamart;
    private SqlSelectTree allNodes;
    private SqlTreeNode currentNode;
    private SqlSnapshot sqlSnapshot;
    private Entity entity;

}
