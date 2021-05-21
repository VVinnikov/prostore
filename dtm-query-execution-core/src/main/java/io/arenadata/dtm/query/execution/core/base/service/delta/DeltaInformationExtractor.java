package io.arenadata.dtm.query.execution.core.base.service.delta;

import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.delta.DeltaInformationResult;
import io.arenadata.dtm.query.calcite.core.node.SqlSelectTree;
import io.arenadata.dtm.query.calcite.core.node.SqlTreeNode;
import org.apache.calcite.sql.SqlNode;

public interface DeltaInformationExtractor {

    DeltaInformationResult extract(SqlNode root);

    DeltaInformation getDeltaInformation(SqlSelectTree tree, SqlTreeNode n);

    DeltaInformation getDeltaInformationAndReplace(SqlSelectTree tree, SqlTreeNode n);
}
