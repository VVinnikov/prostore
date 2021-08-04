package io.arenadata.dtm.query.execution.plugin.adp.dml;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.query.calcite.core.dto.EnrichmentTemplateRequest;
import io.arenadata.dtm.query.calcite.core.extension.dml.SqlDynamicLiteral;
import io.arenadata.dtm.query.calcite.core.node.SqlSelectTree;
import io.arenadata.dtm.query.calcite.core.node.SqlTreeNode;
import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.calcite.core.service.impl.AbstractQueryTemplateExtractor;
import io.arenadata.dtm.query.calcite.core.util.SqlNodeUtil;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.Iterator;
import java.util.List;

@Service("adpQueryTemplateExtractor")
public class AdpQueryTemplateExtractor extends AbstractQueryTemplateExtractor {

    @Autowired
    public AdpQueryTemplateExtractor(@Qualifier("adpCalciteDefinitionService") DefinitionService<SqlNode> definitionService,
                                     @Qualifier("adpSqlDialect") SqlDialect sqlDialect) {
        super(definitionService, sqlDialect);
    }

    @Override
    public SqlNode enrichTemplate(EnrichmentTemplateRequest request) {
        SqlSelectTree selectTree = new SqlSelectTree(SqlNodeUtil.copy(request.getTemplateNode()));
        List<SqlTreeNode> dynamicNodes = selectTree.findNodesByPath(DYNAMIC_PARAM_PATH);

        Iterator<SqlNode> paramIterator = request.getParams().iterator();
        int paramNum = 1;
        for (SqlTreeNode dynamicNode : dynamicNodes) {
            SqlNode param;
            if (!paramIterator.hasNext()) {
                paramIterator = request.getParams().iterator();
            }
            param = paramIterator.next();
            if (param.getKind() == SqlKind.DYNAMIC_PARAM) {
                param = new SqlDynamicLiteral(paramNum, SqlTypeName.ANY, param.getParserPosition());
                paramNum++;
            }
            dynamicNode.getSqlNodeSetter().accept(param);
        }
        if (paramIterator.hasNext()) {
            throw new DtmException("The number of passed parameters and parameters in the template does not match");
        } else {
            return selectTree.getRoot().getNode();
        }
    }
}
