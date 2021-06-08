package io.arenadata.dtm.query.execution.core.dml.service.view;

import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Slf4j
@Component("logicViewReplacer")
public class LogicViewReplacer implements ViewReplacer {
    private final DefinitionService<SqlNode> definitionService;

    @Autowired
    public LogicViewReplacer(@Qualifier("coreCalciteDefinitionService") DefinitionService<SqlNode> definitionService) {
        this.definitionService = definitionService;
    }

    @Override
    public Future<String> replace(String sql, String datamart) {
        // TODO Remove
        return null;
    }

    @Override
    public Future<Void> replace(ViewReplaceContext context) {
        ViewReplacerService replacerService = context.getViewReplacerService();
        context.setViewQueryNode(definitionService.processingQuery(context.getEntity().getViewQuery()));
        return replacerService.replace(context);
    }
}


