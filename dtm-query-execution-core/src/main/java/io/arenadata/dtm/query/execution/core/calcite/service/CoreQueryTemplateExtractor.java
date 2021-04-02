package io.arenadata.dtm.query.execution.core.calcite.service;

import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.calcite.core.service.impl.AbstractQueryTemplateExtractor;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service("coreQueryTemplateExtractor")
public class CoreQueryTemplateExtractor extends AbstractQueryTemplateExtractor {

    @Autowired
    public CoreQueryTemplateExtractor(@Qualifier("coreCalciteDefinitionService") DefinitionService<SqlNode> definitionService,
                                      @Qualifier("coreSqlDialect") SqlDialect sqlDialect) {
        super(definitionService, sqlDialect);
    }
}
