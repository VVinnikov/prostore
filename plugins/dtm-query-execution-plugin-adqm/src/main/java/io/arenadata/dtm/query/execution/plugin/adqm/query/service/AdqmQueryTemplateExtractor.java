package io.arenadata.dtm.query.execution.plugin.adqm.query.service;

import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.calcite.core.service.impl.AbstractQueryTemplateExtractor;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service("adqmQueryTemplateExtractor")
public class AdqmQueryTemplateExtractor extends AbstractQueryTemplateExtractor {

    @Autowired
    public AdqmQueryTemplateExtractor(@Qualifier("adqmCalciteDefinitionService") DefinitionService<SqlNode> definitionService,
                                      @Qualifier("adqmSqlDialect") SqlDialect sqlDialect) {
        super(definitionService, sqlDialect);
    }
}
