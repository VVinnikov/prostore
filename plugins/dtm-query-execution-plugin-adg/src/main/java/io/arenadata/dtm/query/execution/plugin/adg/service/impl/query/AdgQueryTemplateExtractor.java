package io.arenadata.dtm.query.execution.plugin.adg.service.impl.query;

import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.calcite.core.service.impl.AbstractQueryTemplateExtractor;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service("adgQueryTemplateExtractor")
public class AdgQueryTemplateExtractor extends AbstractQueryTemplateExtractor {

    @Autowired
    public AdgQueryTemplateExtractor(@Qualifier("adgCalciteDefinitionService") DefinitionService<SqlNode> definitionService,
                                     @Qualifier("adgSqlDialect") SqlDialect sqlDialect) {
        super(definitionService, sqlDialect);
    }
}
