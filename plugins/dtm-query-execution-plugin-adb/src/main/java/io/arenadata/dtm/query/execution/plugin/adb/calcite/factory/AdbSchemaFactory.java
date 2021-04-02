package io.arenadata.dtm.query.execution.plugin.adb.calcite.factory;

import io.arenadata.dtm.query.calcite.core.factory.impl.DtmSchemaFactory;
import io.arenadata.dtm.query.calcite.core.schema.dialect.DtmConvention;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.adb.calcite.dto.schema.dialect.AdbDtmConvention;
import org.apache.calcite.linq4j.tree.Expression;
import org.springframework.stereotype.Service;

@Service("adbSchemaFactory")
public class AdbSchemaFactory extends DtmSchemaFactory {
    @Override
    protected DtmConvention createDtmConvention(Datamart datamart, Expression expression) {
        return new AdbDtmConvention(datamart, expression);
    }
}
