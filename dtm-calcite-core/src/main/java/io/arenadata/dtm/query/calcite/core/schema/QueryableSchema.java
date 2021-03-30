package io.arenadata.dtm.query.calcite.core.schema;

import io.arenadata.dtm.query.calcite.core.schema.dialect.DtmConvention;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.calcite.schema.impl.AbstractSchema;

@Data
@EqualsAndHashCode(callSuper = true)
public class QueryableSchema extends AbstractSchema {

    private final DtmConvention convention;

    public QueryableSchema(DtmConvention convention) {
        this.convention = convention;
    }
}
