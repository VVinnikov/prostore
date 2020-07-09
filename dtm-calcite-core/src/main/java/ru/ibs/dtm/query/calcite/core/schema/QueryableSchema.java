package ru.ibs.dtm.query.calcite.core.schema;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.calcite.schema.impl.AbstractSchema;
import ru.ibs.dtm.query.calcite.core.schema.dialect.DtmConvention;

@Data
@EqualsAndHashCode(callSuper = true)
public class QueryableSchema extends AbstractSchema {

    private final DtmConvention convention;

    public QueryableSchema(DtmConvention convention) {
        this.convention = convention;
    }
}
