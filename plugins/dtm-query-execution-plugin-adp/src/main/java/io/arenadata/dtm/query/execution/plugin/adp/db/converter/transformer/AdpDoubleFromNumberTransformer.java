package io.arenadata.dtm.query.execution.plugin.adp.db.converter.transformer;

import io.arenadata.dtm.common.converter.transformer.impl.DoubleFromNumberTransformer;
import io.vertx.sqlclient.data.Numeric;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;

public class AdpDoubleFromNumberTransformer extends DoubleFromNumberTransformer {
    @Override
    public Collection<Class<?>> getTransformClasses() {
        return Arrays.asList(Double.class, Float.class, Long.class, Integer.class, Numeric.class, BigDecimal.class);
    }
}
