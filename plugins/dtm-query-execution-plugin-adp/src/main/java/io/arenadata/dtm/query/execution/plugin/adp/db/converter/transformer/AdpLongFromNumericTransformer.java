package io.arenadata.dtm.query.execution.plugin.adp.db.converter.transformer;

import io.arenadata.dtm.common.converter.transformer.impl.LongFromNumberTransformer;
import io.vertx.sqlclient.data.Numeric;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;

public class AdpLongFromNumericTransformer extends LongFromNumberTransformer {
    @Override
    public Collection<Class<?>> getTransformClasses() {
        return Arrays.asList(Long.class, Integer.class, BigInteger.class, Numeric.class);
    }
}
