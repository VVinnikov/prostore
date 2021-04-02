package io.arenadata.dtm.query.execution.plugin.adb.base.service.converter.transformer;

import io.arenadata.dtm.common.converter.transformer.impl.FloatFromNumberTransformer;
import io.reactiverse.pgclient.data.Numeric;

import java.util.Arrays;
import java.util.Collection;

public class AdbFloatFromNumberTransformer extends FloatFromNumberTransformer {
    @Override
    public Collection<Class<?>> getTransformClasses() {
        return Arrays.asList(Double.class, Float.class, Long.class, Integer.class, Numeric.class);
    }
}
