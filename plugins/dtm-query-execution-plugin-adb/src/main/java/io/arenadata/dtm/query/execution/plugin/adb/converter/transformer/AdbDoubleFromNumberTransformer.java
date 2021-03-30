/*
 * Copyright © 2021 ProStore
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.arenadata.dtm.query.execution.plugin.adb.converter.transformer;

import io.arenadata.dtm.common.converter.transformer.impl.DoubleFromNumberTransformer;
import io.reactiverse.pgclient.data.Numeric;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;

public class AdbDoubleFromNumberTransformer extends DoubleFromNumberTransformer {
    @Override
    public Collection<Class<?>> getTransformClasses() {
        return Arrays.asList(Double.class,
                Float.class,
                Long.class,
                Integer.class,
                Numeric.class,
                BigDecimal.class);
    }
}
