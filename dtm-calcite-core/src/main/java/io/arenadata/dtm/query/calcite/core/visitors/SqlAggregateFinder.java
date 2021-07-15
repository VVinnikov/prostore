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
package io.arenadata.dtm.query.calcite.core.visitors;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.util.SqlBasicVisitor;

public class SqlAggregateFinder extends SqlBasicVisitor<Object> {
    private boolean foundAggregate;

    @Override
    public Object visit(SqlCall call) {
        if (call.getOperator().isAggregator() || call.getKind() == SqlKind.OTHER_FUNCTION) {
            foundAggregate = true;
            return null;
        }

        return super.visit(call);
    }

    public boolean isFoundAggregate() {
        return foundAggregate;
    }
}
