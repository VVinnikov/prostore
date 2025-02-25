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
package io.arenadata.dtm.query.calcite.core.rel2sql;

import io.arenadata.dtm.query.calcite.core.visitors.SqlDollarReplacementShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;

public class DtmRelToSqlConverter {
    private final SqlDollarReplacementShuttle sqlDollarReplacementShuttle = new SqlDollarReplacementShuttle();
    private final SqlDialect sqlDialect;
    private final boolean allowStarInProject;

    public DtmRelToSqlConverter(SqlDialect sqlDialect, boolean allowStarInProject) {
        this.sqlDialect = sqlDialect;
        this.allowStarInProject = allowStarInProject;
    }

    public DtmRelToSqlConverter(SqlDialect sqlDialect) {
        this.sqlDialect = sqlDialect;
        this.allowStarInProject = true;
    }

    public SqlNode convert(RelNode relNode) {
        SqlNode convertedNode = new NullNotCastableRelToSqlConverter(sqlDialect, allowStarInProject)
                .visitChild(0, relNode)
                .asStatement();

        return convertedNode.accept(sqlDollarReplacementShuttle);
    }
}
