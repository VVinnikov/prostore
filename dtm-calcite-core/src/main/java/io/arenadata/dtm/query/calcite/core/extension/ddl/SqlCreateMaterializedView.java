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
package io.arenadata.dtm.query.calcite.core.extension.ddl;

import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.calcite.core.extension.parser.ParseException;
import io.arenadata.dtm.query.calcite.core.util.SqlNodeUtil;
import lombok.Getter;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;
import java.util.Objects;
import java.util.Set;

@Getter
public class SqlCreateMaterializedView extends SqlCreate implements SqlLogicalCall {
    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator("CREATE MATERIALIZED VIEW",
                    SqlKind.CREATE_MATERIALIZED_VIEW);

    private final SqlIdentifier name;
    private final SqlNodeList columnList;
    private final SqlNode query;
    private final DistributedOperator distributedBy;
    private final Set<SourceType> destination;
    private final boolean isLogicalOnly;

    public SqlCreateMaterializedView(SqlParserPos pos,
                                     SqlIdentifier name,
                                     SqlNodeList columnList,
                                     SqlNodeList distributedBy,
                                     SqlNodeList destination,
                                     SqlNode query,
                                     boolean isLogicalOnly) throws ParseException {
        super(OPERATOR, pos, false, false);
        this.name = Objects.requireNonNull(name);
        this.columnList = columnList;
        this.query = SqlNodeUtil.checkViewQueryAndGet(Objects.requireNonNull(query));
        this.distributedBy = new DistributedOperator(pos, distributedBy);
        this.destination = SqlNodeUtil.extractSourceTypes(destination);
        this.isLogicalOnly = isLogicalOnly;
    }

    public SqlCreateMaterializedView(SqlParserPos pos,
                                     SqlIdentifier name,
                                     SqlNodeList columnList,
                                     DistributedOperator distributedBy,
                                     Set<SourceType> destination,
                                     SqlNode query,
                                     boolean isLogicalOnly) throws ParseException {
        super(OPERATOR, pos, false, false);
        this.name = Objects.requireNonNull(name);
        this.columnList = columnList;
        this.query = SqlNodeUtil.checkViewQueryAndGet(Objects.requireNonNull(query));
        this.distributedBy = distributedBy;
        this.destination = destination;
        this.isLogicalOnly = isLogicalOnly;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, columnList, query, distributedBy);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(this.getOperator().getName());
        name.unparse(writer, leftPrec, rightPrec);
        if (columnList != null) {
            SqlWriter.Frame frame = writer.startList("(", ")");
            for (SqlNode c : columnList) {
                writer.sep(",");
                c.unparse(writer, 0, 0);
            }
            writer.endList(frame);
        }
        if (distributedBy != null) {
            distributedBy.unparse(writer, 0, 0);
        }
        writer.keyword("AS");
        writer.newlineAndIndent();
        query.unparse(writer, 0, 0);
    }
}
