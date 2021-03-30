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
package io.arenadata.dtm.calcite.adqm.extension.dml;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;
import java.util.Objects;

public class SqlFinalTable extends SqlCall {

    private SqlNode tableRef;
    private final SqlOperator finalTableOperator;

    public SqlFinalTable(SqlParserPos pos, SqlNode tableRef) {
        super(pos);
        this.finalTableOperator = new SqlFinalOperator();
        this.tableRef = (SqlNode) Objects.requireNonNull(tableRef);
    }

    @Override
    public SqlOperator getOperator() {
        return finalTableOperator;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(this.tableRef);
    }

    public void setOperand(int i, SqlNode operand) {
        switch(i) {
            case 0:
                this.tableRef = (SqlNode)Objects.requireNonNull(operand);
                break;
            default:
                throw new AssertionError(i);
        }
    }

    public SqlNode getTableRef() {
        return tableRef;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        this.finalTableOperator.unparse(writer, this, 0, rightPrec);
    }

    @Override
    public SqlNode clone(SqlParserPos pos) {
        return new SqlFinalTable(pos, this.tableRef);
    }
}