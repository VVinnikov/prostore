package io.arenadata.dtm.calcite.adqm.extension.dml;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
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
