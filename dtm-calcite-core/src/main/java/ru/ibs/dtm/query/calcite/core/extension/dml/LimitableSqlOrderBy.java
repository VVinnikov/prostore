package ru.ibs.dtm.query.calcite.core.extension.dml;

import lombok.Getter;
import lombok.val;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.math.BigDecimal;

@Getter
public class LimitableSqlOrderBy extends SqlOrderBy {
    private static final SqlSpecialOperator OPERATOR = new LimitableSqlOrderBy.Operator() {
        @Override
        public SqlCall createCall(SqlLiteral functionQualifier,
                                  SqlParserPos pos, SqlNode... operands) {
            return new SqlOrderBy(pos, operands[0], (SqlNodeList) operands[1],
                    operands[2], operands[3]);
        }
    };
    private final boolean isLimited;
    private long limit;

    public LimitableSqlOrderBy(SqlParserPos pos,
                               SqlNode query,
                               SqlNodeList orderList,
                               SqlNode offset,
                               SqlNode fetch,
                               boolean isLimited) {
        super(pos, query, orderList, offset, fetch);
        this.isLimited = isLimited;
        if (isLimited) {
            if (fetch instanceof SqlNumericLiteral) {
                val value = ((SqlNumericLiteral) fetch).getValueAs(BigDecimal.class);
                limit = value.longValue();
            }
        }
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }


    /**
     * Definition of {@code ORDER BY} operator.
     */
    private static class Operator extends SqlSpecialOperator {
        private Operator() {
            // NOTE:  make precedence lower then SELECT to avoid extra parens
            super("ORDER BY", SqlKind.ORDER_BY, 0);
        }

        public SqlSyntax getSyntax() {
            return SqlSyntax.POSTFIX;
        }

        public void unparse(
                SqlWriter writer,
                SqlCall call,
                int leftPrec,
                int rightPrec) {
            LimitableSqlOrderBy orderBy = (LimitableSqlOrderBy) call;
            final SqlWriter.Frame frame =
                    writer.startList(SqlWriter.FrameTypeEnum.ORDER_BY);
            orderBy.query.unparse(writer, getLeftPrec(), getRightPrec());
            if (orderBy.orderList != SqlNodeList.EMPTY) {
                writer.sep(getName());
                writer.list(SqlWriter.FrameTypeEnum.ORDER_BY_LIST, SqlWriter.COMMA,
                        orderBy.orderList);
            }
            if (orderBy.offset != null) {
                final SqlWriter.Frame frame2 =
                        writer.startList(SqlWriter.FrameTypeEnum.OFFSET);
                writer.newlineAndIndent();
                writer.keyword("OFFSET");
                orderBy.offset.unparse(writer, - 1, - 1);
                writer.keyword("ROWS");
                writer.endList(frame2);
            }

            if (orderBy.fetch != null) {
                final SqlWriter.Frame frame3 =
                        writer.startList(SqlWriter.FrameTypeEnum.FETCH);
                if (orderBy.isLimited) {
                    writer.newlineAndIndent();
                    writer.keyword("LIMIT");
                    orderBy.fetch.unparse(writer, - 1, - 1);
                    writer.endList(frame3);
                } else {
                    writer.newlineAndIndent();
                    writer.keyword("FETCH");
                    writer.keyword("NEXT");
                    orderBy.fetch.unparse(writer, - 1, - 1);
                    writer.keyword("ROWS");
                    writer.keyword("ONLY");
                    writer.endList(frame3);
                }
            }
            writer.endList(frame);
        }
    }
}
