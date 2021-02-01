package io.arenadata.dtm.query.calcite.core.extension.dml;

import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Litmus;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.reflect.FieldUtils;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.List;

@Getter
@Setter
public class LimitableSqlOrderBy extends SqlOrderBy {
    private static final SqlSpecialOperator OPERATOR = new LimitableSqlOrderBy.Operator() {
        @Override
        public SqlCall createCall(SqlLiteral functionQualifier,
                                  SqlParserPos pos, SqlNode... operands) {
            return new LimitableSqlOrderBy(pos, operands[0], (SqlNodeList) operands[1],
                    operands[2], operands[3], (SqlLiteral) operands[4]);
        }
    };
    public SqlNode query;
    public SqlNodeList orderList;
    public SqlNode offset;
    public SqlNode fetch;
    private SqlLiteral isLimited;
    private SqlNode[] operands;
    private SqlKind kind;
    private Long limit;

    public LimitableSqlOrderBy(SqlParserPos pos,
                               SqlNode query,
                               SqlNodeList orderList,
                               SqlNode offset,
                               SqlNode fetch,
                               SqlLiteral isLimited) {
        super(pos, query, orderList, offset, fetch);
        operands = new SqlNode[5];
        operands[0] = query;
        operands[1] = orderList;
        operands[2] = offset;
        operands[3] = fetch;
        operands[4] = isLimited;
        kind = SqlKind.ORDER_BY;
        this.isLimited = isLimited;
        this.orderList = orderList;
        this.offset = offset;
        this.query = query;
        this.fetch = fetch;

        if (isLimited()) {
            if (fetch instanceof SqlNumericLiteral) {
                val value = ((SqlNumericLiteral) fetch).getValueAs(BigDecimal.class);
                limit = value.longValue();

            }
        }
    }

    public BigDecimal getLimit(SqlNumericLiteral fetch) {
        return fetch.getValueAs(BigDecimal.class);
    }

    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(query,
                orderList,
                offset,
                fetch,
                SqlLiteral.createBoolean(isLimited(), SqlParserPos.ZERO));
    }

    private boolean isLimited() {
        return isLimited.booleanValue();
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public SqlKind getKind() {
        return kind;
    }

    @Override
    public SqlNode clone(SqlParserPos pos) {
        return new LimitableSqlOrderBy(
                pos,
                query,
                orderList,
                offset,
                fetch,
                isLimited
        );
    }

    private void setOperand(Object operand, String query) throws IllegalAccessException {
        writeField(this, query, operand);
    }

    private void writeField(Object target, String fieldName, Object value) throws IllegalAccessException {
        Validate.notNull(target, "target object must not be null");
        Class<?> cls = target.getClass();
        Field field = FieldUtils.getField(cls, fieldName, true);
        Validate.isTrue(field != null, "Cannot locate declared field %s.%s", cls.getName(), fieldName);
        FieldUtils.writeField(field, target, value, true);
    }

    @Override
    public void setOperand(int i, SqlNode operand) {
        operands[i] = operand;
        switch (i) {
            case 0:
                query = operand;
                break;
            case 1:
                orderList = (SqlNodeList) operand;
                break;
            case 2:
                offset = operand;
                break;
            case 3:
                fetch = operand;
                break;
            case 4:
                isLimited = (SqlLiteral) operand;
                break;
        }
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
            if (!orderBy.orderList.equalsDeep(SqlNodeList.EMPTY, Litmus.IGNORE)) {
                writer.sep(getName());
                writer.list(SqlWriter.FrameTypeEnum.ORDER_BY_LIST, SqlWriter.COMMA,
                        orderBy.orderList);
            }
            if (orderBy.offset != null) {
                final SqlWriter.Frame frame2 =
                        writer.startList(SqlWriter.FrameTypeEnum.OFFSET);
                writer.newlineAndIndent();
                writer.keyword("OFFSET");
                orderBy.offset.unparse(writer, -1, -1);
                writer.keyword("ROWS");
                writer.endList(frame2);
            }

            if (orderBy.fetch != null) {
                final SqlWriter.Frame frame3 =
                        writer.startList(SqlWriter.FrameTypeEnum.FETCH);
                if (orderBy.isLimited()) {
                    writer.newlineAndIndent();
                    writer.keyword("LIMIT");
                    orderBy.fetch.unparse(writer, -1, -1);
                    writer.endList(frame3);
                } else {
                    writer.newlineAndIndent();
                    writer.keyword("FETCH");
                    writer.keyword("NEXT");
                    orderBy.fetch.unparse(writer, -1, -1);
                    writer.keyword("ROWS");
                    writer.keyword("ONLY");
                    writer.endList(frame3);
                }
            }
            writer.endList(frame);
        }
    }
}

