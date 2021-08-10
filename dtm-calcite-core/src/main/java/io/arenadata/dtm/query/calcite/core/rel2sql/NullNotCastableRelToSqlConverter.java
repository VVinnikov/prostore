package io.arenadata.dtm.query.calcite.core.rel2sql;

import lombok.val;
import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.adapter.enumerable.EnumerableSort;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class NullNotCastableRelToSqlConverter extends RelToSqlConverter {
    private static final String dollarReplacement = "__";

    public NullNotCastableRelToSqlConverter(SqlDialect dialect) {
        super(dialect);
    }

    @Override
    public Result visit(Project e) {
        e.getVariablesSet();
        Result x = visitChild(0, e.getInput());
        parseCorrelTable(e, x);
        if (isStar(e.getChildExps(), e.getInput().getRowType(), e.getRowType())) {
            return x;
        }
        final Builder builder = x.builder(e, Clause.SELECT);
        final List<SqlNode> selectList = new ArrayList<>();
        for (RexNode ref : e.getChildExps()) {
            SqlNode sqlExpr = builder.context.toSql(null, ref);
            addSelect(selectList, sqlExpr, e.getRowType());
        }

        builder.setSelect(new SqlNodeList(selectList, POS));
        return builder.result();
    }

    protected void parseCorrelTable(RelNode relNode, Result x) {
        for (CorrelationId id : relNode.getVariablesSet()) {
            correlTableMap.put(id, x.qualifiedContext());
        }
    }

    @Override
    public Result visit(RelNode e) {
        if (e instanceof EnumerableLimit) {
            e.getVariablesSet();
            RelNode input = e.getInput(0);
            if (input instanceof EnumerableSort) {
                val node = (EnumerableSort) input;
                val sort = EnumerableSort.create(node.getInput(),
                        node.getCollation(),
                        ((EnumerableLimit) e).offset,
                        ((EnumerableLimit) e).fetch);
                return visitChild(0, sort);
            } else {
                Result x = visitChild(0, input);
                parseCorrelTable(e, x);
                final Builder builder = x.builder(input, Clause.SELECT, Clause.FETCH, Clause.OFFSET);
                setFetch(builder, e);
                setOffset(builder, e);
                return builder.result();
            }
        } else {
            throw new AssertionError("Need to implement " + e.getClass().getName());
        }
    }

    @Override
    public void addSelect(List<SqlNode> selectList, SqlNode node, RelDataType rowType) {
        String name = rowType.getFieldNames().get(selectList.size());
        String alias = SqlValidatorUtil.getAlias(node, -1);
        SqlNode fixedSqlNode = replaceDollarIdentifier(node);
        if (alias == null || !alias.equals(name)) {
            name = name.replace("$", dollarReplacement);
            selectList.add(as(fixedSqlNode, name));
            return;
        }

        selectList.add(fixedSqlNode);
    }

    private SqlNode replaceDollarIdentifier(SqlNode node) {
        if (node == null || node.getClass() != SqlIdentifier.class) {
            return node;
        }

        List<String> fixedNames = ((SqlIdentifier) node).names.stream()
                .map(s -> s.replace("$", dollarReplacement))
                .collect(Collectors.toList());
        return new SqlIdentifier(fixedNames, node.getParserPosition());
    }

    private void setOffset(Builder builder, RelNode node) {
        RexNode offset = ((EnumerableLimit) node).offset;
        if (offset != null) {
            builder.setOffset(SqlLiteral.createExactNumeric(offset.toStringRaw(), POS));
        }
    }

    private void setFetch(Builder builder, RelNode node) {
        RexNode fetch = ((EnumerableLimit) node).fetch;
        if (fetch != null) {
            builder.setFetch(SqlLiteral.createExactNumeric(fetch.toStringRaw(), POS));
        }
    }

    @Override
    protected boolean isAnon() {
        return false;
    }
}
