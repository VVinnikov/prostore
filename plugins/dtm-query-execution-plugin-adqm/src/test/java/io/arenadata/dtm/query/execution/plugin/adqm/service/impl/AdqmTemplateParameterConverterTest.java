package io.arenadata.dtm.query.execution.plugin.adqm.service.impl;

import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AdqmTemplateParameterConverterTest {
    private static final List<SqlNode> EXPECTED = Lists.newArrayList(
            SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO),
            SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO),
            SqlLiteral.createExactNumeric("-1577869199999999", SqlParserPos.ZERO),
            SqlLiteral.createExactNumeric("54000000000", SqlParserPos.ZERO),
            SqlLiteral.createExactNumeric("-18263", SqlParserPos.ZERO)
    );
    private final AdqmTemplateParameterConverter converter = new AdqmTemplateParameterConverter();

    @Test
    void convert() {
        List<SqlNode> actual = converter.convert(
                Lists.newArrayList(
                        SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
                        SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
                        SqlLiteral.createCharString("1920-01-01 15:00:00.000001", SqlParserPos.ZERO),
                        SqlLiteral.createCharString("15:00:00", SqlParserPos.ZERO),
                        SqlLiteral.createCharString("1920-01-01", SqlParserPos.ZERO)
                ),
                Lists.newArrayList(
                        SqlTypeName.BOOLEAN,
                        SqlTypeName.BOOLEAN,
                        SqlTypeName.TIMESTAMP,
                        SqlTypeName.TIME,
                        SqlTypeName.DATE
                )
        );
        assertEquals(EXPECTED, actual);
    }
}
