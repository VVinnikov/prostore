package io.arenadata.dtm.query.execution.plugin.adg.base.service.client.impl;

import io.arenadata.dtm.query.execution.plugin.adg.base.service.query.AdgTemplateParameterConverter;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AdgTemplateParameterConverterTest {

    private static final List<SqlNode> EXPECTED = Lists.newArrayList(
            SqlLiteral.createExactNumeric("-1577869199999999", SqlParserPos.ZERO),
            SqlLiteral.createExactNumeric("54000000000", SqlParserPos.ZERO),
            SqlLiteral.createExactNumeric("-18263", SqlParserPos.ZERO)
    );
    private final AdgTemplateParameterConverter converter = new AdgTemplateParameterConverter();

    @Test
    void convert() {
        List<SqlNode> actual = converter.convert(
                Lists.newArrayList(
                        SqlLiteral.createCharString("1920-01-01 15:00:00.000001", SqlParserPos.ZERO),
                        SqlLiteral.createCharString("15:00:00", SqlParserPos.ZERO),
                        SqlLiteral.createCharString("1920-01-01", SqlParserPos.ZERO)
                ),
                Lists.newArrayList(
                        SqlTypeName.TIMESTAMP,
                        SqlTypeName.TIME,
                        SqlTypeName.DATE
                )
        );
        assertEquals(EXPECTED, actual);
    }

}
