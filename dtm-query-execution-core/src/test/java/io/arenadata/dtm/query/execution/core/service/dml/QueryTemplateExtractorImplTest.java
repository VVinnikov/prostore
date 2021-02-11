package io.arenadata.dtm.query.execution.core.service.dml;

import io.arenadata.dtm.common.reader.QueryTemplateResult;
import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.dto.EnrichmentTemplateRequest;
import io.arenadata.dtm.query.calcite.core.service.impl.AbstractQueryTemplateExtractor;
import io.arenadata.dtm.query.calcite.core.service.impl.CalciteDefinitionService;
import io.arenadata.dtm.query.execution.core.service.query.impl.CoreQueryTemplateExtractor;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class QueryTemplateExtractorImplTest {
    public static final String EXPECTED_SQL = "SELECT *\n" +
            "FROM \"tbl1\"\n" +
            "WHERE \"x\" = 1 AND \"x\" > 2 AND \"x\" < 3 AND \"x\" <= 4 AND \"x\" >= 5 AND \"x\" <> 6 AND \"z\" = '8'";

    public static final String EXPECTED_FULL_SQL = "SELECT *\n" +
            "FROM \"tbl1\"\n" +
            "WHERE \"x\" = 1" +
            " AND 2 = 2" +
            " AND 3 < \"x\"" +
            " AND \"z\" = \"x\"";

    public static final String EXPECTED_FULL_TEMPLATE = "SELECT *\n" +
            "FROM \"tbl1\"\n" +
            "WHERE \"x\" = ? AND 2 = 2 AND ? < \"x\" AND \"z\" = \"x\"";

    public static final String EXPECTED_SQL_WITH_SYS_COLUMNS = "SELECT *\n" +
            "FROM \"tbl1\"\n" +
            "WHERE \"x\" = 1 AND \"x\" > 2 AND \"x\" < 3 AND \"x\" <= 4 AND \"x\" >= 5 AND \"x\" <> 6 AND \"z\" = '8'" +
            " AND \"sys_from\" = 1";
    public static final String EXPECTED_BETWEEN_SQL = "SELECT *\n" +
            "FROM \"tbl1\"\n" +
            "WHERE \"x\" BETWEEN ASYMMETRIC 1 AND 5 AND \"z\" = \"x\"";
    public static final String EXPECTED_SUB_SQL = "SELECT *\n" +
            "FROM (SELECT *\n" +
            "FROM \"tbl1\" AS \"t2\"\n" +
            "WHERE \"t2\".\"x\" = 1 AND \"t2\".\"x\" > 2 AND \"t2\".\"x\" < 3 AND \"t2\".\"x\" <= 4 AND \"t2\".\"x\" >= 5 AND \"t2\".\"x\" <> 6 AND \"t2\".\"z\" = '8') AS \"t\"\n" +
            "WHERE \"x\" = 1 AND \"x\" > 2 AND \"x\" < 3 AND \"x\" <= 4 AND \"x\" >= 5 AND \"x\" <> 6 AND \"z\" = '8'";
    private static final String EXPECTED_TEMPLATE = "SELECT *\n" +
            "FROM \"tbl1\"\n" +
            "WHERE \"x\" = ? AND \"x\" > ? AND \"x\" < ? AND \"x\" <= ? AND \"x\" >= ? AND \"x\" <> ? AND \"z\" = ?";
    private static final String EXPECTED_SUB_TEMPLATE = "SELECT *\n" +
            "FROM (SELECT *\n" +
            "FROM \"tbl1\" AS \"t2\"\n" +
            "WHERE \"t2\".\"x\" = ? AND \"t2\".\"x\" > ? AND \"t2\".\"x\" < ? AND \"t2\".\"x\" <= ? AND \"t2\".\"x\" >= ? AND \"t2\".\"x\" <> ? AND \"t2\".\"z\" = ?) AS \"t\"\n" +
            "WHERE \"x\" = ? AND \"x\" > ? AND \"x\" < ? AND \"x\" <= ? AND \"x\" >= ? AND \"x\" <> ? AND \"z\" = ?";
    private static final String EXPECTED_TEMPLATE_WITH_SYS_COLUMNS = "SELECT *\n" +
            "FROM \"tbl1\"\n" +
            "WHERE \"x\" = ? AND \"x\" > ? AND \"x\" < ? AND \"x\" <= ? AND \"x\" >= ? AND \"x\" <> ? AND \"z\" = ?" +
            " AND \"sys_from\" = 1";
    private static final String EXPECTED_SQL_WITH_BETWEEN_TEMPLATE = "SELECT *\n" +
            "FROM \"tbl1\"\n" +
            "WHERE \"x\" BETWEEN ASYMMETRIC ? AND ? AND \"z\" = \"x\"";

    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private AbstractQueryTemplateExtractor extractor;

    @BeforeEach
    void setUp() {
        SqlParser.Config parserConfig = SqlParser.configBuilder()
                .setParserFactory(calciteCoreConfiguration.eddlParserImplFactory())
                .setConformance(SqlConformanceEnum.DEFAULT)
                .setLex(Lex.MYSQL)
                .setCaseSensitive(false)
                .setUnquotedCasing(Casing.TO_LOWER)
                .setQuotedCasing(Casing.TO_LOWER)
                .setQuoting(Quoting.DOUBLE_QUOTE)
                .build();
        CalciteDefinitionService definitionService = new CalciteDefinitionService(parserConfig) {
        };
        extractor = new CoreQueryTemplateExtractor(definitionService, SqlDialect.CALCITE);
    }

    @Test
    void extract() {
        assertExtract(EXPECTED_SQL, EXPECTED_TEMPLATE, 7);
    }

    @Test
    void extractSubSql() {
        assertExtract(EXPECTED_SUB_SQL, EXPECTED_SUB_TEMPLATE, 14);
    }

    @Test
    void extractWithSysColumn() {
        assertExtract(EXPECTED_SQL_WITH_SYS_COLUMNS,
                EXPECTED_TEMPLATE_WITH_SYS_COLUMNS,
                7,
                Collections.singletonList("sys_from"));
    }

    @Test
    void extractWithFull() {
        assertExtract(EXPECTED_FULL_SQL, EXPECTED_FULL_TEMPLATE, 2);
    }

    @Test
    void extractWithBetween() {
        assertExtract(EXPECTED_BETWEEN_SQL, EXPECTED_SQL_WITH_BETWEEN_TEMPLATE, 2);
    }

    private void assertExtract(String sql, String template, int paramsSize) {
        assertExtract(sql, template, paramsSize, Collections.emptyList());
    }

    private void assertExtract(String sql, String template, int paramsSize, List<String> excludeColumns) {
        QueryTemplateResult templateResult = excludeColumns.isEmpty() ?
                extractor.extract(sql) : extractor.extract(sql, excludeColumns);
        assertEquals(paramsSize, templateResult.getParams().size());
        assertEquals(template, templateResult.getTemplate());
        SqlNode enrichTemplate = extractor.enrichTemplate(
                new EnrichmentTemplateRequest(templateResult.getTemplateNode(), templateResult.getParams())
        );
        System.out.println(enrichTemplate.toString());
        assertEquals(sql, enrichTemplate.toSqlString(SqlDialect.CALCITE).toString());
    }
}