package io.arenadata.dtm.query.calcite.core.service.impl;

import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.dto.EnrichmentTemplateRequest;
import io.arenadata.dtm.common.reader.QueryTemplateResult;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class QueryTemplateExtractorImplTest {
    public static final String EXPECTED_SQL = "SELECT *\n" +
        "FROM \"tbl1\"\n" +
        "WHERE \"x\" = 1 AND \"x\" > 2 AND \"x\" < 3 AND \"x\" <= 4 AND \"x\" >= 5 AND \"x\" <> 6 AND \"z\" = '8'";
    public static final String EXPECTED_SQL_WITH_SYS_COLUMNS = "SELECT *\n" +
            "FROM \"tbl1\"\n" +
            "WHERE \"x\" = 1 AND \"x\" > 2 AND \"x\" < 3 AND \"x\" <= 4 AND \"x\" >= 5 AND \"x\" <> 6 AND \"z\" = '8'" +
            " AND \"sys_from\" = 1";
    private static final String EXPECTED_TEMPLATE = "SELECT *\n" +
        "FROM \"tbl1\"\n" +
        "WHERE \"x\" = ? AND \"x\" > ? AND \"x\" < ? AND \"x\" <= ? AND \"x\" >= ? AND \"x\" <> ? AND \"z\" = ?";
    private static final String EXPECTED_TEMPLATE_WITH_SYS_COLUMNS = "SELECT *\n" +
            "FROM \"tbl1\"\n" +
            "WHERE \"x\" = ? AND \"x\" > ? AND \"x\" < ? AND \"x\" <= ? AND \"x\" >= ? AND \"x\" <> ? AND \"z\" = ?" +
            " AND \"sys_from\" = 1";
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private QueryTemplateExtractorImpl extractor;

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
        extractor = new QueryTemplateExtractorImpl(new CalciteDefinitionService(parserConfig) {
        }, SqlDialect.CALCITE);
    }

    @Test
    void extract() {
        QueryTemplateResult templateResult = extractor.extract(EXPECTED_SQL);
        assertEquals(EXPECTED_TEMPLATE, templateResult.getTemplate());
        assertEquals(7, templateResult.getParams().size());
    }

    @Test
    void extractWithSysColumn() {
        QueryTemplateResult templateResult = extractor.extract(EXPECTED_SQL_WITH_SYS_COLUMNS);
        assertEquals(EXPECTED_TEMPLATE_WITH_SYS_COLUMNS, templateResult.getTemplate());
        assertEquals(7, templateResult.getParams().size());
    }

    @Test
    void enrichTemplate() {
        QueryTemplateResult templateResult = extractor.extract(EXPECTED_SQL);
        SqlNode sqlNode = extractor.enrichTemplate(new EnrichmentTemplateRequest(templateResult.getTemplate(), templateResult.getParams()));
        String enrichmentSql = sqlNode.toSqlString(SqlDialect.CALCITE).toString();
        assertEquals(EXPECTED_SQL, enrichmentSql);
    }
}
