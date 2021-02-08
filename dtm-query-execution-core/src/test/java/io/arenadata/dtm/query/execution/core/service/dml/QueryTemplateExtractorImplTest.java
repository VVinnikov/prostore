package io.arenadata.dtm.query.execution.core.service.dml;

import io.arenadata.dtm.common.reader.QueryTemplateResult;
import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.node.SqlSelectTree;
import io.arenadata.dtm.query.calcite.core.service.impl.AbstractQueryTemplateExtractor;
import io.arenadata.dtm.query.calcite.core.service.impl.CalciteDefinitionService;
import io.arenadata.dtm.query.execution.core.service.query.impl.CoreQueryTemplateExtractor;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

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
    private static final String EXPECTED_TEMPLATE = "SELECT *\n" +
            "FROM \"tbl1\"\n" +
            "WHERE \"x\" = ? AND \"x\" > ? AND \"x\" < ? AND \"x\" <= ? AND \"x\" >= ? AND \"x\" <> ? AND \"z\" = ?";
    private static final String EXPECTED_TEMPLATE_WITH_SYS_COLUMNS = "SELECT *\n" +
            "FROM \"tbl1\"\n" +
            "WHERE \"x\" = ? AND \"x\" > ? AND \"x\" < ? AND \"x\" <= ? AND \"x\" >= ? AND \"x\" <> ? AND \"z\" = ?" +
            " AND \"sys_from\" = 1";
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private AbstractQueryTemplateExtractor extractor;
    private CalciteDefinitionService definitionService;

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
        definitionService = new CalciteDefinitionService(parserConfig) {
        };
        extractor = new CoreQueryTemplateExtractor(definitionService, SqlDialect.CALCITE);
    }

    @Test
    void extract() {
        SqlSelectTree selectTree = new SqlSelectTree(definitionService.processingQuery(EXPECTED_SQL));
        SqlSelectTree copy = selectTree.copy();
        QueryTemplateResult templateResult = extractor.extract(EXPECTED_SQL);
        assertEquals(EXPECTED_TEMPLATE, templateResult.getTemplate());
        assertEquals(7, templateResult.getParams().size());
    }

    @Test
    void extractWithSysColumn() {
        QueryTemplateResult templateResult = extractor.extract(EXPECTED_SQL_WITH_SYS_COLUMNS, Collections.singletonList("sys_from"));
        assertEquals(EXPECTED_TEMPLATE_WITH_SYS_COLUMNS, templateResult.getTemplate());
        assertEquals(7, templateResult.getParams().size());
    }

    @Test
    void extractWithFull() {
        QueryTemplateResult templateResult = extractor.extract(EXPECTED_FULL_SQL);
        assertEquals(2, templateResult.getParams().size());
        assertEquals(EXPECTED_FULL_TEMPLATE, templateResult.getTemplate());
    }


    @Test
    void enrichTemplate() {
        //FIXME
        QueryTemplateResult templateResult = extractor.extract(EXPECTED_SQL);
        //SqlNode sqlNode = extractor.enrichTemplate(new EnrichmentTemplateRequest(templateResult.getTemplate(),
        //    null,
        //    templateResult.getParams()));
        //String enrichmentSql = sqlNode.toSqlString(SqlDialect.CALCITE).toString();
        //assertEquals(EXPECTED_SQL, enrichmentSql);
    }
}
