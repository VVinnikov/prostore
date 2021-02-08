package io.arenadata.dtm.query.execution.core.service.dml;

import io.arenadata.dtm.common.dml.SelectCategory;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.framework.DtmCalciteFramework;
import io.arenadata.dtm.query.execution.core.configuration.calcite.CalciteConfiguration;
import io.arenadata.dtm.query.execution.core.service.dml.impl.SelectCategoryQualifierImpl;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import lombok.val;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SelectCategoryQualifierTest {

    private static final String SELECT_LEFT_JOIN = "SELECT *\n" +
            "FROM transactions t \n" +
            "  LEFT JOIN accounts a ON t.account_id = a.account_id\n" +
            "WHERE a.account_type = 'D'";
    private static final String SELECT_JOIN = "SELECT sum(t.amount)\n" +
            "FROM transactions t \n" +
            "  JOIN accounts a ON t.account_id = a.account_id\n" +
            "GROUP BY a.account_type";
    private static final String SELECT_SUBQUERY = "SELECT \n" +
            "  (SELECT sum(amount) FROM transactions t WHERE t.account_id = a.account_id)\n" +
            "FROM accounts a \n" +
            "WHERE a.account_type <> 'D' AND a.account_id = 5";
    private static final String SELECT_SUBQUERY_WHERE = "SELECT count(*)\n" +
            "FROM accounts a \n" +
            "WHERE EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id)";
    private static final String SELECT_SUBQUERY_WHERE_AND = "SELECT count(*)\n" +
            "FROM accounts a \n" +
            "WHERE EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id) AND account_id > 0";
    private static final String SELECT_GROUP_BY = "SELECT sum(t.amount)\n" +
            "FROM transactions t\n" +
            "GROUP BY t.transaction_date";
    private static final String SELECT_AGGREGATION = "SELECT count(*)\n" +
            "FROM transactions t";
    private static final String SELECT_GROUP_BY_HAVING = "SELECT account_id\n" +
            "FROM transactions t\n" +
            "GROUP BY account_id\n" +
            "HAVING count(*) > 1";
    private static final String SELECT_PRIMARY_KEY_EQUALS = "SELECT *\n" +
            "FROM transactions t\n" +
            "WHERE t.transaction_id = 1 AND amount > 0";
    private static final String SELECT_PRIMARY_KEY_IN = "SELECT *\n" +
            "FROM transactions t\n" +
            "WHERE t.transaction_id IN (1,2,3) AND amount < 0";
    private static final String SELECT_PRIMARY_KEY_BETWEEN = "SELECT *\n" +
            "FROM transactions t\n" +
            "WHERE (t.transaction_id > 1 AND t.transaction_id < 10) OR t.transaction_id BETWEEN 1001 AND 2000";
    private static final String SELECT_UNDEFINED = "SELECT *\n" +
            "FROM transactions t\n" +
            "WHERE amount < 0";
    private static final String SELECT_OR_UNDEFINED = "SELECT *\n" +
            "FROM transactions t\n" +
            "WHERE t.transaction_id = 1 OR amount < 0";
    private static final String DATAMART = "datamart";

    private SelectCategoryQualifier selectCategoryQualifier = new SelectCategoryQualifierImpl();
    private CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private SqlParser.Config parserConfig = calciteConfiguration.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory());
    private Planner planner;
    private List<Datamart> schema;

    @BeforeEach
    public void setup() {
        EntityField transactionId = EntityField.builder()
                .name("transaction_id")
                .primaryOrder(1)
                .build();
        Entity transactionsEntity = Entity.builder()
                .name("transactions")
                .fields(Collections.singletonList(transactionId))
                .build();

        schema = Collections.singletonList(Datamart.builder()
                .mnemonic(DATAMART)
                .entities(Collections.singletonList(transactionsEntity))
                .build());

        DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
        FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
        planner = DtmCalciteFramework.getPlanner(frameworkConfig);
    }

    @Test
    void testSelectUndefined() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_UNDEFINED);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertEquals(SelectCategory.UNDEFINED, category);
    }

    @Test
    void testSelectOrUndefined() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_OR_UNDEFINED);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertEquals(SelectCategory.UNDEFINED, category);
    }

    @Test
    void testSelectLeftJoin() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_LEFT_JOIN);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertEquals(SelectCategory.RELATIONAL, category);
    }

    @Test
    void testSelectJoin() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_JOIN);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertEquals(SelectCategory.RELATIONAL, category);
    }

    @Test
    void testSelectSubquery() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_SUBQUERY);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertEquals(SelectCategory.RELATIONAL, category);
    }

    @Test
    void testSelectSubqueryWhere() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_SUBQUERY_WHERE);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertEquals(SelectCategory.RELATIONAL, category);
    }

    @Test
    void testSelectSubqueryWhereAnd() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_SUBQUERY_WHERE_AND);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertEquals(SelectCategory.RELATIONAL, category);
    }

    @Test
    void testSelectGroupBy() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_GROUP_BY);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertEquals(SelectCategory.ANALYTICAL, category);
    }

    @Test
    void testSelectAggregation() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_AGGREGATION);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertEquals(SelectCategory.ANALYTICAL, category);
    }

    @Test
    void testSelectGroupByHaving() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_GROUP_BY_HAVING);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertEquals(SelectCategory.ANALYTICAL, category);
    }

    @Test
    void testSelectPrimaryKeyEquals() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_PRIMARY_KEY_EQUALS);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertEquals(SelectCategory.DICTIONARY, category);
    }

    @Test
    void testSelectPrimaryKeyIn() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_PRIMARY_KEY_IN);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertEquals(SelectCategory.DICTIONARY, category);
    }

    @Test
    void testSelectPrimaryKeyBetween() throws SqlParseException {
        SqlNode sqlNode = planner.parse(SELECT_PRIMARY_KEY_BETWEEN);
        val category = selectCategoryQualifier.qualify(schema, sqlNode);
        assertEquals(SelectCategory.DICTIONARY, category);
    }
}
