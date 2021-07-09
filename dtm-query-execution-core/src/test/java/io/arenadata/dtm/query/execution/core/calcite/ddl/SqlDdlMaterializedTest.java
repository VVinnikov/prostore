package io.arenadata.dtm.query.execution.core.calcite.ddl;

import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.extension.ddl.DistributedOperator;
import io.arenadata.dtm.query.calcite.core.extension.ddl.SqlCreateMaterializedView;
import io.arenadata.dtm.query.calcite.core.extension.dml.SqlSelectExt;
import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.execution.core.calcite.configuration.CalciteConfiguration;
import io.arenadata.dtm.query.execution.core.calcite.service.CoreCalciteDefinitionService;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlKeyConstraint;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

public class SqlDdlMaterializedTest {
    private static final String SCHEMA_MAT = "matviewmart";
    private static final String VIEW_MAT_TABLE_NAME = "test";

    private final SqlDialect sqlDialect = new SqlDialect(SqlDialect.EMPTY_CONTEXT);
    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final DefinitionService<SqlNode> definitionService =
            new CoreCalciteDefinitionService(calciteConfiguration.configEddlParser(
                    calciteCoreConfiguration.eddlParserImplFactory()));

    @Test
    void shouldBeValidWithAllParamsWithSchema() {
        // arrange
        String query = "CREATE MATERIALIZED VIEW matviewmart.test (acc_id bigint, acc_name varchar(1), PRIMARY KEY (acc_id))\n" +
                "DISTRIBUTED BY (acc_id)\n" +
                "DATASOURCE_TYPE (ADG,ADQM)\n" +
                "AS select * from tblmart.a\n" +
                "DATASOURCE_TYPE = 'ADB'";

        // act
        SqlNode sqlNode = definitionService.processingQuery(query);

        // assert
        assertSame(SqlCreateMaterializedView.class, sqlNode.getClass());
        SqlCreateMaterializedView sqlCreateMaterializedView = (SqlCreateMaterializedView) sqlNode;

        assertEquals(SCHEMA_MAT, sqlCreateMaterializedView.getName().names.get(0));
        assertEquals(VIEW_MAT_TABLE_NAME, sqlCreateMaterializedView.getName().names.get(1));

        MatcherAssert.assertThat(sqlCreateMaterializedView.getDestination(), containsInAnyOrder(
                equalTo(SourceType.ADG),
                equalTo(SourceType.ADQM)
        ));

        SqlNodeList columnList = sqlCreateMaterializedView.getColumnList();
        assertEquals(3, columnList.size());
        SqlColumnDeclaration firstColumn = (SqlColumnDeclaration) columnList.get(0);
        assertEquals("acc_id", ((SqlIdentifier) firstColumn.getOperandList().get(0)).getSimple());
        assertEquals("BIGINT", ((SqlDataTypeSpec) firstColumn.getOperandList().get(1)).getTypeName().getSimple());

        SqlColumnDeclaration secondColumn = (SqlColumnDeclaration) columnList.get(1);
        assertEquals("acc_name", ((SqlIdentifier) secondColumn.getOperandList().get(0)).getSimple());
        assertEquals("VARCHAR", ((SqlDataTypeSpec) secondColumn.getOperandList().get(1)).getTypeName().getSimple());
        assertEquals(1, ((SqlBasicTypeNameSpec) ((SqlDataTypeSpec) secondColumn.getOperandList().get(1)).getTypeNameSpec()).getPrecision());

        SqlKeyConstraint thirdColumn = (SqlKeyConstraint) columnList.get(2);
        assertEquals("acc_id", ((SqlIdentifier) ((SqlNodeList) thirdColumn.getOperandList().get(1)).get(0)).getSimple());

        DistributedOperator distributedByOperator = sqlCreateMaterializedView.getDistributedBy();
        SqlNodeList distributedBy = distributedByOperator.getDistributedBy();
        assertEquals(1, distributedBy.size());
        SqlIdentifier distributedByItem = (SqlIdentifier) distributedBy.get(0);
        assertEquals("acc_id", distributedByItem.names.get(0));

        SqlSelectExt selectQuery = (SqlSelectExt) sqlCreateMaterializedView.getQuery();
        SqlCharStringLiteral datasourceType = selectQuery.getDatasourceType();
        assertEquals("ADB", datasourceType.getNlsString().getValue());

        String expectedParsedQuery = "SELECT *\n" +
                "FROM tblmart.a";
        assertThat(expectedParsedQuery).isEqualToNormalizingNewlines(selectQuery.toSqlString(sqlDialect).toString());
        String expected = "CREATE MATERIALIZED VIEW matviewmart.test (acc_id BIGINT, acc_name VARCHAR(1), PRIMARY KEY (acc_id)) DISTRIBUTED BY (acc_id) AS\n" +
                "SELECT *\nFROM tblmart.a";
        assertThat(expected).isEqualToNormalizingNewlines(sqlNode.toSqlString(sqlDialect).toString());
    }

    @Test
    void shouldBeParsedMinimally() {
        // arrange
        String query = "CREATE MATERIALIZED VIEW test\n" +
                "AS select * from tblmart.a\n";

        // act
        SqlNode sqlNode = definitionService.processingQuery(query);

        // assert
        assertSame(SqlCreateMaterializedView.class, sqlNode.getClass());
        SqlCreateMaterializedView sqlCreateMaterializedView = (SqlCreateMaterializedView) sqlNode;

        assertEquals(VIEW_MAT_TABLE_NAME, sqlCreateMaterializedView.getName().names.get(0));

        assertNull(sqlCreateMaterializedView.getDestination());
        assertNull(sqlCreateMaterializedView.getDistributedBy().getDistributedBy());
        assertNull(sqlCreateMaterializedView.getColumnList());

        SqlSelectExt selectQuery = (SqlSelectExt) sqlCreateMaterializedView.getQuery();
        assertNull(selectQuery.getDatasourceType());

        String expectedParsedQuery = "SELECT *\n" +
                "FROM tblmart.a";
        assertThat(expectedParsedQuery).isEqualToNormalizingNewlines(selectQuery.toSqlString(sqlDialect).toString());
        String expected = "CREATE MATERIALIZED VIEW test AS\n" +
                "SELECT *\nFROM tblmart.a";
        assertThat(expected).isEqualToNormalizingNewlines(sqlNode.toSqlString(sqlDialect).toString());
    }
}
