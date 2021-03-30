package io.arenadata.dtm.query.execution.core.utils;

import io.arenadata.dtm.common.reader.InformationSchemaView;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MetaDataQueryPreparerTest {

    @Test
    void findInformationSchemaViewsSingle() {
        String sql = "select * from INFORMATION_SCHEMA.TABLE_CONSTRAINTS";

        assertEquals(InformationSchemaView.TABLE_CONSTRAINTS,
                MetaDataQueryPreparer.findInformationSchemaViews(sql).get(0).getView());
    }

    @Test
    void findInformationSchemaViewsSingleWithQuotesInTable() {
        String sql = "select * from INFORMATION_SCHEMA.\"TABLE_CONSTRAINTS\"";

        assertEquals(InformationSchemaView.TABLE_CONSTRAINTS,
                MetaDataQueryPreparer.findInformationSchemaViews(sql).get(0).getView());
    }

    @Test
    void findInformationSchemaViewsSingleWithQuotesInSchema() {
        String sql = "select * from \"INFORMATION_SCHEMA\".TABLE_CONSTRAINTS";

        assertEquals(InformationSchemaView.TABLE_CONSTRAINTS,
                MetaDataQueryPreparer.findInformationSchemaViews(sql).get(0).getView());
    }

    @Test
    void findInformationSchemaViewsSingleWithQuotesInTableAndSchema() {
        String sql = "select * from \"INFORMATION_SCHEMA\".\"TABLE_CONSTRAINTS\"";

        assertEquals(InformationSchemaView.TABLE_CONSTRAINTS,
                MetaDataQueryPreparer.findInformationSchemaViews(sql).get(0).getView());
    }

    @Test
    void findInformationSchemaViewsSingleCaseInsensitive() {
        String sql = "select * from infORMATION_SCheMA.SCheMaTa";

        assertEquals(InformationSchemaView.SCHEMATA,
                MetaDataQueryPreparer.findInformationSchemaViews(sql).get(0).getView());
    }

    @Test
    void findInformationSchemaViewsMulti() {
        String sql = "select (select max(deltas.load_id) from information_schema.\"deltas\" deltas) as load_id, tables.*" +
                " from INFORMATION_SCHEMA.TABLES tables" +
                " where exists(select 1 from \"INFORMATION_SCHEMA\".schemata schemata where schemata.schema_name = tables.table_schema)";

        assertEquals(2, MetaDataQueryPreparer.findInformationSchemaViews(sql).size());
    }

    @Test
    void modifySingle() {
        String sql = "select * from infORMATION_SCheMA.SCheMaTa";
        String expectedSql = "select * from logic_schema_datamarts";
        assertEquals(expectedSql,
                MetaDataQueryPreparer.modify(sql).toLowerCase());
    }

    @Test
    void modifyMulti() {
        String sql = "select (select max(deltas.load_id) from information_schema.\"deltas\" deltas) as load_id, tables.*" +
                " from INFORMATION_SCHEMA.TABLES tables" +
                " where exists(select 1 from \"INFORMATION_SCHEMA\".schemata schemata where schemata.schema_name = tables.table_schema)";
        String expectedSql = "select (select max(deltas.load_id) from information_schema.\"deltas\" deltas) as load_id, tables.* " +
                "from logic_schema_entities tables where exists(select 1 from logic_schema_datamarts schemata " +
                "where schemata.schema_name = tables.table_schema)";
        assertEquals(expectedSql,
                MetaDataQueryPreparer.modify(sql).toLowerCase());
    }
}
