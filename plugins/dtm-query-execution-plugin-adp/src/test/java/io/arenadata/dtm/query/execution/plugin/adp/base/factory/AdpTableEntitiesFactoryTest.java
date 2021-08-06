package io.arenadata.dtm.query.execution.plugin.adp.base.factory;

import io.arenadata.dtm.query.execution.plugin.adp.base.dto.metadata.AdpTableColumn;
import io.arenadata.dtm.query.execution.plugin.adp.base.dto.metadata.AdpTableEntity;
import io.arenadata.dtm.query.execution.plugin.adp.base.factory.metadata.AdpTableEntitiesFactory;
import lombok.val;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.arenadata.dtm.query.execution.plugin.adp.util.TestUtils.adpTableColumnsFromEntityFields;
import static io.arenadata.dtm.query.execution.plugin.adp.util.TestUtils.createAllTypesTable;
import static org.assertj.core.api.Assertions.assertThat;

class AdpTableEntitiesFactoryTest {
    private final AdpTableEntitiesFactory entitiesFactory = new AdpTableEntitiesFactory();

    @Test
    void createSuccess() {
        val allTypesTable = createAllTypesTable();
        List<AdpTableColumn> columns = adpTableColumnsFromEntityFields(allTypesTable.getFields());
        columns.add(new AdpTableColumn("sys_from", "int8", true));
        columns.add(new AdpTableColumn("sys_to", "int8", true));
        columns.add(new AdpTableColumn("sys_op", "int4", true));
        AdpTableEntity expectedActual = new AdpTableEntity(allTypesTable.getName() + "_actual",
                allTypesTable.getSchema(),
                columns,
                Arrays.asList("id", "sys_from"));
        AdpTableEntity expectedHistory = new AdpTableEntity(allTypesTable.getName() + "_history",
                allTypesTable.getSchema(),
                columns,
                Arrays.asList("id", "sys_from"));
        AdpTableEntity expectedStaging = new AdpTableEntity(allTypesTable.getName() + "_staging",
                allTypesTable.getSchema(),
                columns,
                new ArrayList<>());

        val adpTables = entitiesFactory.create(allTypesTable, "env");
        assertThat(adpTables.getActual()).usingRecursiveComparison().isEqualTo(expectedActual);
        assertThat(adpTables.getHistory()).usingRecursiveComparison().isEqualTo(expectedHistory);
        assertThat(adpTables.getStaging()).usingRecursiveComparison().isEqualTo(expectedStaging);
    }

}
