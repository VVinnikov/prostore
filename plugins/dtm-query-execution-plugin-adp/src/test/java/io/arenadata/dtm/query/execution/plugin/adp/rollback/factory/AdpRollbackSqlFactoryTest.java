package io.arenadata.dtm.query.execution.plugin.adp.rollback.factory;

import io.arenadata.dtm.common.model.ddl.Entity;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class AdpRollbackSqlFactoryTest {

    @Test
    void shouldBeCorrectRollbackSql() {
        // arrange
        String datamart = "datamart";
        Entity entity = Entity.builder()
                .name("adpTable")
                .build();
        Long sysCn = 10L;

        // act
        String rollbackSql = AdpRollbackSqlFactory.getRollbackSql(datamart, entity, sysCn);

        // assert
        Assertions.assertThat(rollbackSql).isEqualToNormalizingNewlines("TRUNCATE datamart.adpTable_staging;\n" +
                "DELETE FROM datamart.adpTable_actual WHERE sys_from = 10;\n" +
                "UPDATE datamart.adpTable_actual SET sys_to = NULL, sys_op = 0 WHERE sys_to = 9;");
    }

}