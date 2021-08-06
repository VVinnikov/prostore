package io.arenadata.dtm.query.execution.plugin.adp.check.factory;

import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByCountRequest;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByHashInt32Request;
import lombok.val;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.UUID;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.plugin.adp.util.TestUtils.createAllTypesTable;
import static org.junit.jupiter.api.Assertions.assertEquals;

class AdpCheckDataQueryFactoryTest {

    @Test
    void testCreateCheckDataByCountQuery() {
        val expectedSql = "SELECT count(1) as cnt FROM (SELECT 1 FROM datamart.table_actual WHERE (sys_to = 0 AND sys_op = 1) OR sys_from = 1) AS tmp";
        CheckDataByCountRequest request = new CheckDataByCountRequest(
                createAllTypesTable(),
                1L,
                "",
                UUID.randomUUID(),
                "");

        val checkQuery = AdpCheckDataQueryFactory.createCheckDataByCountQuery(request);
        assertEquals(expectedSql, checkQuery);
    }

    @Test
    void testCreateCheckDataByHashInt32QueryEmptyColumns() {
        val expectedSql = "SELECT sum(dtmInt32Hash(MD5(concat())::bytea)) as hash_sum FROM\n" +
                "(\n" +
                "  SELECT  \n" +
                "  FROM datamart.table_actual \n" +
                "  WHERE (sys_to = 0 AND sys_op = 1) OR sys_from = 1) AS tmp";
        CheckDataByHashInt32Request request = new CheckDataByHashInt32Request(
                createAllTypesTable(),
                1L,
                new HashSet<>(),
                "",
                UUID.randomUUID(),
                "");

        val checkQuery = AdpCheckDataQueryFactory.createCheckDataByHashInt32Query(request);
        assertEquals(expectedSql, checkQuery);
    }

    @Test
    void testCreateCheckDataByHashInt32QueryAllColumns() {
        val expectedSql = "SELECT sum(dtmInt32Hash(MD5(concat(uuid_col,';',char_col,';',int_col,';',link_col,';',bigint_col,';',boolean_col::int,';',date_col - make_date(1970, 01, 01),';',float_col,';',varchar_col,';',int32_col,';',(extract(epoch from timestamp_col)*1000000)::bigint,';',id,';',(extract(epoch from time_col)*1000000)::bigint,';',double_col))::bytea)) as hash_sum FROM\n" +
                "(\n" +
                "  SELECT uuid_col,';',char_col,';',int_col,';',link_col,';',bigint_col,';',boolean_col,';',date_col,';',float_col,';',varchar_col,';',int32_col,';',timestamp_col,';',id,';',time_col,';',double_col \n" +
                "  FROM datamart.table_actual \n" +
                "  WHERE (sys_to = 0 AND sys_op = 1) OR sys_from = 1) AS tmp";
        val entity = createAllTypesTable();
        CheckDataByHashInt32Request request = new CheckDataByHashInt32Request(
                entity,
                1L,
                entity.getFields().stream().map(EntityField::getName).collect(Collectors.toSet()),
                "",
                UUID.randomUUID(),
                "");
        val checkQuery = AdpCheckDataQueryFactory.createCheckDataByHashInt32Query(request);
        assertEquals(expectedSql, checkQuery);
    }

}
