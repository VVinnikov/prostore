package ru.ibs.dtm.query.execution.plugin.adb.service.impl.ddl;

import org.junit.jupiter.api.Test;

import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AdbDdlServiceTest {
  @Test
  void isCreateTable() {
    final Predicate<String> p = AdbDdlService.IS_CREATE_TABLE;
    assertTrue(p.test("create table datamart.table (id integer not null, table_field varchar(1))"));
    assertTrue(p.test(" CREATE   TABLE table (some fields)"));
    assertFalse(p.test("create temporary table table (some fields)"));
  }

  @Test
  void isDropTable() {
    final Predicate<String> p = AdbDdlService.IS_DROP_TABLE;
    assertTrue(p.test("drop table if exists table"));
  }
}
