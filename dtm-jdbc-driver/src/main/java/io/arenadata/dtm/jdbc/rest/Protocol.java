package io.arenadata.dtm.jdbc.rest;

import io.arenadata.dtm.jdbc.core.QueryResult;
import io.arenadata.dtm.jdbc.model.ColumnInfo;
import io.arenadata.dtm.jdbc.model.SchemaInfo;
import io.arenadata.dtm.jdbc.model.TableInfo;

import java.sql.SQLException;
import java.util.List;

/**
 * Интерфейс протокола взаимодействия с источником данных
 */
public interface Protocol {
    /**
     * Получение информации о схемах
     * @return список схем
     */
    List<SchemaInfo> getDatabaseSchemas();

    /**
     * Получение информации о таблицах в указанной схеме
     * @param schemaPattern - название схемы
     * @return список таблиц
     */
    List<TableInfo> getDatabaseTables(String schemaPattern);

    /**
     * Получение информации о колонках указанной таблицы для заданной схемы
     * @param schema - название схемы
     * @param tableName - название таблицы
     * @return список колонок
     */
    List<ColumnInfo> getDatabaseColumns(String schema, String tableName);

    /**
     * Выполнение sql-запроса без параметров
     * @param sql - запрос для выполнения
     * @return результат выполнения запроса
     */
    QueryResult executeQuery(String sql) throws SQLException;
}
