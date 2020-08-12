package ru.ibs.dtm.common.plugin.exload;

import io.vertx.core.json.JsonObject;
import lombok.Data;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Параметры выполнения выгрузки
 */
@Data
public class QueryExloadParam {

    /**
     * Идентификатор
     */
    private UUID id;

    /**
     * Схема данных
     */
    private String datamart;

    /**
     * Наименование таблицы
     */
    private String tableName;

    /**
     * sql запрос
     */
    private String sqlQuery;

    /**
     * Тип назначения выгрузки
     */
    private Type locationType;

    /**
     * Путь назначения выгрузки
     */
    private String locationPath;

    /**
     * Формат
     */
    private Format format;

    /**
     * Количество записей в chunk
     */
    private Integer chunkSize;
    private List<TableAttribute> tableAttributes;
    private JsonObject avroSchema;
}
