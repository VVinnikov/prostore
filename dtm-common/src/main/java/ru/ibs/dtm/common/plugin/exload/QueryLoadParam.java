package ru.ibs.dtm.common.plugin.exload;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import ru.ibs.dtm.common.plugin.exload.Format;
import ru.ibs.dtm.common.plugin.exload.TableAttribute;
import ru.ibs.dtm.common.plugin.exload.Type;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Параметры выполнения загрузки
 */
@Data
@EqualsAndHashCode
@ToString
public class QueryLoadParam {
    /**
     * Идентификатор
     */
    private UUID id;
    /**
     * Схема данных
     */
    private String datamart;
    /**
     * Наименование таблицы приемника
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
     * id загружаемой дельты
     */
    private Long deltaHot;
    /**
     *  таймаут потока Kafka, ms
     */
    private Integer kafkaStreamTimeoutMs;
    /**
     * периодичность проверки статусов плагинов, ms
     */
    private Integer pluginStatusCheckPeriodMs;
    /**
     * Количество записей в сообщении
     */
    private Integer messageLimit;
}
