package io.arenadata.dtm.query.execution.core.edml.mppr.dto;

import io.arenadata.dtm.query.execution.core.edml.dto.BaseExtTableRecord;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Запись из внешней таблицы выгрузки
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class DownloadExtTableRecord extends BaseExtTableRecord {
    private Integer chunkSize;
}
