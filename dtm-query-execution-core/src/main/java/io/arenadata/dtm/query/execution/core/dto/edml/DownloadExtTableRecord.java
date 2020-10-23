package io.arenadata.dtm.query.execution.core.dto.edml;

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
