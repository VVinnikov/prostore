package ru.ibs.dtm.common.status.ddl;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class DatamartSchemaChangedEvent {
    private LocalDateTime changeDateTime;
    private String datamart;
}
