package ru.ibs.dtm.query.execution.plugin.adg.dto.connector;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.ibs.dtm.query.execution.plugin.adg.dto.AdgHelperTableNames;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AdgTransferDataConnectorRequest {
    private AdgHelperTableNames helperTableNames;
    private long deltaNumber;
}
