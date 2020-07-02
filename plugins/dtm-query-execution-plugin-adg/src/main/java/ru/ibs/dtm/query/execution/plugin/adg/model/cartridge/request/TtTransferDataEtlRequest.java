package ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.request;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.ibs.dtm.query.execution.plugin.adg.dto.AdgHelperTableNames;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TtTransferDataEtlRequest {
    private AdgHelperTableNames helperTableNames;
    private long deltaNumber;
}
