package io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.request;

import io.arenadata.dtm.query.execution.plugin.adg.base.dto.AdgHelperTableNames;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AdgTransferDataEtlRequest {
    private AdgHelperTableNames helperTableNames;
    private long deltaNumber;
}
