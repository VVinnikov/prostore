package io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.request;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AdgDeleteTablesRequest {

    List<String> tableList;
}
