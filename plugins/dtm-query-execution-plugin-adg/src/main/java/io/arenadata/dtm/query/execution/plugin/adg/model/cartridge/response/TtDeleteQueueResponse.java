package io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.response;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class TtDeleteQueueResponse {
    private List<String> droppedTableList;
}
