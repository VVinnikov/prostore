package io.arenadata.dtm.query.execution.plugin.api.request;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.plugin.api.dto.PluginRequest;
import lombok.Builder;
import lombok.Getter;
import org.apache.calcite.sql.SqlKind;

import java.util.UUID;

@Getter
public class DdlRequest extends PluginRequest {

	private SqlKind sqlKind;
	private Entity entity;

	@Builder(toBuilder = true)
	public DdlRequest(UUID requestId,
					  String envName,
					  String datamartMnemonic,
					  Entity entity,
					  SqlKind sqlKind) {
		super(requestId, envName, datamartMnemonic);
		this.entity = entity;
		this.sqlKind = sqlKind;
	}

	@Override
	public String toString() {
		return "DdlRequest{" +
				super.toString() +
				", classTable=" + entity +
				'}';
	}
}
