package ru.ibs.dtm.query.execution.core.factory.impl;

import io.vertx.core.json.JsonObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import ru.ibs.dtm.common.plugin.exload.QueryExloadParam;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.core.factory.MpprKafkaRequestFactory;
import ru.ibs.dtm.query.execution.plugin.api.mppr.MpprRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.MpprRequest;

import java.net.URI;

@Slf4j
public class MpprKafkaRequestFactoryImpl implements MpprKafkaRequestFactory {

	public static final int DEFAULT_ZOOKEEPER_PORT = 2181;

	public static KafkaTopicUri parseLocationPath(String locationPath) {
		try {
			URI uri = URI.create(locationPath);
			String topic = uri.getPath().substring(1);
			String[] authorityArray = uri.getAuthority().split(":");
			String host = authorityArray[0];
			int port = authorityArray.length > 1 ? Integer.parseInt(authorityArray[1]) : DEFAULT_ZOOKEEPER_PORT;
			return new KafkaTopicUri(host, topic, port);
		} catch (Exception e) {
			String errMsg = String.format("Ошибка парсинга locationPath [%s]: %s", locationPath, e.getMessage());
			log.error(errMsg, e);
			throw new RuntimeException(errMsg, e);
		}
	}

	@Override
	public MpprRequestContext create(QueryRequest queryRequest, QueryExloadParam queryExloadParam, JsonObject schema) {
		val request = new MpprRequest(queryRequest, queryExloadParam, schema);
		KafkaTopicUri kafkaTopicUri = parseLocationPath(queryExloadParam.getLocationPath());
		request.setTopic(kafkaTopicUri.getTopic());
		request.setZookeeperHost(kafkaTopicUri.getHost());
		request.setZookeeperPort(kafkaTopicUri.getPort());
		return new MpprRequestContext(request);
	}

	@Data
	@AllArgsConstructor
	public final static class KafkaTopicUri {
		private String host;
		private String topic;
		private int port;
	}
}
