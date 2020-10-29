package io.arenadata.dtm.query.execution.core.configuration;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.common.schema.codec.AvroEncoder;
import io.vertx.core.json.jackson.DatabindCodec;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.Environment;

@Configuration
public class AppConfiguration {

	private Environment environment;

	@Autowired
	public AppConfiguration(Environment environment) {
		this.environment = environment;
	}

	public Integer httpPort() {
		return environment.getProperty("core.http.port", Integer.class);
	}

	public String getEnvName() {
		return environment.getProperty("core.env.name", String.class);
	}

	@Bean("coreObjectMapper")
	@Primary
	public ObjectMapper objectMapper() {
		SimpleModule simpleModule = new SimpleModule();
		ObjectMapper mapper = DatabindCodec.mapper();
		mapper.registerModule(simpleModule);
		mapper.registerModule(new JavaTimeModule());
		mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
		mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
		return mapper;
	}

    @Bean
    public AvroEncoder avroEncoder() {
        return new AvroEncoder();
    }

}
