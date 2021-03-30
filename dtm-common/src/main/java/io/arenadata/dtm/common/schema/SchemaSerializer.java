package io.arenadata.dtm.common.schema;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.avro.Schema;

import java.io.IOException;

public class SchemaSerializer extends StdSerializer<Schema> {

    public SchemaSerializer() {
        super(Schema.class);
    }

    @Override
    public void serialize(Schema schema, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
        jsonGenerator.writeRawValue(schema.toString());
    }
}
