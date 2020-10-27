package io.arenadata.dtm.common.schema;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class KafkaAvroArrayDeserializer implements Deserializer<GenericData.Array> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaAvroRecordDeserializer.class);
    private Map<String, VersionedSchema> readerSchemasByName;
    private SchemaProvider schemaProvider;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        schemaProvider = SchemaUtils.getSchemaProvider(configs);
        readerSchemasByName = SchemaUtils.getVersionedSchemas(configs, schemaProvider);
    }

    @Override
    public GenericData.Array deserialize(String s, byte[] bytes) {
        try (ByteArrayInputStream stream = new ByteArrayInputStream(bytes)) {

            int schemaId = SchemaUtils.readSchemaId(stream);
            VersionedSchema writerSchema = schemaProvider.get(schemaId);

            VersionedSchema readerSchema = readerSchemasByName.get(writerSchema.getName());
            GenericData.Array avroRecord = readAvroRecord(stream, writerSchema.getSchema(), readerSchema.getSchema());
            return avroRecord;
        } catch (Exception e) {
            LOGGER.error("Deserialization error", e);
            throw new RuntimeException(e);
        }
    }

    private GenericData.Array readAvroRecord(InputStream stream, Schema writerSchema, Schema readerSchema) throws IOException {
        DatumReader<Object> datumReader = new GenericDatumReader<>(writerSchema, readerSchema);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(stream, null);
        GenericData.Array array = new GenericData.Array<GenericRecord>(0, readerSchema);
        datumReader.read(array, decoder);
        return array;
    }

    @Override
    public void close() {
        try {
            schemaProvider.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}