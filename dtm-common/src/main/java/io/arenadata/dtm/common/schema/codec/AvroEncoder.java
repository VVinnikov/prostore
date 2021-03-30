package io.arenadata.dtm.common.schema.codec;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.util.List;

@Slf4j
public class AvroEncoder<T> extends AvroSerdeHelper {

    @SneakyThrows
    public byte[] encode(List<T> values, Schema schema) {
        try (val writer = new DataFileWriter<T>(new SpecificDatumWriter<>(schema))) {
            val baos = new ByteArrayOutputStream();
            writer.create(schema, baos);
            for (T value : values) {
                writer.append(value);
            }
            writer.flush();
            return baos.toByteArray();
        } catch (Exception e) {
            log.error("AVRO serialization error", e);
            throw e;
        }
    }

    @SneakyThrows
    public byte[] encode(List<T> values, Schema schema, CodecFactory codec) {
        try (val writer = new DataFileWriter<T>(new SpecificDatumWriter<>(schema))) {
            val baos = new ByteArrayOutputStream();
            writer.setCodec(codec);
            writer.create(schema, baos);
            for (T value : values) {
                writer.append(value);
            }
            writer.flush();
            return baos.toByteArray();
        } catch (Exception e) {
            log.error("AVRO serialization error", e);
            throw e;
        }
    }
}
