package ru.ibs.dtm.common.schema.codec;

import lombok.SneakyThrows;
import lombok.val;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.reflect.ReflectDatumWriter;

import java.io.ByteArrayOutputStream;
import java.util.List;

public class AvroEncoder<T> {

    @SneakyThrows
    public byte[] encode(List<T> values, Schema schema) {
        try (val writer = new DataFileWriter<T>(new ReflectDatumWriter<>(schema))) {
            val baos = new ByteArrayOutputStream();
            writer.create(schema, baos);
            for (T value : values) {
                writer.append(value);
            }
            return baos.toByteArray();
        }
    }
}
