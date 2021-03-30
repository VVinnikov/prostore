package io.arenadata.dtm.jdbc.core;

public class Tuple {

    final Object[] data;

    public Tuple(int length) {
        this(new Object[length]);
    }

    public Tuple(Object[] data) {
        this.data = data;
    }

    public int fieldCount() {
        return this.data.length;
    }

    public Object get(int index) {
        return this.data[index];
    }

    public void set(int index, Object fieldData) {
        this.data[index] = fieldData;
    }
}
