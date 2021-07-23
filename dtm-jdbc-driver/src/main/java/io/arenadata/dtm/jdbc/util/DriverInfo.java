package io.arenadata.dtm.jdbc.util;

public class DriverInfo {
    public static final String DATABASE_PRODUCT_NAME = "DTM";
    public static final String DRIVER_NAME = "DTM JDBC Driver";
    public static final String DRIVER_SHORT_NAME = "DtmJDBC";
    public static final int MAJOR_VERSION = 4;
    public static final int MINOR_VERSION = 1;
    public static final int PATCH_VERSION = 0;
    public static final String DRIVER_VERSION = String.format("%s.%s.%s", MAJOR_VERSION, MINOR_VERSION, PATCH_VERSION);
    public static final String DRIVER_FULL_NAME = DRIVER_NAME + " " + DRIVER_VERSION;
    public static final String JDBC_VERSION = "4.2";
    public static final int JDBC_MAJOR_VERSION = "4.2".charAt(0) - 48;
    public static final int JDBC_MINOR_VERSION = "4.2".charAt(2) - 48;

    private DriverInfo() {
    }
}
