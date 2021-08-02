package io.arenadata.dtm.jdbc.util;

public class DriverInfo {
    public static final String DATABASE_PRODUCT_NAME = "DTM";
    public static final String DRIVER_NAME = "DTM JDBC Driver";

    public static String DRIVER_VERSION;
    public static int MAJOR_VERSION;
    public static int MINOR_VERSION;

    public static final int JDBC_MAJOR_VERSION = "4.2".charAt(0) - 48;
    public static final int JDBC_MINOR_VERSION = "4.2".charAt(2) - 48;

    private DriverInfo() {
    }

    static {
        DRIVER_VERSION = DriverInfo.class.getPackage().getImplementationVersion();

        String[] versions = DRIVER_VERSION.split("\\.");
        MAJOR_VERSION = Integer.parseInt(versions[0]);
        MINOR_VERSION = Integer.parseInt(versions[1]);
    }

}
