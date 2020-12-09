-- ADG
-- drop database loadtest;
create database loadtest;

-- drop table loadtest.all_types_table;
CREATE TABLE loadtest.all_types_table
(
    id            int not null,
    double_col    double,
    float_col     float,
    varchar_col   varchar(36),
    boolean_col   boolean,
    int_col       int,
    bigint_col    bigint,
    date_col      date,
    timestamp_col timestamp,
    time_col      time(5),
    uuid_col      uuid,
    char_col      char(10),
    primary key (id)
) distributed by (id)
datasource_type(ADG);
----------
-- MPP-W
----------
-- drop upload external table loadtest.all_types_table_ext;
create upload external table loadtest.all_types_table_ext
(
    id            int not null,
    double_col    double,
    float_col     float,
    varchar_col   varchar(36),
    boolean_col   boolean,
    int_col       int,
    bigint_col    bigint,
    date_col      date,
    timestamp_col timestamp,
    time_col      time(5),
    uuid_col      uuid,
    char_col      char(10)
)
LOCATION 'kafka://ads-z-1:2181/ALL_TYPES_TABLE_10_KK'
FORMAT 'AVRO';

use loadtest;

BEGIN DELTA;
INSERT INTO loadtest.all_types_table SELECT * FROM loadtest.all_types_table_ext;
COMMIT DELTA;

select count(*) from loadtest.all_types_table;
-----
--  MPP-R
-----
-- drop DOWNLOAD external table loadtest.all_types_table_download;
create DOWNLOAD external table loadtest.all_types_table_download
(
    id            int not null,
    double_col    double,
    float_col     float,
    varchar_col   varchar(36),
    boolean_col   boolean,
    int_col       int,
    bigint_col    bigint,
    date_col      date,
    timestamp_col timestamp,
    time_col      time(5),
    uuid_col      uuid,
    char_col      char(10)
)
LOCATION 'kafka://ads-z-1:2181/UNLOAD_ALL_TYPES_TABLE_10_KK_ADG'
FORMAT 'AVRO'
chunk_size 10000;
INSERT INTO loadtest.all_types_table_download SELECT * FROM loadtest.all_types_table;





