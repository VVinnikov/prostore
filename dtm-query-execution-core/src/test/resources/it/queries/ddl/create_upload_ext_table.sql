create upload external table %s.%s
(
  id int not null,
  double_col double,
  float_col float,
  varchar_col varchar(36),
  boolean_col boolean,
  int_col int,
  bigint_col bigint,
  date_col date,
  timestamp_col timestamp,
  time_col time(5),
  uuid_col uuid,
  char_col char(10)
)
LOCATION 'kafka://%s/%s'
FORMAT 'AVRO';