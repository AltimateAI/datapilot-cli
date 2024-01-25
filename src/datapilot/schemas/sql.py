from enum import Enum


class Dialect(Enum):
    SNOWFLAKE = "snowflake"
    POSTGRES = "postgres"
    REDSHIFT = "redshift"
    MYSQL = "mysql"
    BIGQUERY = "bigquery"
    OTHERS = "others"
