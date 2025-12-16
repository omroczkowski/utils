from pyspark.sql.types import *

def spark_type_to_sql(dt):
    if isinstance(dt, StringType):
        return "STRING"
    if isinstance(dt, IntegerType):
        return "INT"
    if isinstance(dt, LongType):
        return "BIGINT"
    if isinstance(dt, ShortType):
        return "SMALLINT"
    if isinstance(dt, ByteType):
        return "TINYINT"
    if isinstance(dt, FloatType):
        return "FLOAT"
    if isinstance(dt, DoubleType):
        return "DOUBLE"
    if isinstance(dt, BooleanType):
        return "BOOLEAN"
    if isinstance(dt, DateType):
        return "DATE"
    if isinstance(dt, TimestampType):
        return "TIMESTAMP"
    if isinstance(dt, DecimalType):
        return f"DECIMAL({dt.precision},{dt.scale})"
    if isinstance(dt, ArrayType):
        return f"ARRAY<{spark_type_to_sql(dt.elementType)}>"
    if isinstance(dt, StructType):
        fields = ", ".join(
            f"{f.name}:{spark_type_to_sql(f.dataType)}"
            for f in dt.fields
        )
        return f"STRUCT<{fields}>"

    # fallback
    return dt.simpleString().upper()

ddl_columns = ",\n  ".join(
    f"{field.name} {spark_type_to_sql(field.dataType)}"
    for field in df.schema.fields
)
