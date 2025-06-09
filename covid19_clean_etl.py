import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame

# Script generated for node Custom Transform
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
     
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection
    from pyspark.sql.functions import col, lag, when, coalesce, lit, avg, round as spark_round
    from pyspark.sql.window import Window

    input_dyf = dfc.select(list(dfc.keys())[0])
    df = input_dyf.toDF()

    # Rename + cast
    df = df.withColumnRenamed("last_update", "date")
    df = df.withColumnRenamed("country_region", "country")
    df = df.withColumn("date", col("date").cast("date"))
    df = df.withColumn("confirmed", col("confirmed").cast("int"))

    # New Cases & Moving Average
    w = Window.partitionBy("country").orderBy("date")
    df = df.withColumn("prev_confirmed", lag("confirmed").over(w))
    df = df.withColumn("New_Cases", col("confirmed") - col("prev_confirmed"))
    df = df.fillna({'New_Cases': 0})
    df = df.withColumn("New_Cases", when(col("New_Cases") < 0, 0).otherwise(col("New_Cases")))
    df = df.withColumn("prev_confirmed", coalesce(col("prev_confirmed"), lit(0)))
    df = df.withColumn("New_Cases_MA7", spark_round(avg("New_Cases").over(w.rowsBetween(-6, 0)), 2))

    # Round decimals
    df = df.withColumn("incident_rate", spark_round(col("incident_rate"), 2))
    df = df.withColumn("case_fatality_ratio", spark_round(col("case_fatality_ratio"), 2))

        # Remove nulls only from key fields
    df = df.filter(
        col("country").isNotNull() &
        col("province_state").isNotNull() &
        col("date").isNotNull() &
        col("confirmed").isNotNull() &
        col("deaths").isNotNull() &
        col("New_Cases").isNotNull()
    )

    # Remove rows where confirmed, deaths, New_Cases are 0
    df = df.filter(
        (col("confirmed") != 0) &
        (col("deaths") != 0) &
        (col("New_Cases") != 0)
    )
    from pyspark.sql.functions import col, when, round

    # Replace nulls in active and recovered with "-"
    df = df.withColumn("active", when(col("active").isNull(), "-").otherwise(col("active")))
    df = df.withColumn("recovered", when(col("recovered").isNull(), "-").otherwise(col("recovered")))
    df = df.withColumn("fips", when(col("fips").isNull(), "-").otherwise(col("fips")))
    # Filter key fields for nulls
    df = df.filter(
    col("country").isNotNull() &
    col("province_state").isNotNull() &
    col("date").isNotNull() &
    col("confirmed").isNotNull() &
    col("deaths").isNotNull() &
    col("New_Cases").isNotNull()
    )

    # Remove rows with 0 in critical numeric columns
    df = df.filter(
    (col("confirmed") != 0) &
    (col("deaths") != 0) &
    (col("New_Cases") != 0)
    )

    # Optional: round off New_Cases_MA7 to 2 decimal places
    df = df.withColumn("New_Cases_MA7", round(col("New_Cases_MA7"), 2))


    # Return as collection
    df_out = DynamicFrame.fromDF(df, glueContext, "df_out")
    return DynamicFrameCollection({"CleanedData": df_out}, glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1749453035797 = glueContext.create_dynamic_frame.from_catalog(database="covid19_db", table_name="rawdata", transformation_ctx="AWSGlueDataCatalog_node1749453035797")

# Script generated for node Custom Transform
CustomTransform_node1749456579459 = MyTransform(glueContext, DynamicFrameCollection({"AWSGlueDataCatalog_node1749453035797": AWSGlueDataCatalog_node1749453035797}, glueContext))

# Script generated for node Select From Collection
SelectFromCollection_node1749460859872 = SelectFromCollection.apply(dfc=CustomTransform_node1749456579459, key=list(CustomTransform_node1749456579459.keys())[0], transformation_ctx="SelectFromCollection_node1749460859872")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SelectFromCollection_node1749460859872, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1749452985999", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1749459259780 = glueContext.write_dynamic_frame.from_options(frame=SelectFromCollection_node1749460859872, connection_type="s3", format="glueparquet", connection_options={"path": "s3://covid19-datapipeline/cleaned/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1749459259780")

job.commit()