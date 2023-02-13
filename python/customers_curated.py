import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node customer
customer_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="avishni_udmde_stedi",
    table_name="customers_trusted1",
    transformation_ctx="customer_node1",
)

# Script generated for node accelerometer
accelerometer_node1675923345643 = glueContext.create_dynamic_frame.from_catalog(
    database="avishni_udmde_stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_node1675923345643",
)

# Script generated for node join
join_node2 = Join.apply(
    frame1=customer_node1,
    frame2=accelerometer_node1675923345643,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="join_node2",
)

# Script generated for node Drop Fields
DropFields_node1675923823880 = DropFields.apply(
    frame=join_node2,
    paths=["z", "user", "y", "x", "timestamp"],
    transformation_ctx="DropFields_node1675923823880",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1675923895660 = DynamicFrame.fromDF(
    DropFields_node1675923823880.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1675923895660",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1675923895660,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://avishni-udmde/customers_curated/",
        "partitionKeys": [],
    },
    format_options={"compression": "uncompressed"},
    transformation_ctx="S3bucket_node3",
)

job.commit()
