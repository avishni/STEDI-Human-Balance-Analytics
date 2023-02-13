import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1675940353295 = glueContext.create_dynamic_frame.from_catalog(
    database="avishni_udmde_stedi",
    table_name="accelerometer_trusted1",
    transformation_ctx="AmazonS3_node1675940353295",
)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="avishni_udmde_stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Join
Join_node1675940394278 = Join.apply(
    frame1=S3bucket_node1,
    frame2=AmazonS3_node1675940353295,
    keys1=["sensorreadingtime"],
    keys2=["timestamp"],
    transformation_ctx="Join_node1675940394278",
)

# Script generated for node Drop Fields
DropFields_node1675940818711 = DropFields.apply(
    frame=Join_node1675940394278,
    paths=["sensorreadingtime"],
    transformation_ctx="DropFields_node1675940818711",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1675940818711,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://avishni-udmde/machine_learning_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
