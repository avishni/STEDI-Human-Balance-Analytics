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

# Script generated for node Amazon S3
AmazonS3_node1675932618741 = glueContext.create_dynamic_frame.from_catalog(
    database="avishni_udmde_stedi",
    table_name="customers_curated",
    transformation_ctx="AmazonS3_node1675932618741",
)

# Script generated for node step_trainer
step_trainer_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="avishni_udmde_stedi",
    table_name="step_trainer_landing",
    transformation_ctx="step_trainer_node1",
)

# Script generated for node Change Schema (Apply Mapping)
ChangeSchemaApplyMapping_node1675933391548 = ApplyMapping.apply(
    frame=AmazonS3_node1675932618741,
    mappings=[("serialnumber", "string", "serialnumber", "string")],
    transformation_ctx="ChangeSchemaApplyMapping_node1675933391548",
)

# Script generated for node Change Schema (Apply Mapping)
ChangeSchemaApplyMapping_node1675933377569 = ApplyMapping.apply(
    frame=step_trainer_node1,
    mappings=[
        ("sensorreadingtime", "long", "sensorreadingtime", "long"),
        ("serialnumber", "string", "serial", "string"),
        ("distancefromobject", "int", "distancefromobject", "int"),
    ],
    transformation_ctx="ChangeSchemaApplyMapping_node1675933377569",
)

# Script generated for node Join
Join_node1675932574653 = Join.apply(
    frame1=ChangeSchemaApplyMapping_node1675933377569,
    frame2=ChangeSchemaApplyMapping_node1675933391548,
    keys1=["serial"],
    keys2=["serialnumber"],
    transformation_ctx="Join_node1675932574653",
)

# Script generated for node Drop Fields
DropFields_node1675939787276 = DropFields.apply(
    frame=Join_node1675932574653,
    paths=["serial"],
    transformation_ctx="DropFields_node1675939787276",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1675940142859 = DynamicFrame.fromDF(
    DropFields_node1675939787276.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1675940142859",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1675940142859,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://avishni-udmde/step_trainer_trusted/",
        "partitionKeys": [],
    },
    format_options={"compression": "uncompressed"},
    transformation_ctx="S3bucket_node3",
)

job.commit()
