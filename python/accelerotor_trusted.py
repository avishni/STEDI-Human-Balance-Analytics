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

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="avishni_udmde_stedi",
    table_name="accelerometer_landing",
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Amazon S3
AmazonS3_node1675774601012 = glueContext.create_dynamic_frame.from_catalog(
    database="avishni_udmde_stedi",
    table_name="customers_trusted1",
    transformation_ctx="AmazonS3_node1675774601012",
)

# Script generated for node Change Schema (Apply Mapping)
ChangeSchemaApplyMapping_node1675774790904 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("timestamp", "long", "timestamp", "long"),
        ("user", "string", "user", "string"),
        ("x", "float", "x", "float"),
        ("y", "float", "y", "float"),
        ("z", "float", "z", "float"),
    ],
    transformation_ctx="ChangeSchemaApplyMapping_node1675774790904",
)

# Script generated for node Change Schema (Apply Mapping)
ChangeSchemaApplyMapping_node1675774745242 = ApplyMapping.apply(
    frame=AmazonS3_node1675774601012,
    mappings=[
        ("customername", "string", "customername", "string"),
        ("email", "string", "email", "string"),
        ("phone", "string", "phone", "string"),
        ("birthday", "string", "birthday", "string"),
        ("serialnumber", "string", "serialnumber", "string"),
        ("registrationdate", "long", "registrationdate", "long"),
        ("lastupdatedate", "long", "lastupdatedate", "long"),
        ("sharewithresearchasofdate", "long", "sharewithresearchasofdate", "long"),
        ("sharewithpublicasofdate", "long", "sharewithpublicasofdate", "long"),
    ],
    transformation_ctx="ChangeSchemaApplyMapping_node1675774745242",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = Join.apply(
    frame1=ChangeSchemaApplyMapping_node1675774745242,
    frame2=ChangeSchemaApplyMapping_node1675774790904,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Drop Fields
DropFields_node1675794860106 = DropFields.apply(
    frame=ApplyMapping_node2,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
    ],
    transformation_ctx="DropFields_node1675794860106",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1675794860106,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://avishni-udmde/accelerometer_trusted/",
        "partitionKeys": [],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="S3bucket_node3",
)

job.commit()
