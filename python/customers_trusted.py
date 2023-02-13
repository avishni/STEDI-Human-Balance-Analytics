import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs
import re


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="avishni_udmde_stedi",
    table_name="customers_landing",
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1675162623746 = DynamicFrame.fromDF(
    S3bucket_node1.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1675162623746",
)

# Script generated for node Calculate Age
SqlQuery8 = """
select *, birthday_fixed ,round(datediff(current_date(), to_date(birthday_fixed, 'yyyy-MM-dd')) / 365) as age 
from (
    select *, 
    case
        when length(birthday) = 10 then birthday
        when length(birthday) = 9 then concat('1', birthday)
        when length(birthday) = 8 then concat('19', birthday)
        else null
    end as birthday_fixed
    from myDataSource
)
"""
CalculateAge_node1675162226060 = sparkSqlQuery(
    glueContext,
    query=SqlQuery8,
    mapping={"myDataSource": DropDuplicates_node1675162623746},
    transformation_ctx="CalculateAge_node1675162226060",
)

# Script generated for node Filter Age (120)
FilterAge120_node1675162702797 = Filter.apply(
    frame=CalculateAge_node1675162226060,
    f=lambda row: (row["age"] < 120),
    transformation_ctx="FilterAge120_node1675162702797",
)

# Script generated for node Change Schema (Apply Mapping)
ChangeSchemaApplyMapping_node1675164947642 = ApplyMapping.apply(
    frame=FilterAge120_node1675162702797,
    mappings=[
        ("customername", "string", "customername", "string"),
        ("email", "string", "email", "string"),
        ("phone", "string", "phone", "string"),
        ("birthday", "string", "birthday", "string"),
        ("serialnumber", "string", "serialnumber", "string"),
        ("registrationdate", "bigint", "registrationdate", "long"),
        ("lastupdatedate", "bigint", "lastupdatedate", "long"),
        ("sharewithresearchasofdate", "bigint", "sharewithresearchasofdate", "long"),
        ("sharewithfriendsasofdate", "bigint", "sharewithfriendsasofdate", "long"),
        ("sharewithpublicasofdate", "bigint", "sharewithpublicasofdate", "long"),
        ("birthday_fixed", "string", "birthday_fixed", "string"),
        ("age", "double", "age", "double"),
    ],
    transformation_ctx="ChangeSchemaApplyMapping_node1675164947642",
)

# Script generated for node Filter (share with research)
Filtersharewithresearch_node1675162786030 = Filter.apply(
    frame=ChangeSchemaApplyMapping_node1675164947642,
    f=lambda row: (row["sharewithresearchasofdate"] >= 1),
    transformation_ctx="Filtersharewithresearch_node1675162786030",
)

# Script generated for node SQL Query
SqlQuery7 = """
select 
    customername
    ,serialnumber 
    ,phone
    ,email
    ,max(birthday_fixed) as birthday
    ,min(registrationdate) as registrationdate
    ,max(lastupdatedate) as lastupdatedate
    ,max(sharewithpublicasofdate) as sharewithpublicasofdate
    ,max(sharewithresearchasofdate) as sharewithresearchasofdate
from myDataSource
group by customername, serialnumber ,phone , email
"""
SQLQuery_node1675765880814 = sparkSqlQuery(
    glueContext,
    query=SqlQuery7,
    mapping={"myDataSource": Filtersharewithresearch_node1675162786030},
    transformation_ctx="SQLQuery_node1675765880814",
)

# Script generated for node Amazon S3
AmazonS3_node1675162813304 = glueContext.write_dynamic_frame.from_options(
    frame=SQLQuery_node1675765880814,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://avishni-udmde/customers_trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1675162813304",
)

job.commit()
