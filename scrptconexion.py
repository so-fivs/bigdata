import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1732775086228 = glueContext.create_dynamic_frame.from_catalog(database="finalheadline", table_name="final", transformation_ctx="AWSGlueDataCatalog_node1732775086228")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1732775100856 = glueContext.write_dynamic_frame.from_catalog(frame=AWSGlueDataCatalog_node1732775086228, database="noticiascopia", table_name="final", transformation_ctx="AWSGlueDataCatalog_node1732775100856")

job.commit()