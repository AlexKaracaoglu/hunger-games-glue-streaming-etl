import sys
import decimal
import boto3
import json
from pyspark.context import SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

# Set up glue context, spark, and read in arguments
glue_context = GlueContext(SparkContext())
spark = glue_context.spark_session
job = Job(glue_context)

arguments = getResolvedOptions(sys.argv, ['JOB_NAME',
                                          'BUCKET_NAME',
                                          'DYNAMO_OUTPUT_TABLE',
                                          'GLUE_DATABASE_NAME',
                                          'GLUE_TABLE_NAME',
                                          'KINESIS_STREAM_NAME',
                                          'STATIC_DYNAMO_TABLE',
                                          'AWS_REGION'
                                          ])

job.init(arguments['JOB_NAME'], arguments)

bucket_name = arguments['BUCKET_NAME']
dynamo_output_table = arguments['DYNAMO_OUTPUT_TABLE']
glueDatabaseName = arguments['GLUE_DATABASE_NAME']
glue_table_name = arguments['GLUE_TABLE_NAME']
stream_name = arguments['KINESIS_STREAM_NAME']
static_dynamo_table = arguments['STATIC_DYNAMO_TABLE']
aws_region = arguments['AWS_REGION']
checkpoint_location = f's3a://{bucket_name}/checkpoint'


# Helper to make writing full data to s3 work by changing decimals to strings
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)


# Write relevant data to dynamo - allows for most up-to-date view on tributes
def write_data_to_dynamo(row):
    dynamodb = boto3.resource('dynamodb', region_name = aws_region)
    dynamodb.Table(dynamo_output_table).put_item(
        Item = {
            'tributeId': row['tributeid'],
            'name': row['firstName'],
            'district': row['district'],
            'age': row['age'],
            'status': row['status'],
            'heartRate': str(row['heartrate']),
            'painStatus': row['painstatus'],
            'hydrationStatus': row['hydrationstatus'],
            'hungerStatus': row['hungerstatus'],
            'xCoordinate': str(row['xcoordinate']),
            'yCoordinate': str(row['ycoordinate']),
            'locationStatus': str(row['locationstatus'])
        }
    )


# Write full data to s3 - allows for historical data view
def write_data_to_s3(row):
    s3 = boto3.resource('s3', region_name = aws_region)
    content = row.asDict()
    path = 'data/' + row['streamingeventid'] + '.json'
    s3.Object(bucket_name, path).put(Body=bytes(json.dumps(content, cls = DecimalEncoder).encode('UTF-8')))


# Calls dynamo and s3 write functions
def write_data(row):
    print(row)
    print(row.asDict())
    write_data_to_dynamo(row)
    write_data_to_s3(row)

# Read in static tribute data from s3
s3_static_tribute_data = spark.read.format("csv").option("header", "true").load(
    f"s3a://{bucket_name}/staticData/s3/tributeData.csv").cache()
s3_static_tribute_data.printSchema()

# Read in static game data from dynamo
static_dynamo_dynamic_frame = glue_context.create_dynamic_frame.from_options(
    "dynamodb",
    connection_options={
        "dynamodb.input.tableName": static_dynamo_table,
        "dynamodb.throughput.read.percent": "1.5"
    }
)
dynamo_static_game_data = static_dynamo_dynamic_frame.toDF().cache()
dynamo_static_game_data.printSchema()

# Read in the streaming data
tribute_streaming_data = glue_context.create_data_frame.from_catalog(database = glueDatabaseName,
                                                                     table_name = glue_table_name, additional_options = {
        "startingPosition": "TRIM_HORIZON", "inferSchema": "false"})

# The combination of streaming data and static data from s3 and dynamo
combined_df = tribute_streaming_data.join(s3_static_tribute_data, "tributeId") \
    .join(dynamo_static_game_data, "gameid")

# Analytics based on the streaming data compared against the tribute and game data
output_df = combined_df \
    .withColumn('hydrationstatus',
                when((tribute_streaming_data.hydrationlevel < s3_static_tribute_data.minHydrationThreshold),
                     "DEHYDRATED") \
                .when(((tribute_streaming_data.hydrationlevel - s3_static_tribute_data.minHydrationThreshold) < 0.5),
                      "APPROACHING DEHYDRATION").otherwise("OK")) \
    .withColumn('hungerstatus',
                when((tribute_streaming_data.hungerlevel > s3_static_tribute_data.maxHungerThreshold), "HUNGRY") \
                .when(((s3_static_tribute_data.maxHungerThreshold - tribute_streaming_data.hungerlevel) < 0.5),
                      "GETTING HUNGRY").otherwise(
                    "OK")) \
    .withColumn('painstatus',
                when((tribute_streaming_data.painlevel > s3_static_tribute_data.maxPainThreshold), "INJURED").otherwise(
                    "OK")) \
    .withColumn('status', when((tribute_streaming_data.heartrate == 0), "DEAD").otherwise("ALIVE")) \
    .withColumn('locationstatus', when(((tribute_streaming_data.xcoordinate > dynamo_static_game_data.maxXCoordinate) |
                                        (tribute_streaming_data.xcoordinate < dynamo_static_game_data.minXCoordinate) |
                                        (tribute_streaming_data.ycoordinate > dynamo_static_game_data.maxYCoordinate) |
                                        (tribute_streaming_data.ycoordinate < dynamo_static_game_data.minYCoordinate)),
                                       "OUT OF BOUNDS") \
                .when((((dynamo_static_game_data.maxXCoordinate - tribute_streaming_data.xcoordinate) < 5) |
                       ((dynamo_static_game_data.maxYCoordinate - tribute_streaming_data.ycoordinate) < 5) |
                       ((tribute_streaming_data.xcoordinate - dynamo_static_game_data.minXCoordinate) < 5) |
                       ((tribute_streaming_data.ycoordinate - dynamo_static_game_data.minYCoordinate) < 5)),
                      "APPROACHING THE BOUNDARY").otherwise(
    "IN BOUNDS"))

output_df.printSchema()

write = output_df \
    .writeStream \
    .foreach(write_data) \
    .outputMode("update") \
    .option("checkpointLocation", checkpoint_location) \
    .start()

write.awaitTermination()

job.commit()
