from airflow.plugins_manager import AirflowPlugin

from operators import StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator, CreateRedshiftOperator, CreateTableOperator, S3ToRedshiftOperator
import helpers

# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
         StageToRedshiftOperator,
         LoadFactOperator,
        LoadDimensionOperator,
        DataQualityOperator,
        CreateRedshiftOperator,
        CreateTableOperator,
        S3ToRedshiftOperator
    ]
    helpers = [
        helpers.SqlQueries
    ]
