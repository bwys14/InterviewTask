from pyspark.sql import SparkSession
from pytest import fixture


@fixture(name="spark_sess", scope="session")
def spark_session_fixture() -> SparkSession:
    """
    This function initializes spark session
    """
    spark_sess = SparkSession.builder.appName("test_session").getOrCreate()
    yield spark_sess
    spark_sess.stop()
