import urllib.request
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
import sqlite3


os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"
os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk1.8.0_291"
ldir = os.path.dirname(__file__)

DB_PATH = os.path.join(ldir, "data/Loaded", "nyctaxi.db")
TIP_STATS_TABLE_NAME = "TipStats"
SPEED_STATS_TABLE_NAME = "SpeedStats"


class TaxiETL(object):
	def __init__(self, year=2020):
		"""
		The goal of this assignment is to perform all the data transformation process and output insights via a REST
		endpoint. This class concerns the ETL part of the assignment.
		:param year: int
			the year for which ETL needs to run
		"""
		self.year = year
		self.quarter_months = {
			1: [1, 2, 3],
			2: [4, 5, 6],
			3: [7, 8, 9],
			4: [10, 11, 12]
		}
		self.spark = SparkSession.builder \
						.master("local[*]") \
						.appName("TaxiETL") \
						.getOrCreate()
		self.dbpath = DB_PATH
		self.tipStatsTableName = TIP_STATS_TABLE_NAME
		self.speedStatsTableName = SPEED_STATS_TABLE_NAME
		self.create_speed_stats_query = """
			CREATE TABLE IF NOT EXISTS {} (
				id integer PRIMARY KEY,
				year integer NOT NULL,
				month integer NOT NULL,
				day integer NOT NULL,
				hour integer not NULL,
				maxSpeed real not NULL
			)
			""".format(SPEED_STATS_TABLE_NAME)
		self.create_tip_stats_query = """
			CREATE TABLE IF NOT EXISTS {} (
				id integer PRIMARY KEY,
				year integer NOT NULL,
				quarter integer NOT NULL,
				DOLocationID integer NOT NULL,
				maxTipPercentage real not NULL
			)
			""".format(TIP_STATS_TABLE_NAME)


	def create_db(self):
		"""
		Creates SQLite database for TaxiETL with tables
		"""
		conn = sqlite3.connect(self.dbpath)
		cursor = conn.cursor()
		cursor.execute(self.create_tip_stats_query)
		cursor.execute(self.create_speed_stats_query)
		conn.close()

	def db_connection(self):
		"""
		Returns SQLite-DB connection
		:return: <sqlite3.connection>
			SQLite-DB connection
		"""
		conn = None
		try:
			conn = sqlite3.connect(self.dbpath)
		except sqlite3.Error as e:
			print(e)
		return conn

	def extract(self):
		"""
		Extracts Yellow Taxi Data from Amazon S3
		"""
		if not os.path.exists(os.path.join(ldir, 'data/Extracted')):
			os.makedirs(os.path.join(ldir, 'data/Extracted'))

		for month in range(1, 13):
			filename ="Extracted/yellow_tripdata_{0}-{1:0=2d}.csv".format(self.year, month)
			urllib.request.urlretrieve(
				url="https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_{0}-{1:02d}.csv".format(self.year, month),
				filename=os.path.join(ldir, 'data', filename)
			)
			print("Downloaded {}".format(filename))

	def transform(self):
		if not os.path.exists(os.path.join(ldir, 'data/Transformed')):
			os.makedirs(os.path.join(ldir, 'data/Transformed'))

		filename = os.path.join(ldir, 'data/Extracted', "yellow_tripdata_*.csv")
		data = self.spark.read.csv(filename, inferSchema=True, header=True)

		# Data cleaning
		query = """
			SELECT
				*
			FROM
			   data
			WHERE 
				tpep_pickup_datetime BETWEEN '2020-01-01' AND '2020-12-31' 
				AND tpep_dropoff_datetime BETWEEN '2020-01-01' AND '2020-12-31'
				AND tpep_dropoff_datetime >= tpep_pickup_datetime
				AND passenger_count > 0
				AND trip_distance >= 0 
				AND tip_amount >= 0 
				AND tolls_amount >= 0 
				AND mta_tax >= 0 
				AND fare_amount >= 0
			AND total_amount >= 0
		"""
		data.registerTempTable("data")
		cleaned_data = self.spark.sql(query).drop_duplicates()
		cleaned_data.registerTempTable("cleaned_data")
		cleaned_data = (
			cleaned_data.withColumn(
				"year", func.year(func.col("tpep_pickup_datetime"))
			).withColumn(
				"month", func.month(func.col("tpep_pickup_datetime"))
			)
		)
		cleaned_data.write.partitionBy("year", "month").mode("overwrite").parquet(
			os.path.join(ldir, "data/Transformed/cleaned_data")
		)

	def load_tip_stats(self):
		"""
		Calculates tip speed stats and store it in the DB
		"""
		conn = self.db_connection()
		cursor = conn.cursor()
		for quarter in [1, 2, 3, 4]:
			df = self.spark.read.parquet(
				*[
					os.path.join(ldir, 'data', "Transformed/cleaned_data/year=2020/month={}".format(month))
					for month in self.quarter_months[quarter]]
			)
			df.registerTempTable("df")
			query = "select DOLocationID, MAX((CASE WHEN total_amount=0 THEN 0 ELSE ROUND(tip_amount*100/total_amount,2) END)) as maxTipPercentage from df group by DOLocationID"
			df2 = self.spark.sql(query)
			result = df2.orderBy(func.desc("maxTipPercentage")).take(1)
			if result:
				insert_tip_stats_sql = """INSERT INTO {} (year, quarter, DOLocationID, maxTipPercentage)
                 VALUES (?, ?, ?, ?)""".format(self.tipStatsTableName)
				cursor = cursor.execute(insert_tip_stats_sql, (self.year, quarter, result[0].DOLocationID, result[0].maxTipPercentage))
				conn.commit()

	def load_speed_stats(self):
		"""
		Calculates max speed stats and store it in the DB
		"""
		conn = self.db_connection()
		cursor = conn.cursor()
		for month in range(1, 13):
			df = self.spark.read.parquet(
				os.path.join(ldir, 'data', "Transformed/cleaned_data/year=2020/month={}".format(month))
			)

			df = (
				df.withColumn(
					'time_duration_in_secs',
					func.col("tpep_dropoff_datetime").cast("long") - func.col('tpep_pickup_datetime').cast("long")
				).withColumn("hour", func.hour(func.col("tpep_pickup_datetime"))
							 ).withColumn("day", func.dayofmonth(func.col("tpep_pickup_datetime"))))

			df.registerTempTable("df")
			query = "select *, ROUND(trip_distance*3600/time_duration_in_secs, 2) as speed from df where time_duration_in_secs > 0"
			df2 = self.spark.sql(query)
			df2.registerTempTable("df2")
			query = "select day, hour, MAX(speed) as maxSpeed from df2 where speed <= 200 group by day, hour"
			result = self.spark.sql(query).collect()
			if result:
				insert_speed_stats_sql = """INSERT INTO {} (year, month, day, hour, maxSpeed)
							                 VALUES (?, ?, ?, ?, ?)""".format(self.speedStatsTableName)
				for elem in result:
					cursor = cursor.execute(
						insert_speed_stats_sql,
						(self.year, month, elem.day, elem.hour, elem.maxSpeed)
					)
				conn.commit()

	def load(self):
		"""
		Creates a SQLite DB to store the results after calculating tip and speed stats
		"""
		if not os.path.exists(os.path.join(ldir, 'data/Loaded')):
			os.makedirs(os.path.join(ldir, 'data/Loaded'))
		self.create_db()
		self.load_tip_stats()
		self.load_speed_stats()
