from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff, avg, desc

# Spark session
spark = SparkSession.builder.appName("DeliveryTimeAnalysis").getOrCreate()

# Load CSV
orders = spark.read.csv("orders.csv", header=True, inferSchema=True)

# Calculate delay (delivery_date - expected_date)
orders = orders.withColumn(
    "delay_days", 
    datediff(col("delivery_date"), col("expected_date"))
)

# 1. Show all orders with delay
print("All Orders:")
orders.show()

# 2. Filter delayed orders (delay > 0)
delayed_orders = orders.filter(col("delay_days") > 0)
print("\nDelayed Orders:")
delayed_orders.show()

# 3. Average delay per city
avg_delay = delayed_orders.groupBy("city").agg(avg("delay_days").alias("avg_delay"))
print("\nAverage Delay Per City:")
avg_delay.show()

# 4. Top 5 worst cities (highest delay)
worst_cities = avg_delay.orderBy(desc("avg_delay")).limit(5)
print("\nTop 5 Worst Delivery Cities:")
worst_cities.show()

# Stop Spark session
spark.stop()
