from pyspark import SparkContext

# Initialize Spark Context
sc = SparkContext("local", "RDD Pipeline Lab")

# Sample Dataset (List of Transactions)
# Format: (TransactionID, Product, Category, Amount, Date)
data = [
    (1, "Laptop", "Electronics", 1200.00, "2025-01-01"),
    (2, "Smartphone", "Electronics", 800.00, "2025-01-02"),
    (3, "Washing Machine", "Home Appliances", 1500.00, "2025-01-03"),
    (4, "Shirt", "Clothing", 300.00, "2025-01-04"),
    (5, "Refrigerator", "Home Appliances", 1000.00, "2025-01-05"),
    (6, "Jeans", "Clothing", 700.00, "2025-01-06"),
    (7, "Headphones", "Electronics", 200.00, "2025-01-07"),
    (8, "Television", "Electronics", 1500.00, "2025-01-08"),
]

# Create an RDD from the list
rdd = sc.parallelize(data)

# Step 1: Filter transactions where the amount > 500
filtered_rdd = rdd.filter(lambda record: record[3] > 500)

# Step 2: Map data to extract (Category, Amount)
mapped_rdd = filtered_rdd.map(lambda record: (record[2], record[3]))

# Step 3: Reduce by key to calculate total amount spent per category
reduced_rdd = mapped_rdd.reduceByKey(lambda x, y: x + y)

# Step 4: Sort categories by total amount spent in descending order
sorted_rdd = reduced_rdd.sortBy(lambda record: record[1], ascending=False)

# Step 5: FlatMap to create detailed output strings
detailed_rdd = sorted_rdd.flatMap(lambda record: [f"Category: {record[0]}, Total: {record[1]}"])

# Collect and Print Results
results = detailed_rdd.collect()
for result in results:
    print(result)

# Stop Spark Context
sc.stop()
