from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff, avg, desc
import json

# Spark session
spark = SparkSession.builder.appName("DeliveryTimeAnalysis").getOrCreate()

# Load CSV
orders = spark.read.csv("orders.csv", header=True, inferSchema=True)

# Calculate delay (delivery_date - expected_date)
orders = orders.withColumn(
    "delay_days", 
    datediff(col("delivery_date"), col("expected_date"))
)

# Collect all orders
all_orders = orders.collect()
all_orders_data = [{
    "order_id": row.order_id,
    "city": row.city,
    "expected_date": str(row.expected_date),
    "delivery_date": str(row.delivery_date),
    "delay_days": row.delay_days
} for row in all_orders]

# Filter delayed orders
delayed_orders = orders.filter(col("delay_days") > 0)
delayed_count = delayed_orders.count()
total_count = orders.count()
on_time_count = total_count - delayed_count

# Average delay per city
avg_delay = delayed_orders.groupBy("city").agg(avg("delay_days").alias("avg_delay"))
city_stats = avg_delay.orderBy(desc("avg_delay")).collect()
city_data = [{
    "city": row.city,
    "avg_delay": round(row.avg_delay, 2)
} for row in city_stats]

# Prepare data for HTML
html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Delivery Time Analysis Dashboard</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }}

        .container {{
            max-width: 1400px;
            margin: 0 auto;
        }}

        header {{
            text-align: center;
            color: white;
            margin-bottom: 40px;
        }}

        h1 {{
            font-size: 2.5em;
            margin-bottom: 10px;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        }}

        .subtitle {{
            font-size: 1.2em;
            opacity: 0.9;
        }}

        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}

        .stat-card {{
            background: white;
            padding: 30px;
            border-radius: 15px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
            text-align: center;
            transition: transform 0.3s ease;
        }}

        .stat-card:hover {{
            transform: translateY(-5px);
        }}

        .stat-number {{
            font-size: 3em;
            font-weight: bold;
            margin: 10px 0;
        }}

        .stat-label {{
            color: #666;
            font-size: 1.1em;
            text-transform: uppercase;
            letter-spacing: 1px;
        }}

        .stat-card.total .stat-number {{
            color: #667eea;
        }}

        .stat-card.delayed .stat-number {{
            color: #e74c3c;
        }}

        .stat-card.ontime .stat-number {{
            color: #27ae60;
        }}

        .content-section {{
            background: white;
            border-radius: 15px;
            padding: 30px;
            margin-bottom: 30px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
        }}

        h2 {{
            color: #333;
            margin-bottom: 20px;
            font-size: 1.8em;
            border-bottom: 3px solid #667eea;
            padding-bottom: 10px;
        }}

        .city-bars {{
            margin-top: 20px;
        }}

        .city-bar-item {{
            margin-bottom: 20px;
        }}

        .city-name {{
            display: flex;
            justify-content: space-between;
            margin-bottom: 8px;
            font-weight: 600;
            color: #333;
        }}

        .bar-background {{
            background: #f0f0f0;
            border-radius: 10px;
            height: 40px;
            overflow: hidden;
            position: relative;
        }}

        .bar-fill {{
            height: 100%;
            background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
            border-radius: 10px;
            display: flex;
            align-items: center;
            justify-content: flex-end;
            padding-right: 15px;
            color: white;
            font-weight: bold;
            transition: width 1s ease;
        }}

        .bar-fill.worst {{
            background: linear-gradient(90deg, #e74c3c 0%, #c0392b 100%);
        }}

        .bar-fill.bad {{
            background: linear-gradient(90deg, #f39c12 0%, #e67e22 100%);
        }}

        .bar-fill.good {{
            background: linear-gradient(90deg, #27ae60 0%, #229954 100%);
        }}

        table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 20px;
        }}

        th {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 15px;
            text-align: left;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 1px;
        }}

        td {{
            padding: 12px 15px;
            border-bottom: 1px solid #ddd;
        }}

        tr:hover {{
            background-color: #f5f5f5;
        }}

        .delay-badge {{
            display: inline-block;
            padding: 5px 12px;
            border-radius: 20px;
            font-weight: bold;
            font-size: 0.9em;
        }}

        .delay-badge.positive {{
            background-color: #e74c3c;
            color: white;
        }}

        .delay-badge.zero {{
            background-color: #27ae60;
            color: white;
        }}

        .delay-badge.negative {{
            background-color: #3498db;
            color: white;
        }}

        @media (max-width: 768px) {{
            h1 {{
                font-size: 2em;
            }}

            .stats-grid {{
                grid-template-columns: 1fr;
            }}

            table {{
                font-size: 0.9em;
            }}

            th, td {{
                padding: 10px;
            }}
        }}

        .footer {{
            text-align: center;
            color: white;
            margin-top: 40px;
            padding: 20px;
            opacity: 0.8;
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>📦 Delivery Time Analysis Dashboard</h1>
            <p class="subtitle">Performance Overview & City Statistics</p>
        </header>

        <div class="stats-grid">
            <div class="stat-card total">
                <div class="stat-label">Total Orders</div>
                <div class="stat-number">{total_count}</div>
            </div>
            <div class="stat-card delayed">
                <div class="stat-label">Delayed Orders</div>
                <div class="stat-number">{delayed_count}</div>
            </div>
            <div class="stat-card ontime">
                <div class="stat-label">On-Time Orders</div>
                <div class="stat-number">{on_time_count}</div>
            </div>
        </div>

        <div class="content-section">
            <h2>🏆 Cities Ranked by Average Delay</h2>
            <div class="city-bars">
"""

# Add city bars
max_delay = max([city["avg_delay"] for city in city_data]) if city_data else 1
for i, city in enumerate(city_data):
    width_percent = (city["avg_delay"] / max_delay) * 100
    bar_class = "worst" if i == 0 else "bad" if i < len(city_data) // 2 else "good"
    html_content += f"""
                <div class="city-bar-item">
                    <div class="city-name">
                        <span>#{i+1} {city["city"]}</span>
                        <span>{city["avg_delay"]} days avg delay</span>
                    </div>
                    <div class="bar-background">
                        <div class="bar-fill {bar_class}" style="width: {width_percent}%">
                            {city["avg_delay"]} days
                        </div>
                    </div>
                </div>
"""

html_content += """
            </div>
        </div>

        <div class="content-section">
            <h2>📋 All Orders Details</h2>
            <table>
                <thead>
                    <tr>
                        <th>Order ID</th>
                        <th>City</th>
                        <th>Expected Date</th>
                        <th>Delivery Date</th>
                        <th>Delay (Days)</th>
                    </tr>
                </thead>
                <tbody>
"""

# Add order rows
for order in all_orders_data:
    delay_class = "positive" if order["delay_days"] > 0 else "zero"
    delay_text = f"+{order['delay_days']}" if order["delay_days"] > 0 else str(order["delay_days"])
    html_content += f"""
                    <tr>
                        <td><strong>{order["order_id"]}</strong></td>
                        <td>{order["city"]}</td>
                        <td>{order["expected_date"]}</td>
                        <td>{order["delivery_date"]}</td>
                        <td><span class="delay-badge {delay_class}">{delay_text}</span></td>
                    </tr>
"""

html_content += """
                </tbody>
            </table>
        </div>

        <div class="footer">
            <p>Generated with PySpark | Delivery Analysis System</p>
        </div>
    </div>

    <script>
        // Animate bars on load
        window.addEventListener('load', function() {
            const bars = document.querySelectorAll('.bar-fill');
            bars.forEach(bar => {
                const width = bar.style.width;
                bar.style.width = '0%';
                setTimeout(() => {
                    bar.style.width = width;
                }, 100);
            });
        });
    </script>
</body>
</html>
"""

# Write HTML file
with open("dashboard.html", "w", encoding="utf-8") as f:
    f.write(html_content)

print("✅ Dashboard generated successfully!")
print("📊 Open 'dashboard.html' in your browser to view the results")

# Stop Spark session
spark.stop()
