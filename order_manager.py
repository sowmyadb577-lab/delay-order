import csv
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff, avg, desc

def display_menu():
    print("\n" + "="*60)
    print("📦 DELIVERY ORDER MANAGEMENT SYSTEM")
    print("="*60)
    print("1. ➕ Add New Order")
    print("2. 📋 View All Orders")
    print("3. 📊 Run Analysis & Generate Dashboard")
    print("4. 🚪 Exit")
    print("="*60)

def add_order():
    print("\n--- Add New Order ---")
    
    try:
        order_id = input("Enter Order ID: ").strip()
        city = input("Enter City: ").strip()
        expected_date = input("Enter Expected Date (YYYY-MM-DD): ").strip()
        delivery_date = input("Enter Delivery Date (YYYY-MM-DD): ").strip()
        
        # Validate dates
        datetime.strptime(expected_date, '%Y-%m-%d')
        datetime.strptime(delivery_date, '%Y-%m-%d')
        
        # Append to CSV
        with open('orders.csv', 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([order_id, city, expected_date, delivery_date])
        
        print(f"\n✅ Order {order_id} added successfully!")
        
    except ValueError:
        print("\n❌ Invalid date format! Please use YYYY-MM-DD")
    except Exception as e:
        print(f"\n❌ Error: {e}")

def view_orders():
    print("\n--- All Orders ---")
    
    try:
        with open('orders.csv', 'r') as f:
            reader = csv.reader(f)
            headers = next(reader)
            
            print(f"\n{headers[0]:<12} {headers[1]:<15} {headers[2]:<15} {headers[3]:<15}")
            print("-" * 60)
            
            count = 0
            for row in reader:
                print(f"{row[0]:<12} {row[1]:<15} {row[2]:<15} {row[3]:<15}")
                count += 1
            
            print(f"\nTotal Orders: {count}")
    
    except FileNotFoundError:
        print("❌ orders.csv file not found!")
    except Exception as e:
        print(f"❌ Error: {e}")

def run_analysis():
    print("\n--- Running Analysis ---")
    print("⏳ Processing data with PySpark...")
    
    try:
        # Spark session
        spark = SparkSession.builder.appName("DeliveryTimeAnalysis").getOrCreate()
        
        # Load CSV
        orders = spark.read.csv("orders.csv", header=True, inferSchema=True)
        
        # Calculate delay
        orders = orders.withColumn(
            "delay_days", 
            datediff(col("delivery_date"), col("expected_date"))
        )
        
        # Display console results
        print("\n" + "="*60)
        print("ALL ORDERS:")
        print("="*60)
        orders.show(truncate=False)
        
        # Filter delayed orders
        delayed_orders = orders.filter(col("delay_days") > 0)
        delayed_count = delayed_orders.count()
        total_count = orders.count()
        on_time_count = total_count - delayed_count
        
        print("\n" + "="*60)
        print("DELAYED ORDERS:")
        print("="*60)
        delayed_orders.show(truncate=False)
        
        # Average delay per city
        avg_delay = delayed_orders.groupBy("city").agg(avg("delay_days").alias("avg_delay"))
        city_stats = avg_delay.orderBy(desc("avg_delay"))
        
        print("\n" + "="*60)
        print("AVERAGE DELAY PER CITY:")
        print("="*60)
        city_stats.show(truncate=False)
        
        # Summary statistics
        print("\n" + "="*60)
        print("📊 SUMMARY STATISTICS:")
        print("="*60)
        print(f"Total Orders: {total_count}")
        print(f"Delayed Orders: {delayed_count}")
        print(f"On-Time Orders: {on_time_count}")
        print(f"Delay Rate: {(delayed_count/total_count*100):.1f}%")
        
        # Generate HTML Dashboard
        print("\n⏳ Generating HTML Dashboard...")
        
        # Collect data for HTML
        all_orders_data = [{
            "order_id": row.order_id,
            "city": row.city,
            "expected_date": str(row.expected_date),
            "delivery_date": str(row.delivery_date),
            "delay_days": row.delay_days
        } for row in orders.collect()]
        
        city_data = [{
            "city": row.city,
            "avg_delay": round(row.avg_delay, 2)
        } for row in city_stats.collect()]
        
        # Generate HTML (simplified inline)
        html_content = generate_html_dashboard(total_count, delayed_count, on_time_count, city_data, all_orders_data)
        
        with open("dashboard.html", "w", encoding="utf-8") as f:
            f.write(html_content)
        
        print("✅ Dashboard generated successfully!")
        print("📊 Opening dashboard.html in browser...")
        
        # Open dashboard
        os.system('start dashboard.html')
        
        spark.stop()
        
    except Exception as e:
        print(f"❌ Error running analysis: {e}")

def generate_html_dashboard(total_count, delayed_count, on_time_count, city_data, all_orders_data):
    # Build HTML with statistics
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Delivery Analysis Dashboard</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); min-height: 100vh; padding: 20px; }}
        .container {{ max-width: 1400px; margin: 0 auto; }}
        header {{ text-align: center; color: white; margin-bottom: 40px; }}
        h1 {{ font-size: 2.5em; margin-bottom: 10px; text-shadow: 2px 2px 4px rgba(0,0,0,0.3); }}
        .subtitle {{ font-size: 1.2em; opacity: 0.9; }}
        .stats-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 20px; margin-bottom: 30px; }}
        .stat-card {{ background: white; padding: 30px; border-radius: 15px; box-shadow: 0 10px 30px rgba(0,0,0,0.2); text-align: center; transition: transform 0.3s ease; }}
        .stat-card:hover {{ transform: translateY(-5px); }}
        .stat-number {{ font-size: 3em; font-weight: bold; margin: 10px 0; }}
        .stat-label {{ color: #666; font-size: 1.1em; text-transform: uppercase; letter-spacing: 1px; }}
        .stat-card.total .stat-number {{ color: #667eea; }}
        .stat-card.delayed .stat-number {{ color: #e74c3c; }}
        .stat-card.ontime .stat-number {{ color: #27ae60; }}
        .content-section {{ background: white; border-radius: 15px; padding: 30px; margin-bottom: 30px; box-shadow: 0 10px 30px rgba(0,0,0,0.2); }}
        h2 {{ color: #333; margin-bottom: 20px; font-size: 1.8em; border-bottom: 3px solid #667eea; padding-bottom: 10px; }}
        .city-bars {{ margin-top: 20px; }}
        .city-bar-item {{ margin-bottom: 20px; }}
        .city-name {{ display: flex; justify-content: space-between; margin-bottom: 8px; font-weight: 600; color: #333; }}
        .bar-background {{ background: #f0f0f0; border-radius: 10px; height: 40px; overflow: hidden; position: relative; }}
        .bar-fill {{ height: 100%; background: linear-gradient(90deg, #667eea 0%, #764ba2 100%); border-radius: 10px; display: flex; align-items: center; justify-content: flex-end; padding-right: 15px; color: white; font-weight: bold; transition: width 1s ease; }}
        .bar-fill.worst {{ background: linear-gradient(90deg, #e74c3c 0%, #c0392b 100%); }}
        .bar-fill.bad {{ background: linear-gradient(90deg, #f39c12 0%, #e67e22 100%); }}
        .bar-fill.good {{ background: linear-gradient(90deg, #27ae60 0%, #229954 100%); }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
        th {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 15px; text-align: left; font-weight: 600; text-transform: uppercase; letter-spacing: 1px; }}
        td {{ padding: 12px 15px; border-bottom: 1px solid #ddd; }}
        tr:hover {{ background-color: #f5f5f5; }}
        .delay-badge {{ display: inline-block; padding: 5px 12px; border-radius: 20px; font-weight: bold; font-size: 0.9em; }}
        .delay-badge.positive {{ background-color: #e74c3c; color: white; }}
        .delay-badge.zero {{ background-color: #27ae60; color: white; }}
        .footer {{ text-align: center; color: white; margin-top: 40px; padding: 20px; opacity: 0.8; }}
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
            <div class="city-bars">"""
    
    max_delay = max([city["avg_delay"] for city in city_data]) if city_data else 1
    for i, city in enumerate(city_data):
        width_percent = (city["avg_delay"] / max_delay) * 100
        bar_class = "worst" if i == 0 else "bad" if i < len(city_data) // 2 else "good"
        html += f"""
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
                </div>"""
    
    html += """
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
                <tbody>"""
    
    for order in all_orders_data:
        delay_class = "positive" if order["delay_days"] > 0 else "zero"
        delay_text = f"+{order['delay_days']}" if order["delay_days"] > 0 else str(order["delay_days"])
        html += f"""
                    <tr>
                        <td><strong>{order["order_id"]}</strong></td>
                        <td>{order["city"]}</td>
                        <td>{order["expected_date"]}</td>
                        <td>{order["delivery_date"]}</td>
                        <td><span class="delay-badge {delay_class}">{delay_text}</span></td>
                    </tr>"""
    
    html += """
                </tbody>
            </table>
        </div>
        <div class="footer">
            <p>Generated with PySpark | Delivery Analysis System</p>
        </div>
    </div>
    <script>
        window.addEventListener('load', function() {
            const bars = document.querySelectorAll('.bar-fill');
            bars.forEach(bar => {
                const width = bar.style.width;
                bar.style.width = '0%';
                setTimeout(() => { bar.style.width = width; }, 100);
            });
        });
    </script>
</body>
</html>"""
    
    return html

def main():
    while True:
        display_menu()
        choice = input("\nEnter your choice (1-4): ").strip()
        
        if choice == '1':
            add_order()
        elif choice == '2':
            view_orders()
        elif choice == '3':
            run_analysis()
        elif choice == '4':
            print("\n👋 Thank you for using the Delivery Order Management System!")
            break
        else:
            print("\n❌ Invalid choice! Please enter 1-4")
        
        input("\nPress Enter to continue...")

if __name__ == "__main__":
    main()
