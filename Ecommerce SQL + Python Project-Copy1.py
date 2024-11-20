#!/usr/bin/env python
# coding: utf-8

# In[1]:


pip install mysql-connector-python


# In[1]:


pip install seaborn


# In[3]:


import pandas as pd
import mysql.connector
import os

# List of CSV files and their corresponding table names
csv_files = [
    ('customers.csv', 'customers'),
    ('orders.csv', 'orders'),
    ('sellers.csv', 'sellers'),
    ('products.csv', 'products'),
    ('geolocation.csv', 'geolocation'),
    ('payments.csv', 'payments'),
    ('order_items.csv','order_items')# Added payments.csv for specific handling
]

# Connect to the MySQL database
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='973066',
    database='ecommerce'
)
cursor = conn.cursor()

# Folder containing the CSV files
folder_path = 'D:\Ecommerce project python + mysql'

def get_sql_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return 'INT'
    elif pd.api.types.is_float_dtype(dtype):
        return 'FLOAT'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'DATETIME'
    else:
        return 'TEXT'

for csv_file, table_name in csv_files:
    file_path = os.path.join(folder_path, csv_file)
    
    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(file_path)
    
    # Replace NaN with None to handle SQL NULL
    df = df.where(pd.notnull(df), None)
    
    # Debugging: Check for NaN values
    print(f"Processing {csv_file}")
    print(f"NaN values before replacement:\n{df.isnull().sum()}\n")

    # Clean column names
    df.columns = [col.replace(' ', '').replace('-', '').replace('.', '_') for col in df.columns]

    # Generate the CREATE TABLE statement with appropriate data types
    columns = ', '.join([f'{col} {get_sql_type(df[col].dtype)}' for col in df.columns])
    create_table_query = f'CREATE TABLE IF NOT EXISTS {table_name} ({columns})'
    cursor.execute(create_table_query)

    # Insert DataFrame data into the MySQL table
    for _, row in df.iterrows():
        # Convert row to tuple and handle NaN/None explicitly
        values = tuple(None if pd.isna(x) else x for x in row)
        sql = f"INSERT INTO {table_name} ({', '.join(['' + col + '' for col in df.columns])}) VALUES ({', '.join(['%s'] * len(row))})"
        cursor.execute(sql, values)

    # Commit the transaction for the current CSV file
    conn.commit()

# Close the connection
conn.close()


# In[3]:


import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import mysql.connector
import numpy as np

db = mysql.connector.connect(host = 'localhost',
                             username = 'root',
                             password = '973066',
                             database = 'ecommerce')

cur = db.cursor()


# ## Q.1 List all the unique cities where customers are located.

# In[24]:



query =  """ select distinct customer_city from customers"""

cur.execute(query)

data = cur.fetchall()

df = pd.DataFrame(data,columns = ["Cities"])
df


# ## Q.2. Count the no. of orders placed in 2017

# In[20]:


query = """ select count(order_id) from orders where year(order_purchase_timestamp)= 2017 """

cur.execute(query)

data = cur.fetchall()

"Total orders placed in 2017 are", data[0][0]


# ## Q.3) Find the total sales per category

# In[30]:


query = """ SELECT 
    upper(products.productcategory)category,
    ROUND(SUM(payments.payment_value), 2) sales
FROM
    products
        JOIN
    order_items ON products.product_id = order_items.product_id
        JOIN
    payments ON payments.order_id = order_items.order_id
GROUP BY category """

cur.execute(query)

data = cur.fetchall()

df = pd.DataFrame(data,columns = ["Category","Sales"])
df


#  ## Q.4) Calculate the percentage of orders that were paid in installments
# 

# In[31]:



query = """ SELECT 
    (SUM(CASE
        WHEN payment_installments >= 1 THEN 1
        ELSE 0
    END)) / COUNT(*) * 100 AS percent_of_orders_in_installmetns
FROM
    payments """

cur.execute(query)

data = cur.fetchall()

"Percentage_of_orders in installments is",data[0][0]


#  ## Q.5) Count the number of customers from each state.

# In[60]:


query =  """ SELECT 
customer_state, COUNT(customer_id)
FROM
    customers
GROUP BY customer_state """

cur.execute(query)

data = cur.fetchall()

df = pd.DataFrame(data,columns = ["State","Count_of_Customers"])
df = df.sort_values(by = "Count_of_Customers",ascending = False)

# Vizualisation
plt.figure(figsize = (8,4))
plt.title("Count the number of customers from each state",fontsize = 15)
plt.bar(df["State"], df["Count_of_Customers"],color = 'yellow',label = "No.of Customers")
plt.xlabel("States")
plt.ylabel("Count_of_Customers")
plt.xticks(rotation = 90)
plt.legend()
plt.show() 
 


           


# ## Q.6) Calculate the number of orders per month in 2018

# In[32]:


query =  """ SELECT 
    MONTHNAME(order_purchase_timestamp) months,
    COUNT(order_id) order_count
FROM
    orders
WHERE
    YEAR(order_purchase_timestamp) = 2018
GROUP BY months"""

cur.execute(query)

data = cur.fetchall()
df = pd.DataFrame(data,columns = ["Months","Orders_Count"])
o = ["January","February","March","April","May","June","July","August","September","October"]

plt.figure(figsize = (10,4))
plt.title("Count of Orders Per Month in 2018",fontsize = 15)
ax = sns.barplot(x = df["Orders_Count"], y = df["Months"],data = df,orient = 'h',order = o,color = "aqua",
            alpha = 0.7)
ax.bar_label(ax.containers[0])
plt.xlabel("Orders_Count")
plt.ylabel("Months")

plt.show()


# ## Q.7) Find the average number of products per order, grouped by customer city.

# In[49]:


query =  """ with count_per_order as
(select orders.order_id, orders.customer_id,
    count(order_items.order_id) as oc
from orders join order_items
    on orders.order_id = order_items.order_id
group by orders.order_id,orders.customer_id)
   
   select customers.customer_city,round(avg(count_per_order.oc),2) as average_orders
from customers join count_per_order
on customers.customer_id = count_per_order.customer_id
group by customers.customer_city order by average_orders desc"""

cur.execute(query)

data = cur.fetchall()
df = pd.DataFrame(data,columns = ["Customer_City","Average_Orders"])
df.head(10)


# ## Q.8) Calculate the percentage of total revenue contributed by each product category

# In[50]:


query = """ SELECT 
    products.productcategory category,
    round((SUM(payments.payment_value)/( select sum(payment_value) from payments)) * 100,2) sales_percentage
FROM
    products
        JOIN
    order_items ON products.product_id = order_items.product_id
        JOIN
    payments ON payments.order_id = order_items.order_id
GROUP BY category order by sales_percentage desc"""

cur.execute(query)

data = cur.fetchall()

df = pd.DataFrame(data,columns = ["Category","Sales_Percentage"])
df


# ## Q.9) Identify the correlation between product price and the number of times a product has been purchased.

# In[59]:


query = """ SELECT 
    products.productcategory,
    COUNT(order_items.product_id) as order_count,
    ROUND(AVG(order_items.price), 2) as price
FROM
    products
        JOIN
    order_items ON products.product_id = order_items.product_id
GROUP BY products.productcategory """

cur.execute(query)

data = cur.fetchall()

df = pd.DataFrame(data,columns = ["Product_category","Order_count","Price"])

arr1 = df["Order_count"]
arr2 = df["Price"]

a = np.corrcoef([arr1,arr2])
print("The correlation between price and the number of times a product has been purchased is",a [0][1])


#  ## Q.10) Calculate the total revenue generated by each seller, and revenue by each them.

# In[90]:


query = """  select *,dense_rank() over(order by revenue desc) as rnk from
 (select order_items.seller_id,round(sum(payments.payment_value),2) as revenue
 from order_items
 join payments on order_items.order_id = payments.order_id
 group by order_items.seller_id)as a;"""

cur.execute(query)

data = cur.fetchall()

df = pd.DataFrame(data,columns = ["Seller_id","Revenue","Rank"])
df = df.head(5)

plt.title("Total revenue generated by each seller, and revenue by each them",fontsize = 12)
plt.bar(df[ "Seller_id"], df["Revenue"],color = 'orangered',)
plt.xticks(rotation = 90)
plt.xlabel("Seller_id")
plt.ylabel("Revenue")

for i in range(5):
    plt.text(i,df["Revenue"][i],df["Revenue"][i],ha= 'center',va= 'bottom')

plt.show()


# ## Q.11) Calculate the moving average of order values for each customer over their order history.

# In[92]:


query = """  SELECT customer_id,order_purchase_timestamp,payment,
AVG(payment) OVER (PARTITION BY customer_id ORDER BY order_purchase_timestamp
ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg
FROM
(SELECT orders.customer_id,orders.order_purchase_timestamp,
payments.payment_value as payment
FROM payments JOIN orders
ON payments.order_id = orders.order_id) AS a"""

cur.execute(query)

data = cur.fetchall()

df = pd.DataFrame(data,columns = ["Customer_id","Order_Purchase_Timestamp","Payent","Moving_avg"])
df


# ## Q.12) Calculate the cumulative sales per month for each year.

# In[95]:


query = """ SELECT years,months,SUM(payment)
OVER(ORDER BY years, months) AS cumulative_sales FROM
(SELECT YEAR(orders.order_purchase_timestamp) as years,
MONTH(orders.order_purchase_timestamp) as months,
ROUND(SUM(payments.payment_value),2) as payment FROM orders
JOIN payments ON 
orders.order_id = payments.order_id
GROUP BY years,months ORDER BY years,months) AS a"""

cur.execute(query)

data = cur.fetchall()

df = pd.DataFrame(data,columns = ["Years","Months","Cumulative_sales"])


# ## Q.12) Calculate the year-over-year growth rate of total sales

# In[30]:


query = """ SELECT YEAR(orders.order_purchase_timestamp) as years,
ROUND(SUM(payments.payment_value),2) as payment FROM orders
JOIN payments ON 
orders.order_id = payments.order_id
GROUP BY years ORDER BY years """

cur.execute(query)

data = cur.fetchall()

df = pd.DataFrame(data,columns = ["Years","YoY % of Sales"])


sns.barplot(x = "Years", y = "YoY % of Sales",data = df,label = "Sales",color = "lime")
plt.title("Year-over-year growth rate of total sales",fontsize = 15)
plt.xlabel("Years")
plt.ylabel("YoY % of Sales")
plt.legend()

for i in range(3):
    plt.text(i,df["YoY % of Sales"][i],df["YoY % of Sales"][i],ha= 'center',va= 'bottom')
plt.show()


# ## Q.13) Calculate the retention rate of customers, defined as the percentage of customers who make another purchase within 6 months

# In[35]:


query = """ WITH a AS
(SELECT customers.customer_id,
MIN(orders.order_purchase_timestamp) AS first_order
FROM customers JOIN orders
ON customers.customer_id = orders.customer_id
GROUP BY customers.customer_id),
b AS (SELECT a.customer_id,COUNT(DISTINCT orders.order_purchase_timestamp)
FROM a JOIN orders
ON orders.customer_id = a.customer_id
AND orders.order_purchase_timestamp > first_order
AND orders.order_purchase_timestamp <
DATE_ADD(first_order,interval 6 month)
GROUP BY a.customer_id)

SELECT 100 * (COUNT(DISTINCT a.customer_id)/ COUNT(DISTINCT b.customer_id))
FROM a LEFT JOIN b 
ON a.customer_id = b.customer_id """

cur.execute(query)

data = cur.fetchall()

"None of the Custoemrs who make another purchase within 6 months",data[0][0]


# ## Q.14) Identify the top 3 customers who spent the most money in each year.

# In[69]:


query = """ SELECT years,customer_id,payment,d_rank 
FROM
(SELECT YEAR(orders.order_purchase_timestamp) years ,customer_id,
sum(payments.payment_value)payment,
DENSE_RANK() OVER(PARTITION BY YEAR(orders.order_purchase_timestamp) ORDER BY SUM(payments.payment_value) DESC) d_rank
FROM orders JOIN payments
ON payments.order_id = orders.order_id
GROUP BY YEAR(orders.order_purchase_timestamp),orders.customer_id) as a
WHERE d_rank <= 3 """

cur.execute(query)
data = cur.fetchall()

plt.figure(figsize=(8,6))
plt.title("The top 3 customers who spent the most money in each year",fontsize = 15)
df = pd.DataFrame(data,columns = ["years","id","payment","rank"])
sns.barplot(x = "id", y = "payment",data = df,hue = "years")
plt.xticks(rotation = 90)
plt.show()


# In[ ]:





# In[ ]:





# In[ ]:




