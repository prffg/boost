#!/usr/bin/env python
# coding: utf-8

# In[4]:


import pandas as pd


# In[5]:


df = pd.read_csv('nyt.csv', nrows=100)
df.head(10)


# In[10]:


print(pd.io.sql.get_schema(df, name='yellow_taxi_data'))


# In[12]:


df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)


# In[15]:


from sqlalchemy import create_engine as CE


# In[19]:


engine = CE('postgresql://root:root@localhost:5432/ny_taxi')


# In[21]:


print(pd.io.sql.get_schema(df, name='yellow_taxi_data',con = engine))


# In[22]:


df_full = pd.read_csv('nyt.csv')


# In[26]:


num_filas, num_columnas = df_full.shape
print(f'El archivo CSV tiene {num_filas} filas y {num_columnas} columnas.')


# In[28]:


df_full.describe().round(2)


# In[59]:


df_iter = pd.read_csv('nyt.csv', iterator = True, chunksize = 100000)


# In[60]:


df= next(df_iter)


# In[61]:


df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)


# In[46]:


df.head(0).to_sql(name = 'yellow_taxi_data',con = engine, if_exists='replace')


# In[62]:


get_ipython().run_line_magic('time', "df.to_sql(name = 'yellow_taxi_data',con = engine, if_exists='replace')")


# In[63]:


from time import time


# In[64]:


try:
    while True:
        t_start = time()

        try:
            df = next(df_iter)
        except StopIteration:
            print('No more data to process.')
            break
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        try:
            df.to_sql(name = 'yellow_taxi_data',con = engine, if_exists='append')
        except Exception as e:
            print(f"Error inserting chunk into database: {e}")
            continue

        t_end = time()

        print(f"Inserted a chunk, took {t_end - t_start:.3f} seconds")
except Exception as e:
    print(f"An error ocurred during processing: {e}")
            

