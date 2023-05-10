#!/usr/bin/env python
# coding: utf-8

# ### References: 
# 1. https://data36.com/scrape-multiple-web-pages-beautiful-soup-tutorial/
# 2. https://www.youtube.com/watch?v=MH3641s3Roc&ab_channel=Pythonology

# In[1]:


#Imorting requests package to access our urls for scraping and beautiful soup to parse html script
import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import numpy as np
from datetime import datetime
#Importing psycopg2 to access postgresql server and create DB and table
import psycopg2
#Importing sqlalchemy and create_engine to write dataframes to SQL
from sqlalchemy import create_engine


# In[2]:


#Creating empty lists for the elements we scrape
address = [ ]
price = [ ]
bed = [ ]
amenities = [ ]

#Creating forloop to scrape 14 pages from site
for i in range(1,15):
    #set url and create headers to trick our site into thinking we are legitimate users
    url = f"https://www.apartments.com/austin-tx/min-1-bedrooms-800-to-1550/{i}/"
    headers={"User-Agent": "Mozilla/5.0 (X11; CrOS x86_64 12871.102.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.141 Safari/537.36"}
    #define response as requests.get and pass our url and headers through it to access the site and get the html content
    response = requests.get(url, headers=headers)
    response
    response = response.content
    #define our soup object as our html parser we are accessing through beautifulsoup and print our soup title to confirm we are accessing each url
    soup = bs(response,'html.parser')
    print(soup.title)
    #
    for div in soup.find_all('div', class_= "property-address js-url"):
        address.append(div.get_text(strip=True))
    for p in soup.find_all('p', class_= "property-pricing"):
        price.append(p.get_text(strip=True))
    for p in soup.find_all('p', class_= "property-beds"):
        bed.append(p.get_text(strip=True))
    for p in soup.find_all('p', class_="property-amenities"):
        amenities.append(p.get_text(strip=True, separator=", "))


# In[3]:


for div in soup.find_all('div',class_="property-information"):
    name = div.a['aria-label']
name


# In[4]:


print('Total No Addresses:',len(address))
for a in address[:5]:
    print(a)


# In[5]:


print('Total No of Prices:',len(price))
for p in price[:5]:
    print(p)


# In[6]:


print('Total No of Beds:',len(bed))
for b in bed[:10]:
    print(b)


# In[7]:


print('Total No of amenities:',len(amenities))
for l in amenities[:5]:
    print(l)


# In[8]:


apartments = [ ]

for i in range(1,15):
    url = f"https://www.apartments.com/austin-tx/min-1-bedrooms-800-to-1550/{i}/"
    headers={"User-Agent": "Mozilla/5.0 (X11; CrOS x86_64 12871.102.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.141 Safari/537.36"}
    response = requests.get(url, headers=headers)
    response
    response = response.content
    soup = bs(response,'html.parser')
    print(soup.title)
    for listing in soup.find_all('li', class_= "mortar-wrapper"):
        apartment = {}
        for div in listing.find_all('div',class_="property-information"):
            apartment['name'] = div.a['aria-label']
        for div in listing.find_all('div', class_= "property-address js-url"):
            apartment['address'] = div.get_text(strip=True)
        for p in listing.find_all('p', class_= "property-pricing"):
            apartment['price'] = p.get_text(strip=True)
        for p in listing.find_all('p', class_= "property-beds"):
            apartment['beds'] = p.get_text(strip=True)
        for p in listing.find_all('p', class_="property-amenities"):
            apartment['amenities'] = p.get_text(strip=True, separator=", ")
        apartments.append(apartment)


# In[9]:


print(len(apartments))
for apartment in apartments[:5]:
    print(apartment)


# In[10]:


apartments_df = pd.DataFrame(apartments)
apartments_df.head(5)


# In[11]:


apartments_df['name'] = apartments_df['name'].str.replace(r',[^,]*$', '')
apartments_df['name'] = apartments_df['name'].str.replace(r',[^,]*$', '')


# In[12]:


apartments_df


# In[13]:


#Cleaning and Seperating Address Column
apartments_df[['street', 'city', 'state/zip']] = apartments_df['address'].str.split(',', 2, expand=True)
apartments_df[['state', 'poscode']] = apartments_df['state/zip'].str.split(' ', 1, expand=True)
apartments_df[['state', 'zipcode']] = apartments_df['poscode'].str.split(' ', 1, expand=True)
apartments_df = apartments_df.drop(columns=['poscode', 'state/zip'])


# In[14]:


#Cleaning Price Columns
apartments_df[['minprice', 'maxprice']] = apartments_df['price'].str.split('-', 1, expand=True)
apartments_df['minprice'] = apartments_df['minprice'].str.replace('$','',)
apartments_df['maxprice'] = apartments_df['maxprice'].str.replace('$','',)
apartments_df['minprice'] = apartments_df['minprice'].str.replace(',','',)
apartments_df['maxprice'] = apartments_df['maxprice'].str.replace(',','',)
apartments_df = apartments_df.drop(columns=['price'])


# In[15]:


apartments_df['minprice'] = apartments_df['minprice'].str.replace('Call for Rent','0',)
apartments_df['maxprice'] = apartments_df['maxprice'].str.replace('Call for Rent','0',)


# In[16]:


#Cleaning bedroom column
apartments_df[['minbed', 'maxbed']] = apartments_df['beds'].str.split('-', 1, expand=True)
apartments_df['minbed'] = apartments_df['minbed'].str.replace('Beds','',)
apartments_df['minbed'] = apartments_df['minbed'].str.replace('Bed','',)
apartments_df['maxbed'] = apartments_df['maxbed'].str.replace('Beds','',)
apartments_df['maxbed'] = apartments_df['maxbed'].str.replace('Bed','',)
apartments_df = apartments_df.drop(columns=['beds'])


# In[17]:


apartments_df = apartments_df.fillna(0)


# In[18]:


today = datetime.now()
print(today)
date = today.strftime("%d/%m/%Y %H:%M:%S")
date


# In[19]:


apartments_df['building_type'] = 'APT'
apartments_df['date'] = date


# In[20]:


apartments_df.head(5)


# In[21]:


apartments_df['city'].value_counts()


# In[22]:


apartments_df.dtypes


# In[23]:


convert_dict = {'minprice': float,
                'maxprice': float,
                'minbed': int,
                'maxbed': int,
                }
apartments_df = apartments_df.astype(convert_dict)
apartments_df['date'] = apartments_df['date'].astype('datetime64[ns]')
print(apartments_df.dtypes)


# In[24]:


apartments_df


# In[25]:


e = today.strftime("%d.%m.%Y")
print(e)
fname = "./Addresses/addresses_{}.xlsx".format(e)
print(fname)
apartments_df.to_excel(fname)


# I would like to schedule this book to automatically run once a day or maybe once a week to collect apartment data for the austin area. 
# A Keith Galli video I found on this could be super helpful for this: How to schedule and automatically run python code 
# He mentions crontab, a tool that could be helpful in setting thhis up. This sould be fun to dig into and play with and I hope it works
# 

# SAVE TABLE TO SQL

# In[26]:


hostname = 'localhost'
database = 'austin'
username = 'postgres'
pwd = '032197An'
port_id = 5432


# In[27]:


try:    
    conn = psycopg2.connect(
        host = hostname,
        dbname = database,
        user = username,
        password = pwd,
        port = port_id)
except Exception as error:
    print(error)


# In[28]:


cur = conn.cursor()


# In[29]:


from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)


# In[30]:


#Created a new database to in SQL to store the date
#cur.execute('CREATE DATABASE austin')


# In[31]:


#Deleted any tables named austin if they existed
#cur.execute('DROP TABLE IF EXISTS apartments')


# In[32]:


#Created new table in database to collect the data in the dataframe
#cur.execute("CREATE TABLE IF NOT EXISTS apartments (name VARCHAR(256), address VARCHAR(256),amenities VARCHAR(256), street VARCHAR(256), city VARCHAR(256), state VARCHAR(5), zipcode VARCHAR(10), minprice FLOAT, maxprice FLOAT, minbed INT, maxbed INT, building_type VARCHAR(5), date TIMESTAMP)")


# In[33]:


#Need to research more about this,
engine = create_engine('postgresql+psycopg2://postgres:032197An@localhost/austin')
engine


# In[34]:


apartments_df.to_sql('apartments', engine, if_exists='append', index=False)


# In[35]:


conn.commit


# In[36]:


conn.close()


# In[1]:


get_ipython().system('pip install jupytext')


# In[ ]:




