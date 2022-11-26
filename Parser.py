import requests
from bs4 import BeautifulSoup
import pandas as pd
import re

headers = {'User-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}

url = "https://m2.ru/moskva/nedvizhimost/kupit-kvartiru/?pageNumber="
html_text = requests.get(url, headers=headers).text
soup = BeautifulSoup(html_text, 'lxml')

page_nav = soup.find_all('span', class_='base-module__text___3Z3Vp')
count_page = int(page_nav[len(page_nav)-2].text)


d = {}
id = 0
for l in range(count_page):
    cur_url = url + str(l + 1)
    print('Парсинг страницы №:', (l + 1))
    info = soup.find_all('div', class_="LayoutSnippet__generalInfo")
    count = 0
    for item in range(len(info)):
        ad = info[item]
        id += 1
        d[id] = {}
        price = ad.find('div', class_='Price').text.replace(u'\xa0', '').replace(u'\u2009', '')
        address = ad.find('div', class_='LayoutSnippet__address').text.replace(u'\xa0', '').replace(u'\u2009', '')
        meter = ad.find('div', class_="LayoutSnippet__priceDetail").text.replace(u'\xa0', '').replace(u'\u2009', '')
        d[id]["Цена"] = float(re.sub("[^0-9]", "", price))
        d[id]["Адрес"] = address
        d[id]["Цена за метр"] = float(re.sub("[^0-9]", "", meter))


df = pd.DataFrame(d)
df = df.T


def Distance(address):
    try:
        float(address)
        return float(address)
    except ValueError:
        return 0


df.loc[:, 'Площадь, кв.м'] = df['Цена'] / df['Цена за метр']
df.loc[:, 'Расстояние от центра, км'] = [i[-7:-3].replace(',', '.') for i in df['Адрес']]
df['Расстояние от центра'] = df.apply(lambda x: Distance(x['Расстояние от центра, км']), axis=1)
df = df.drop(columns=['Расстояние от центра, км'], axis=1)
print(df)

table = 'real_estate_price.csv'
df.to_csv(table)
