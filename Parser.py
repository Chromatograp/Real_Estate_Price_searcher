import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import json

url = "https://m2.ru/moskva/nedvizhimost/kupit-kvartiru/?pageNumber="

max_pages = 9999


d = {}
id = 0
for l in range(max_pages):
    cur_url = url + str(l + 1)
    print('Парсинг страницы №:', (l + 1))
    html_text = requests.get(cur_url).text
    soup = BeautifulSoup(html_text, 'lxml')
    info = soup.find_all('div', class_="LayoutSnippet__generalInfo")

    page_nav = soup.find_all('a', class_ = 'base-module__base___7FUQ1 base-module__base-text____L6Jb base-module__button___3TuZj base-module__size-m___2sS92 paginator-button-module__paginator-button___20KL4 paginator-button-module__active___3uii7')
    text = [i.text for i in page_nav]
    href = [i.get('href') for i in page_nav]
    a, *text = text
    print(href)
    print(a)
    if a == str(l + 1):
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
    else:
        print("Максимальный номер страницы: %d" % (l + 1))
        break


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