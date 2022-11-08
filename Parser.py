import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import asyncio
import aiohttp
import time

d = dict()
start_time = time.time()
max_pages = 9999


async def get_data(session, page):
    global d
    global df
    url = f"https://m2.ru/moskva/nedvizhimost/kupit-kvartiru/?pageNumber={page}"

    async with session.get(url=url) as response:
        response_text = await response.text()
        soup = BeautifulSoup(response_text, 'lxml')
        info = soup.find_all('div', class_="LayoutSnippet__generalInfo")
        id = 0
        for l in range(max_pages):
            print('Парсинг страницы №:', page)
            print(url)
            page_nav = soup.find_all('a', class_ = 'base-module__base___7FUQ1 base-module__base-text____L6Jb base-module__button___3TuZj base-module__size-m___2sS92 paginator-button-module__paginator-button___20KL4 paginator-button-module__active___3uii7')
            text = [i.text for i in page_nav]
            a, *text = text
            print(a)
            print(page)
            if int(a) == page:
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
            df.loc[:, 'Площадь, кв.м'] = df['Цена'] / df['Цена за метр']
            df.loc[:, 'Расстояние от центра, км'] = [i[-7:-3].replace(',', '.') for i in df['Адрес']]
            df['Расстояние от центра'] = df.apply(lambda x: Distance(x['Расстояние от центра, км']), axis=1)
            df = df.drop(columns=['Расстояние от центра, км'], axis=1)
            print(df)
            return df


async def gather_data():
    url = "https://m2.ru/moskva/nedvizhimost/kupit-kvartiru/"
    async with aiohttp.ClientSession() as session:
        response = await session.get(url=url)
        soup = BeautifulSoup(await response.text(), "lxml")
        pages_count = soup.find('a', class_ = 'base-module__base___7FUQ1 base-module__base-text____L6Jb base-module__button___3TuZj base-module__size-m___2sS92 paginator-button-module__paginator-button___20KL4 paginator-button-module__active___3uii7')
        text = [i.text for i in pages_count]
        a, *text = text
        tasks = []

        for page in range(1, max_pages + 1):
            task = asyncio.create_task(get_data(session, page))
            tasks.append(task)

        await asyncio.gather(*tasks)
    pass


def Distance(address):
    try:
        float(address)
        return float(address)
    except ValueError:
        return 0


def main():
    asyncio.run(gather_data())
    finish_time = time.time() - start_time
    print(f'Время работы скрипта {finish_time}')
    with open(f"real_estate_price.csv", "w") as file:
        df.to_csv(file)


if __name__ == "__main__":
    main()