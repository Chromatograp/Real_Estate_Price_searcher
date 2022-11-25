import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import asyncio
import aiohttp
import time
import random
from random import choice
from aiohttp import ClientSession

d = dict()
start_time = time.time()
max_pages = 9999

desktop_agents = ['Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
                 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
                 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
                 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/602.2.14 (KHTML, like Gecko) Version/10.0.1 Safari/602.2.14',
                 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36',
                 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36',
                 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36',
                 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36',
                 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
                 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:50.0) Gecko/20100101 Firefox/50.0']


def random_headers():
    return {'User-Agent': choice(desktop_agents), 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'}


proxies = open('proxies.txt').read().split('\n')


def proxy():
    return 'http://' + choice(proxies)


async def get_data(session, page):
    global d
    global df
    url = f"https://m2.ru/moskva/nedvizhimost/kupit-kvartiru/?pageNumber={page}"

    try:
        async with ClientSession(trust_env=True) as session:
            async with session.get(url=url, headers=random_headers(), proxy=proxy()) as response:
                response_text = await response.text()
                soup = BeautifulSoup(response_text, 'lxml')
                info = soup.find_all('div', class_="LayoutSnippet__generalInfo")
                id = 0
                c = 0

                for l in range(max_pages):
                    try:
                        print('Парсинг страницы №:', page)
                        print(url)
                        page_nav = soup.find_all('a', class_ = 'base-module__base___7FUQ1 base-module__base-text____L6Jb base-module__button___3TuZj base-module__size-m___2sS92 paginator-button-module__paginator-button___20KL4 paginator-button-module__active___3uii7')
                        text = [i.text for i in page_nav]
                        print(text)
                        c += 1
                        print(c)
                        a, *text = text
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
                    except ValueError:
                        pass
                    try:
                        df = pd.DataFrame(d)
                        df = df.T
                        df.loc[:, 'Площадь, кв.м'] = df['Цена'] / df['Цена за метр']
                        df.loc[:, 'Расстояние от центра, км'] = [i[-7:-3].replace(',', '.') for i in df['Адрес']]
                        df['Расстояние от центра'] = df.apply(lambda x: Distance(x['Расстояние от центра, км']), axis=1)
                        df = df.drop(columns=['Расстояние от центра, км'], axis=1)
                    except KeyError:
                        pass
                    await asyncio.sleep(random.uniform(3, 10))
                return df
    except ConnectionRefusedError:
        pass



async def gather_data():
    url = "https://m2.ru/moskva/nedvizhimost/kupit-kvartiru/"
    async with aiohttp.ClientSession(trust_env=True) as session:
        response = await session.get(url=url)
        await asyncio.sleep(random.uniform(3, 10))
        soup = BeautifulSoup(await response.text(), "lxml")
        pages_count = soup.find('a', class_ = 'base-module__base___7FUQ1 base-module__base-text____L6Jb base-module__button___3TuZj base-module__size-m___2sS92 paginator-button-module__paginator-button___20KL4 paginator-button-module__active___3uii7')
        tasks = []

        count = 0
        for page in range(1, max_pages):
            task = asyncio.create_task(get_data(session, page))
            tasks.append(task)
            count += 1
            if count == max_pages:
                break

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