import asyncio
import aiohttp
import lxml.html
import yaml
from tvCrawler.SimpleMySqlClass import SimpleMySqlClass
from tvCrawler.db_update import db_update


def get_movie_list():
    with open('move_list.yml', 'r', encoding='utf-8') as f:
        data = f.read()
        result = yaml.load(data, Loader=yaml.FullLoader)
        return result['movies']


async def get_movie_info(url):
    print(url)  # https://www.xingchenys.com/vod/detail/41/275529
    movie_info = {
        "m_id": url.rsplit("/", 1)[1],
        "title": "",
        "info": "",
        "image_url": '',
        "c_list": []
    }
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(verify_ssl=False, limit=25),
                                     trust_env=True) as session:
        try:
            async with session.get(url=url, timeout=10) as response:
                if response.status == 200:
                    html = await response.text()
                    tree = lxml.html.fromstring(html)
                    info_elements = tree.xpath('/html/body/div[8]/div[3]/div[2]/div[1]/div[1]/text()')
                    image_elements = tree.xpath('/html/body/div[8]/div[2]/div[1]/div[1]/div/img/@src')
                    title_elements = tree.xpath('/html/body/div[8]/div[2]/div[1]/div[2]/h3/text()')
                    label_tags = tree.xpath('/html/body/div[9]/div[2]/div[1]/div/a/text()')
                    antholgy_elements = tree.xpath('/html/body/div[9]/div[2]/div[2]/div')

                    for image_element in image_elements:
                        print(f"Image src: {image_element}")
                        movie_info["image_url"] = image_element
                    for title_element in title_elements:
                        print(f"title: {title_element}")
                        movie_info["title"] = title_element
                    for info in info_elements:
                        movie_info["info"] = info

                    for index, antholgy in enumerate(antholgy_elements):
                        link_map = {
                            "label": label_tags[index].strip(),
                            "index": index,
                            "c_id": "",
                            "list": []
                        }
                        tags_elements = antholgy.xpath('.//ul/li/a')
                        tasks = []
                        for tag_index, tag in enumerate(tags_elements):
                            link_href = tag.get('href').rsplit("/", 1)[1]
                            text = tag.text.strip()
                            el_info = {
                                'title': title_element,
                                'sort': tag_index,
                                "text": text,
                                'c_id': link_href,
                                "href": "https://www.xingchenys.com/openapi/playline/" + link_href,
                                "file_url": None  # 初始化为 None
                            }
                            link_map["c_id"] = link_href
                            link_map["list"].append(el_info)
                            tasks.append(asyncio.create_task(
                                get_file_url("https://www.xingchenys.com/openapi/playline/" + link_href)))

                        # 等待所有 get_file_url 任务完成
                        file_urls = await asyncio.gather(*tasks)
                        for i, file_url in enumerate(file_urls):
                            link_map["list"][i]["file_url"] = file_url

                        movie_info["c_list"].append(link_map)

                else:
                    print(f"Failed to fetch {url}, status code: {response.status}")
        except asyncio.TimeoutError:
            print(f"Timeout occurred for {url}")
        except Exception as e:
            print(f"An error occurred for {url}: {str(e)}")

        # print(movie_info)
        await db_update(movie_info)


async def get_file_url(url):
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(verify_ssl=False, limit=25),
                                     trust_env=True) as session:
        try:
            async with session.get(url=url, timeout=10) as response:
                if response.status == 200:
                    info = await response.json()
                    # print(f"Fetched {html}")
                    return info["info"]['file']  # 返回实际需要的数据
                else:
                    print(f"Failed to fetch {url}, status code: {response.status}")
                    return None
        except asyncio.TimeoutError:
            print(f"Timeout occurred for {url}")
            return None
        except Exception as e:
            print(f"An error occurred for {url}: {str(e)}")
            return None


async def main():
    # 初始化数据库连接
    db = await SimpleMySqlClass.get_instance('123.56.104.248', 'like_home', 'root', 'r*we9omBG34j')
    tasks = [asyncio.create_task(get_movie_info(i)) for i in get_movie_list()]
    await asyncio.gather(*tasks)
    print("这是主程序")
    # 关闭数据库连接
    await db.close()


if __name__ == "__main__":
    host = '123.56.104.248'
    database = "like_home"
    username = "root"
    password = "r*we9omBG34j"
    # SimpleMySqlClass(host,database,username,password)
    asyncio.run(main())
