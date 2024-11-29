import asyncio

from tvCrawler import SimpleMySqlClass


async def ins_col(m_id, info, label):
    db = await SimpleMySqlClass.get_instance('123.56.104.248', 'like_home', 'root', 'r*we9omBG34j')
    select_co_sql = f"SELECT * FROM movie_collection  WHERE cId={info['c_id']}"
    insert_co_sql = f"INSERT INTO movie_collection (cId,label,fileUrl,movieId,sort,text) VALUES ('{info['c_id']}', '{label}','{info['file_url']}',{m_id},{info['sort']},'{info['text']}')"
    result = await db.query_sql(select_co_sql)
    if len(result) == 0:
        c_id = await db.execute(insert_co_sql)
        if c_id > 1:
            print(f"{info['title']}-{label}-{info['text']}插入成功")
        else:
            print(f"{info['title']}-{label}-{info['text']}插入失败")
    else:
        print(f"{info['title']}-{label}-{info['text']}已经存在")


print("哈哈哈")
pass


async def db_update(movie_info):
    tasks = []
    db = await SimpleMySqlClass.get_instance('123.56.104.248', 'like_home', 'root', 'r*we9omBG34j')
    # 查询数据
    select_move_sql = f"SELECT * FROM movie  WHERE mid={movie_info['m_id']}"
    insert_move_sql = f"INSERT INTO movie (mId,title,imgUrl,info) VALUES ('{movie_info['m_id']}', '{movie_info['title']}','{movie_info['image_url']}','{movie_info['info']}')"

    result = await db.query_sql(select_move_sql)
    m_id = None
    if len(result) == 0:
        n_id = await db.execute(insert_move_sql)
        m_id = n_id
        # await ins_col(n_id, movie_info["c_list"])
        # print(n_id)
    else:
        o_id = result[0][0]
        m_id = o_id
        # await ins_col(o_id, movie_info["c_list"])

    for info in movie_info["c_list"]:
        for col in info['list']:
            tasks.append(asyncio.create_task(ins_col(m_id, col, info['label'])))
    await asyncio.gather(*tasks)
    print(movie_info)
