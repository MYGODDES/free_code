# !/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import time
import pandas as pd
from DrissionPage import ChromiumPage
from DrissionPage import ChromiumOptions


co = ChromiumOptions()
co.set_headless(False)
co.set_argument('--user-agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36')

saveFileName = "测试文件" # 保存文件名称

def parse_data(dataJson,username):
    instructions = dataJson["data"]["user"]["result"]["timeline_v2"]["timeline"]["instructions"]
    entries = []
    for i in instructions:
        entries = i.get("entries")
    resultList = []
    if entries:
        for e in entries:
            try:
                text = e["content"]["itemContent"]["tweet_results"]["result"]["legacy"]["full_text"]
                item = {
                    "用户名":username,
                    "文本":text
                }
                print(item)
                resultList.append(item)
            except:
                pass
    save_data(resultList)


def save_data(resultList):
    if resultList:
        df = pd.DataFrame(resultList)
        if not os.path.exists(f'./{saveFileName}.csv'):
            df.to_csv(f'./{saveFileName}.csv', index=False, mode='a', sep=",", encoding="utf_8_sig")
        else:
            df.to_csv(f'./{saveFileName}.csv', index=False, mode='a', sep=",", encoding="utf_8_sig", header=False)
        print("保存成功")

def get_data(username):
    # 创建页面对象
    page = ChromiumPage(addr_driver_opts=co)
    page.set.window.maximized()
    page.listen.start('UserTweets')  # 开始监听，指定获取包含该文本的数据包
    page.set.window.maximized()
    url = f"https://twitter.com/{username}"
    page.get(url)
    page.wait.load_complete()
    # input("请登录推特：") #
    while True:
        res = page.wait.data_packets()  # 等待并获取一个数据包
        # print(res)
        dataJson = res.response.body
        parse_data(dataJson,username)
        time.sleep(5) #休眠时间
        page.scroll.to_bottom()

if __name__ == '__main__':
    username = "elonmusk"
    get_data(username)
