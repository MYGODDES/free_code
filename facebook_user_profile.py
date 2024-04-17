import os
import json
import time
import random
import re
import requests
import datetime
import pandas as pd
from retrying import retry

import warnings
warnings.filterwarnings("ignore")

class FaceBookProfile:
    def __init__(self,saveFileName):
        self.saveFileName = saveFileName
        self.uid = None
        self.account = None
        self.flag_list = []
        self.cookies = {'fr': '0jgQFHFtO3sQTzIYX.AWWPUF8_tK3P0pPWcYNm36hQGV0.BlIiZn.0J.AAA.0.0.BlIiZ2.AWXJthlPW4A',
                   'xs': '13%3AO_xk1GNKxEOICA%3A2%3A1696736882%3A-1%3A-1', 'datr': 'ZyYiZU4OAYvmnI3D-QgX4XkF',
                   'dpr': '1.25', 'c_user': '100086062107297', 'locale': 'tr_TR', 'wd': '1536x739',
                   'sb': 'ZyYiZfsydge-SoQ5N1MoteJa'}
        self.headers = {
            "authority": "www.facebook.com",
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
            "cache-control": "no-cache",
            "content-type": "application/x-www-form-urlencoded",
            "dpr": "0.9",
            "origin": "https://www.facebook.com",
            "pragma": "no-cache",
            "referer": "https://www.facebook.com/profile.php?id=61551922813043",
            "sec-ch-prefers-color-scheme": "light",
            "sec-ch-ua": "\"Not_A Brand\";v=\"8\", \"Chromium\";v=\"120\", \"Google Chrome\";v=\"120\"",
            "sec-ch-ua-full-version-list": "\"Not_A Brand\";v=\"8.0.0.0\", \"Chromium\";v=\"120.0.6099.225\", \"Google Chrome\";v=\"120.0.6099.225\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-model": "\"\"",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-ch-ua-platform-version": "\"8.0.0\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "viewport-width": "1446",
            "x-asbd-id": "129477",
            "x-fb-friendly-name": "ProfileCometTimelineFeedRefetchQuery",
            "x-fb-lsd": "NNp-wKp9d_ZylbJso306Cr"
        }
        self.__user = None
        self.fb_dtsg = None

    def transfromTime(self,ts):
        date = datetime.datetime.fromtimestamp(ts)
        gmt_offset = datetime.timedelta(hours=8)
        return (date + gmt_offset).strftime("%Y-%m-%d %H:%M:%S")

    def get_data(self,uid_,cursor):
        if cursor == "-1":
            preData = "{\"afterTime\":null,\"beforeTime\":null,\"count\":3,\"cursor\":null,\"feedLocation\":\"TIMELINE\",\"feedbackSource\":0,\"focusCommentID\":null,\"memorializedSplitTimeFilter\":null,\"omitPinnedPost\":true,\"postedBy\":{\"group\":\"OWNER\"},\"privacy\":null,\"privacySelectorRenderLocation\":\"COMET_STREAM\",\"renderLocation\":\"timeline\",\"scale\":1,\"stream_count\":1,\"taggedInOnly\":null,\"useDefaultActor\":false,\"id\":\"%s\",\"__relay_internal__pv__IsWorkUserrelayprovider\":false,\"__relay_internal__pv__IsMergQAPollsrelayprovider\":false,\"__relay_internal__pv__CometUFIReactionsEnableShortNamerelayprovider\":false,\"__relay_internal__pv__CometUFIIsRTAEnabledrelayprovider\":false,\"__relay_internal__pv__StoriesArmadilloReplyEnabledrelayprovider\":false,\"__relay_internal__pv__StoriesRingrelayprovider\":true}" % uid_
        else:
            preData = "{\"afterTime\":null,\"beforeTime\":null,\"count\":3,\"cursor\":\"%s\",\"feedLocation\":\"TIMELINE\",\"feedbackSource\":0,\"focusCommentID\":null,\"memorializedSplitTimeFilter\":null,\"omitPinnedPost\":true,\"postedBy\":{\"group\":\"OWNER\"},\"privacy\":null,\"privacySelectorRenderLocation\":\"COMET_STREAM\",\"renderLocation\":\"timeline\",\"scale\":1,\"stream_count\":1,\"taggedInOnly\":null,\"useDefaultActor\":false,\"id\":\"%s\",\"__relay_internal__pv__IsWorkUserrelayprovider\":false,\"__relay_internal__pv__IsMergQAPollsrelayprovider\":false,\"__relay_internal__pv__CometUFIReactionsEnableShortNamerelayprovider\":false,\"__relay_internal__pv__CometUFIIsRTAEnabledrelayprovider\":false,\"__relay_internal__pv__StoriesArmadilloReplyEnabledrelayprovider\":false,\"__relay_internal__pv__StoriesRingrelayprovider\":true}"%(cursor,uid_)
        self.data = {
            "av": self.__user,
            "__user": self.__user,
            "__a": "1",
            "__req": "3",
            "__hs": "19748.HYP:comet_pkg.2.1..2.1",
            "dpr": "1",
            "__ccg": "EXCELLENT",
            "__rev": "1011040152",
            "__s": "llu0xy:ows4ky:704h1m",
            "__hsi": "7328354018454787718",
            "__dyn": "7AzHK4HwkEng5K8G6EjBAo2nDwAxu13wFG14xt3odE98K361twYwJyE24wJwpUe8hwaG0Z82_CxS320om78bbwto886C11xmfz83WwgEcEhwGxu782lwv89kbxS2218wc60D8vwRwlE-U2exi4UaEW2au1NxGm2SUbElxm3y11xfxmu3W3y261eBx_wHwdG7FoarCwLyESE6C14wwwOg2cwMwhEkxebwHwNxe6Uak1xwJwxyo566E6C13whEeE4WVU-4E",
            "__csr": "hllqXEF2lfibiJXrObnlW8F7OWlAOKKLiiP9f9WkzHiZbWZqGJW8ZhelqFnGGCO9tazAFcDG_aRoDBpQFqLADQqAhpaSFaABUjCiGujV8xJ3Vk8UHz5U_gV4xummvDggXDV9UgAhVV-dDBKpeiEG9DzEW9yqxqdUJk9BKaG4EaK4F8KqaGmUbE9EB288VodEGumVEbEforxGawrVoWq2S9wFxC58C4EC5ouy8427UdFoiyU2JwQhU6y0CqwGwko2xw4cyUsw6JwaS0_U0sdwkE01ij40YE-0mWawaG0h903cU1qopw3go0xV2Fm0MVo08Y81LU0imw5rw1qy024G03JO221p80r2",
            "__comet_req": "15",
            "fb_dtsg": self.fb_dtsg,
            "jazoest": "25401",
            "lsd": "NNp-wKp9d_ZylbJso306Cr",
            "__aaid": "0",
            "__spin_r": "1011040152",
            "__spin_b": "trunk",
            "__spin_t": "1706265383",
            "qpl_active_flow_ids": "1056839232",
            "fb_api_caller_class": "RelayModern",
            "fb_api_req_friendly_name": "ProfileCometTimelineFeedRefetchQuery",
            # "variables": "{\"afterTime\":null,\"beforeTime\":null,\"count\":3,\"cursor\":\"Cg8Ob3JnYW5pY19jdXJzb3IJAAAB70FRSFJiSjZnaEUzY2FQLUhtbXBHNGpqRUVHMV9nS0hCcjMtdTlJUlZ5dFNQRkFBY0ZMY2VhRVNMU3d3YnpYeVBlbFpuQzNuLTY0Wk5OWXd3QnFzMnI0bEY2WGJFSmZQemE0akFGbk5GWU9ydU9BS3pINmlGR29HWUR4NDZZQXJRVlRyVlJHR3pBQmhSTnZlSy1hWDhvejM5Rk9xcWxLNm1HVnRCbUR4OWpqRjVFNlpDMWE5ZkN4Ry1GWnNadUN3VW5ySndkNHpsRmt2M2tKS05kWjdUTlVDS0FUNHU0V0hiYkdyQktVRGNEUEZ4Q1hEeFdXMmszQmIydHRSMUJIeGxuVE9rWjBtTVlrdW5zMndtcEhrbFkydkE5YWszdVVYbmRRV2llTzRwYUxVX19LX3c2U0NOY0ZKdHRPeVk0dm9xZEZIM3lEaU9mWUIzZEp1U2EzV3dsODEzc2MycHVRcURnTXpGajFRMFg5cG40TDJYV0tlTlpKeW04MkRDZ29JcEZjNW5UNTZuTGVaSlpOay1wUU42ekRJRkt4eEhqUlZJTDNodm5FZ3pGbU9fbmFvM1llNmt4cjJJX0Y1SGt6c2dYcGI0cmE5Z1g2RGFXakZaM2Q4MmJQMUdDbXhfQmdpeWxTUW9ESXpfTHUzc3RaZw8JYWRfY3Vyc29yDg8PZ2xvYmFsX3Bvc2l0aW9uAgAPBm9mZnNldAIADxBsYXN0X2FkX3Bvc2l0aW9uAv8PNWlzX2ZiX3N0b3J5X2luY2x1ZGVkX2luX2N1cnJlbnRfb3JfcHJldmlvdXNfaXRlcmF0aW9uEQAB\",\"feedLocation\":\"TIMELINE\",\"feedbackSource\":0,\"focusCommentID\":null,\"memorializedSplitTimeFilter\":null,\"omitPinnedPost\":true,\"postedBy\":{\"group\":\"OWNER\"},\"privacy\":null,\"privacySelectorRenderLocation\":\"COMET_STREAM\",\"renderLocation\":\"timeline\",\"scale\":1,\"stream_count\":1,\"taggedInOnly\":null,\"useDefaultActor\":false,\"id\":\"61551922813043\",\"__relay_internal__pv__IsWorkUserrelayprovider\":false,\"__relay_internal__pv__IsMergQAPollsrelayprovider\":false,\"__relay_internal__pv__CometUFIReactionsEnableShortNamerelayprovider\":false,\"__relay_internal__pv__CometUFIIsRTAEnabledrelayprovider\":false,\"__relay_internal__pv__StoriesArmadilloReplyEnabledrelayprovider\":false,\"__relay_internal__pv__StoriesRingrelayprovider\":true}",

            "variables": preData,
            "server_timestamps": "true",
            "doc_id": "24584482391166363"
        }


    @retry(stop_max_attempt_number=5)
    def get_userId(self,):
        if self.account.isdigit():
            return self.account
        else:
            headers = {
                "authority": "www.facebook.com",
                "accept": "*/*",
                "accept-language": "zh-CN,zh;q=0.9",
                "cache-control": "no-cache",
                "content-type": "application/x-www-form-urlencoded",
                "dpr": "0.9",
                "origin": "https://www.facebook.com",
                "pragma": "no-cache",
                "referer": f"https://www.facebook.com/{self.account}",
                "sec-ch-prefers-color-scheme": "light",
                "sec-ch-ua": "\"Chromium\";v=\"116\", \"Not)A;Brand\";v=\"24\", \"Google Chrome\";v=\"116\"",
                "sec-ch-ua-full-version-list": "\"Chromium\";v=\"116.0.5845.188\", \"Not)A;Brand\";v=\"24.0.0.0\", \"Google Chrome\";v=\"116.0.5845.188\"",
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-model": "\"\"",
                "sec-ch-ua-platform": "\"Windows\"",
                "sec-ch-ua-platform-version": "\"8.0.0\"",
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
                "viewport-width": "878",
                "x-asbd-id": "129477",
                "x-fb-lsd": "AsuQaJo8uEt7zXFq2GSyef"
            }
            url = "https://www.facebook.com/ajax/bulk-route-definitions/"
            data_ = {
                "route_urls[0]": f"/{self.account}",
                "routing_namespace": "fb_comet",
                "__user": self.__user,
                "__a": "1",
                "__req": "1",
                "__hs": "19633.HYP:comet_pkg.2.1..2.1",
                "dpr": "1",
                "__ccg": "EXCELLENT",
                "__rev": "1009000310",
                "__s": "x3i97v:7j0gvl:3sanoc",
                "__hsi": "7285716874224245633",
                "__dyn": "7xeu4HwBwIxt0mUyEqxemhw9-2i5U4e0ykdwSwAyUco38yE2OwpUe8hw9-awfi0LVE4W0om78bbwto2awgolzUO0-E4a3a4oaEd82lwv89k2C0iK0D8vwRwpHw8W58jwGzE2swwwJK2W5olwUwgojxuu3W3y261Qw4DBwFKq2-azqwqo6m3e0z8c84q58jwTwNxe6Uak1xwmo6O1Fwcm",
                "__csr": "1mZ7qkLGpQWdbGBiIKyTirjWpBFGWJrf9Th2F8yuLqoBiqF9WLGJ8x5nQALJ44PZQmmQHiGFAXGnimujpkkxaHADBKaVay8zVKiicySiax57GijpeRDy9948hFoCBFFJpeEy4p8B2aybGV4m8hQcyaG599k4qAyVulypu-WhEKrAgjBAz4i4Ai69EjwNyoSifChefBK4VQleWHzEuwOG4qK48sy8hzoG6Eixe2bxm9Ax64oKm3eKczEqxiFaCyei5VFoa8uxCiA3ScDArwGx6798kxu2W9wVwkE_xK7EOuq3a1Wz9Ujy8txaaCxG1Mwl8K8zo8U2xwhEmK1MwhEat02fo1n83AwnU25Cwki0au58c8swGw0xtw1Vq03s100aL-2a0j-0xE5y0lG0U8O2a0Ao1rES2nG0To10U18EcU0Y-2S0ju290Az60cqw2IQ04pE09zbo0zW26E0NO0BU04ka0So0CK0W4fwzz65klj6wGgdU1h82Uo2CCwaK1Zw",
                "__comet_req": "15",
                "fb_dtsg": self.fb_dtsg,
                "jazoest": "25282",
                "lsd": "eGq9bCYRGtC0yBjihQohgG",
                "__aaid": "0",
                "__spin_r": "1009000310",
                "__spin_b": "trunk",
                "__spin_t": "1696338149"
            }
            response = requests.post(url, headers=headers, cookies=self.cookies,data=data_,verify=False)
            aaa = re.findall('"userID":"(.*?)",',response.text)
            return aaa[0]

    def get(self):
        url = "https://www.facebook.com/api/graphql/"
        while True:
            try:
                response = requests.post(url,
                                         headers=self.headers,
                                         cookies=self.cookies,
                                         data=self.data,
                                         verify=False,
                                         timeout=(3, 20))
                status = response.status_code
                if status == 200:
                    text = response.content.decode()
                    return text
            except Exception as e:
                print("请求发生错误：", e)
                if "HTTPSConnectionPool" in str(e):
                    time.sleep(random.random() * 2)

    def parse_data(self, node):
        e = node
        try:
            post_id = e.get("post_id")
            comment_id = e.get('feedback',{}).get("id")
            story = e["comet_sections"]["content"]["story"]
            message = story.get("message")
            if message:
                content = message.get("text")
            else:
                content = None
            if not content:
                try:
                    content = story["attachments"][0]["styles"]["attachment"]["title_with_entities"].get("text")
                except:
                    pass
            if not content:
                try:
                    content = story["attached_story"]['message']['text']
                except:
                    content = ""
            # content = content.get("text") if content else ""
            attachments = e["comet_sections"]["content"]["story"].get("attachments")
            image_url_list = []
            video_url_list = []
            if attachments:
                # try:
                #     __typename = attachments[0]["target"]["__typename"]
                # except:
                #     __typename = attachments[0]["styles"]["attachment"]["media"]["__typename"]

                style_list = attachments[0]["style_list"]

                if "Photo" in style_list or "photo" in style_list:
                    try:
                        img_url = attachments[0]["styles"]["attachment"]["media"]["photo_image"]["uri"]
                        if img_url:
                            image_url_list.append(img_url)
                    except:
                        pass

                if "Share" in style_list or "share" in style_list:
                    try:
                        img_url = attachments[0]["styles"]["attachment"]["media"]["large_share_image"]["uri"]
                        if img_url:
                            image_url_list.append(img_url)
                    except:
                        pass

                if "album" in style_list or "Album" in style_list:
                    try:
                        all_subattachments_nodes = attachments[0]["styles"]["attachment"]["all_subattachments"]['nodes']
                        for a in all_subattachments_nodes:
                            try:
                                img_url = a["media"]["image"]["uri"]
                                if img_url:
                                    image_url_list.append(img_url)
                            except:
                                pass
                    except:
                        pass

                if "Video" in style_list or "video" in style_list:
                    try:
                        img_url = attachments[0]["styles"]["attachment"]["media"]["image"]["uri"]
                        video_url = attachments[0]["styles"]["attachment"]["media"]["browser_native_hd_url"]
                        if not video_url:
                            video_url = attachments[0]["styles"]["attachment"]["media"]["browser_native_sd_url"]
                        if video_url:
                            video_url_list.append(video_url)
                        if img_url:
                            image_url_list.append(img_url)
                    except:
                        pass
            # print(image_url_list)
            # print(video_url_list)
            image_urls = "; ".join(image_url_list)
            video_urls = "; ".join(video_url_list)

            d_url = e["comet_sections"]["content"]["story"].get('wwwURL')
            uname = e["comet_sections"]["content"]["story"]['actors'][0].get('name')
            uid = e["comet_sections"]["content"]["story"]['actors'][0].get("id")
            profile_url = e["comet_sections"]["content"]["story"]['actors'][0].get("url")
            attached_story = e["comet_sections"]["content"]["story"].get('attached_story')
            is_origin = "转发" if attached_story else "原创"
            like_count = \
                e["comet_sections"]["feedback"]["story"]["feedback_context"]["feedback_target_with_context"][
                    "ufi_renderer"][
                    "feedback"]["comet_ufi_summary_and_actions_renderer"]["feedback"]["reaction_count"].get("count")
            share_count = \
                e["comet_sections"]["feedback"]["story"]["feedback_context"]["feedback_target_with_context"][
                    "ufi_renderer"][
                    "feedback"]["comet_ufi_summary_and_actions_renderer"]["feedback"]["share_count"].get("count")
            total_comment_count = \
                e["comet_sections"]["feedback"]["story"]["feedback_context"]["feedback_target_with_context"][
                    "ufi_renderer"][
                    "feedback"]["comet_ufi_summary_and_actions_renderer"]["feedback"].get("total_comment_count")
            try:
                video_view_count = e["comet_sections"]["feedback"]["story"][
                    "feedback_context"]["feedback_target_with_context"]["ufi_renderer"]["feedback"][
                    "comet_ufi_summary_and_actions_renderer"]["feedback"]["video_view_count"]
                if not video_view_count:
                    video_view_count = 0
            except:
                video_view_count = 0
            publish_time = 0
            metadata = \
                e["comet_sections"]["content"]["story"]["comet_sections"]["context_layout"]["story"]["comet_sections"][
                    "metadata"]
            for m in metadata:
                try:
                    publish_time = m["story"]["creation_time"]
                    publish_time = self.transfromTime(publish_time)
                    break
                except:
                    pass
            if publish_time == 0:
                try:
                    publish_time = \
                        e["comet_sections"]["content"]["story"]["attachments"][0]["styles"]["attachment"]["style_infos"][0][
                            "fb_shorts_story"]["creation_time"]
                    publish_time = self.transfromTime(publish_time)
                except:
                    pass
            item = {
                "media_id": post_id,
                "userid": uid,
                "username": uname,
                "profile_url": profile_url,
                "publicate_time": publish_time,
                "URL": d_url,
                "content": content,
                "like_count": like_count,
                "comment_count": total_comment_count,
                "repost_count": share_count,
                "play_count":video_view_count,
                "is_origin": is_origin,
                "img_url": image_urls,
                "video_url": video_urls
            }
            print(item)
            self.save_data([item])

        except Exception as ex:
            print(f"解析错误：{ex}")
            print(f"原数据：{e}")

    def save_data(self,resultList):
        if resultList:
            df = pd.DataFrame(resultList)
            if not os.path.exists(f'./{self.saveFileName}.csv'):
                df.to_csv(f'./{self.saveFileName}.csv', index=False, mode='a', sep=",", encoding="utf_8_sig")
            else:
                df.to_csv(f'./{self.saveFileName}.csv', index=False, mode='a', sep=",", encoding="utf_8_sig",
                          header=False)
            print("保存成功")

    def run(self,uid_):
        before_cursor = "-1"
        end_cursor = "-1"
        page = 1
        has_next_page = True
        while has_next_page:
            print(f"page:{page},end_cursor:{end_cursor}")
            self.get_data(uid_,end_cursor)
            text = self.get()
            # print(text)
            if 'Rate limit exceeded' in text:
                print("请求限制中...")
                break
            if "errorSummary" in text or "errorDescription" in text:
                print([text])
                break
            else:
                data_str = text.split("\n")[0]
                end_cursor_data_str = "{}"
                profileCometTimelineFeeds = [data_str]
                for p in text.split("\n"):
                    if "ProfileCometTimelineFeed_user$stream$ProfileCometTimelineFeed_user_timeline_list_feed_units" in p:
                        profileCometTimelineFeeds.append(p)
                    if "ProfileCometTimelineFeed_user$defer$ProfileCometTimelineFeed_user_timeline_list_feed_units$page_info" in p:
                        end_cursor_data_str = p
                try:
                    for feed in profileCometTimelineFeeds:
                        data_json = json.loads(feed)
                        # print([data_json])
                        try:
                            edges = data_json["data"]["node"]["timeline_list_feed_units"]["edges"]
                            if edges:
                                node = edges[0]['node']
                            else:
                                break
                        except:
                            node = data_json["data"]["node"]
                        if node:
                            self.parse_data(node)
                        else:
                            has_next_page = False
                    end_cursor_data_json = json.loads(end_cursor_data_str)
                    end_cursor = end_cursor_data_json["data"]["page_info"]["end_cursor"]
                    has_next_page = end_cursor_data_json["data"]["page_info"]["has_next_page"]
                    page += 1
                    if end_cursor!=before_cursor:
                        before_cursor = end_cursor
                    else:
                        break
                except Exception as e:
                    if "list index out of range" in str(e):
                        break

if __name__ == '__main__':
    """
    Nature 2016-2010 ScienceMagazine 2022-
    """
    # 登录的cookie
    ck = {}
    # 账号登录认证
    fb_dtsg = ""
    # 采集账号名
    user = "mms"
    # 保存数据名称
    saveFileName = "xxx"
    fb = FaceBookProfile(saveFileName)
    fb.cookies = ck
    fb.__user = ck['c_user']
    fb.fb_dtsg = fb_dtsg
    while True:
        fb.account = user
        try:
            uid_ = fb.get_userId()
        except Exception as e:
            print("发生错误：", e)
            continue
        print(f"获取的账号ID：{uid_}")
        fb.run(uid_)
