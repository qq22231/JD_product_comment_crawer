# coding='utf-8'
import requests
import json
import time
import random
import xlwt
import xlutils.copy
import xlrd
import threading
import pandas as pd
import os
from multiprocessing import Process, Lock

# 创建一个全局字典，用于保存每个线程爬取的数据
data_dict = {i: [] for i in [0, 1, 2, 4, 5, 7]}  # 修改score的值
# 创建一个锁，用于保证线程安全
lock = threading.Lock()


def get_produce_id():
    k = input("请输入你要搜索的商品:")
    produce_id = []
    # 默认爬10页
    for page in range(1, 11):
        url = f"https://search.jd.com/Search?keyword={k}&suggest=1.def.0.~SAK8%7CMIXTAG_SAK8R%2CBUNCH_A_SAK8_R%2CSAK8_M_AM_L34075%2CSAK8_M_GUD_R%2CSAK8_S_AM_R%2CSAK8_D_HSP_L36277%2CSAK8_SC_PD_R%2CSAK8_SM_PB_R%2CSAK8_SM_PRK_R%2CSAK8_SM_PRC_R%2CSAK8_SM_PRR_R%2CSAK8_SS_PM_R%7C&pvid=d94d672aa4ee47a8b81cfac1a7e81268&isList=0&page={page}&s=56&click=0&log_id=1714712981183.2941"
        headers = {
            "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Mobile Safari/537.36",
            "Cookie": 'shshshfpa=695bf289-b0ab-cde5-dfa1-7a74834da5cd-1700819123; shshshfpx=695bf289-b0ab-cde5-dfa1-7a74834da5cd-1700819123; __jdu=17008191235031403315919; pinId=qzLMmYZWxuiEYdwRXNCWpQ; pin=wdqNbVMgrtCSCy; unick=qNbVMgrtCSCy; _tp=8KpUYIdC7WDDQWJeDlOwJQ%3D%3D; _pst=wdqNbVMgrtCSCy; areaId=7; PCSYCityID=CN_410000_410100_0; rkv=1.0; ipLoc-djd=7-412-416-47178; unpl=JF8EAKJnAGQiaE1TBksHE0USSg0AWFhaTBEKaGcGUQ5RSVUEHAdOFhkZbVdfXg1XFgdzZgdFXF5AUAMQARwiEEhtVF9cCE8VBWxhA2RtaEhUNRoyGxQRSlRUVl0LSRMCbWcAVV9aS1ANGworEyBMXV1ubThLFABuYAdkXGhLXAEfChwUFk1VZBUzCQYXBW5mDFRVWEhWARoAGxcRSV9UWlUIQycCX2Q; __jdv=76161171|kong|t_1001537277_|tuiguang|762a49f30ae74c6786124b830064d58c|1714695271396; ceshi3.com=000; wxa_level=1; retina=0; cid=9; jxsid=17146987874017612659; appCode=ms0ca95114; webp=1; mba_muid=17008191235031403315919; visitkey=8231062084993559639; wlfstk_smdl=utjq4l3pwgwo9vuq1tw2khix838av0ap; TrackID=12WV98ZmZTqZnfnz1P4HpQRyBo1uiH7K27l1_bjD9HvTM91qAciw1g5fn_X2CRs-D42CkczrUjnWOtYOa7SOe7zz2F-HlsLF-YWsj--ezMhs; thor=1F19F09BFA78DBAA36068FAAA078250B4C612859A3F14D6CA28BFA1A35C35E4B3BD66F11B73B8FEA5CBFE5D85EB54F9A00C3B72C4B971463A359773C8B44EF5DE66C60F1C430D4629251CAAA8123BF65DFB78CDFA5DD765DECFDD39D23F5DD4421CE0D386DEC52396034169EE20B707DB90987469687183D1393E610432BCAC21924F675E583BCB902A1DB3162F618D8; flash=2_pO1JKLc9S75GR37bzEgPoP3Tc8sCjl2OZypPGsd4hHSxTjRYIXHYS8HC3qTtRrQrb3_G5Pwwtk6Shv_Y2dH54RhneutNR7kGzKQ4NnN0y-_S-DwDeoPkTx47wX_alLgwqoagQFV_r5e018GaN8JI16nllQ89AgxxP7uGPNsNYvh*; x-rp-evtoken=N-nAb5Oj6OS1u8hkvixIgJFCUp-ln4mLMRr_Uux9hd9muaWvwcNzAnNDD8i7YE3XeF1-BCAcLZdSDLZAnI-FBrJ9usebVI_DyuFSNW7NZf6lEh6mumhkddcTrGJkRwGkPaE37nMDpjB-_x_FlJJMFWB69FJDX3F1Iz2q34rqDj5ZoN3tiWlveBF5lcADMFXb2PP3Z1emhc9J7EigjdPY0TTh6gCcpWhG4SuImW1xEHA%3D; qrsc=3; sbx_hot_h=null; cd_eid=jdd03JL6BWZYDK3S6ZA4T64WNA6EHR26TTNRQ7YWZBF45L22T2DY4A7BHZLISIZMLJGLZ5UQXCBFGRMSJRQP5T25PIBGG3EAAAAMPHTHL7OQAAAAACNCDCL5GYYDFZEX; PPRD_P=UUID.17008191235031403315919; jxsid_s_u=https%3A//so.m.jd.com/ware/search.action; sc_width=1536; mt_xid=V2_52007VwMUU1xRUlIfShtYBGUDF1NfWlJeF0kRbAEwCxFVX1xaRh9BGVgZYgMXVkELW1xMVU5aATNRFQZYUFEIGnkaXQZiHxNWQVlQSx9KElgBbAYXYl9oUmofShFVDWEEFFtbWWJaHEob; __wga=1714712186577.1714712137918.1714712137918.1714712137918.3.1; jxsid_s_t=1714712186818; __jd_ref_cls=MLoginRegister_Login; xapieid=jdd03JL6BWZYDK3S6ZA4T64WNA6EHR26TTNRQ7YWZBF45L22T2DY4A7BHZLISIZMLJGLZ5UQXCBFGRMSJRQP5T25PIBGG3EAAAAMPHTHL7OQAAAAACNCDCL5GYYDFZEX; 3AB9D23F7A4B3C9B=JL6BWZYDK3S6ZA4T64WNA6EHR26TTNRQ7YWZBF45L22T2DY4A7BHZLISIZMLJGLZ5UQXCBFGRMSJRQP5T25PIBGG3E; __jdc=181111935; chat.jd.com=20170206; RT="z=1&dm=jd.com&si=voz7oc04nb&ss=lvq9n9g6&sl=0&tt=0"; 3AB9D23F7A4B3CSS=jdd03JL6BWZYDK3S6ZA4T64WNA6EHR26TTNRQ7YWZBF45L22T2DY4A7BHZLISIZMLJGLZ5UQXCBFGRMSJRQP5T25PIBGG3EAAAAMPHUFBSIIAAAAADTENSLP5TLLBHUX; __jda=181111935.17008191235031403315919.1700819123.1714712116.1714716026.16; shshshfpb=BApXc920CPupArVyZQuwjUW1oEwM4ixQXBkLAlgxv9xJ1MsU5coO2'

        }
        response = requests.get(url, headers=headers)
        data = response.text
        produce_id_num = re.findall(r'<li data-sku=\"(.*?)\" data', data)
        produce_id.extend(produce_id_num)
    return produce_id


def start(page, score, productId):
    # 获取URL
    url = f'https://club.jd.com/comment/productPageComments.action?&productId={productId}&score={score}&sortType=5&page={page}&pageSize=10&isShadowSku=0&fold=1'
    headers= {
        "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Mobile Safari/537.36"
    }
    time.sleep(2)
    test = requests.get(url=url, headers=headers)
    data = json.loads(test.text)
    if not data['comments']:  # 如果评论数据为空，返回None
        return None
    return data

def parse(data, score):
    items = data['comments']
    for i in items:
        result = (
            f"【评论类型{score}】" + i['nickname'],  # 用户名
            i['id'],  # 用户id
            i['content'],  # 内容
            i['creationTime'],  # 时间
            i.get('location', ''),  # 地点
            i['score'],  # 评分
            i['referenceName']  # 商品名称
        )
        print(result)  # 输出爬取的数据信息
        yield result
def excel(items):
    # 第一次写入
    newTable = "test.xls"  # 创建文件
    wb = xlwt.Workbook(encoding='utf-8')
    ws = wb.add_sheet('sheet1')  # 创建表
    headDate = ['nickname', 'id', '内容', '时间', '地点', '评分', '商品名称']  # 定义标题
    for i in range(len(headDate)):  # for循环遍历写入
        ws.write(0, i, headDate[i], xlwt.easyxf('font: bold on'))

    index = 1  # 行数
    for data in items:  # items是十条数据 data是其中一条（一条下有七个内容）
        for i in range(len(data)):  # 列数
            print(data[i])
            ws.write(index, i, data[i])  # 行 列 数据（一条一条自己写入）
        print('______________________')
        index += 1  # 等上一行写完了 在继续追加行数
        wb.save(newTable)

def another(items, j):  # 如果不是第一次写入 以后的就是追加数据了 需要另一个函数
    index = (j - 1) * 10 + 1  # 这里是 每次写入都从11 21 31..等开始 所以我才传入数据 代表着从哪里开始写入
    data = xlrd.open_workbook('test.xls')
    ws = xlutils.copy.copy(data)
    # 进入表
    table = ws.get_sheet(0)

    for test in items:
        for i in range(len(test)):  # 跟excel同理
            print(test[i])
            table.write(index, i, test[i])  # 只要分配好 自己塞入
        print('_______________________')
        index += 1
        ws.save('test.xls')



def main(score, productId):
    j = 1

    while True:
        time.sleep(1.5)
        first = start(j, score, productId)
        if first is None:  # 如果start函数返回None，跳出循环
            break
        test = parse(first, score)

        # 使用锁来保证线程安全
        with lock:
            # 将爬取的数据添加到相应的列表中
            data_dict[score].extend(test)
        print(f'第{j}页抓取完毕\n')
        j = j + 1

def write_all_data(productId):
    # 等待所有线程完成后，将所有数据写入文件
    all_data = []
    for score, items in data_dict.items():
        if items:
            df = pd.DataFrame(items, columns=['nickname', 'id', '内容', '时间', '地点', '评分', '商品名称'])
            df.to_excel(f'test_{productId}_{score}.xlsx', index=False)  # 修改文件格式为.xlsx，并在文件名中包含productId和score
            all_data.append(df)

    # 合并所有的.xlsx文件
    df_all = pd.concat(all_data)
    df_all.to_excel(f'all_data_{productId}.xlsx', index=False)
def process_main(productId, lock):
    threads = []
    for score in [0, 1, 2, 4, 5, 7]:  # 创建6个线程，每个线程爬取一个不同的score层级的数据
        t = threading.Thread(target=main, args=(score, productId))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()  # 等待所有线程完成

    with lock:  # 使用进程锁来保证在一个时间点只有一个进程可以写入文件
        write_all_data(productId)  # 写入所有数据
if __name__ == '__main__':
    productIds=get_produce_id()
    lock = Lock()  # 创建一个进程锁
    processes = []

    for i in range(0, len(productIds), 5):  # 每次创建5个进程
        for productId in productIds[i:i+5]:
            p = Process(target=process_main, args=(productId, lock))
            p.start()
            processes.append(p)

        for p in processes:
            p.join()  # 等待所有进程完成

    # 合并所有的.xlsx文件
    all_data = []
    for productId in productIds:
        if os.path.exists(f'all_data_{productId}.xlsx'):  # 检查文件是否存在
            df = pd.read_excel(f'all_data_{productId}.xlsx')
            all_data.append(df)
            for score in [0, 1, 2, 4, 5, 7]:  # 删除每个线程产生的所有文件
                os.remove(f'test_{productId}_{score}.xlsx')
    df_all = pd.concat(all_data)
    df_all.to_excel('all_data.xlsx', index=False)