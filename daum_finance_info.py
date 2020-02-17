# -*- coding: utf-8 -*-

import gzip, pickle
from bs4 import BeautifulSoup
from selenium import webdriver
import pandas as pd
import multiprocessing

class Crawler:
    def __init__(self,file_path="C:\ChromeDriver\chromedriver",*chrome_opts):
        options = webdriver.ChromeOptions()

        for option_value in chrome_opts:
            options.add_argument(option_value)

        self.driver = webdriver.Chrome(file_path, chrome_options=options)
        self.driver.implicitly_wait(3)


def get_siga(code):
    #code = '005930'
    url = 'https://wisefn.finance.daum.net/v1/company/c1010001.aspx?cmp_cd={}'.format(code)

    options = webdriver.ChromeOptions()
    options.add_argument('headless')
    options.add_argument('window-size=1920x1080')
    options.add_argument("disable-gpu")
    options.add_argument("lang=ko_KR")
    options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) "
                         "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36")

    driver = webdriver.Chrome("C:\ChromeDriver\chromedriver", chrome_options=options)
    driver.implicitly_wait(3)
    driver.get(url)

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    print(int(soup.find(id='cTB11').find_all('tr')[4].find('td').text.strip().replace(',',''))*1)

    return int(soup.find(id='cTB11').find_all('tr')[4].find('td').text.strip().replace(',',''))

def crawler(codes='005930'):
    print("[{}] Work...".format(codes))
    options = webdriver.ChromeOptions()
    options.add_argument('headless')
    options.add_argument('window-size=1920x1080')
    options.add_argument("disable-gpu")
    options.add_argument("lang=ko_KR")
    options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) "
                         "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36")

    driver = webdriver.Chrome("C:\ChromeDriver\chromedriver", chrome_options=options)
    driver.implicitly_wait(3)

    #url = 'http://finance.daum.net/quotes/A{}#home'.format(codes)

    url = 'https://wisefn.finance.daum.net/v1/company/c1030001_1.aspx?cmp_cd={}'.format(codes)
    #
    driver.get(url) ;print(url)
    driver.implicitly_wait(3)

    repo_type_on_xpath = {'포괄손익계산서' : '//*[@id="rpt_tab1"]',
                          '재무상태표' : '//*[@id="rpt_tab2"]',
                          '현금흐름표' : '//*[@id="rpt_tab3"]'}

    date_type_on_xpath = {"연간" : '//*[@id="frqTyp0"]', '분기' : '//*[@id="frqTyp1"]'}

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    #html_head = soup.find('div', id='fixLayer')


    #############################################################################################
    #############################################################################################
    Dataframe_list = list()
    Dataframe_dict = dict()

    for repo_type, repo_xpath in repo_type_on_xpath.items():
        for date_type, date_xpath in date_type_on_xpath.items():
            driver.find_element_by_xpath(repo_xpath).click()
            driver.find_element_by_xpath(date_xpath).click()

            driver.switch_to_frame('frmFS')  # iframe 안으로 들어가기

            iframe_soup = BeautifulSoup(driver.page_source, 'html.parser')

            current_table = None

            for table in iframe_soup.find_all('table'):  # 여러 테이블(포손1,포손2,재무1,재무2 ...) 중 보여지는 테이블 가져오는 과정
                # [2 if a.get() == 5, for a in 10] 줄일수잇을까???
                if table.get('style') == 'display: block;':
                    current_table = table
                    break

            data = list()

            for line_data in current_table.find_all('tr'):  # 보여진 테이블 내에 줄단위 데이터 가져오는거

                if line_data.get('style') in ["display:none;", "display: none;"]:
                    continue

                data.append([sel_data.text for sel_data in line_data.find_all('td')])

            columns_header = [val.text for val in iframe_soup.find('div', id='fixLayer').find_all('th')]
            Dataframe_list.append(pd.DataFrame(columns=columns_header,data=data).set_index('항목'))
            #Dataframe_dict[repo_type][date_type].update(pd.DataFrame(columns=columns_header, data=data).set_index('항목'))

            driver.switch_to_default_content()  # iframe에서 메인 페이지로 나오기

            driver.implicitly_wait(3)

    with gzip.open('pickle/' + codes, 'wb') as f:
        print("[{}] Saved".format(codes))
        pickle.dump(Dataframe_list, f)
    """
    with gzip.open('pickle/' + codes + "_dict", 'wb') as f:
        print("[{}] Saved".format(codes))
        pickle.dump(Dataframe_dict, f)
    """
    return Dataframe_list

if __name__ == "__main__":
    get_siga('035720')
    input("test")
    import time as t

    start = t.time()

    crp_list = ['005930','035720','000060','000020']
    
    print("Process Pool 시작");   re = multiprocessing.Pool(processes=8)
    print("MAP 시작");    mv = re.map(crawler,crp_list)

    t1 = t.time() - start

    print("##################################")
    start = t.time()

    crp_list = ['005930', '035720', '000060', '000020']

    for crp in crp_list:
        crawler(crp)

    t2 = t.time() - start

    print("Processed        : ", t1)
    print("Non Processed    : ", t2)