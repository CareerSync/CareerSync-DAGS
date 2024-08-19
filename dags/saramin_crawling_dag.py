from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import json
import random
# 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

# User-Agent 정의
user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36 OPR/70.0.3728.178',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36 Edge/17.17134'
]

# Python 함수 정의 (작업 내용)
# def init_driver(path):
#     options = webdriver.ChromeOptions()
#     options.add_argument("--no-sandbox")
#     options.add_argument("--disable-dev-shm-usage")
#     return webdriver.Chrome(path, options=options)

def get_job_urls(user_agents: list): # driver, page_count
    from bs4 import BeautifulSoup
    import time
    import requests
    url_list = []
    for page in range(1, 2 + 1): # page_count + 1
        user_agent = user_agents[page % len(user_agents)]
        soup = requests.get(f'https://www.saramin.co.kr/zf_user/search?cat_mcls=2&company_cd=0%2C1%2C2%2C3%2C4%2C5%2C6%2C7%2C9%2C10&panel_type=&search_optional_item=y&search_done=y&panel_count=y&preview=y&recruitPage={page}&recruitSort=reg_dt&recruitPageCount=200&inner_com_type=&searchword=&show_applied=&quick_apply=&except_read=&ai_head_hunting=&mainSearch=n', headers={'User-Agent': user_agent})
        time.sleep(random.randint(1, 5))
        html = BeautifulSoup(soup.text, 'html.parser')
        jobs = html.select('div.item_recruit')
        
        for job in jobs:
            try:
                url = 'https://www.saramin.co.kr' + job.select_one('a')['href']
                url_list.append(url)
                print(f'{len(url_list)}번째 url을 성공적으로 추출하였습니다.')
            except Exception:
                pass
    return url_list

# DAG 정의
with DAG(
    dag_id="saramin_crawling_dag",
    default_args=default_args,
    description="Job crawling from Saramin using Selenium and BeautifulSoup",
    schedule_interval="0 18 * * *",  # 매일 저녁 6시에 실행
    catchup=False,  # 과거 실행을 수행하지 않음
) as dag:

    # PythonOperator를 사용한 작업 정의
    # init_driver_task = PythonOperator(
    #     task_id='init_driver',
    #     python_callable=init_driver, # 실행할 Python 함수
    #     provide_context=True
    # )

    get_job_urls_task = PythonOperator(
        task_id='get_job_urls',
        python_callable=get_job_urls,  # 실행할 Python 함수
        op_args=[user_agents],  # 함수에 전달할 인자
        provide_context=True # kwargs를 함수에 전달
    )

    # DAG에 작업 추가
    # init_driver_task >> get_job_urls_task
    get_job_urls_task

