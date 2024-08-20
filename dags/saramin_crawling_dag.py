from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from selenium import webdriver
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

def init_driver(**kwargs):
    from selenium.webdriver.chrome.options import Options
    options = Options()
    options.add_argument('--headless')
    options.add_argument('window-size=1200x600')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')

    # remote_chromedriver 컨테이너에 연결
    remote_webdriver = 'http://remote_chromedriver:4444/wd/hub'
    driver = webdriver.Remote(remote_webdriver, options=options)
    
    return driver

def get_job_urls(user_agents: list, **kwargs): # driver, page_count
    from bs4 import BeautifulSoup
    import time
    import requests
    import random
    url_list = []
    for page in range(1, 1 + 1): # page_count + 1
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

def process_job_urls(**kwargs):
    ti = kwargs['ti']  # Task Instance 가져오기
    url_list = ti.xcom_pull(task_ids='get_job_urls')  # get_job_urls Task에서 XCom으로 저장된 데이터 가져오기
    
    # 가져온 URL 리스트 처리
    for url in url_list:
        print(f"Processing URL: {url}")
        # 여기서 원하는 처리를 수행

def extract_metadata(soup):
    import re
    meta = soup.select_one('head > meta:nth-child(6)')
    if meta:
        meta_content = meta['content']
        match = re.search(r'마감일:(\S+)', meta_content)
        end_date = match.group(1) if match else None
        return end_date
    return None

def extract_basic_info(driver):
    dt = {'경력': 'Work_history', '학력': 'Education', '근무형태': 'Job_type'}
    job_info = {}
    for index in range(1, len(dt) + 1):
        dt_elements = driver.find_elements(By.XPATH, f'//*[@id="content"]/div[3]/section[1]/div[1]/div[2]/div/div[1]/dl[{index}]/dt')
        dd_elements = driver.find_elements(By.XPATH, f'//*[@id="content"]/div[3]/section[1]/div[1]/div[2]/div/div[1]/dl[{index}]/dd')
        if dt_elements and dd_elements:
            dt_text = dt_elements[0].text.strip()
            if dt_text in dt:
                key = dt[dt_text]
                job_info[key] = dd_elements[0].text.strip()
    return job_info

def extract_additional_info(driver):
    dt2 = {'급여': 'Salary', '근무지역': 'working area', '근무일시': 'working time', '직급/직책': 'Position', '근무요일': 'working day'}
    job_info = {}
    basic_info_element2 = driver.find_elements(By.XPATH, '//*[@id="content"]/div[3]/section[1]/div[1]/div[2]/div/div[2]')
    for i in basic_info_element2:
        basic_info2 = i.text
    parts2 = basic_info2.split('\n')
    for i in range(0, len(parts2), 2):
        key = parts2[i]
        value = parts2[i + 1]
        if key in dt2:
            job_info[dt2[key]] = value
        else:
            job_info[key] = value
    return job_info

def extract_job_details(driver, headers):
    from selenium.webdriver.common.by import By
    from bs4 import BeautifulSoup
    import requests
    import time
    iframe_html = driver.find_element(By.XPATH, '//*[@id="iframe_content_0"]')
    iframe_url = iframe_html.get_attribute('src')
    soup = requests.get(iframe_url, headers=headers)
    time.sleep(random.randint(1, 2))
    iframe_soup = BeautifulSoup(soup.text, 'html.parser')
    
    for script in iframe_soup.find_all("script"):
        script.replace_with("")
    for element in iframe_soup(text=lambda text: isinstance(text, Comment)):
        element.replace_with("")
    
    keyword_list = ['logo', 'icon', 'watermark', 'banner', 'stack', 'topimg', 'interview_pc_1', 'top']
    img_list = []
    logo_url = None
    
    for img in iframe_soup.find_all("img"):
        src = img['src']
        if src.startswith("//"):
            src = "https:" + src
            
        if 'logo' in src:
            logo_url = src
        
        if all(keyword not in src for keyword in keyword_list if keyword != 'logo') and len(src) <= 100:
            img_list.append(src)
    
    driver.switch_to.frame("iframe_content_0")
    detail_info = driver.find_element(By.CLASS_NAME, "user_content").text
    
    return img_list, detail_info, logo_url

def extract_company_name(driver):
    from selenium.webdriver.common.by import By
    company_name_element = driver.find_elements(By.XPATH, '//*[@id="content"]/div[3]/section[1]/div[1]/div[1]/div[1]/div[1]/a[1]')
    for i in company_name_element:
        return i.text
    return None

def task2(**kwargs):
    from selenium.webdriver.common.by import By
    from bs4 import BeautifulSoup
    import time
    import requests
    today = datetime.now().strftime('%Y-%m-%d')
    headers = {'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"}
    
    url_list = kwargs['ti'].xcom_pull(task_ids='get_urls_task')
    
    with open('job_data.jsonl', 'w', encoding='utf-8') as f:
        for index, url in enumerate(url_list):
            driver = init_driver()  # 각 URL마다 새로운 driver 인스턴스 생성
            job_info_all = {'Datetime': today}
            data = requests.get(url, headers=headers)
            soup = BeautifulSoup(data.text, 'html.parser')
            driver.get(url)
            driver.implicitly_wait(time_to_wait=60)
            start_time_all = time.time()
            
            try:
                job_info_all['title'] = driver.find_element(By.XPATH, '//*[@id="content"]/div[3]/section[1]/div[1]/div[1]/div/h1').text
                job_info_all['URL'] = url
            except Exception as e:
                print(f'Error processing 채용공고 제목 {url}: {e}')
                pass

            try:
                end_date = extract_metadata(soup)
                if end_date:
                    job_info_all['end_date'] = end_date
                job_info_all['start_date'] = driver.find_element(By.CLASS_NAME, "info_period").text.split('\n')[1]
            except Exception as e:
                print(f'Error processing metadata {url}: {e}')
                pass

            try:
                job_info_all.update(extract_basic_info(driver))
            except Exception as e:
                print(f'Error processing 기본정보 1 {url}: {e}')
                pass

            try:
                job_info_all.update(extract_additional_info(driver))
            except Exception as e:
                print(f'Error processing 기본정보 2 {url}: {e}')
                pass

            try:
                job_info_all['Co_name'] = extract_company_name(driver)
            except Exception as e:
                print(f'Error processing 기업 이름 {url}: {e}')
                pass

            try:
                img_list, detail_data, logo_url = extract_job_details(driver, headers)
                job_info_all['img_list'] = img_list
                job_info_all['detail_data'] = detail_data
                job_info_all['logo_url'] = logo_url
            except Exception as e:
                print(f'Error processing 채용공고 내용 {url}: {e}')
                continue

            json.dump(job_info_all, f, ensure_ascii=False)
            f.write('\n')

            end_time_all = time.time()
            execution_time_all = end_time_all - start_time_all
            print(f"{job_info_all.get('Co_name')} 크롤링 소요시간 : {round(execution_time_all, 2)} seconds")

            driver.quit()  # 각 URL 처리 후 WebDriver 세션 종료

    print('크롤링이 완료되었습니다.')


def print_job_data(**kwargs):
    try:
        with open('job_data.jsonl', 'r', encoding='utf-8') as f:
            for line in f:
                print(line.strip())
    except Exception as e:
        print(f'Error reading job_data.jsonl: {e}')

# DAG 정의
with DAG(
    dag_id="saramin_crawling_dag",
    default_args=default_args,
    description="Job crawling from Saramin using Selenium and BeautifulSoup",
    schedule_interval="0 18 * * *",  # 매일 저녁 6시에 실행
    catchup=False,  # 과거 실행을 수행하지 않음
) as dag:

    get_urls_task = PythonOperator(
        task_id='get_urls_task',
        python_callable=get_job_urls,
        op_kwargs={'user_agents' : user_agents},
        dag=dag,
    )

    crawl_job_task = PythonOperator(
        task_id='crawl_job_data',
        python_callable=task2,
        dag=dag,
    )
    print_job_data_task = PythonOperator(
        task_id='print_job_data_task',
        python_callable=print_job_data,
        dag=dag,
    )
        # DAG에 작업 추가
        # init_driver_task >> get_job_urls_task
    get_urls_task >> crawl_job_task >> print_job_data_task

