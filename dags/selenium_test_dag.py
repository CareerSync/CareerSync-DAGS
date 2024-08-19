from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    catchup=False,
}

# DAG 정의
dag = DAG(
    'selenium_test_dag',
    default_args=default_args,
    description='A DAG to test Selenium with remote chromedriver',
    schedule_interval=timedelta(days=1),
)

def test_selenium(**kwargs):
    options = Options()
    options.add_argument('--headless')
    options.add_argument('window-size=1200x600')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')

    # remote_chromedriver 컨테이너에 연결
    remote_webdriver = 'http://remote_chromedriver:4444/wd/hub'
    with webdriver.Remote(remote_webdriver, options=options) as driver:
        # 웹 페이지 방문 및 테스트 작업 수행
        driver.get("https://n.news.naver.com/mnews/article/005/0001606450")
        print(driver.title)  # 웹 페이지 제목을 출력 (로그에 기록됨)

# PythonOperator를 사용하여 test_selenium 함수를 실행
selenium_test_task = PythonOperator(
    task_id='test_selenium_task',
    python_callable=test_selenium,
    provide_context=True,
    dag=dag,
)

# DAG의 Task 순서 정의
selenium_test_task
