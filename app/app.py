import os
import sys
import json
from tqdm import tqdm
from datetime import datetime
import shutil
import requests
from bs4 import BeautifulSoup
import pandas as pd
from typing import List, Dict
import random
from PyQt6.QtWidgets import (
    QApplication, QVBoxLayout, QWidget, QLineEdit, QPushButton, QLabel,
    QSpinBox, QCheckBox, QFileDialog, QHBoxLayout, QMessageBox, QProgressBar
)
from PyQt6.QtGui import QFont, QIntValidator
from PyQt6.QtCore import QDate, QThread, pyqtSignal
# pip install pyinstaller tqdm PyQt6 requests pandas beautifulsoup4
# pyinstaller --onefile --windowed app.py


class Worker(QThread):
    update_progress_signal = pyqtSignal(int, int, int, int)
    finished_signal = pyqtSignal()

    def __init__(self, setting: Dict[str, any], parent=None):
        super(Worker, self).__init__(parent)
        self.DEFAULT_SETTING = {
            "search_term": "",
            "start_page": 1,
            "end_page": 3,
            "save_as_json": True,
            "save_location": os.getcwd(),
            "batch_size": 10,
            "queryly_key": "31a35d40a9a64ab3",
            "additionalindexes": "4cd6f71fbf22424d, 937d600b0d0d4e23, 3bfbe40caee7443e, 626fdfcd96444f28"
        }
        self.setting = {**self.DEFAULT_SETTING, **setting}
        self.cur_page = self.setting['start_page']

    def update_setting(self, setting):
        self.setting = {**self.DEFAULT_SETTING, **setting}

    def get_cbnc_article(self, url: str, id: str) -> bool:
        try:
            response = requests.get(url)

            if response.status_code == 200:
                html = response.text

                soup = BeautifulSoup(html, 'html.parser')
                if not soup:
                    raise Exception("Soup is None")
                # class가 ArticleBody-articleBody 인 div를 찾는다.
                article_body = soup.find(
                    'div', class_='ArticleBody-articleBody')
                if not article_body:
                    raise Exception(
                        f"ArticleBody-articleBody not found in {url}")

                # 그 뒤 클래스 이름이 group 인 첫 번째 div를 찾는다.
                group = article_body.find('div', class_='group')
                if not group:
                    raise Exception("group not found")

                # group 안에 있는 모든 p 태그를 찾아 텍스트를 추출한다.
                paragraphs = group.find_all('p')
                text = '\n'.join([p.get_text() for p in paragraphs])
                if not text or text.replace('\n', '').replace(' ', '') == '':
                    raise Exception("Text is empty")

                # 저장
                with open(f"{self.setting['full_path']}/articles/{id}.txt", 'w') as f:
                    f.write(text)
                return True
            else:
                raise Exception(
                    f"Response Error with status code {response.status_code}")

        except Exception as e:
            # print(f"Error: {e}")
            return False

    def get_api(self, search_term: str, page: int):
        queryly_key = self.setting['queryly_key']
        additionalindexes = self.setting['additionalindexes']
        batch_size = self.setting['batch_size']
        return f"https://api.queryly.com/cnbc/json.aspx?queryly_key={queryly_key}&query={search_term}&endindex={page*batch_size}&batchsize={batch_size}&callback=&showfaceted=false&timezoneoffset=-540&facetedfields=formats&facetedkey=formats%7C&facetedvalue=!Press%20Release%7C&additionalindexes={additionalindexes}"

    def save_array(self, array, file_name, save_as_json=True):
        if save_as_json:  # json으로 저장
            with open(f'{file_name}.json', 'w') as f:
                f.write(json.dumps(array, indent=4))
            print(
                f"Save page info as json: {file_name}.json\n")
        else:  # 엑셀로 저장
            df = pd.DataFrame(array)
            df.to_excel(
                f'{file_name}.xlsx', index=False)
            print(
                f"Save page info as excel: {file_name}.xlsx\n")

    def get_article_page(self, search_term: str, page: int):
        # Get Response
        response = requests.get(
            self.get_api(search_term, page - 1))
        if response.status_code != 200:
            raise Exception(
                f"Response Error with status code {response.status_code}")

        # Get Article Info
        data = response.json()
        article_info_list = data["results"]
        if not article_info_list:
            raise Exception("Article info not found")

        # Extract Article Info
        new_article_info_list = []
        except_article_count = 0
        for article_info in article_info_list:
            try:
                # Pro article은 제외: section이 Pro로 시작하면 제외
                section = str(article_info["section"])
                if section.split(':')[0].replace(' ', '') == "Pro":
                    raise Exception("Pro Article")

                # Video article은 제외: url에 https://www.cnbc.com/video/가 포함되어 있으면 제외
                url = article_info["url"]
                if "https://www.cnbc.com/video/" in url:
                    raise Exception("Video Article")

                new_article_info_list.append({
                    "title": article_info["cn:title"],
                    "keyword": article_info["cn:keyword"],
                    "description": article_info["description"],
                    "url": url,
                    "_id": article_info["_id"],
                    "id": article_info["@id"],
                    "datePublished": article_info["datePublished"],
                    "author": article_info["author"],
                    "summary": article_info["summary"],
                })
            except Exception as e:
                # print(f"Error: {e}")
                except_article_count += 1

        print(
            f"Total {len(new_article_info_list)} articles found in page {page},  Get articles...")

        return new_article_info_list

    def get_article_list(self, new_article_info_list: List[Dict[str, any]]):
        # Get Article
        error_index_list = []
        start_page = self.setting['start_page']
        end_page = self.setting['end_page']
        current_page_idx = self.cur_page - start_page + 1
        total_pages_idx = end_page - start_page + 1
        total_news_count = len(new_article_info_list)

        for i in tqdm(range(total_news_count)):
            sucess = self.get_cbnc_article(
                new_article_info_list[i]["url"], new_article_info_list[i]["id"])
            if not sucess:
                error_index_list.append(i)

            self.update_progress_signal.emit(
                current_page_idx, total_pages_idx, i + 1, total_news_count)

        # pop error items
        for i in error_index_list[::-1]:
            new_article_info_list.pop(i)

        return new_article_info_list

    def run(self):
        try:
            print("Crawling Start\n")
            start_page = self.setting['start_page']
            end_page = self.setting['end_page']
            search_term = self.setting['search_term']
            info_list = []

            for page in range(start_page, end_page + 1):
                if not self.isInterruptionRequested():
                    self.cur_page = page
                    unclear_new_article_info_list = self.get_article_page(
                        search_term, page)
                    new_article_info_list = self.get_article_list(
                        unclear_new_article_info_list)
                    print(
                        f"Suceessfully get {len(new_article_info_list)} articles in page {page}")

                    # 로그 저장
                    filename = os.path.join(
                        self.setting['full_path'], "info_logs", f"{search_term}_{page}")
                    self.save_array(new_article_info_list, filename,
                                    self.setting['save_as_json'])
                    info_list += new_article_info_list
                else:
                    break

            # 전체 로그 저장
            print("Crawling Finished!")
            self.save_array(
                info_list, f"{self.setting['full_path']}/info_{search_term}", self.setting['save_as_json'])
            shutil.rmtree(os.path.join(self.setting['full_path'], "info_logs"))
            self.finished_signal.emit()
        except Exception as e:
            print(f"Exception in worker thread: {str(e)}")
            self.finished_signal.emit()

    def stop(self):
        self.requestInterruption()


class NewsCrawlerApp(QWidget):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("News Crawler")
        self.setGeometry(100, 100, 420, 600)
        layout = QVBoxLayout()

        # 제목 설정
        main_title = QLabel("메인 설정")
        main_title.setFont(QFont('Arial', 14, QFont.Weight.Bold))
        layout.addWidget(main_title)

        # 검색 키워드
        search_layout = QHBoxLayout()
        search_layout.addWidget(QLabel("검색 키워드"))
        self.search_term_edit = QLineEdit()
        self.search_term_edit.setPlaceholderText("검색 키워드를 입력하세요")
        self.search_term_edit.setStyleSheet(
            "border-radius: 5px; padding: 2px;")
        search_layout.addWidget(self.search_term_edit)
        layout.addLayout(search_layout)

        # 시작 페이지
        start_page_layout = QHBoxLayout()
        start_page_layout.addWidget(QLabel("시작 페이지"))
        self.start_page_edit = QLineEdit()
        self.start_page_edit.setValidator(
            QIntValidator(1, 10000, self))
        self.start_page_edit.setText("1")  # 기본 값 설정
        self.start_page_edit.setStyleSheet(
            "border-radius: 5px; padding: 2px;")
        start_page_layout.addWidget(self.start_page_edit)
        self.start_date_btn = QPushButton("페이지 날짜 확인")
        self.start_date_label = QLabel("0000-00-00")
        self.start_date_btn.clicked.connect(
            lambda: self.get_page_date(self.start_date_label, True))
        start_page_layout.addWidget(self.start_date_btn)
        start_page_layout.addWidget(self.start_date_label)
        layout.addLayout(start_page_layout)

        # 끝 페이지
        end_page_layout = QHBoxLayout()
        end_page_layout.addWidget(QLabel("끝 페이지"))
        self.end_page_edit = QLineEdit()
        self.end_page_edit.setValidator(
            QIntValidator(1, 10000, self))  # 예시 범위는 1에서 10000까지
        self.end_page_edit.setText("3")  # 기본 값 설정
        self.end_page_edit.setStyleSheet(
            "border-radius: 5px; padding: 2px;")
        end_page_layout.addWidget(self.end_page_edit)
        self.end_date_btn = QPushButton("페이지 날짜 확인")
        self.end_date_label = QLabel("0000-00-00")
        self.end_date_btn.clicked.connect(
            lambda: self.get_page_date(self.end_date_label, False))
        end_page_layout.addWidget(self.end_date_btn)
        end_page_layout.addWidget(self.end_date_label)
        layout.addLayout(end_page_layout)

        # 저장 위치
        save_layout = QHBoxLayout()
        self.save_location_edit = QLineEdit()
        self.save_location_edit.setPlaceholderText("저장 위치를 선택하세요")
        self.save_location_edit.setStyleSheet(
            "border-radius: 5px; padding: 2px;")
        save_layout.addWidget(QLabel("저장 위치"))
        self.save_button = QPushButton("저장 위치 선택")
        self.save_button.clicked.connect(self.set_save_location)
        save_layout.addWidget(self.save_location_edit)
        save_layout.addWidget(self.save_button)
        layout.addLayout(save_layout)

        # Json/Excel 저장
        self.save_as_json_check = QCheckBox("Json/Excel로 저장")
        self.save_as_json_check.setChecked(True)
        layout.addWidget(self.save_as_json_check)

        # 부가 설정 제목
        extra_settings_title = QLabel("부과 설정 - 되도록이면 수정하지 말 것")
        extra_settings_title.setFont(QFont('Arial', 14, QFont.Weight.Bold))
        layout.addWidget(extra_settings_title)

        # 배치 크기
        batch_size_layout = QHBoxLayout()
        batch_size_layout.addWidget(QLabel("배치 크기"))
        self.batch_size_edit = QSpinBox()
        self.batch_size_edit.setValue(10)
        self.batch_size_edit.setStyleSheet(
            "QSpinBox { border-radius: 2px; padding: 2px; min-width: 100px; }")
        batch_size_layout.addWidget(self.batch_size_edit)
        layout.addLayout(batch_size_layout)

        # Queryly Key
        queryly_key_layout = QHBoxLayout()
        queryly_key_layout.addWidget(QLabel("queryly_key"))
        self.queryly_key_edit = QLineEdit("queryly_key")
        self.queryly_key_edit.setPlaceholderText("Queryly 키를 입력하세요")
        self.queryly_key_edit.setText("31a35d40a9a64ab3")
        self.queryly_key_edit.setStyleSheet(
            "border-radius: 5px; padding: 2px;")
        queryly_key_layout.addWidget(self.queryly_key_edit)
        layout.addLayout(queryly_key_layout)

        # 추가 인덱스
        additionalindexes_layout = QHBoxLayout()
        additionalindexes_layout.addWidget(QLabel("additionalindexes"))
        self.additionalindexes_edit = QLineEdit("additionalindexes")
        self.additionalindexes_edit.setPlaceholderText("추가 인덱스를 입력하세요")
        self.additionalindexes_edit.setText(
            "4cd6f71fbf22424d, 937d600b0d0d4e23, 3bfbe40caee7443e, 626fdfcd96444f28")
        self.additionalindexes_edit.setStyleSheet(
            "border-radius: 5px; padding: 2px;")
        additionalindexes_layout.addWidget(self.additionalindexes_edit)
        layout.addLayout(additionalindexes_layout)

        # 뉴스 가져오기 버튼 설정 및 시그널 연결
        self.get_news_button = QPushButton("뉴스 가져오기")
        self.get_news_button.setStyleSheet(
            "QPushButton { background-color: #4C8BF5; color: white; font-size: 14px; padding: 4px; border-radius: 5px;}")
        self.get_news_button.clicked.connect(self.toggle_crawling)
        layout.addWidget(self.get_news_button)

        # 페이지 및 뉴스 항목 진행 상태를 나타내는 프로그래스 바와 레이블
        self.setup_progress_bars(layout)

        self.setLayout(layout)
        self.is_crawling = False

    def get_page_date(self, label, is_start: bool):
        try:
            search_term = self.search_term_edit.text()
            if not self.search_term_edit.text():
                QMessageBox.warning(self, "입력 필드 오류", "검색 키워드를 채워주세요")
                return

            page = int(self.start_page_edit.text()) if is_start else int(
                self.end_page_edit.text())
            batch_size = int(self.batch_size_edit.value())
            api_url = f"https://api.queryly.com/cnbc/json.aspx?queryly_key={self.queryly_key_edit.text()}&query={search_term}&endindex={(page-1)*batch_size}&batchsize={batch_size}&callback=&showfaceted=false&timezoneoffset=-540&facetedfields=formats&facetedkey=formats%7C&facetedvalue=!Press%20Release%7C&additionalindexes={self.additionalindexes_edit.text()}"
            response = requests.get(api_url)
            if response.status_code != 200:
                raise Exception(
                    f"Response Error with status code {response.status_code}")

            # Get Article Info
            data = response.json()
            article_info_list = data["results"]
            if not article_info_list:
                raise Exception("Article info not found")
            label.setText(article_info_list[len(
                article_info_list) // 2]['datePublished'].split('T')[0])
        except Exception as e:
            print(f"Error: {e}")
            label.setText("NaN")

    def set_save_location(self):
        location = QFileDialog.getExistingDirectory(self, "Select Directory")
        if location:
            self.save_location_edit.setText(location)

    def set_random_date(self, label):
        current_year = QDate.currentDate().year()
        random_date = QDate(
            current_year - 1, random.randint(1, 12), random.randint(1, 28))
        label.setText(random_date.toString("yyyy-MM-dd"))

    def setup_progress_bars(self, layout):
        page_progress_layout = QHBoxLayout()
        self.page_progress = QProgressBar(self)
        self.page_progress_label = QLabel("0/0")
        page_progress_layout.addWidget(self.page_progress)
        page_progress_layout.addWidget(self.page_progress_label)
        layout.addLayout(page_progress_layout)

        news_progress_layout = QHBoxLayout()
        self.news_progress = QProgressBar(self)
        self.news_progress_label = QLabel("0/0")
        news_progress_layout.addWidget(self.news_progress)
        news_progress_layout.addWidget(self.news_progress_label)
        layout.addLayout(news_progress_layout)

    def toggle_crawling(self):
        if not self.is_crawling:
            # 시작 전 설정 확인
            if not self.validate_input():
                return

            # Set stop button
            self.is_crawling = True
            self.get_news_button.setText("중지")
            self.get_news_button.setStyleSheet(
                "QPushButton { background-color: grey; color: white; font-size: 14px; padding: 4px; border-radius: 5px;}")

            # OS setup
            save_location = self.save_location_edit.text()
            search_term = self.search_term_edit.text()
            full_path = self.os_setup(save_location, search_term)

            # 입력 값 가져오기
            setting = {
                'search_term': search_term,
                'start_page': int(self.start_page_edit.text()),
                'end_page': int(self.end_page_edit.text()),
                'save_as_json': self.save_as_json_check.isChecked(),
                'save_location': save_location,
                'full_path': full_path,
                'batch_size': int(self.batch_size_edit.value()),
                'queryly_key': self.queryly_key_edit.text(),
                'additionalindexes': self.additionalindexes_edit.text()
            }

            # Set new thread
            self.worker = Worker(setting, self)
            self.worker.update_progress_signal.connect(self.update_progress)
            self.worker.finished_signal.connect(self.crawling_finished)
            self.worker.start()
        else:
            self.stop_crawling()

    def stop_crawling(self):
        self.worker.stop()
        self.reset_ui_after_crawling()

    def crawling_finished(self):
        self.reset_ui_after_crawling()

    def reset_ui_after_crawling(self):
        self.is_crawling = False
        self.get_news_button.setText("뉴스 가져오기")
        self.get_news_button.setStyleSheet(
            "QPushButton { background-color: #4C8BF5; color: white; font-size: 14px; padding: 4px; border-radius: 5px;}")

    def os_setup(self, save_location: str, search_term: str):
        # OS 디렉토리 설정
        project_dir_name = f"{search_term}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        full_path = os.path.join(save_location, project_dir_name)
        self.full_path = full_path
        os.makedirs(full_path, exist_ok=True)
        os.makedirs(os.path.join(full_path, "articles"), exist_ok=True)
        os.makedirs(os.path.join(full_path, "info_logs"), exist_ok=True)
        return full_path

    def validate_input(self):
        if not self.search_term_edit.text():
            QMessageBox.warning(self, "입력 필드 오류", "검색 키워드를 채워주세요")
            return False
        if not self.save_location_edit.text():
            QMessageBox.warning(self, "입력 필드 오류", "저장 위치를 채워주세요")
            return False
        if int(self.start_page_edit.text()) > int(self.end_page_edit.text()):
            QMessageBox.warning(self, "입력 필드 오류", "시작 페이지가 끝 페이지보다 큽니다")
            return False
        return True

    def update_progress(self, current_page, total_pages, current_news, news_count):
        self.page_progress.setMaximum(total_pages)
        self.page_progress.setValue(current_page)
        self.page_progress_label.setText(f"{current_page}/{total_pages}")
        self.news_progress.setMaximum(news_count)
        self.news_progress.setValue(current_news)
        self.news_progress_label.setText(f"{current_news}/{news_count}")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    ex = NewsCrawlerApp()
    ex.show()
    sys.exit(app.exec())
