import requests
from bs4 import BeautifulSoup
import pandas as pd
from typing import List, Dict, Optional
from datetime import datetime
from tqdm import tqdm
import os
import shutil
import json


DEFAULT_SETTING = {
    "batch_size": 10,
    "save_as_json": True
}


def get_cbnc_article(url: str, id: str) -> bool:
    try:
        response = requests.get(url)

        if response.status_code == 200:
            html = response.text

            soup = BeautifulSoup(html, 'html.parser')
            if not soup:
                raise Exception("Soup is None")
            # class가 ArticleBody-articleBody 인 div를 찾는다.
            article_body = soup.find('div', class_='ArticleBody-articleBody')
            if not article_body:
                raise Exception(f"ArticleBody-articleBody not found in {url}")

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
            with open(f'articles/{id}.txt', 'w') as f:
                f.write(text)
            return True
        else:
            raise Exception(
                f"Response Error with status code {response.status_code}")

    except Exception as e:
        # print(f"Error: {e}")
        return False


def get_api(search_term: str, page: int, setting: Dict[str, any]):
    queryly_key = setting['private_key']['queryly_key']
    additionalindexes = setting['private_key']['additionalindexes']
    batch_size = setting['batch_size']
    return f"https://api.queryly.com/cnbc/json.aspx?queryly_key={queryly_key}&query={search_term}&endindex={page*batch_size}&batchsize={batch_size}&callback=&showfaceted=false&timezoneoffset=-540&facetedfields=formats&facetedkey=formats%7C&facetedvalue=!Press%20Release%7C&additionalindexes={additionalindexes}"


def save_array(array, file_name, save_as_json=True):
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


def get_page_date(search_term: str, page: int):
    try:
        response = requests.get(get_api(search_term, page - 1, setting))
        if response.status_code != 200:
            raise Exception(
                f"Response Error with status code {response.status_code}")

        # Get Article Info
        data = response.json()
        article_info_list = data["results"]
        if not article_info_list:
            raise Exception("Article info not found")
        return article_info_list[len(article_info_list) // 2]['datePublished'].split('T')[0]
    except Exception as e:
        # print(f"Error: {e}")
        return "NaN"


def get_page_date2(search_term: str, page: int, setting: Dict[str, any]) -> Optional[str]:
    try:
        batch_size = setting['batch_size']
        api_url = f"https://api.queryly.com/cnbc/json.aspx?"\
            f"queryly_key={setting['queryly_key']}&"\
            f"query={search_term}&"\
            f"endindex={(page-1)*batch_size}&"\
            f"batchsize={batch_size}&callback=&showfaceted=false&timezoneoffset=-540&facetedfields=formats&facetedkey=formats%7C&facetedvalue=!Press%20Release%7C&"\
            f"additionalindexes={setting['additionalindexes']}"
        # print(api_url)
        response = requests.get(api_url)
        if response.status_code != 200:
            raise Exception(
                f"Response Error with status code {response.status_code}")

        # Get Article Info
        data = response.json()
        article_info_list = data["results"]
        if not article_info_list:
            raise Exception("Article info not found")
        return article_info_list[len(article_info_list) // 2]['datePublished'].split('T')[0]
    except Exception as e:
        # print(f"Error: {e}")
        return None


def get_total_page(search_term: str, setting: Dict[str, any]) -> int:
    try:
        batch_size = setting['batch_size']
        api_url = f"https://api.queryly.com/cnbc/json.aspx?"\
            f"queryly_key={setting['queryly_key']}&"\
            f"query={search_term}&"\
            f"endindex={batch_size}&"\
            f"batchsize={batch_size}&callback=&showfaceted=false&timezoneoffset=-540&facetedfields=formats&facetedkey=formats%7C&facetedvalue=!Press%20Release%7C&"\
            f"additionalindexes={setting['additionalindexes']}"
        # print(api_url)
        response = requests.get(api_url)
        if response.status_code != 200:
            raise Exception(
                f"Response Error with status code {response.status_code}")

        # Get Article Info
        data = response.json()
        meta = data["metadata"]
        if not meta:
            raise Exception("metadata not found")
        return int(meta['totalpage'])
    except Exception as e:
        print(f"Error: {e}")
        return 1


def get_article_page(search_term: str, page: int, setting: Dict[str, any]):
    # Get Response
    response = requests.get(get_api(search_term, page - 1, setting))
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
        f"Total {len(new_article_info_list)} articles found in page {page}", end="  |  Get articles...\n")

    return new_article_info_list


def get_article_list(new_article_info_list: List[Dict[str, any]], page: int, setting: Dict[str, any]):
    # Get Article
    error_index_list = []
    for i in tqdm(range(len(new_article_info_list))):
        sucess = get_cbnc_article(
            new_article_info_list[i]["url"], new_article_info_list[i]["id"])
        if not sucess:
            error_index_list.append(i)
    # pop error items
    for i in error_index_list[::-1]:
        new_article_info_list.pop(i)

    return new_article_info_list


def get_closest_page(keyword: str, target_date: datetime, setting: Dict[str, any]) -> int:
    # 이진 탐색으로 2024-01-01에 가장 가까운 페이지 찾기
    left = 1
    right = get_total_page(keyword, setting)
    while left < right:
        mid = (left + right) // 2
        page_date = get_page_date2(keyword, mid, setting)
        print(f"Searching[ page: {mid}, date: {page_date} ]")
        if page_date is None:
            print("Error: page_date is None")
            break
        try:
            date = datetime.strptime(page_date, '%Y-%m-%d')
        except ValueError:
            print(
                f"Error: page_date is not valid datetime format: {page_date}")
            break
        if date > target_date:
            left = mid + 1
        else:
            right = mid
    return left


def compare_lists(list1, list2):
    set1 = set([str(i) for i in list1])
    set2 = set([str(i) for i in list2])

    common_elements = set1 & set2  # 두 집합의 교집합 (공통 요소)
    # 첫 번째 집합에서 두 번째 집합을 뺀 차집합 (list1에서만 존재하는 요소)
    unique_to_list1 = set1 - set2
    # 두 번째 집합에서 첫 번째 집합을 뺀 차집합 (list2에서만 존재하는 요소)
    unique_to_list2 = set2 - set1

    return {
        "Common": list(common_elements),
        "Only in list1": list(unique_to_list1),
        "Only in list2": list(unique_to_list2)
    }


def get_continue_start_page(keyword: str, continue_folder_path: str) -> int:
    # 이어서 다운로드 하기를 원할 경우 해당 파일의 경로를 입력.
    # 1. 몇 페이지까지 진행했는지 확인
    # 2. 로그 값들에서 id값들을 중복 제거해서 가져옴.
    # 3. articles 파일 이름들 중 id값들을 제외하고 모두 제거함.
    # 4. 다시 시작
    log_list_path = os.path.join(continue_folder_path, keyword, 'info_logs')
    log_file_list = [file for file in os.listdir(
        log_list_path) if file.endswith('.json') or file.endswith('.xlsx')]

    combined_id = []
    for file in log_file_list:
        is_json = file.endswith('.json')
        try:
            if is_json:
                log_df = pd.read_json(os.path.join(log_list_path, file))
            else:
                log_df = pd.read_excel(os.path.join(log_list_path, file))
            combined_id.extend(log_df['id'].tolist())
        except Exception as e:
            print(f"Remove file: {file}")
            os.remove(os.path.join(log_list_path, file))

            print("문제가 발생했습니다. 해결 완료 되었으니 다시 실행해주세요.")
            exit(0)
    combined_id = list(set(combined_id))
    # print(combined_id)

    article_list_path = os.path.join(continue_folder_path, keyword, 'articles')
    article_file_list = [file for file in os.listdir(
        article_list_path) if file.endswith('.txt')]

    compare_result = compare_lists(
        combined_id, [file.split('.')[0] for file in article_file_list])
    have_to_delete_files = compare_result['Only in list2']
    for file in have_to_delete_files:
        print(f"Remove file: {file}")
        os.remove(os.path.join(article_list_path, f'{file}.txt'))

    return len(log_file_list) + 1


def remove_duplicates_id(info_list: List[Dict[str, any]]) -> List[Dict[str, any]]:
    id_list = []
    new_info_list = []
    for info in info_list:
        if info['id'] not in id_list:
            id_list.append(info['id'])
            new_info_list.append(info)
    return new_info_list


def remove_file_that_not_in_info(info_list: List[Dict[str, any]], folder_name: str):
    id_list = [info['id'] for info in info_list]
    file_list = [file for file in os.listdir(
        folder_name) if file.endswith('.txt')]
    for file in file_list:
        if file.split('.')[0] not in id_list:
            print(f"Remove empty file: {file}")
            os.remove(os.path.join(folder_name, file))


"""     HYPERPARAMETERS START   """

# 주 설정
# keyword_list = ['apple', 'samsung', 'amazon', 'facebook',
#                 'google', 'microsoft', 'tesla', 'netflix', 'alibaba', 'tencent']
keyword_list = ['samsung', 'netflix', 'sk']
start_page = 1  # 1부터 시작
end_page = 3    # 설정 여부와 관계없이 자동으로 target_date에 가장 가까운 end_page를 찾음.
save_as_json = False
target_date = datetime(2024, 4, 10)
save_location = os.getcwd()

# 이어서 다운로드 하기를 원할 경우 해당 파일의 경로를 입력.
continue_folder_path = None

"""     HYPERPARAMETERS END     """

if continue_folder_path != None:
    print(f"Continue from {continue_folder_path}")

# 부과 설정
batch_size = 10
queryly_key = "31a35d40a9a64ab3"
additionalindexes = "4cd6f71fbf22424d, 937d600b0d0d4e23, 3bfbe40caee7443e, 626fdfcd96444f28"

# OS setup
if continue_folder_path == None:
    project_dir_name = f"cnbc_news_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    os.chdir(save_location)
    os.mkdir(project_dir_name)
else:
    project_dir_name = continue_folder_path
os.chdir(project_dir_name)

# Combine with default setting
setting = {**DEFAULT_SETTING, **{
    'batch_size': batch_size,
    'save_as_json': save_as_json,
    'private_key': {
        'queryly_key': queryly_key,
        'additionalindexes': additionalindexes
    }
}}

# 마지막으로 진행하였던 페이지 번호(자동으로 설정됨. 사용자가 설정하는 것 아님)
continue_start_page = None

# Get Article List
for keyword in keyword_list:
    if continue_folder_path != None:
        cur_path = os.getcwd()
        if keyword in os.listdir(cur_path):
            if os.path.exists(os.path.join(cur_path, keyword, f'info_{keyword}.json')) or os.path.exists(os.path.join(cur_path, keyword, f'info_{keyword}.xlsx')):
                print(f"{keyword} is already downloaded.")
                continue
            else:  # 폴더는 존재하는데 info_{keyword}.json 파일이 없는 경우 하다가 중단된 곳.
                print(f"Continue from {keyword}")
                continue_start_page = get_continue_start_page(
                    keyword, cur_path)
    search_term = keyword
    os.chdir(os.path.join(save_location, project_dir_name))
    os.makedirs(search_term, exist_ok=True)
    os.chdir(search_term)
    os.makedirs("articles", exist_ok=True)
    os.makedirs("info_logs", exist_ok=True)

    print(
        f"Searching closest page for {target_date.strftime('%Y/%m/%d')}...")
    target_page_num = get_closest_page(keyword, target_date, {
        'batch_size': batch_size,
        'queryly_key': queryly_key,
        'additionalindexes': additionalindexes,
    }) + 1
    print(f"Find closest page: {target_page_num}\n")

    info_list = []
    if continue_start_page != None:
        start_page = continue_start_page
        continue_start_page = None
        continue_folder_path = None
        print(f"Continue from page {start_page}")

    for page in range(start_page, target_page_num + 1):
        unclear_new_article_info_list = get_article_page(
            search_term, page, setting)
        new_article_info_list = get_article_list(
            unclear_new_article_info_list, page, setting)
        print(
            f"Get {len(new_article_info_list)} articles in page {page}", end="  |  ")
        # Save Array
        save_array(new_article_info_list,
                   f"info_logs/{search_term}_{page}", setting["save_as_json"])
        info_list += new_article_info_list

    # Save Total Info
    info_list = remove_duplicates_id(info_list)
    remove_file_that_not_in_info(info_list, "articles")
    save_array(info_list, f"info_{search_term}", save_as_json)
    print(f"Done {search_term}!\n")
    shutil.rmtree("info_logs")
