import requests
import json
import os
import time

def fetch_vacancies(query, area, page, per_page):
    url = 'https://api.hh.ru/vacancies'
    params = {
        'text': query,
        'area': area,
        'page': page,
        'per_page': per_page
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json().get('items', [])
    except requests.RequestException as e:
        print(f"Ошибка при выполнении запроса: {e}")
        return []

def fetch_employers(vacancies):
    employers = {}
    for vacancy in vacancies:
        employer_id = vacancy.get('employer', {}).get('id')
        if employer_id and employer_id not in employers:
            url = f'https://api.hh.ru/employers/{employer_id}'
            try:
                response = requests.get(url)
                response.raise_for_status()
                employers[employer_id] = response.json()
                time.sleep(0.2)  # Небольшая задержка между запросами
            except requests.RequestException as e:
                print(f"Ошибка при получении данных о работодателе {employer_id}: {e}")
    return employers

def save_to_json(data, filename):
    current_dir = os.path.dirname(__file__)
    json_path = os.path.join(current_dir, filename)
    try:
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"Данные успешно записаны в файл {json_path}")
    except IOError as e:
        print(f"Ошибка при записи в файл {filename}: {e}")

if __name__ == "__main__":
    vacancies = fetch_vacancies('Python developer', 1, 0, 10)
    if vacancies:
        employers = fetch_employers(vacancies)
        save_to_json(vacancies, 'vacancies.json')
        save_to_json(employers, 'employers.json')
    else:
        print("Не удалось получить вакансии. Проверьте параметры запроса.")
