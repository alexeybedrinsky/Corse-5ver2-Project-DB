import requests
from src.db_manager import DBManager
import time


def get_vacancies(company_id):
    print(f"Поиск вакансий для компании ID: {company_id}")
    url = f"https://api.hh.ru/vacancies"
    params = {
        "employer_id": company_id,
        "per_page": 100,
        "page": 0
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    }
    all_vacancies = []

    while True:
        response = requests.get(url, params=params, headers=headers)
        if response.status_code != 200:
            print(f"Не удалось получить данные: {response.status_code}")
            break

        data = response.json()
        all_vacancies.extend(data['items'])

        if len(all_vacancies) >= data['found'] or len(all_vacancies) >= 500:
            break

        params['page'] += 1
        time.sleep(0.2)  # Добавляем небольшую задержку между запросами

    print(f"Всего найдено вакансий: {len(all_vacancies)}")
    return all_vacancies


def main():
    print("Запуск основной функции")
    db_manager = DBManager()
    print("DBManager экземпляр создан")

    companies = [
        ("Сбербанк", "3529"),
        ("Тинькофф", "78638"),
        ("Авито", "84585"),
        ("ВТБ", "4181"),
        ("Альфа-Банк", "80"),
        ("Аэрофлот", "1373"),
        ("РЖД", "23427"),
        ("Газпром", "39305"),
        ("Озон", "2180"),
        ("Циан", "1429999")
    ]

    print(f"Количество компаний для обработки: {len(companies)}")

    print("\nВставка компаний и получение вакансий:")
    for company_name, company_hh_id in companies:
        print(f"Сканируем компанию: {company_name}")
        employer_id = db_manager.insert_employer(company_name, company_hh_id)
        print(f"Inserted {company_name} with id {employer_id}")

        vacancies = get_vacancies(company_hh_id)
        print(f"Найдено {len(vacancies)} вакансий для {company_name}")

        for vacancy in vacancies:
            name = vacancy["name"]
            salary = vacancy["salary"]
            url = vacancy["alternate_url"]
            requirement = vacancy.get("snippet", {}).get("requirement", "")
            responsibility = vacancy.get("snippet", {}).get("responsibility", "")

            db_manager.insert_vacancy(name, employer_id, salary, url, requirement, responsibility)

        print(f"Найдено вакансий для {company_name}")

    print("Компании и вакансии успешно добавлены")

    print("\nУчет компаний и вакансий:")
    companies_and_vacancies = db_manager.get_companies_and_vacancies_count()
    for company, count in companies_and_vacancies:
        print(f"{company}: {count} vacancies")

    print("\nПодсчет средней зарплаты:")
    avg_salary = db_manager.get_avg_salary()
    print(f"AСредняя зарплата: {avg_salary:.2f} RUB")

    print("\nПолучение вакансий с зарплатами выше средней:")
    high_salary_vacancies = db_manager.get_vacancies_with_higher_salary()
    for vacancy in high_salary_vacancies[:5]:
        print(f"{vacancy[0]} at {vacancy[1]}, Зарплата: {vacancy[2]}, URL: {vacancy[3]}")

    print("\nПоиск вакансий по ключевому слову 'Python':")
    python_vacancies = db_manager.get_vacancies_with_keyword('Python')
    for vacancy in python_vacancies[:5]:
        print(f"{vacancy[0]} at {vacancy[1]}, Зарплата: {vacancy[2]}, URL: {vacancy[3]}")

    print("Closing database connection")
    db_manager.close()
    print("Main function completed")


if __name__ == "__main__":
    main()
