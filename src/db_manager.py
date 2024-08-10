import psycopg2
from psycopg2 import sql
import json
from src.config import config

class DBManager:
    def __init__(self):
        try:
            params = config()
            print(f"Connection parameters: host={params.get('host')}, dbname={params.get('database')}, user={params.get('user')}")
            self.conn = psycopg2.connect(**params)
            self.conn.autocommit = True
            print("Successfully connected to the database")
            self.create_tables()  # Create tables if they don't exist
        except (Exception, psycopg2.Error) as error:
            print(f"Error connecting to the database: {error}")
            raise

    def create_tables(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS employers (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    hh_id VARCHAR(50) UNIQUE NOT NULL
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS vacancies (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    employer_id INTEGER REFERENCES employers(id),
                    salary JSONB,
                    url TEXT,
                    requirement TEXT,
                    responsibility TEXT
                )
            """)
        print("Tables created successfully")

    def insert_employer(self, name, hh_id):
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO employers (name, hh_id) VALUES (%s, %s) ON CONFLICT (hh_id) DO NOTHING RETURNING id",
                (name, hh_id)
            )
            return cur.fetchone()[0] if cur.rowcount > 0 else None

    def insert_vacancy(self, name, employer_id, salary, url, requirement, responsibility):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO vacancies (name, employer_id, salary, url, requirement, responsibility)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (name, employer_id, json.dumps(salary) if salary else None, url, requirement, responsibility)
            )

    def get_companies_and_vacancies_count(self):
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT e.name, COUNT(v.id) as vacancy_count
                    FROM employers e
                    LEFT JOIN vacancies v ON e.id = v.employer_id
                    GROUP BY e.id, e.name
                    ORDER BY vacancy_count DESC
                """)
                return cur.fetchall()
        except (Exception, psycopg2.Error) as error:
            print(f"Error fetching companies and vacancies count: {error}")
            return []

    def get_all_vacancies(self):
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT v.name, e.name, v.salary, v.url
                    FROM vacancies v
                    JOIN employers e ON v.employer_id = e.id
                """)
                return cur.fetchall()
        except (Exception, psycopg2.Error) as error:
            print(f"Error fetching all vacancies: {error}")
            return []

    def get_avg_salary(self):
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT AVG((salary->>'from')::int) as avg_salary
                    FROM vacancies
                    WHERE salary->>'from' IS NOT NULL
                """)
                return cur.fetchone()[0]
        except (Exception, psycopg2.Error) as error:
            print(f"Error fetching average salary: {error}")
            return None

    def get_vacancies_with_higher_salary(self):
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    WITH avg_salary AS (
                        SELECT AVG((salary->>'from')::int) as avg
                        FROM vacancies
                        WHERE salary->>'from' IS NOT NULL
                    )
                    SELECT v.name, e.name, v.salary, v.url
                    FROM vacancies v
                    JOIN employers e ON v.employer_id = e.id
                    WHERE (v.salary->>'from')::int > (SELECT avg FROM avg_salary)
                """)
                return cur.fetchall()
        except (Exception, psycopg2.Error) as error:
            print(f"Error fetching vacancies with higher salary: {error}")
            return []

    def get_vacancies_with_keyword(self, keyword):
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT v.name, e.name, v.salary, v.url
                    FROM vacancies v
                    JOIN employers e ON v.employer_id = e.id
                    WHERE v.name ILIKE %s OR v.requirement ILIKE %s
                """, (f'%{keyword}%', f'%{keyword}%'))
                return cur.fetchall()
        except (Exception, psycopg2.Error) as error:
            print(f"Error fetching vacancies with keyword '{keyword}': {error}")
            return []

    def close(self):
        if self.conn:
            self.conn.close()
            print("Database connection closed")

