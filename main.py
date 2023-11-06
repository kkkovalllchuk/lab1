import psycopg2
import itertools
import time
import csv
import re
import os
from config import connection



def create_table(conn):
    cur = conn.cursor()
    cur.execute("""
                DROP TABLE IF EXISTS zno;
                CREATE TABLE zno (
                    out_id VARCHAR PRIMARY KEY,
                     birth VARCHAR,
                     sex VARCHAR,
                     region VARCHAR,
                     area VARCHAR,
                     tername VARCHAR,
                     reg_type VARCHAR,
                     ter_type VARCHAR,
                     class_profile VARCHAR,
                     class_lang VARCHAR,
                     EOName VARCHAR,
                     EOType VARCHAR,
                     EOReg VARCHAR,
                     EOArea VARCHAR,
                     EOTer VARCHAR,
                     EOParent VARCHAR,
                     ukr_test VARCHAR,
                     ukr_test_stat VARCHAR,
                     ukr_100 FLOAT,
                     ukr_12 FLOAT,
                     ukr_ball FLOAT,
                     ukr_adapt FLOAT,
                     ukrPTName VARCHAR,
                     ukrPTReg VARCHAR,
                     ukrPTArea VARCHAR,
                     ukrPTTer VARCHAR,
                     hist_test VARCHAR,
                     hist_lang VARCHAR,
                     hist_test_stat VARCHAR,
                     hist_100 FLOAT,
                     hist_12 FLOAT,
                     hist_ball FLOAT,
                     histPTName VARCHAR,
                     histPTReg VARCHAR,
                     histPTArea VARCHAR,
                     histPTTer VARCHAR,
                     math_test VARCHAR,
                     math_lang VARCHAR,
                     math_test_stat VARCHAR,
                     math_100 FLOAT,
                     math_12 FLOAT,
                     math_ball FLOAT,
                     mathPTName VARCHAR,
                     mathPTReg VARCHAR,
                     mathPTArea VARCHAR,
                     mathPTTer VARCHAR,
                     phys_test VARCHAR,
                     phys_lang VARCHAR,
                     phys_test_stat VARCHAR,
                     phys_100 FLOAT,
                     phys_12 FLOAT,
                     phys_ball FLOAT,
                     physPTName VARCHAR,
                     physPTReg VARCHAR,
                     physPTArea VARCHAR,
                     physPTTer VARCHAR,
                     chem_test VARCHAR,
                     chem_lang VARCHAR,
                     chem_test_stat VARCHAR,
                     chem_100 FLOAT,
                     chem_12 FLOAT,
                     chem_ball FLOAT,
                     chemPTName VARCHAR,
                     chemPTReg VARCHAR,
                     chemPTArea VARCHAR,
                     chemPTTer VARCHAR,
                     bio_test VARCHAR,
                     bio_lang VARCHAR,
                     bio_test_stat VARCHAR,
                     bio_100 FLOAT,
                     bio_12 FLOAT,
                     bio_ball FLOAT,
                     bioPTName VARCHAR,
                     bioPTReg VARCHAR,
                     bioPTArea VARCHAR,
                     bioPTTer VARCHAR,
                     geo_test VARCHAR,
                     geo_lang VARCHAR,
                     geo_test_stat VARCHAR,
                     geo_100 FLOAT,
                     geo_12 FLOAT,
                     geo_ball FLOAT,
                     geoPTName VARCHAR,
                     geoPTReg VARCHAR,
                     geoPTArea VARCHAR,
                     geoPTTer VARCHAR,
                     eng_test VARCHAR,
                     eng_test_stat VARCHAR,
                     eng_100 FLOAT,
                     eng_12 FLOAT,
                     eng_dpa VARCHAR,
                     eng_ball FLOAT,
                     engPTName VARCHAR,
                     engPTReg VARCHAR,
                     engPTArea VARCHAR,
                     engPTTer VARCHAR,
                     fra_test VARCHAR,
                     fra_test_stat VARCHAR,
                     fra_100 FLOAT,
                     fra_12 FLOAT,
                     fra_dpa VARCHAR,
                     fra_ball FLOAT,
                     fraPTName VARCHAR,
                     fraPTReg VARCHAR,
                     fraPTArea VARCHAR,
                     fraPTTer VARCHAR,
                     deu_test VARCHAR,
                     deu_test_stat VARCHAR,
                     deu_100 FLOAT,
                     deu_12 FLOAT,
                     deu_dpa VARCHAR,
                     deu_ball FLOAT,
                     deuPTName VARCHAR,
                     deuPTReg VARCHAR,
                     deuPTArea VARCHAR,
                     deuPTTer VARCHAR,
                     spa_test VARCHAR,
                     spa_test_stat VARCHAR,
                     spa_100 FLOAT,
                     spa_12 FLOAT,
                     spa_dpa VARCHAR,
                     spa_ball FLOAT,
                     spaPTName VARCHAR,
                     spaPTReg VARCHAR,
                     spaPTArea VARCHAR,
                     spaPTTer VARCHAR,
                     year VARCHAR);""")
    conn.commit()
    cur.close()
    return conn


def drop_table(conn):
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS zno")
    conn.commit()
    cur.close()
    return conn


def is_float(string):
    if re.match('^\\d+,\\d+$', string):
        return True
    return False


def create_formatted_row(old_row, year):
    for i in range(len(old_row)):
        if is_float(old_row[i]):
            old_row[i] = old_row[i].replace(',', '.')

    formatted_row = ';'.join(old_row)

    return formatted_row + ';' + year + '\n'


def insert_data(file_name, conn, size):
    start = time.time()

    cur = conn.cursor()
    inserted_parts = 0
    is_final = False

    try:
        os.remove('tmp.csv')
    except OSError:
        pass

    with open(file_name, encoding='windows-1251') as f:
        reader = csv.reader(f, delimiter=';', quoting=csv.QUOTE_ALL)
        year = file_name[5:9]
        next(reader, None)

        count = 0
        with open('tmp.csv', 'w+') as tmp:
            while not is_final:
                try:
                    for line in reader:
                        tmp.write(create_formatted_row(line, year))
                        count += 1

                        if count == size:
                            tmp.seek(0, 0)
                            cur.copy_from(tmp, 'zno', sep=';', null='null')
                            conn.commit()
                            tmp.seek(0, 0)
                            tmp.truncate(0)
                            inserted_parts += 1
                            count = 0

                    if count != 0:
                        tmp.seek(0, 0)
                        cur.copy_from(tmp, 'zno', sep=';', null='null')
                        conn.commit()
                        is_final = True

                except psycopg2.OperationalError as error:
                    print(error)
                    print("Втрачено підключення до бази даних...\nПовторюємо підключення...\n")
                    is_connected = False
                    while not is_connected:
                        try:
                            conn = psycopg2.connect(**connection)
                            cur = conn.cursor()
                            is_connected = True
                        except psycopg2.OperationalError as error:
                            time.sleep(1)

                    print("Підключення до бази даних відновлено!\nДоопрацьовуємо дані...")
                    f.seek(0, 0)
                    reader = itertools.islice(csv.reader(f, delimiter=';'),
                                              inserted_parts * size + 1, None)
                    tmp.seek(0, 0)
                    tmp.truncate(0)

    cur.close()
    os.remove('tmp.csv')
    end = time.time()

    with open('time-measurements.txt', 'a', encoding='windows-1251') as log:
        log.write(f'Запис та обробка файлу {file_name}: {end - start} секунд.\n')

    return conn


def task(conn):
    query = '''
    SELECT * FROM zno LIMIT 5
    '''

    cur = conn.cursor()
    cur.execute(query)

    with open('results.csv', 'w') as res:
        writer = csv.writer(res, lineterminator='\n')
        writer.writerow(['Регіон', 'Рік', 'Середній бал з математики'])
        for row in cur:
            writer.writerow(row)


def main():

    conn = None
    repeat = 0

    while repeat < 10:
        try:
            print('Спроба з\'єднатися з базою даних...')
            conn = psycopg2.connect(**connection)
            print('З\'єднання встановлено!\n')

            print('Видаляємо таблицю, якщо вона існує...')
            conn = drop_table(conn)
            print('Таблиця видалена!\n')

            print('Створюємо таблицю...')
            conn = create_table(conn)
            print('Таблиця створена!\n')

            print('Опрацьовуємо перший файл...')
            conn = insert_data(r'Odata2019File.csv', conn, 100)
            print('Перший файл занесений в базу даних!\n')

            print('Опрацьовуємо другий файл...')
            conn = insert_data(r'Odata2020File.csv', conn, 100)
            print('Другий файл занесений в базу даних!\n')

            print("""Виконуємо порівняння середніх балів з математики...\n""")
            task(conn)
            print('Результати доступні в файлі results.csv')

            conn.close()
            break

        except psycopg2.DatabaseError as error:
            print(error)
            print('Помилка з\'єднання з базою даних. Повторюємо спробу...\n')
            time.sleep(2)

        finally:
            repeat += 1
            if repeat == 10:
                print(f'Не вдалося з\'єднатися з базою даних. Всього запитів: {repeat}')
            if conn is not None:
                conn.close()


if __name__ == '__main__':
    main()
