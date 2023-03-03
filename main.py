import telebot
import psycopg2
import pytz
import re
import os
import threading
import time
import datetime
from telebot import types
from config import BOT_TOKEN, admins, host, user, password, port, db_name, sslmode

bot = telebot.TeleBot(BOT_TOKEN, threaded=False, skip_pending=True)

try:
    DATABASE_URL = os.environ['DATABASE_URL']
except Exception as _ex:
    DATABASE_URL = "postgres://" + user + ":" + password + "@" + host + ":" + port + "/" + db_name


def database_daily_update():

    # При первом запуске поток засыпает до нужного времени активации (10:00 серверного времени следующего буднего дня)

    current_date = datetime.datetime.now()
    weekends = current_date.isoweekday()

    # Если сегодня выходные
    if weekends >= 6:
        sleep_days = abs(8 - weekends)
        # В субботу добавляем 2 дня (вне зависимости от времени запуска)
        # В воскресение добавляем 1 день (вне зависимости от времени запуска)
        delta = datetime.timedelta(days=sleep_days)
        next_time_for_loop = current_date + delta

    # Если сегодня пятница
    elif weekends == 5:
        # Если позже 10 часов утра
        if 10 <= current_date.hour <= 23:
            delta = datetime.timedelta(days=3)
            # Добавляем к текущей дате ровно 3 суток до понедельника (учитывая месяцы, високосные года и тп)
            next_time_for_loop = current_date + delta
        else:
            # Не добавляем дополнительные сутки
            next_time_for_loop = current_date

    # Если сегодня будни и не пятница
    else:
        if 10 <= current_date.hour <= 23:
            delta = datetime.timedelta(days=1)
            # Добавляем к текущей дате ровно 1 сутки до завтра (учитывая месяцы, високосные года и тп)
            next_time_for_loop = current_date + delta
        else:
            # Не добавляем дополнительные сутки
            next_time_for_loop = current_date

    # Устанавливаем подготовленную дату на 10 часов по серверному времени
    datetime_for_next_loop = datetime.datetime.combine(next_time_for_loop, datetime.time(10))
    # Определяем сколько секунд осталось до следующего дня в 10 часов
    seconds_until_next_cycle = datetime_for_next_loop.timestamp() - current_date.timestamp()
    # Останавливаем поток на нужное количество секунд (до следующих суток в 10)
    print('sleep:', seconds_until_next_cycle)
    time.sleep(seconds_until_next_cycle)

    while True:

        connection = None

        try:
            connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)

            connection.autocommit = True

            with connection.cursor() as cursor:
                cursor.execute(
                    """SELECT id, telegram_id, tariff, investment_amount, investment_start_date, investment_end_date
                    FROM investment
                    WHERE deposit_is_active = True;"""
                )

                all_investments_data = cursor.fetchall()
                current_date = int(datetime.datetime.now().timestamp())

                for row_of_investment_data in all_investments_data:
                    # Если срок инвестиции ещё не закончился и от начала инвестиции прошло больше 10 часов
                    if row_of_investment_data[5] > current_date > row_of_investment_data[4] + 36000:

                        # Расчёт идёт НЕ по сложному проценту. Только от изначальной суммы депозита
                        if row_of_investment_data[2] == 1:
                            # В первом тарифе ежедневная прибыль 0.7%
                            daily_income = row_of_investment_data[3] * 0.007
                        elif row_of_investment_data[2] == 2:
                            # Во втором тарифе ежедневная прибыль 1%
                            daily_income = row_of_investment_data[3] * 0.01
                        elif row_of_investment_data[2] == 3:
                            # В третьем тарифе ежедневная прибыль 1.3%
                            daily_income = row_of_investment_data[3] * 0.013
                        elif row_of_investment_data[2] == 4:
                            # В четвертом тарифе ежедневная прибыль 1.6%
                            daily_income = row_of_investment_data[3] * 0.016

                        cursor.execute(
                            """UPDATE users
                            SET balance = balance + %s, investment_income = investment_income + %s
                            WHERE telegram_id = %s;""",
                            (daily_income, daily_income, row_of_investment_data[1])
                        )

                    # Если прошло меньше 10 часов с даты открытия депозита
                    elif current_date <= row_of_investment_data[4] + 36000:
                        pass

                    # Если срок инвестиции закончился
                    else:

                        # Смотрим во сколько часов был открыт депозит
                        # Если час начала депозита от 10 до 00
                        if 10 <= datetime.datetime.fromtimestamp(row_of_investment_data[4]).hour <= 23:

                            # Просто закрываем депозит
                            cursor.execute(
                                """UPDATE investment
                                SET deposit_is_active = %s
                                WHERE id = %s;""",
                                (False, row_of_investment_data[0])
                            )

                            print("[INFO] The investment was successfully closed")

                        # Если час начала депозита от 00 до 10
                        else:
                            # То начисляем ещё раз проценты

                            if row_of_investment_data[2] == 1:
                                # В первом тарифе ежедневная прибыль 0.7%
                                daily_income = row_of_investment_data[3] * 0.007
                            elif row_of_investment_data[2] == 2:
                                # Во втором тарифе ежедневная прибыль 1%
                                daily_income = row_of_investment_data[3] * 0.01
                            elif row_of_investment_data[2] == 3:
                                # В третьем тарифе ежедневная прибыль 1.3%
                                daily_income = row_of_investment_data[3] * 0.013
                            elif row_of_investment_data[2] == 4:
                                # В четвертом тарифе ежедневная прибыль 1.6%
                                daily_income = row_of_investment_data[3] * 0.016

                            cursor.execute(
                                """UPDATE users
                                SET balance = balance + %s, investment_income = investment_income + %s
                                WHERE telegram_id = %s;""",
                                (daily_income, daily_income, row_of_investment_data[1])
                            )

                            # И закрываем депозит
                            cursor.execute(
                                """UPDATE investment
                                SET deposit_is_active = %s
                                WHERE id = %s;""",
                                (False, row_of_investment_data[0])
                            )

                            print("[INFO] The investment was successfully closed")

        except Exception as _ex:
            print("[INFO] Error wile with PostgreSQL", _ex)
        finally:
            if connection:
                connection.close()
                print("[INFO] Daily update completed successfully and PostgreSQL connection closed")

        # Поток засыпает до следующего времени активации (до 10:00 серверного времени следующего буднего дня)
        current_date = datetime.datetime.now()

        # Если текущий цикл исполняется в пятницу
        if current_date.isoweekday() == 5:
            # То засыпаем до понедельника
            delta = datetime.timedelta(days=3)
        else:
            # Иначе до завтра
            delta = datetime.timedelta(days=1)

        # Добавляем к текущей дате сутки до следующего буднего дня (учитывая месяцы, високосные года и тп)
        next_day = current_date + delta
        # Устанавливаем ровно 10 часов серверного времени на следующих сутках
        datetime_for_next_loop = datetime.datetime.combine(next_day, datetime.time(10))
        # Определяем сколько секунд осталось до следующего дня в 10 часов
        seconds_until_next_cycle = datetime_for_next_loop.timestamp() - current_date.timestamp()
        # Останавливаем поток на нужное количество секунд (до следующих суток в 10)
        time.sleep(seconds_until_next_cycle)


def language_selection_step_1(message, referrer_id=None, start_label=None):

    connection = None
    try:
        connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)
        connection.autocommit = True

        # Определяем какой язык сейчас выбран у этого человека в боте
        with connection.cursor() as cursor:
            cursor.execute('SELECT language FROM users WHERE telegram_id = %s', (message.chat.id,))
            language = cursor.fetchone()

            # Если человек есть в базе данных
            if language:
                language = language[0]

            # Если человека нет в базе данных, добавляем его в базу
            else:
                cursor.execute(
                    """INSERT INTO users (telegram_id, user_name, first_name, last_name)
                    VALUES (%s, %s, %s, %s);""",
                    (message.from_user.id, message.from_user.username, message.from_user.first_name,
                     message.from_user.last_name)
                )

            # Если язык не выбран или введена команда смены языка
            if language is None or message.text == '/language' or message.text == 'Смена языка/Language change':

                mess = 'Выберите язык для бота:' \
                       '\n\nChoose a language for the bot:'
                markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
                button1 = types.KeyboardButton('Русский')
                button2 = types.KeyboardButton('English')
                markup.add(button1, button2)

                bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

                return bot.register_next_step_handler(message, language_selection_step_2, referrer_id, start_label)

            # Если язык уже был выбран
            else:

                if language == 'en':

                    if start_label == 'invalid referral':

                        mess = f'You are registering using a non-working referral link. ' \
                               f'Request another link from the partner or register without a referral program. ' \
                               f'!In the future, you will not be able to stand under any partner!' \
                               f'\n\n<b>Want to sign up without a referral program?</b>'

                        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
                        button1 = types.KeyboardButton('Yes')
                        button2 = types.KeyboardButton('No')
                        markup.add(button1, button2)
                        bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

                        return bot.register_next_step_handler(message, invalid_referral_link_en)

                    # Если был запуск через команду старт
                    elif start_label == 'on start':
                        return to_personal_account_en(message, referrer_id)

                    else:
                        message.text = 'session update'
                        return menu_selection_en(message, None)

                # language == 'ru'
                else:

                    if start_label == 'invalid referral':

                        mess = f'Вы регистрируетесь по нерабочей реферальной ссылке. ' \
                               f'Запросите ещё раз ссылку у партнёра или зарегистрируйтесь' \
                               f' без реферальной программы. !В дальнейшем встать под какого-либо' \
                               f' партнёра не получится!' \
                               f'\n\n<b>Хотите зарегистрироваться без реферальной программы?</b>'

                        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
                        button1 = types.KeyboardButton('Да')
                        button2 = types.KeyboardButton('Нет')
                        markup.add(button1, button2)
                        bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

                        return bot.register_next_step_handler(message, invalid_referral_link_ru)

                    # Если был запуск через команду старт
                    elif start_label == 'on start':
                        return to_personal_account_ru(message, referrer_id)

                    else:
                        message.text = 'session update'
                        return menu_selection_ru(message, None)

    except Exception as _ex:
        print("[INFO] Error wile with PostgreSQL", _ex)

    finally:
        if connection:
            connection.close()
            print("[INFO] PostgreSQL connection closed")


def language_selection_step_2(message, referrer_id=None, start_label=None):

    if message.text == 'Русский':

        connection = None
        try:
            connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)
            connection.autocommit = True

            # Меняем метку языка в базе данных
            with connection.cursor() as cursor:
                cursor.execute(
                    """UPDATE users SET language = 'ru' WHERE telegram_id = %s;""",
                    (message.from_user.id,)
                )

        except Exception as _ex:
            print("[INFO] Error wile with PostgreSQL", _ex)
        finally:
            if connection:
                connection.close()
                print("[INFO] PostgreSQL connection closed")

        if start_label == 'invalid referral':

            mess = f'Вы регистрируетесь по нерабочей реферальной ссылке. ' \
                   f'Запросите ещё раз ссылку у партнёра или зарегистрируйтесь' \
                   f' без реферальной программы. !В дальнейшем встать под какого-либо' \
                   f' партнёра не получится!' \
                   f'\n\n<b>Хотите зарегистрироваться без реферальной программы?</b>'

            markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
            button1 = types.KeyboardButton('Да')
            button2 = types.KeyboardButton('Нет')
            markup.add(button1, button2)
            bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

            return bot.register_next_step_handler(message, invalid_referral_link_ru)

        return to_personal_account_ru(message, referrer_id)

    elif message.text == 'English':

        connection = None
        try:
            connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)
            connection.autocommit = True

            # Меняем метку языка в базе данных
            with connection.cursor() as cursor:
                cursor.execute(
                    """UPDATE users SET language = 'en' WHERE telegram_id = %s;""",
                    (message.from_user.id,)
                )

        except Exception as _ex:
            print("[INFO] Error wile with PostgreSQL", _ex)
        finally:
            if connection:
                connection.close()
                print("[INFO] PostgreSQL connection closed")

        if start_label == 'invalid referral':

            mess = f'You are registering using a non-working referral link. ' \
                   f'Request another link from the partner or register without a referral program. ' \
                   f'!In the future, you will not be able to stand under any partner!' \
                   f'\n\n<b>Want to sign up without a referral program?</b>'

            markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
            button1 = types.KeyboardButton('Yes')
            button2 = types.KeyboardButton('No')
            markup.add(button1, button2)
            bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

            return bot.register_next_step_handler(message, invalid_referral_link_en)

        return to_personal_account_en(message, referrer_id)

    elif '/start' in message.text:
        return start(message)

    else:
        return language_selection_step_1(message, referrer_id, start_label)


def database_check_id(message, referrer_id=None):

    connection = None
    try:
        connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)
        connection.autocommit = True

        # Ищем регистрирующегося человека в базе данных
        with connection.cursor() as cursor:
            cursor.execute(
                """SELECT * FROM users WHERE telegram_id = %s;""",
                (message.from_user.id,)
            )
            # Запоминаем id регистрирующегося
            user_data = cursor.fetchone()

        # Если регистрирующийся человек есть в базе данных
        if user_data:

            # И у регистрирующегося уже есть емейл в базе, то переходим в его личный кабинет
            if user_data[5]:
                # Возвращать user_data в функции database_check_id нужно только из этого места!
                return user_data

            elif referrer_id:
                # Если у регистрирующегося не зарегистрирован емейл, а пригласивший его человек у нас зарегистрирован,
                # то обновляем у регистрирующегося id пригласившего его человека
                with connection.cursor() as cursor:
                    cursor.execute(
                        """UPDATE users
                        SET invited_by = %s 
                        WHERE telegram_id = %s;""",
                        (referrer_id, message.from_user.id)
                    )

                print("[INFO] Data has been successfully updated")

        elif referrer_id:
            # Если регистрирующегося нет в базе данных, а пригласивший есть,
            # то добавляем регистрирующегося вместе с id пригласившего
            with connection.cursor() as cursor:
                cursor.execute(
                    """INSERT INTO users (telegram_id, user_name, first_name, last_name, invited_by)
                    VALUES (%s, %s, %s, %s, %s);""",
                    (message.from_user.id, message.from_user.username, message.from_user.first_name,
                     message.from_user.last_name, referrer_id)
                )

            print("[INFO] Data was successfully inserted")

        else:
            # Если регистрирующегося нет в базе данных, и пригласившего нет в бд,
            # то добавляем регистрирующегося без id пригласившего
            with connection.cursor() as cursor:
                cursor.execute(
                    """INSERT INTO users (telegram_id, user_name, first_name, last_name)
                    VALUES (%s, %s, %s, %s);""",
                    (message.from_user.id, message.from_user.username, message.from_user.first_name,
                     message.from_user.last_name)
                )

            print("[INFO] Data was successfully inserted")

    except Exception as _ex:
        print("[INFO] Error wile with PostgreSQL", _ex)
    finally:
        if connection:
            connection.close()
            print("[INFO] PostgreSQL connection closed")


def for_unregistered_users_ru(message):

    mess = f'Приветствую, {message.from_user.first_name}!' \
           f'\nЗарегистрируйся по кнопке ниже и начни зарабатывать на инвестициях.' \
           f'\nИли узнай подробней о компании'

    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    button1 = types.KeyboardButton('Зарегистрироваться')
    button2 = types.KeyboardButton('О нас')
    button3 = types.KeyboardButton('Смена языка/Language change')
    markup.add(button1, button2, button3)

    bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

    return bot.register_next_step_handler(message, menu_selection_ru, None)


def registration_ru(message):

    user_data = database_check_id(message)
    if user_data:
        return to_personal_account_ru(message)
    else:
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
        button1 = types.KeyboardButton('Назад')
        markup.add(button1)
        bot.send_message(message.chat.id, 'Введите ваш Email:', reply_markup=markup)

        return bot.register_next_step_handler(message, database_email_registration_ru)


def invalid_referral_link_ru(message):

    if message.text == 'Да':
        return registration_ru(message)

    elif message.text == 'Нет':

        mess = f'Тогда проверьте вашу реферальную ссылку на предмет повреждений или запросите' \
               f' новую ссылку у партнёра.' \
               f'\n\nПравильный формат реферальной ссылки: ' \
               f'\n<code>https://t.me/GlobalFinancialInvestorBot?start=000000000</code>' \
               f'\n\n, где вместо 000000000 должен быть телеграм ID вашего реферера'

        bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=types.ReplyKeyboardRemove())

        return


def database_email_registration_ru(message):
    # Сюда может попасть человек, только если у него нет емейла в базе данных

    if message.text == 'Назад':
        return for_unregistered_users_ru(message)

    elif not re.match(r"^[-\w.]+@([-\w]+\.)+[-\w]{2,4}$", message.text):

        if '/start' in message.text:
            return start(message)
        else:

            markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
            button1 = types.KeyboardButton('Назад')
            markup.add(button1)
            bot.send_message(message.chat.id, 'Вы ввели неверный формат Email. '
                                              'Проверьте данные и попробуйте ещё раз', reply_markup=markup)

            return registration_ru(message)

    connection = None
    try:
        connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)
        connection.autocommit = True

        with connection.cursor() as cursor:
            cursor.execute(
                """UPDATE users
                SET email = %s
                WHERE telegram_id = %s;""",
                (message.text, message.from_user.id)
            )

            print("[INFO] Email has been successfully updated")

            markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
            button1 = types.KeyboardButton('В личный кабинет')
            markup.add(button1)
            bot.send_message(message.chat.id, 'Вы успешно зарегистрировались!'
                                              '\n\nТеперь вы можете войти в личный кабинет', reply_markup=markup)

            # Добавляем всем вышестоящим (до 10 вверх) реферерам по +1 в количество партнёров
            cursor.execute('SELECT invited_by FROM users WHERE telegram_id = %s', (message.from_user.id,))
            invited_id = cursor.fetchone()[0]
            cursor.execute('UPDATE users SET ref_count = ref_count + 1 WHERE telegram_id = %s', (invited_id,))

            for i in range(0, 9):
                cursor.execute('SELECT invited_by FROM users WHERE telegram_id = %s', (invited_id,))
                invited_id = cursor.fetchone()
                if invited_id:
                    invited_id = invited_id[0]
                    cursor.execute('UPDATE users SET ref_count = ref_count + 1 WHERE telegram_id = %s', (invited_id,))
                else:
                    break

            return bot.register_next_step_handler(message, to_personal_account_ru)

    except Exception as _ex:
        print("[INFO] Error wile with PostgreSQL", _ex)
    finally:
        if connection:
            connection.close()
            print("[INFO] PostgreSQL connection closed")


def to_personal_account_ru(message, referrer_id=None, only_buttons=False, mess_for_only_buttons=None):

    bot.clear_step_handler(message)

    user_data = database_check_id(message, referrer_id)

    # Если человек есть в базе данных и у него подтверждён email
    if user_data:

        # Если он есть в списке админов
        if user_data[1] in admins:

            if only_buttons:
                mess = mess_for_only_buttons
            else:
                mess = f'Твой баланс: {user_data[7]}$' \
                       f'\nТвой доход от инвестиций: {user_data[8]}$' \
                       f'\nТвой реферальный доход: {user_data[9]}$' \
                       f'\nТвой партнёрский оборот: {user_data[11]}$' \
                       f'\nВсего партнёров: {user_data[10]}'

            markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
            button1 = types.KeyboardButton('Пополнить баланс')
            button2 = types.KeyboardButton('Инвестировать')
            button3 = types.KeyboardButton('Мои открытые инвестиции')
            button4 = types.KeyboardButton('Вывод денег')
            button5 = types.KeyboardButton('Реферальная программа')
            button6 = types.KeyboardButton('О нас')
            button7 = types.KeyboardButton('Админ панель')
            markup.add(button1, button2, button3, button4, button5, button6, button7)

            bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

            return bot.register_next_step_handler(message, menu_selection_ru, user_data)

        # Если это не админ, но зарегистрированный в базе данных пользователь
        else:

            if only_buttons:
                mess = mess_for_only_buttons
            else:
                mess = f'Твой баланс: {user_data[7]}$' \
                       f'\nТвой доход от инвестиций: {user_data[8]}$' \
                       f'\nТвой реферальный доход: {user_data[9]}$' \
                       f'\nТвой партнёрский оборот: {user_data[11]}$' \
                       f'\nВсего партнёров: {user_data[10]}'

            markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
            button1 = types.KeyboardButton('Пополнить баланс')
            button2 = types.KeyboardButton('Инвестировать')
            button3 = types.KeyboardButton('Мои открытые инвестиции')
            button4 = types.KeyboardButton('Вывод денег')
            button5 = types.KeyboardButton('Реферальная программа')
            button6 = types.KeyboardButton('О нас')
            markup.add(button1, button2, button3, button4, button5, button6)

            bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

            return bot.register_next_step_handler(message, menu_selection_ru, user_data)

    else:
        return for_unregistered_users_ru(message)


def menu_selection_ru(message, user_data):

    bot.clear_step_handler(message)

    if message.text == 'Войти в личный кабинет' or message.text == 'В личный кабинет'\
            or message.text == 'Вернуться в личный кабинет':
        return to_personal_account_ru(message)

    # Возможные команды для зарегистрированных
    elif user_data:
        if message.text == 'Пополнить баланс':

            mess = 'Введите сумму, на которую хотите пополнить баланс кошелька, в долларах:'
            markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
            button1 = types.KeyboardButton('Назад')
            markup.add(button1)

            bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

            return bot.register_next_step_handler(message, balance_replenishment_step_1_ru, user_data)

        elif message.text == 'Инвестировать' or message.text == 'Вернуться к выбору тарифа':

            mess = f'Выберете инвестиционный план:' \
                   f'\n\nАвиационный, железнодорожный, автомобильный, морской. Минимальная сумма инвестиции 50USDT. ' \
                   f'Минимальная сумма вывода 50USDT. Начисления производятся ежедневно, с понедельника по пятницу. ' \
                   f'Прибыль начисляется на следующий день после активации инвестиционного плана в 10.00 +GMT. ' \
                   f'Вывод прибыли доступен раз в три дня. Возврат тела инвестиции в конце срока. ' \
                   f'Досрочный возврат инвестиции -50%. Так же за каждые инвестированные 100 USDT вы получаете' \
                   f' 10 токенов GFT к выводу на кошелек они будут доступны перед публичным ICO.' \
                   f'\n\n<b>Авиационный</b>  мин. сумма 50$, макс. Сумма 1000$, срок вклада 28 дней' \
                   f' ежедневная прибыль 0.7% возврат депозита в конце срока начисления пн.-пт.' \
                   f'\n\n<b>Железнодорожный</b> мин. сумма 500$, макс. Сумма 5000$, срок вклада 42 дня' \
                   f' ежедневная прибыль 1% возврат депозита в конце срока начисления пн.-пт.' \
                   f'\n\n<b>Автомобильный</b> мин. сумма 3000$, макс 50000$, срок вклада 70 дней' \
                   f' ежедневная прибыль 1.3% возврат депозита в конце срока начисления пн.-пт.' \
                   f'\n\n<b>Морской</b> мин. сумма 5000$, макс. Сумма 100000$, срок вклада 91 день' \
                   f' ежедневная прибыль 1.6% возврат депозита в конце срока начисления пн.-пт.'

            markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
            button1 = types.KeyboardButton('Авиационный')
            button2 = types.KeyboardButton('Железнодорожный')
            button3 = types.KeyboardButton('Автомобильный')
            button4 = types.KeyboardButton('Морской')
            button5 = types.KeyboardButton('Назад')
            markup.add(button1, button2, button3, button4, button5)

            bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

            return bot.register_next_step_handler(message, investment_ru, user_data)

        elif message.text == 'Мои открытые инвестиции':
            return investment_status_ru(message, user_data)

        elif message.text == 'Вывод денег':
            return withdrawal_history_ru(message, user_data)

        elif message.text == 'Реферальная программа':
            return referral_program_ru(message, user_data)

        elif message.text == 'О нас':
            return about_us_ru(message, user_data)

        # Отдельно админские команды
        elif user_data[1] in admins:

            if message.text == 'Админ панель' or message.text == 'Вернуться в админ панель':

                return admin_panel_ru(message, user_data)

            elif message.text == 'Заявки на пополнение баланса' or message.text == 'Вернуться к заявкам на пополнение':

                return admin_replenishment_step_1_ru(message, user_data)

            elif message.text == 'Заявки на вывод денег' or message.text == 'Вернуться к заявкам на вывод':

                return admin_withdrawal_step_1_ru(message, user_data)

            elif message.text == 'Скачать всю базу пользователей':
                mess = 'Идёт создание файла, это может занять несколько минут'
                bot.send_message(message.chat.id, mess, parse_mode='html')

                return send_all_database_ru(message, user_data)

            else:
                return menu_selection_ru(message, None)

        else:
            return menu_selection_ru(message, None)

    # Команды для незарегистрированных
    else:

        if message.text == 'Зарегистрироваться':
            return registration_ru(message)

        elif message.text == 'О нас':
            return about_us_ru(message, user_data)

        elif message.text == '/help':
            mess = 'Если у вас возникли проблемы с ботом, то напишите админу @Helper13'
            markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
            button1 = types.KeyboardButton('Войти в личный кабинет')
            button2 = types.KeyboardButton('Зарегистрироваться')
            markup.add(button1, button2)

            bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

            return bot.register_next_step_handler(message, menu_selection_ru, None)

        elif message.text == '/language' or message.text == 'Смена языка/Language change':
            return language_selection_step_1(message)

        elif '/start' in message.text:
            return start(message)

        else:
            mess = 'Команда не распознана, возможно сессия была обновлена или вы не использовали кнопки снизу. ' \
                   'Попробуйте всё заново' \
                   '\n\nЕсли у вас проблемы с ботом, то используйте команду /help'
            bot.send_message(message.chat.id, mess, parse_mode='html')

            return to_personal_account_ru(message)


def balance_replenishment_step_1_ru(message, user_data):

    if message.text == 'Назад':
        return to_personal_account_ru(message)

    elif message.text.isdigit():
        replenishment_amount = int(message.text)

        if replenishment_amount < 50:
            markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
            button1 = types.KeyboardButton('Назад')
            markup.add(button1)
            bot.send_message(message.chat.id, '<b>Минимальная сумма пополнения 50$</b>',
                             parse_mode='html', reply_markup=markup)
            # Отправляем на повторение шага
            message.text = 'Пополнить баланс'
            return menu_selection_ru(message, user_data)

    else:
        bot.send_message(message.chat.id, 'Число должно быть целое, без точек,'
                                          ' запятых и других символов. '
                                          'Попробуйте заново', parse_mode='html')
        # Отправляем на повторение шага
        message.text = 'Пополнить баланс'
        return menu_selection_ru(message, user_data)

    mess = f'Для пополнения счёта в боте, отправьте <b>{replenishment_amount} USDT</b> на этот кошелёк:' \
           f'\n\n<code>1q2w3e4r5t6y7u8i9o0</code>' \
           f'\n\nПосле перевода, скопируйте/сохраните ваш Transaction Hash (hash id) и нажмите' \
           f' <b>Подтвердить транзакцию</b> для подтверждения вашего перевода' \
           f'\n\nВ течении 48 часов произойдёт подтверждение вашего платежа и пополнение баланса в боте.'
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
    button1 = types.KeyboardButton('Подтвердить транзакцию')
    button2 = types.KeyboardButton('Назад')
    markup.add(button1, button2)

    bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

    return bot.register_next_step_handler(message, balance_replenishment_step_2_ru, user_data, replenishment_amount)


def balance_replenishment_step_2_ru(message, user_data, replenishment_amount):

    if message.text == 'Назад':
        message.text = 'Пополнить баланс'
        return menu_selection_ru(message, user_data)

    elif message.text == 'Подтвердить транзакцию':

        mess = '<b>Введите Transaction Hash (hash id) в чат для подтверждения вашего перевода:</b>'
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
        button1 = types.KeyboardButton('Назад')
        markup.add(button1)

        bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

        return bot.register_next_step_handler(message, balance_replenishment_step_3_ru, user_data, replenishment_amount)

    else:
        return balance_replenishment_step_3_ru(message, user_data, replenishment_amount)


def balance_replenishment_step_3_ru(message, user_data, replenishment_amount):

    if message.text == 'Назад':
        message.text = replenishment_amount
        return balance_replenishment_step_1_ru(message, user_data)

    connection = None
    try:
        connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)
        connection.autocommit = True

        current_date = int(datetime.datetime.now().timestamp())

        with connection.cursor() as cursor:
            cursor.execute(
                """INSERT INTO balance_replenishment (telegram_id, user_name, 
                replenishment_amount, transaction_hash, request_date, request_status)
                VALUES (%s, %s, %s, %s, %s, %s);""",
                (user_data[1], user_data[2], replenishment_amount, message.text, current_date, 'in processing')
            )

            print("[INFO] The balance replenishment was successfully inserted")

            mess = 'Ваша заявка успешно отправлена! ' \
                   '\n\nОжидайте подтверждение платежа и зачисления средств в течении 48 часов'
            markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
            button1 = types.KeyboardButton('Вернуться в личный кабинет')
            markup.add(button1)

            bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

            tz = pytz.timezone('Europe/Moscow')
            current_date = datetime.datetime.now(tz)
            current_date_format = current_date.strftime("%H:%M, %d.%m.%Y")

            mess_for_admins_ru = f'Новая заявка на пополнение баланса:' \
                                 f'\n\nTransaction Hash: <code>{message.text}</code>' \
                                 f'\nСумма для пополнения: <b>{replenishment_amount}</b>$' \
                                 f'\nТелеграм id: <code>{user_data[1]}</code>' \
                                 f'\nТелеграм ник: @{user_data[2]}' \
                                 f'\nЕго текущий баланс: {user_data[7]}' \
                                 f'\nВремя и дата заявки: {current_date_format}'

            mess_for_admins_en = f'New application for balance replenishment:' \
                                 f'\n\nTransaction Hash: <code>{message.text}</code>' \
                                 f'\nAmount to top up: $<b>{replenishment_amount}</b>' \
                                 f'\nTelegram id: <code>{user_data[1]}</code>' \
                                 f'\nTelegram username: @{user_data[2]}' \
                                 f'\nHis current balance: {user_data[7]}' \
                                 f'\nTime and date of application: {current_date_format}'

            # Отправка оповещения всем админам
            for i in admins:
                # Определяем какой язык сейчас выбран у админа в боте
                cursor.execute('SELECT language FROM users WHERE telegram_id = %s', (i,))
                language = cursor.fetchone()[0]

                if language == 'en':
                    bot.send_message(i, mess_for_admins_en, parse_mode='html')
                else:
                    bot.send_message(i, mess_for_admins_ru, parse_mode='html')

            return bot.register_next_step_handler(message, menu_selection_ru, user_data)

    except Exception as _ex:
        print("[INFO] Error wile with PostgreSQL", _ex)
    finally:
        if connection:
            connection.close()
            print("[INFO] PostgreSQL connection closed")


def investment_ru(message, user_data):

    if message.text == 'Назад':
        return to_personal_account_ru(message)

    elif message.text == 'Авиационный':

        mess = 'Вы выбрали Авиационный инвестиционный план. Мин. сумма 50$, макс. Сумма 1000$,' \
               ' срок вклада 28 дней  ежедневная прибыль 0.7% возврат депозита в конце срока начисления пн.-пт.' \
               f'\n\n<b>Введите сумму инвестиций</b>:'
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
        button1 = types.KeyboardButton('Назад')
        markup.add(button1)

        bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)
        tariff = 1

    elif message.text == 'Железнодорожный':

        mess = 'Вы выбрали Железнодорожный инвестиционный план. Мин. сумма 500$, макс. Сумма 5000$,' \
               ' срок вклада 42 дня ежедневная прибыль 1% возврат депозита в конце срока начисления пн.-пт.' \
               '\n\n<b>Введите сумму инвестиций</b>:'
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
        button1 = types.KeyboardButton('Назад')
        markup.add(button1)

        bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)
        tariff = 2

    elif message.text == 'Автомобильный':

        mess = 'Вы выбрали Автомобильный инвестиционный план. Мин. сумма 3000$, макс 50000$,' \
               ' срок вклада 70 дней ежедневная прибыль 1.3% возврат депозита в конце срока начисления пн.-пт.' \
               '\n\n<b>Введите сумму инвестиций</b>:'
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
        button1 = types.KeyboardButton('Назад')
        markup.add(button1)

        bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)
        tariff = 3

    elif message.text == 'Морской':

        mess = 'Вы выбрали Морской инвестиционный план. Мин. сумма 5000$, макс. Сумма 100000$,' \
               ' срок вклада 91 день ежедневная прибыль 1.6% возврат депозита в конце срока начисления пн.-пт.' \
               '\n\n<b>Введите сумму инвестиций</b>:'
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
        button1 = types.KeyboardButton('Назад')
        markup.add(button1)

        bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)
        tariff = 4

    else:
        return menu_selection_ru(message, None)

    return bot.register_next_step_handler(message, successful_investment_ru, tariff, user_data)


def successful_investment_ru(message, tariff, user_data):

    if message.text == 'Назад':

        # Отправляем на шаг назад
        message.text = 'Инвестировать'
        return menu_selection_ru(message, user_data)

    elif message.text.isdigit():
        amount = int(message.text)

        # Проверяем есть ли введённая сумма на счету у клиента
        if user_data[7] >= amount:

            # Проверяем входит ли введённая сумма в рамки тарифа
            if tariff == 1 and (amount < 50 or amount > 1000):

                markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
                button1 = types.KeyboardButton('Вернуться к выбору тарифа')
                markup.add(button1)

                bot.send_message(message.chat.id, 'В Авиационном плане можно инвестировать'
                                                  ' только от 50$ до 1000$', parse_mode='html', reply_markup=markup)

                return bot.register_next_step_handler(message, menu_selection_ru, user_data)

            elif tariff == 2 and (amount < 500 or amount > 5000):

                markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
                button1 = types.KeyboardButton('Вернуться к выбору тарифа')
                markup.add(button1)

                bot.send_message(message.chat.id, 'В Железнодорожном плане можно инвестировать'
                                                  ' только от 500$ до 5000$', parse_mode='html', reply_markup=markup)

                return bot.register_next_step_handler(message, menu_selection_ru, user_data)

            elif tariff == 3 and (amount < 3000 or amount > 50000):

                markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
                button1 = types.KeyboardButton('Вернуться к выбору тарифа')
                markup.add(button1)

                bot.send_message(message.chat.id, 'В Автомобильном плане можно инвестировать'
                                                  ' только от 3000$ до 50000$', parse_mode='html', reply_markup=markup)

                return bot.register_next_step_handler(message, menu_selection_ru, user_data)

            elif tariff == 4 and (amount < 5000 or amount > 100000):

                markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
                button1 = types.KeyboardButton('Вернуться к выбору тарифа')
                markup.add(button1)

                bot.send_message(message.chat.id, 'В Морском плане можно инвестировать'
                                                  ' только от 5000$ до 100000$', parse_mode='html', reply_markup=markup)

                return bot.register_next_step_handler(message, menu_selection_ru, user_data)

            # Если сумма входит в рамки тарифа
            else:

                connection = None

                try:
                    connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)

                    connection.autocommit = True

                    with connection.cursor() as cursor:
                        cursor.execute(
                            """UPDATE users
                            SET balance = balance - %s
                            WHERE telegram_id = %s;""",
                            (amount, user_data[1])
                        )

                        print("[INFO] Balance has been successfully updated")

                        current_date = int(datetime.datetime.now().timestamp())

                        if tariff == 1:
                            # В первом тарифе длительность депозита 28 дней
                            expiration_of_investment = current_date + (604800 * 4)
                        elif tariff == 2:
                            # Во втором тарифе длительность депозита 42 дня
                            expiration_of_investment = current_date + (604800 * 6)
                        elif tariff == 3:
                            # В третьем тарифе длительность депозита 70 дней
                            expiration_of_investment = current_date + (604800 * 10)
                        elif tariff == 4:
                            # В четвертом тарифе длительность депозита 91 дней
                            expiration_of_investment = current_date + (604800 * 13)

                        cursor.execute(
                            """INSERT INTO investment (telegram_id, tariff, investment_amount, 
                            investment_start_date, investment_end_date, deposit_is_active)
                            VALUES (%s, %s, %s, %s, %s, %s);""",
                            (user_data[1], tariff, amount, current_date, expiration_of_investment, True)
                        )

                        print("[INFO] The investment was successfully inserted")

                        referral_matrix = [
                            [0.05, 0.06, 0.07, 0.08, 0.1, 0.12],
                            [0.03, 0.03, 0.04, 0.05, 0.06, 0.06],
                            [0.02, 0.02, 0.02, 0.02, 0.03, 0.04],
                            [0.01, 0.01, 0.01, 0.01, 0.01, 0.01],
                            [0.005, 0.005, 0.01, 0.01, 0.01, 0.01],
                            [0, 0.005, 0.005, 0.005, 0.005, 0.01],
                            [0, 0, 0.005, 0.005, 0.005, 0.005],
                            [0, 0, 0, 0.005, 0.005, 0.005],
                            [0, 0, 0, 0, 0.005, 0.005],
                            [0, 0, 0, 0, 0, 0.005]
                        ]

                        cursor.execute('SELECT invited_by FROM users WHERE telegram_id = %s', (user_data[1],))
                        # Запоминаем id вышестоящего человека
                        invited_id = cursor.fetchone()[0]

                        # Добавляем реферальные проценты всем до 10й линии, в зависимости от их оборота и линии,
                        #  относительно первого человека
                        for i in range(0, 10):

                            # Если вышестоящий человек есть
                            if invited_id:
                                # То определяем его оборот и смотрим кто его пригласил (для будущего цикла)
                                cursor.execute('SELECT turnover, invited_by FROM users WHERE telegram_id = %s',
                                               (invited_id,))

                                temp_data = cursor.fetchone()
                                turnover = temp_data[0]

                                if turnover < 50000:
                                    current_rank = 0
                                elif turnover < 100000:
                                    current_rank = 1
                                elif turnover < 300000:
                                    current_rank = 2
                                elif turnover < 500000:
                                    current_rank = 3
                                elif turnover < 1000000:
                                    current_rank = 4
                                else:
                                    current_rank = 5

                                referral_percentage = referral_matrix[i][current_rank]
                                ref_bonus = amount * referral_percentage

                                if ref_bonus != 0:
                                    cursor.execute(
                                        """UPDATE users 
                                        SET balance = balance + %s, ref_balance = ref_balance + %s 
                                        WHERE telegram_id = %s""",
                                        (ref_bonus, ref_bonus, invited_id)
                                    )

                                # Теперь увеличиваем оборот этого человека, в зависимости от линии
                                # Оборот увеличивается только для верхних 5 линий
                                if i < 5:
                                    if i == 0:
                                        line_coefficient = 1
                                    elif i == 1:
                                        line_coefficient = 0.8
                                    elif i == 2:
                                        line_coefficient = 0.6
                                    elif i == 3:
                                        line_coefficient = 0.4
                                    elif i == 4:
                                        line_coefficient = 0.2
                                    else:
                                        line_coefficient = 0

                                    turnover_increment = amount * line_coefficient

                                    cursor.execute(
                                        """UPDATE users 
                                        SET turnover = turnover + %s
                                        WHERE telegram_id = %s""",
                                        (turnover_increment, invited_id)
                                    )

                                    # Проверяем не перешёл ли этот человек на новый ранг и на какой
                                    turnover_new = turnover + turnover_increment

                                    if turnover_new < 50000:
                                        new_rank = 0
                                    elif turnover_new < 100000:
                                        new_rank = 1
                                    elif turnover_new < 300000:
                                        new_rank = 2
                                    elif turnover_new < 500000:
                                        new_rank = 3
                                    elif turnover_new < 1000000:
                                        new_rank = 4
                                    else:
                                        new_rank = 5

                                    rank_difference = new_rank - current_rank
                                    # Если было повышение хотя бы на один ранг
                                    if rank_difference > 0:

                                        # Определяем какой язык сейчас выбран у этого человека в боте
                                        cursor.execute('SELECT language FROM users WHERE telegram_id = %s',
                                                       (invited_id,))
                                        language = cursor.fetchone()[0]

                                        rank_up_bonus = 0
                                        for n in range(0, rank_difference):

                                            if current_rank == 0:

                                                if language == 'en':
                                                    mess = 'Your referral turnover has exceeded $50,000, so you have' \
                                                           ' moved to the 2nd rank of the referral program.' \
                                                           ' Congratulations!' \
                                                           '\n\nNow you get more percentage of your lines,' \
                                                           ' and the number' \
                                                           ' of referral levels has increased to 6 in depth'
                                                else:
                                                    mess = 'Ваш реферальный оборот превысил 50000$, поэтому' \
                                                           ' вы перешли на 2й ранг реферальной программы.' \
                                                           ' Поздравляем!' \
                                                           '\n\nТеперь вы получаете больше процентов от ваших линий,' \
                                                           ' а количество реферальных уровней повысилось до 6 в глубину'

                                                bot.send_message(invited_id, mess, parse_mode='html')

                                            elif current_rank == 1:

                                                if language == 'en':
                                                    mess = 'Your referral turnover has exceeded $100,000, so you have' \
                                                           ' moved to the 3rd rank of the referral program.' \
                                                           ' Congratulations!' \
                                                           '\n\nYou get a 1% bonus from $100,000 to your balance.' \
                                                           '\nYou also get more percentage of your lines, and the' \
                                                           ' number of referral levels has increased to 7 in depth.'
                                                else:
                                                    mess = 'Ваш реферальный оборот превысил 100000$, поэтому' \
                                                           ' вы перешли на 3й ранг реферальной программы.' \
                                                           ' Поздравляем!' \
                                                           '\n\nВы получаете бонус 1% от 100000$ на баланс.' \
                                                           '\nТак же вы получаете больше процентов от ваших линий,' \
                                                           ' а количество реферальных уровней повысилось до 7 в глубину'

                                                bot.send_message(invited_id, mess, parse_mode='html')

                                                rank_up_bonus = rank_up_bonus + 1000

                                            elif current_rank == 2:

                                                if language == 'en':
                                                    mess = 'Your referral turnover has exceeded $300,000, so you have' \
                                                           ' moved to the 4rd rank of the referral program.' \
                                                           ' Congratulations!' \
                                                           '\n\nYou get a 1% bonus from $300,000 to your balance.' \
                                                           '\nYou also get more percentage of your lines, and the' \
                                                           ' number of referral levels has increased to 8 in depth.'
                                                else:
                                                    mess = 'Ваш реферальный оборот превысил 300000$, поэтому' \
                                                           ' вы перешли на 4й ранг реферальной программы.' \
                                                           ' Поздравляем!' \
                                                           '\n\nВы получаете бонус 1% от 300000$ на баланс.' \
                                                           '\nТак же вы получаете больше процентов от ваших линий,' \
                                                           ' а количество реферальных уровней повысилось до 8 в глубину'

                                                bot.send_message(invited_id, mess, parse_mode='html')

                                                rank_up_bonus = rank_up_bonus + 3000

                                            elif current_rank == 3:

                                                if language == 'en':
                                                    mess = 'Your referral turnover has exceeded $500,000, so you have' \
                                                           ' moved to the 5rd rank of the referral program.' \
                                                           ' Congratulations!' \
                                                           '\n\nYou get a 1% bonus from $500,000 to your balance.' \
                                                           '\nYou also get more percentage of your lines, and the' \
                                                           ' number of referral levels has increased to 9 in depth.'
                                                else:
                                                    mess = 'Ваш реферальный оборот превысил 500000$, поэтому' \
                                                           ' вы перешли на 5й ранг реферальной программы.' \
                                                           ' Поздравляем!' \
                                                           '\n\nВы получаете бонус 1% от 500000$ на баланс.' \
                                                           '\nТак же вы получаете больше процентов от ваших линий,' \
                                                           ' а количество реферальных уровней повысилось до 9 в глубину'

                                                bot.send_message(invited_id, mess, parse_mode='html')

                                                rank_up_bonus = rank_up_bonus + 5000

                                            elif current_rank == 4:

                                                if language == 'en':
                                                    mess = 'Your referral turnover has exceeded $1,000,000, so you' \
                                                           ' have moved to the 6rd rank of the referral program.' \
                                                           ' Congratulations!' \
                                                           '\n\nYou get a 1% bonus from $1,000,000 to your balance.' \
                                                           '\nYou also get more percentage of your lines, and the' \
                                                           ' number of referral levels has increased to 10 in depth.'
                                                else:
                                                    mess = 'Ваш реферальный оборот превысил 1000000$, поэтому' \
                                                           ' вы перешли на максимальный 6й ранг реферальной' \
                                                           ' программы. Поздравляем!' \
                                                           '\n\nВы получаете бонус 1% от 1000000$ на баланс.' \
                                                           '\nТак же вы получаете больше процентов от ваших линий, ' \
                                                           'а количество реферальных уровней повысилось до 10 в глубину'

                                                bot.send_message(invited_id, mess, parse_mode='html')

                                                rank_up_bonus = rank_up_bonus + 10000

                                            current_rank = current_rank + 1

                                        # Если перешёл на новые ранги, то добавляем ему на баланс суммарно бонусы от
                                        # каждого ранга, которого он достиг
                                        if rank_up_bonus:
                                            cursor.execute(
                                                """UPDATE users 
                                                SET balance = balance + %s 
                                                WHERE telegram_id = %s""",
                                                (rank_up_bonus, invited_id)
                                            )

                                # Берём id следующего человека, который пригласил текущего
                                invited_id = temp_data[1]

                            else:
                                # Если поле пригласившего человека пустое, то выходим из цикла. Цепочка оборвалась
                                break

                        print("[INFO] Referral balance and turnover has been successfully updated")

                        mess = f'Вы успешно инвестировали <b>{amount}$</b> по {tariff}му тарифному плану! ' \
                               f'\nОжидайте своей прибыли начиная со следующих суток в 10 GMT' \
                               f'\n\nВклад будет активен до ' \
                               f'{datetime.datetime.fromtimestamp(expiration_of_investment).strftime("%d.%m.%Y")}'

                        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
                        button1 = types.KeyboardButton('Вернуться в личный кабинет')
                        markup.add(button1)

                        bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

                        return bot.register_next_step_handler(message, to_personal_account_ru)

                except Exception as _ex:
                    print("[INFO] Error wile with PostgreSQL", _ex)
                finally:
                    if connection:
                        connection.close()
                        print("[INFO] PostgreSQL connection closed")

        else:

            markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
            button1 = types.KeyboardButton('Пополнить баланс')
            button2 = types.KeyboardButton('Вернуться в личный кабинет')
            markup.add(button1, button2)

            bot.send_message(message.chat.id, 'У вас недостаточно средств'
                                              ' на счету', parse_mode='html', reply_markup=markup)

            return bot.register_next_step_handler(message, menu_selection_ru, user_data)

    else:
        bot.send_message(message.chat.id, 'Число должно быть целое, без точек,'
                                          ' запятых и других символов. '
                                          'Попробуйте заново', parse_mode='html')

        # Определяем на каком шаге был неверный ввод
        if tariff == 1:
            message.text = 'Авиационный'
        elif tariff == 2:
            message.text = 'Железнодорожный'
        elif tariff == 3:
            message.text = 'Автомобильный'
        elif tariff == 4:
            message.text = 'Морской'
        # Отправляем на повторение шага
        return investment_ru(message, user_data)


def investment_status_ru(message, user_data):

    connection = None
    try:
        connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)
        connection.autocommit = True

        # Выбираем только активные депозиты по id человека
        with connection.cursor() as cursor:
            cursor.execute(
                """SELECT id, tariff, investment_amount, investment_start_date, investment_end_date 
                FROM investment 
                WHERE telegram_id = %s and deposit_is_active = True
                ORDER BY investment_start_date;""",
                (user_data[1],)
            )
            all_investment_data = cursor.fetchall()

    except Exception as _ex:
        print("[INFO] Error wile with PostgreSQL", _ex)
    finally:
        if connection:
            connection.close()
            print("[INFO] PostgreSQL connection closed")

    if all_investment_data:

        mess = 'Список активных инвестиций:\n\n1. '
        for i, investment_data in enumerate(all_investment_data):

            if i != 0:
                mess = mess + f'\n\n\n{i + 1}. '

            if investment_data[1] == 1:
                mess = mess + f'Тариф: Авиационный' \
                              f'\nСтавка по тарифу: 0.7%/день' \
                              f'\nСумма инвестиций: {investment_data[2]}' \
                              f'\nДата начала инвест. плана: ' \
                              f'{datetime.datetime.fromtimestamp(investment_data[3]).strftime("%d.%m.%Y")}' \
                              f'\nДата окончания: ' \
                              f'{datetime.datetime.fromtimestamp(investment_data[4]).strftime("%d.%m.%Y")}'

            elif investment_data[1] == 2:
                mess = mess + f'Тариф: Железнодорожный' \
                              f'\nСтавка по тарифу: 1%/день' \
                              f'\nСумма инвестиций: {investment_data[2]}' \
                              f'\nДата начала инвест. плана: ' \
                              f'{datetime.datetime.fromtimestamp(investment_data[3]).strftime("%d.%m.%Y")}' \
                              f'\nДата окончания: ' \
                              f'{datetime.datetime.fromtimestamp(investment_data[4]).strftime("%d.%m.%Y")}'

            elif investment_data[1] == 3:
                mess = mess + f'Тариф: Автомобильный' \
                              f'\nСтавка по тарифу: 1.3%/день' \
                              f'\nСумма инвестиций: {investment_data[2]}' \
                              f'\nДата начала инвест. плана: ' \
                              f'{datetime.datetime.fromtimestamp(investment_data[3]).strftime("%d.%m.%Y")}' \
                              f'\nДата окончания: ' \
                              f'{datetime.datetime.fromtimestamp(investment_data[4]).strftime("%d.%m.%Y")}'

            elif investment_data[1] == 4:
                mess = mess + f'Тариф: Морской' \
                              f'\nСтавка по тарифу: 1.6%/день' \
                              f'\nСумма инвестиций: {investment_data[2]}' \
                              f'\nДата начала инвест. плана: ' \
                              f'{datetime.datetime.fromtimestamp(investment_data[3]).strftime("%d.%m.%Y")}' \
                              f'\nДата окончания: ' \
                              f'{datetime.datetime.fromtimestamp(investment_data[4]).strftime("%d.%m.%Y")}'

        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
        button1 = types.KeyboardButton('Досрочно вернуть выбранные инвестиции')
        button2 = types.KeyboardButton('Назад')
        markup.add(button1, button2)

        bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

        return bot.register_next_step_handler(message, early_refund_step_1_ru, user_data, all_investment_data)

    else:

        mess = 'У вас нет активных вкладов'

        return to_personal_account_ru(message, only_buttons=True, mess_for_only_buttons=mess)


def early_refund_step_1_ru(message, user_data, all_investment_data):

    if message.text == 'Назад':

        # Отправляем на шаг назад
        message.text = 'Вернуться в личный кабинет'
        return menu_selection_ru(message, user_data)

    elif message.text == 'Досрочно вернуть выбранные инвестиции':

        mess = f'При досрочном возврате инвестиции вам возвращается только 50%. Остальные 50% вы теряете' \
               f'\n\nВаш список инвестиций пронумерован в сообщении выше.' \
               f'\n\n\n<b>Введите номер инвестиции, которую хотите вернуть досрочно:</b>'

        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
        button1 = types.KeyboardButton('Вернуться в личный кабинет')
        markup.add(button1)

        bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

        return bot.register_next_step_handler(message, early_refund_step_2_ru, user_data, all_investment_data)

    else:
        return menu_selection_ru(message, None)


def early_refund_step_2_ru(message, user_data, all_investment_data):

    if message.text == 'Вернуться в личный кабинет':
        # Отправляем в личный кабинет
        return menu_selection_ru(message, user_data)

    elif message.text.isdigit():
        number_of_the_investment = int(message.text)

    else:
        mess = 'В номере инвестиции должны быть быть только цифры больше нуля, без дополнительных символов.' \
               '\nПроверьте данные и повторите попытку.'
        bot.send_message(message.chat.id, mess, parse_mode='html')
        # Отправляем на повторение шага
        return early_refund_step_1_ru(message, user_data, all_investment_data)

    len_investment_data = len(all_investment_data)

    if 0 < number_of_the_investment <= len_investment_data:

        investment_data = all_investment_data[number_of_the_investment - 1]

        mess = f'<b>Вы хотите досрочно вернуть этот вклад и получить {investment_data[2] / 2}$ обратно?</b>\n\n'

        if investment_data[1] == 1:
            mess = mess + f'Тариф: Авиационный' \
                          f'\nСтавка по тарифу: 0.7%/день' \
                          f'\nСумма инвестиций: {investment_data[2]}' \
                          f'\nДата начала инвест. плана: ' \
                          f'{datetime.datetime.fromtimestamp(investment_data[3]).strftime("%d.%m.%Y")}' \
                          f'\nДата окончания: ' \
                          f'{datetime.datetime.fromtimestamp(investment_data[4]).strftime("%d.%m.%Y")}'

        elif investment_data[1] == 2:
            mess = mess + f'Тариф: Железнодорожный' \
                          f'\nСтавка по тарифу: 1%/день' \
                          f'\nСумма инвестиций: {investment_data[2]}' \
                          f'\nДата начала инвест. плана: ' \
                          f'{datetime.datetime.fromtimestamp(investment_data[3]).strftime("%d.%m.%Y")}' \
                          f'\nДата окончания: ' \
                          f'{datetime.datetime.fromtimestamp(investment_data[4]).strftime("%d.%m.%Y")}'

        elif investment_data[1] == 3:
            mess = mess + f'Тариф: Автомобильный' \
                          f'\nСтавка по тарифу: 1.3%/день' \
                          f'\nСумма инвестиций: {investment_data[2]}' \
                          f'\nДата начала инвест. плана: ' \
                          f'{datetime.datetime.fromtimestamp(investment_data[3]).strftime("%d.%m.%Y")}' \
                          f'\nДата окончания: ' \
                          f'{datetime.datetime.fromtimestamp(investment_data[4]).strftime("%d.%m.%Y")}'

        elif investment_data[1] == 4:
            mess = mess + f'Тариф: Морской' \
                          f'\nСтавка по тарифу: 1.6%/день' \
                          f'\nСумма инвестиций: {investment_data[2]}' \
                          f'\nДата начала инвест. плана: ' \
                          f'{datetime.datetime.fromtimestamp(investment_data[3]).strftime("%d.%m.%Y")}' \
                          f'\nДата окончания: ' \
                          f'{datetime.datetime.fromtimestamp(investment_data[4]).strftime("%d.%m.%Y")}'

        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
        button1 = types.KeyboardButton('Да')
        button2 = types.KeyboardButton('Нет')
        markup.add(button1, button2)

        bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

        return bot.register_next_step_handler(message, early_refund_step_3_ru, user_data, all_investment_data,
                                              investment_data)

    else:

        str_len_investment_data = str(len_investment_data)

        if len_investment_data == 1:
            mess = f'У вас нет вклада под таким номером' \
                   f'\n\nУ вас есть только 1 активный вклад. Поэтому введите в чат цифру "1"'
        elif (len(str_len_investment_data) > 1 and int(str_len_investment_data[-2]) == 1) \
                or 9 >= int(str_len_investment_data[-1]) > 4:
            mess = f'У вас нет вклада под таким номером' \
                   f'\n\nУ вас есть {len_investment_data} активных вкладов' \
                   f'\n\nВведите номер вашего вклада от <b>1 до {len_investment_data}</b>'
        elif int(str_len_investment_data[-1]) == 1:
            mess = f'У вас нет вклада под таким номером' \
                   f'\n\nУ вас есть {len_investment_data} активный вклад' \
                   f'\n\nВведите номер вашего вклада от <b>1 до {len_investment_data}</b>'
        elif 5 > int(str_len_investment_data[-1]) > 1:
            mess = f'У вас нет вклада под таким номером' \
                   f'\n\nУ вас есть {len_investment_data} активных вклада' \
                   f'\n\nВведите номер вашего вклада от <b>1 до {len_investment_data}</b>'
        else:
            return menu_selection_ru(message, None)

        bot.send_message(message.chat.id, mess, parse_mode='html')

        message.text = 'Досрочно вернуть выбранные инвестиции'
        return early_refund_step_1_ru(message, user_data, all_investment_data)


def early_refund_step_3_ru(message, user_data, all_investment_data, investment_data):

    if message.text == 'Нет':

        # Отправляем на шаг назад
        message.text = 'Досрочно вернуть выбранные инвестиции'
        return early_refund_step_1_ru(message, user_data, all_investment_data)

    elif message.text == 'Да':

        connection = None
        try:
            connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)
            connection.autocommit = True

            current_date = int(datetime.datetime.now().timestamp())

            # Закрываем инвестицию и ставим в дату окончания текущую дату
            with connection.cursor() as cursor:
                cursor.execute(
                    """UPDATE investment
                    SET investment_end_date = %s, deposit_is_active = False
                    WHERE id = %s;""",
                    (current_date, investment_data[0])
                )

                # Добавляем к балансу человека половину от суммы инвестиций
                refund = investment_data[2] / 2
                cursor.execute('UPDATE users SET balance = balance + %s WHERE telegram_id = %s',
                               (refund, user_data[1]))

                print("[INFO] Investment and balance has been successfully updated")

        except Exception as _ex:
            print("[INFO] Error wile with PostgreSQL", _ex)
        finally:
            if connection:
                connection.close()
                print("[INFO] PostgreSQL connection closed")

        mess = f'Инвестиция успешно отменена. Половина от суммы инвестиции вернулась вам на баланс' \
               f'\n\nТеперь ваш баланс составляет: {float(user_data[7]) + refund}$'

        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
        button1 = types.KeyboardButton('Вернуться в личный кабинет')
        markup.add(button1)

        bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

        return bot.register_next_step_handler(message, menu_selection_ru, user_data)

    else:
        return menu_selection_ru(message, None)


def withdrawal_history_ru(message, user_data):

    connection = None
    try:
        connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)
        connection.autocommit = True

        with connection.cursor() as cursor:
            cursor.execute(
                """SELECT id, withdrawal_amount, request_date, request_status 
                FROM money_withdrawal 
                WHERE telegram_id = %s
                ORDER BY request_date;""",
                (user_data[1],)
            )
            all_withdrawal_data = cursor.fetchall()

            if all_withdrawal_data:

                mess = 'Ваши прошлые выводы средств:\n\n1. '
                for i, withdrawal_data in enumerate(all_withdrawal_data):

                    if i != 0:
                        mess = mess + f'\n\n\n{i + 1}. '

                    if withdrawal_data[3] == 'successful':
                        mess = mess + f'Сумма вывода: {withdrawal_data[1]}' \
                                      f'\nДата заявки на вывод: ' \
                        f'{datetime.datetime.fromtimestamp(withdrawal_data[2]).strftime("%H:%M по GMT, %d.%m.%Y")}' \
                                      f'\nСтатус: Успешный вывод'

                    elif withdrawal_data[3] == 'in processing':
                        mess = mess + f'Сумма вывода: {withdrawal_data[1]}' \
                                      f'\nДата заявки на вывод: ' \
                        f'{datetime.datetime.fromtimestamp(withdrawal_data[2]).strftime("%H:%M по GMT, %d.%m.%Y")}' \
                                      f'\nСтатус: Ожидание вывода'

                    elif withdrawal_data[3] == 'unsuccessful':
                        mess = mess + f'Сумма вывода: {withdrawal_data[1]}' \
                                      f'\nДата заявки на вывод: ' \
                        f'{datetime.datetime.fromtimestamp(withdrawal_data[2]).strftime("%H:%M, %d.%m.%Y")}' \
                                      f'\nСтатус: Не одобрено'

                markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
                button1 = types.KeyboardButton('Подать заявку на вывод денег')
                button2 = types.KeyboardButton('Назад')
                markup.add(button1, button2)

                bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

            else:

                mess = 'У вас не было выводов'
                markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
                button1 = types.KeyboardButton('Подать заявку на вывод денег')
                button2 = types.KeyboardButton('Назад')
                markup.add(button1, button2)

                bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

    except Exception as _ex:
        print("[INFO] Error wile with PostgreSQL", _ex)
    finally:
        if connection:
            connection.close()
            print("[INFO] PostgreSQL connection closed")

    return bot.register_next_step_handler(message, withdraw_money_step_1_ru, user_data, all_withdrawal_data)


def withdraw_money_step_1_ru(message, user_data, all_withdrawal_data):

    if message.text == 'Назад':

        # Отправляем на шаг назад
        message.text = 'Вернуться в личный кабинет'
        return menu_selection_ru(message, user_data)

    elif message.text == 'Подать заявку на вывод денег':

        # Если была хотя бы одна заявка на вывод
        if all_withdrawal_data:

            current_date = int(datetime.datetime.now().timestamp())

            # Если от даты последней заявки уже прошло 3 дня
            if all_withdrawal_data[-1][2] + 259200 < current_date:

                mess = f'Вывод происходит в течении 48 часов' \
                       f'\n\n<b>Введите сумму, которую хотите вывести:</b>'

                markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
                button1 = types.KeyboardButton('Вернуться в личный кабинет')
                markup.add(button1)

                bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

                return bot.register_next_step_handler(message, withdraw_money_step_2_ru, user_data, all_withdrawal_data)

            else:

                mess = 'Заявку на вывод можно отправлять не чаще, чем раз в 3 дня' \
                       '\n\nС даты последней заявки ещё не прошло 3 дня, ожидайте'

                markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
                button1 = types.KeyboardButton('Вернуться в личный кабинет')
                markup.add(button1)

                bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

                return bot.register_next_step_handler(message, menu_selection_ru, user_data)

        # Если не было ни одной заявки на вывод
        else:

            # То берём все инвестиции этого человека
            connection = None
            try:
                connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)
                connection.autocommit = True

                with connection.cursor() as cursor:
                    cursor.execute(
                        """SELECT investment_start_date
                        FROM investment 
                        WHERE telegram_id = %s
                        ORDER BY investment_start_date;""",
                        (user_data[1],)
                    )
                    user_investment_data = cursor.fetchall()

            except Exception as _ex:
                print("[INFO] Error wile with PostgreSQL", _ex)
            finally:
                if connection:
                    connection.close()
                    print("[INFO] PostgreSQL connection closed")

            # Проверяем была ли хоть одна инвестиция у этого человека
            if user_investment_data:

                current_date = int(datetime.datetime.now().timestamp())

                # Проверяем прошло 3 дня с даты первой инвестиции
                if user_investment_data[0][0] + 259200 < current_date:

                    mess = f'Вывод происходит в течении 48 часов' \
                           f'\n\n<b>Введите сумму, которую хотите вывести:</b>'

                    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
                    button1 = types.KeyboardButton('Вернуться в личный кабинет')
                    markup.add(button1)
                    bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

                    return bot.register_next_step_handler(message, withdraw_money_step_2_ru,
                                                          user_data, all_withdrawal_data)

                # Инвестиции есть, но не прошло 3 дня с первой инвестиции
                else:

                    mess = 'Деньги можно вывести только, если прошло 3 дня с момента открытия первой инвестиции.' \
                           '\n\nУ вас ещё не прошло 3 дня, ожидайте'
                    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
                    button1 = types.KeyboardButton('Вернуться в личный кабинет')
                    markup.add(button1)
                    bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

                    return bot.register_next_step_handler(message, menu_selection_ru, user_data)

            # Тут у человека нет ни выводов, ни инвестиций
            else:

                mess = 'Деньги можно вывести только, если прошло 3 дня с момента открытия первой инвестиции.' \
                       '\n\nВам нужно открыть хотя бы одну инвестицию, что бы иметь возможность выводить деньги'
                markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
                button1 = types.KeyboardButton('Вернуться в личный кабинет')
                markup.add(button1)
                bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

                return bot.register_next_step_handler(message, menu_selection_ru, user_data)

    else:
        return menu_selection_ru(message, None)


def withdraw_money_step_2_ru(message, user_data, all_withdrawal_data):

    if message.text == 'Вернуться в личный кабинет':
        return to_personal_account_ru(message)

    try:
        withdrawal_amount = int(message.text)
        if not message.text.isdigit():
            raise ValueError

        if withdrawal_amount >= 50:

            if user_data[7] >= withdrawal_amount:

                connection = None
                try:
                    connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)
                    connection.autocommit = True

                    with connection.cursor() as cursor:
                        cursor.execute('SELECT wallet_number FROM users WHERE telegram_id = %s',
                                       (user_data[1],))
                        wallet_number = cursor.fetchone()[0]

                        # Уже есть кошелёк в базе данных
                        if wallet_number:

                            mess = f'Если хотите получить вывод на этот прежний кошелёк:' \
                                   f'\n<code>{wallet_number}</code>' \
                                   f'\n, то нажмите кнопку <b>Сделать вывод на прежний кошелёк</b>' \
                                   f'\n\nЕсли хотите обновить номер кошелька,' \
                                   f' <b>тогда введите новый номер в чат</b> ' \
                                   f'(кошелёк должен быть в валюте USDT и в сети TRC20): '

                            markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
                            button1 = types.KeyboardButton('Сделать вывод на прежний кошелёк')
                            button2 = types.KeyboardButton('Вернуться в личный кабинет')
                            markup.add(button1, button2)

                            bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

                            return bot.register_next_step_handler(message, withdraw_money_step_3_ru, user_data,
                                                                  withdrawal_amount, wallet_number)

                        # Кошелька нет в базе данных
                        else:

                            mess = 'Введите номер вашего крипто-кошелька, куда хотите получить вывод. ' \
                                   'Кошелёк должен быть в валюте USDT и в сети TRC20:'

                            markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
                            button1 = types.KeyboardButton('Вернуться в личный кабинет')
                            markup.add(button1)

                            bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

                            return bot.register_next_step_handler(message, withdraw_money_step_3_ru, user_data,
                                                                  withdrawal_amount)

                except Exception as _ex:
                    print("[INFO] Error wile with PostgreSQL", _ex)
                finally:
                    if connection:
                        connection.close()
                        print("[INFO] PostgreSQL connection closed")

            else:
                bot.send_message(message.chat.id, '<b>У тебя на балансе недостаточно денег.</b>', parse_mode='html')
                # Отправляем на повторение шага
                message.text = 'Подать заявку на вывод денег'
                return withdraw_money_step_1_ru(message, user_data, all_withdrawal_data)

        # Если сумма меньше 50
        else:

            bot.send_message(message.chat.id, '<b>Минимальная сумма вывода 50$</b>', parse_mode='html')
            # Отправляем на повторение шага
            message.text = 'Подать заявку на вывод денег'
            return withdraw_money_step_1_ru(message, user_data, all_withdrawal_data)

    except ValueError:
        bot.send_message(message.chat.id, 'Число должно быть целое, без точек,'
                                          ' запятых и других символов. '
                                          'Попробуйте заново', parse_mode='html')
        # Отправляем на повтор шага
        message.text = 'Подать заявку на вывод денег'
        return withdraw_money_step_1_ru(message, user_data, all_withdrawal_data)


def withdraw_money_step_3_ru(message, user_data, withdrawal_amount, wallet_number=None):

    if message.text == 'Вернуться в личный кабинет':
        return to_personal_account_ru(message)

    elif message.text == 'Сделать вывод на прежний кошелёк':
        # Значит у нас уже есть номер кошелька
        pass

    else:
        # Берём номер кошелька и обновляем его в таблице пользователей
        wallet_number = message.text

        connection = None
        try:
            connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)
            connection.autocommit = True

            with connection.cursor() as cursor:
                cursor.execute('UPDATE users SET wallet_number = %s WHERE telegram_id = %s',
                               (wallet_number, user_data[1]))

                print("[INFO] Wallet number has been successfully updated")

        except Exception as _ex:
            print("[INFO] Error wile with PostgreSQL", _ex)
        finally:
            if connection:
                connection.close()
                print("[INFO] PostgreSQL connection closed")

    connection = None
    try:
        connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)
        connection.autocommit = True

        current_date = int(datetime.datetime.now().timestamp())

        with connection.cursor() as cursor:
            cursor.execute(
                """UPDATE users
                SET balance = balance - %s
                WHERE telegram_id = %s;""",
                (withdrawal_amount, user_data[1])
            )

            print("[INFO] Balance has been successfully updated")

            cursor.execute(
                """INSERT INTO money_withdrawal (telegram_id, withdrawal_amount, request_date, request_status)
                VALUES (%s, %s, %s, %s);""",
                (user_data[1], withdrawal_amount, current_date, 'in processing')
            )

            print("[INFO] The money withdrawal was successfully inserted")

            mess = f'Ваша заявка на вывод {withdrawal_amount}$ на этот кошелёк {wallet_number} успешно отправлена! ' \
                   '\n\nОжидайте подтверждение платежа и зачисления средств на введённый кошелёк в течении 48 часов'
            markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
            button1 = types.KeyboardButton('Вернуться в личный кабинет')
            markup.add(button1)
            bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

            tz = pytz.timezone('Europe/Moscow')
            current_date_msk = datetime.datetime.fromtimestamp(current_date).astimezone(tz)

            mess_for_admins_ru = f'Новая заявка на вывод денег:' \
                                 f'\n\nСумма для вывода: <b>{withdrawal_amount}</b>$' \
                                 f'\nУказанный кошелёк для вывода: ' \
                                 f'\n<code>{wallet_number}</code>' \
                                 f'\nТелеграм id: <code>{user_data[1]}</code>' \
                                 f'\nТелеграм ник: @{user_data[2]}' \
                                 f'\nЕго баланс до вывода: {user_data[7]}' \
                                 f'\nЕго баланс после вывода: {user_data[7] - withdrawal_amount}' \
                                 f'\nВремя и дата заявки: {current_date_msk.strftime("%H:%M, %d.%m.%Y")}'

            mess_for_admins_en = f'New withdrawal request:' \
                                 f'\n\nAmount to withdraw: $<b>{withdrawal_amount}</b>' \
                                 f'\nSpecified wallet for withdrawal: ' \
                                 f'\n<code>{wallet_number}</code>' \
                                 f'\nTelegram id: <code>{user_data[1]}</code>' \
                                 f'\nTelegram username: @{user_data[2]}' \
                                 f'\nHis balance before withdrawal: {user_data[7]}' \
                                 f'\nHis balance after withdrawal: {user_data[7] - withdrawal_amount}' \
                                 f'\nTime and date of application: {current_date_msk.strftime("%H:%M, %d.%m.%Y")}'

            # Отправка оповещения всем админам
            for i in admins:
                # Определяем какой язык сейчас выбран у админа в боте
                cursor.execute('SELECT language FROM users WHERE telegram_id = %s', (i,))
                language = cursor.fetchone()[0]

                if language == 'en':
                    bot.send_message(i, mess_for_admins_en, parse_mode='html')
                else:
                    bot.send_message(i, mess_for_admins_ru, parse_mode='html')

            return bot.register_next_step_handler(message, menu_selection_ru, user_data)

    except Exception as _ex:
        print("[INFO] Error wile with PostgreSQL", _ex)
    finally:
        if connection:
            connection.close()
            print("[INFO] PostgreSQL connection closed")


def referral_program_ru(message, user_data):

    mess = f'Приглашай друзей и получай проценты от их инвестиций!' \
           f'\nВот твоя реферальная ссылка:' \
           f'\n\nhttps://t.me/GlobalFinancialInvestorBot?start={user_data[1]}' \
           f'\n\nКопируй её и отправляй друзьям, знакомым'

    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
    button1 = types.KeyboardButton('Назад')
    markup.add(button1)

    bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

    return bot.register_next_step_handler(message, to_personal_account_ru)


def about_us_ru(message, user_data):

    doc_ru = open(r'about_company_RU.docx', 'rb')
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    button = types.KeyboardButton('Назад')
    markup.add(button)

    bot.send_document(message.chat.id, doc_ru, caption='Здесь всё подробно описано на русском', reply_markup=markup)

    return bot.register_next_step_handler(message, to_personal_account_ru)


def admin_panel_ru(message, user_data):

    admin_information = database_general_information()

    mess = f'Приветствую, {message.from_user.first_name}' \
           f'\nВсего людей заходило в бота: {admin_information[0]}' \
           f'\nВсего зарегистрировано людей: {admin_information[1]}' \
           f'\nЗарегистрировано людей через реферальную программу: {admin_information[2]}' \
           f'\nСуммарный счёт у всех: {admin_information[3]}' \
           f'\nСуммарный заработок через реферальную программу: {admin_information[4]}'

    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    button1 = types.KeyboardButton('Заявки на пополнение баланса')
    button2 = types.KeyboardButton('Заявки на вывод денег')
    button3 = types.KeyboardButton('Скачать всю базу пользователей')
    button4 = types.KeyboardButton('В личный кабинет')
    markup.add(button1, button2, button3, button4)

    bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)
    return bot.register_next_step_handler(message, menu_selection_ru, user_data)


def database_general_information():

    connection = None
    try:
        connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)
        connection.autocommit = True

        with connection.cursor() as cursor:

            cursor.execute(
                """SELECT COUNT(telegram_id) FROM users;"""
            )
            all_users = cursor.fetchone()[0]

            cursor.execute(
                """SELECT COUNT(email) FROM users;"""
            )
            registered_users = cursor.fetchone()[0]

            cursor.execute(
                f"""SELECT COUNT(email) FROM users WHERE invited_by IS NOT NULL;"""
            )
            registered_invited_users = cursor.fetchone()[0]

            cursor.execute(
                """SELECT SUM(balance) FROM users;"""
            )
            sum_balance = cursor.fetchone()[0]

            cursor.execute(
                """SELECT SUM(ref_balance) FROM users;"""
            )
            sum_ref_balance = cursor.fetchone()[0]

    except Exception as _ex:
        print("[INFO] Error wile with PostgreSQL", _ex)
    finally:
        if connection:
            connection.close()
            print("[INFO] PostgreSQL connection closed")

    return all_users, registered_users, registered_invited_users, sum_balance, sum_ref_balance


def admin_replenishment_step_1_ru(message, user_data, only_buttons=False):

    connection = None
    try:
        connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)
        connection.autocommit = True

        with connection.cursor() as cursor:
            cursor.execute(
                """SELECT id, telegram_id, user_name, replenishment_amount, transaction_hash, request_date 
                FROM balance_replenishment 
                WHERE request_status = 'in processing'
                ORDER BY request_date;"""
            )
            all_replenishment_data = cursor.fetchall()

    except Exception as _ex:
        print("[INFO] Error wile with PostgreSQL", _ex)
    finally:
        if connection:
            connection.close()
            print("[INFO] PostgreSQL connection closed")

    if all_replenishment_data:

        # Если нет режима "только клавиши"
        if not only_buttons:
            tz = pytz.timezone('Europe/Moscow')
            mess = 'Необработанные заявки на пополнение:\n\n1. '

            for i, replenishment_data in enumerate(all_replenishment_data):

                request_date_msk = datetime.datetime.fromtimestamp(replenishment_data[5]).astimezone(tz)

                if i != 0:
                    mess = mess + f'\n\n\n{i + 1}. '

                mess = mess + f'Transaction Hash: <code>{replenishment_data[4]}</code>' \
                              f'\nСумма для пополнения: <b>{replenishment_data[3]}</b>$' \
                              f'\nТелеграм id: <code>{replenishment_data[1]}</code>' \
                              f'\nТелеграм ник: @{replenishment_data[2]}' \
                              f'\nВремя и дата заявки: {request_date_msk.strftime("%H:%M по МСК, %d.%m.%Y")}' \
                              f'\nСтатус: В обработке'

            bot.send_message(message.chat.id, mess, parse_mode='html')

        mess = 'Список заявок пронумерован в сообщении выше.' \
               '\n<b>Введите в чат номер заявки, которую хотите обработать:</b>'
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
        button1 = types.KeyboardButton('Скачать историю всех заявок на пополнение')
        button2 = types.KeyboardButton('Назад')
        markup.add(button1, button2)
        bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

        return bot.register_next_step_handler(message, admin_replenishment_step_2_ru, user_data, all_replenishment_data)

    else:

        mess = ''
        # Если нет режима "только клавиши"
        if not only_buttons:
            mess = 'Необработанные заявки на пополнение отсутствуют'

        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
        button1 = types.KeyboardButton('Скачать историю всех заявок на пополнение')
        button2 = types.KeyboardButton('Назад')
        markup.add(button1, button2)
        bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

        return bot.register_next_step_handler(message, admin_replenishment_step_2_ru, user_data, all_replenishment_data)


def admin_replenishment_step_2_ru(message, user_data, all_replenishment_data):

    if message.text == 'Назад':
        # Отправляем в на шаг назад
        return admin_panel_ru(message, user_data)

    elif message.text == 'Скачать историю всех заявок на пополнение':

        connection = None
        try:
            connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)
            connection.autocommit = True

            with connection.cursor() as cursor:

                with open('balance_replenishment.csv', 'w') as f:

                    cursor.execute("""SELECT COLUMN_NAME 
                            FROM INFORMATION_SCHEMA.COLUMNS 
                            WHERE TABLE_NAME = N'balance_replenishment'""")
                    for i, x in enumerate(cursor):
                        if i != 0:
                            f.write(',')
                        f.write(str(x[0]))
                    f.write('\n')

                    cursor.execute('SELECT * FROM balance_replenishment ORDER BY id')
                    for i, row in enumerate(cursor):
                        if i != 0:
                            f.write('\n')
                        for n, x in enumerate(row):
                            if n != 0:
                                f.write(',')
                            f.write(str(x))

            balance_replenishment_data = open(r'balance_replenishment.csv', 'rb')
            bot.send_document(message.chat.id, balance_replenishment_data,
                              caption='Это все заявки пользователей на пополнение баланса')

        except Exception as _ex:
            print("[INFO] Error wile with PostgreSQL", _ex)
        finally:
            if connection:
                connection.close()
                print("[INFO] PostgreSQL connection closed")

        return admin_replenishment_step_1_ru(message, user_data, only_buttons=True)

    elif all_replenishment_data:

        if message.text.isdigit():
            request_number = int(message.text)

        else:
            mess = 'В номере заявки должны быть быть только цифры больше нуля, без дополнительных символов' \
                   '\nПроверьте данные и повторите попытку.'
            bot.send_message(message.chat.id, mess, parse_mode='html')
            # Отправляем на повторение шага
            return admin_replenishment_step_1_ru(message, user_data, only_buttons=True)

        len_replenishment_data = len(all_replenishment_data)

        if 0 < request_number <= len_replenishment_data:

            replenishment_data = all_replenishment_data[request_number - 1]

            tz = pytz.timezone('Europe/Moscow')
            request_date_msk = datetime.datetime.fromtimestamp(replenishment_data[5]).astimezone(tz)

            mess = 'Проверьте данные и одобрите заявку, если человек перечислил нужную сумму. ' \
                   'Или отклоните заявку. ' \
                   '\nПри одобрении заявки, баланс заявителя в боте пополнится автоматически\n\n'

            mess = mess + f'{request_number}. Transaction Hash: <code>{replenishment_data[4]}</code>' \
                          f'\nСумма для пополнения: <b>{replenishment_data[3]}</b>$' \
                          f'\nТелеграм id: <code>{replenishment_data[1]}</code>' \
                          f'\nТелеграм ник: @{replenishment_data[2]}' \
                          f'\nВремя и дата заявки: {request_date_msk.strftime("%H:%M по МСК, %d.%m.%Y")}' \
                          f'\nСтатус: В обработке'

            markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
            button1 = types.KeyboardButton('Одобрить заявку')
            button2 = types.KeyboardButton('Отклонить заявку')
            button3 = types.KeyboardButton('Назад')
            markup.add(button1, button2, button3)

            bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

            return bot.register_next_step_handler(message, admin_replenishment_step_3_ru, user_data, replenishment_data)

        else:

            str_len_replenishment_data = str(len_replenishment_data)

            if len_replenishment_data == 1:
                mess = f'У вас нет заявки под таким номером' \
                       f'\n\nУ вас есть только 1 необработанная заявка. Поэтому введите в чат  цифру "1"'
            elif (len(str_len_replenishment_data) > 1 and int(str_len_replenishment_data[-2]) == 1) \
                    or 9 >= int(str_len_replenishment_data[-1]) > 4:
                mess = f'У вас нет заявки под таким номером' \
                       f'\n\nУ вас есть {len_replenishment_data} необработанных заявок' \
                       f'\n\nВведите номер заявки от <b>1 до {len_replenishment_data}</b>'
            elif int(str_len_replenishment_data[-1]) == 1:
                mess = f'У вас нет заявки под таким номером' \
                       f'\n\nУ вас есть {len_replenishment_data} необработанная заявка' \
                       f'\n\nВведите номер заявки от <b>1 до {len_replenishment_data}</b>'
            elif 5 > int(str_len_replenishment_data[-1]) > 1:
                mess = f'У вас нет заявки под таким номером' \
                       f'\n\nУ вас есть {len_replenishment_data} необработанные заявки' \
                       f'\n\nВведите номер заявки от <b>1 до {len_replenishment_data}</b>'
            else:
                return menu_selection_ru(message, None)

            bot.send_message(message.chat.id, mess, parse_mode='html')

            return admin_replenishment_step_1_ru(message, user_data, only_buttons=True)

    else:
        return menu_selection_ru(message, None)


def admin_replenishment_step_3_ru(message, user_data, replenishment_data):

    if message.text == 'Назад':

        # Отправляем на шаг назад
        return admin_replenishment_step_1_ru(message, user_data)

    elif message.text == 'Одобрить заявку':

        connection = None
        try:
            connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)
            connection.autocommit = True

            with connection.cursor() as cursor:
                cursor.execute(
                    """UPDATE balance_replenishment
                    SET request_status = 'successful'
                    WHERE id = %s;""",
                    (replenishment_data[0],)
                )

                cursor.execute('UPDATE users SET balance = balance + %s WHERE telegram_id = %s',
                               (replenishment_data[3], replenishment_data[1]))

                print("[INFO] Balance replenishment and balance has been successfully updated")

                # Определяем какой язык сейчас выбран у пользователя в боте
                cursor.execute('SELECT language FROM users WHERE telegram_id = %s',
                               (replenishment_data[1],))
                language = cursor.fetchone()[0]

                if language == 'en':
                    mess = f'Your replenishment request has been approved. ${replenishment_data[3]}' \
                           f' credited to your balance'
                else:
                    mess = f'Ваша заявка на пополнение одобрена. Вам на баланс начислено {replenishment_data[3]}$'

                bot.send_message(replenishment_data[1], mess, parse_mode='html')

        except Exception as _ex:
            print("[INFO] Error wile with PostgreSQL", _ex)
        finally:
            if connection:
                connection.close()
                print("[INFO] PostgreSQL connection closed")

        mess = f'Заявка одобрена и убрана в архив. Баланс заявителя @{replenishment_data[2]}' \
               f' пополнен на {replenishment_data[3]}$' \

        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
        button1 = types.KeyboardButton('Вернуться к заявкам на пополнение')
        button2 = types.KeyboardButton('Вернуться в админ панель')
        markup.add(button1, button2)

        bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

        return bot.register_next_step_handler(message, menu_selection_ru, user_data)

    elif message.text == 'Отклонить заявку':

        connection = None
        try:
            connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)
            connection.autocommit = True

            with connection.cursor() as cursor:
                cursor.execute(
                    """UPDATE balance_replenishment
                    SET request_status = 'unsuccessful'
                    WHERE id = %s;""",
                    (replenishment_data[0],)
                )

                print("[INFO] Balance replenishment has been successfully updated")

                # Определяем какой язык сейчас выбран у пользователя в боте
                cursor.execute('SELECT language FROM users WHERE telegram_id = %s',
                               (replenishment_data[1],))
                language = cursor.fetchone()[0]

                if language == 'en':
                    mess = f'Your replenishment request has been rejected. ' \
                           f'\n\nCheck the data and create a new request. ' \
                           f'Or write to technical support using the /help command to find out the' \
                           f' reasons for the rejection'
                else:
                    mess = f'Ваша заявка на пополнение отклонена. ' \
                           f'\n\nПроверьте данные и создайте новую заявку. ' \
                           f'Либо напишите в тех поддержку по команде /help, что бы узнать причины отклонения'

                bot.send_message(replenishment_data[1], mess, parse_mode='html')

                mess = f'Заявка отклонена и убрана в архив.' \
                       f'\nБаланс заявителя не изменился'
                markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
                button1 = types.KeyboardButton('Вернуться к заявкам на пополнение')
                button2 = types.KeyboardButton('Вернуться в админ панель')
                markup.add(button1, button2)

                bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

                return bot.register_next_step_handler(message, menu_selection_ru, user_data)

        except Exception as _ex:
            print("[INFO] Error wile with PostgreSQL", _ex)
        finally:
            if connection:
                connection.close()
                print("[INFO] PostgreSQL connection closed")

    else:
        return menu_selection_ru(message, None)


def admin_withdrawal_step_1_ru(message, user_data, only_buttons=False):

    connection = None
    try:
        connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)
        connection.autocommit = True

        with connection.cursor() as cursor:
            cursor.execute(
                """SELECT id, telegram_id, withdrawal_amount, request_date 
                FROM money_withdrawal 
                WHERE request_status = 'in processing'
                ORDER BY request_date;"""
            )
            all_withdrawal_data = cursor.fetchall()

            if all_withdrawal_data:

                # Если нет режима "только клавиши"
                if not only_buttons:
                    tz = pytz.timezone('Europe/Moscow')
                    mess = 'Необработанные заявки на вывод:\n\n1. '

                    for i, withdrawal_data in enumerate(all_withdrawal_data):

                        cursor.execute(
                            """SELECT user_name, email, balance, wallet_number
                            FROM users 
                            WHERE telegram_id = %s;""",
                            (withdrawal_data[1],)
                        )
                        applicant_data = cursor.fetchone()

                        request_date_msk = datetime.datetime.fromtimestamp(withdrawal_data[3]).astimezone(tz)

                        if i != 0:
                            mess = mess + f'\n\n\n{i + 1}. '

                        # Если пользователя нет в бд
                        if not applicant_data:
                            mess = mess + f'ПОЛЬЗОВАТЕЛЬ, ПОДАВШИЙ ЭТУ ЗАЯВКУ, БОЛЬШЕ НЕ СУЩЕСТВУЕТ' \
                                          f'\nСумма для вывода: <b>{withdrawal_data[2]}</b>$' \
                                          f'\nТелеграм id: <code>{withdrawal_data[1]}</code>' \
                                          f'\nВремя и дата заявки: {request_date_msk.strftime("%H:%M, %d.%m.%Y")}' \
                                          f'\nСтатус: В обработке'
                            continue

                        mess = mess + f'Сумма для вывода: <b>{withdrawal_data[2]}</b>$' \
                                      f'\nУказанный кошелёк для вывода: ' \
                                      f'\n<code>{applicant_data[3]}</code>' \
                                      f'\nТелеграм id: <code>{withdrawal_data[1]}</code>' \
                                      f'\nТелеграм ник: @{applicant_data[0]}' \
                                      f'\nПочта: {applicant_data[1]}' \
                                      f'\nЕго текущий баланс (после вывода): {applicant_data[2]}' \
                                      f'\nВремя и дата заявки: {request_date_msk.strftime("%H:%M, %d.%m.%Y")}' \
                                      f'\nСтатус: В обработке'

                    bot.send_message(message.chat.id, mess, parse_mode='html')

                mess = 'Список заявок пронумерован в сообщении выше.' \
                       '\n<b>Введите в чат номер заявки, которую хотите обработать:</b>'
                markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
                button1 = types.KeyboardButton('Скачать историю всех заявок на вывод')
                button2 = types.KeyboardButton('Назад')
                markup.add(button1, button2)
                bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

                return bot.register_next_step_handler(message, admin_withdrawal_step_2_ru,
                                                      user_data, all_withdrawal_data)

            else:

                mess = ''
                # Если нет режима "только клавиши"
                if not only_buttons:
                    mess = 'Необработанные заявки на вывод отсутствуют'

                markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
                button1 = types.KeyboardButton('Скачать историю всех заявок на вывод')
                button2 = types.KeyboardButton('Назад')
                markup.add(button1, button2)
                bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

                return bot.register_next_step_handler(message, admin_withdrawal_step_2_ru,
                                                      user_data, all_withdrawal_data)

    except Exception as _ex:
        print("[INFO] Error wile with PostgreSQL", _ex)
    finally:
        if connection:
            connection.close()
            print("[INFO] PostgreSQL connection closed")


def admin_withdrawal_step_2_ru(message, user_data, all_withdrawal_data):

    if message.text == 'Назад':
        # Отправляем в на шаг назад
        return admin_panel_ru(message, user_data)

    elif message.text == 'Скачать историю всех заявок на вывод':

        connection = None
        try:
            connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)
            connection.autocommit = True

            with connection.cursor() as cursor:

                with open('money_withdrawal.csv', 'w') as f:

                    cursor.execute("""SELECT COLUMN_NAME 
                            FROM INFORMATION_SCHEMA.COLUMNS 
                            WHERE TABLE_NAME = N'money_withdrawal'""")
                    for i, x in enumerate(cursor):
                        if i != 0:
                            f.write(',')
                        f.write(str(x[0]))
                    f.write('\n')

                    cursor.execute('SELECT * FROM money_withdrawal ORDER BY id')
                    for i, row in enumerate(cursor):
                        if i != 0:
                            f.write('\n')
                        for n, x in enumerate(row):
                            if n != 0:
                                f.write(',')
                            f.write(str(x))

            balance_withdrawal_data = open(r'money_withdrawal.csv', 'rb')
            bot.send_document(message.chat.id, balance_withdrawal_data,
                              caption='Это все заявки пользователей на вывод денег')

            return admin_withdrawal_step_1_ru(message, user_data, only_buttons=True)

        except Exception as _ex:
            print("[INFO] Error wile with PostgreSQL", _ex)
        finally:
            if connection:
                connection.close()
                print("[INFO] PostgreSQL connection closed")

    elif all_withdrawal_data:

        if message.text.isdigit():
            request_number = int(message.text)

        else:
            mess = 'В номере заявки должны быть быть только цифры больше нуля и без дополнительных символов' \
                   '\nПроверьте данные и повторите попытку.'
            bot.send_message(message.chat.id, mess, parse_mode='html')
            # Отправляем на повторение шага
            return admin_withdrawal_step_1_ru(message, user_data, only_buttons=True)

        len_withdrawal_data = len(all_withdrawal_data)

        if 0 < request_number <= len_withdrawal_data:

            withdrawal_data = all_withdrawal_data[request_number - 1]

            tz = pytz.timezone('Europe/Moscow')
            request_date_msk = datetime.datetime.fromtimestamp(withdrawal_data[3]).astimezone(tz)

            connection = None
            try:
                connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)
                connection.autocommit = True

                with connection.cursor() as cursor:
                    cursor.execute(
                        """SELECT user_name, email, balance, wallet_number
                        FROM users 
                        WHERE telegram_id = %s;""",
                        (withdrawal_data[1],)
                    )
                    applicant_data = cursor.fetchone()

            except Exception as _ex:
                print("[INFO] Error wile with PostgreSQL", _ex)
            finally:
                if connection:
                    connection.close()
                    print("[INFO] PostgreSQL connection closed")

            mess = 'Проверьте данные и одобрите заявку, если уже перечислили нужную сумму на кошелёк клиента. ' \
                   'Или отклоните заявку. ' \
                   '\nПри отклонении заявки, деньги автоматически вернуться на баланс заявителя в боте\n\n'

            mess = mess + f'{request_number}. Сумма для вывода: <b>{withdrawal_data[2]}</b>$' \
                          f'\nУказанный кошелёк для вывода: ' \
                          f'\n<code>{applicant_data[3]}</code>' \
                          f'\nТелеграм id: <code>{withdrawal_data[1]}</code>' \
                          f'\nТелеграм ник: @{applicant_data[0]}' \
                          f'\nПочта: {applicant_data[1]}' \
                          f'\nЕго текущий баланс (после вывода): {applicant_data[2]}' \
                          f'\nВремя и дата заявки: {request_date_msk.strftime("%H:%M, %d.%m.%Y")}' \
                          f'\nСтатус: В обработке'

            markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
            button1 = types.KeyboardButton('Одобрить заявку')
            button2 = types.KeyboardButton('Отклонить заявку')
            button3 = types.KeyboardButton('Назад')
            markup.add(button1, button2, button3)

            bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

            return bot.register_next_step_handler(message, admin_withdrawal_step_3_ru, user_data, withdrawal_data,
                                                  applicant_data)

        else:

            str_len_withdrawal_data = str(len_withdrawal_data)

            if len_withdrawal_data == 1:
                mess = f'У вас нет заявки под таким номером' \
                       f'\n\nУ вас есть только 1 необработанная заявка. Поэтому введите в чат цифру "1"'
            elif (len(str_len_withdrawal_data) > 1 and int(str_len_withdrawal_data[-2]) == 1) \
                    or 9 >= int(str_len_withdrawal_data[-1]) > 4:
                mess = f'У вас нет заявки под таким номером' \
                       f'\n\nУ вас есть {len_withdrawal_data} необработанных заявок' \
                       f'\n\nВведите номер заявки от <b>1 до {len_withdrawal_data}</b>'
            elif int(str_len_withdrawal_data[-1]) == 1:
                mess = f'У вас нет заявки под таким номером' \
                       f'\n\nУ вас есть {len_withdrawal_data} необработанная заявка' \
                       f'\n\nВведите номер заявки от <b>1 до {len_withdrawal_data}</b>'
            elif 5 > int(str_len_withdrawal_data[-1]) > 1:
                mess = f'У вас нет заявки под таким номером' \
                       f'\n\nУ вас есть {len_withdrawal_data} необработанные заявки' \
                       f'\n\nВведите номер заявки от <b>1 до {len_withdrawal_data}</b>'
            else:
                return menu_selection_ru(message, None)

            bot.send_message(message.chat.id, mess, parse_mode='html')

            return admin_withdrawal_step_1_ru(message, user_data, only_buttons=True)

    else:
        return menu_selection_ru(message, None)


def admin_withdrawal_step_3_ru(message, user_data, withdrawal_data, applicant_data):

    if message.text == 'Назад':

        # Отправляем на шаг назад
        return admin_withdrawal_step_1_ru(message, user_data)

    elif message.text == 'Одобрить заявку':

        connection = None
        try:
            connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)
            connection.autocommit = True

            with connection.cursor() as cursor:
                cursor.execute(
                    """UPDATE money_withdrawal
                    SET request_status = 'successful'
                    WHERE id = %s;""",
                    (withdrawal_data[0],)
                )
                print("[INFO] Money withdrawal status has been successfully updated")

                # Определяем какой язык сейчас выбран у пользователя в боте
                cursor.execute('SELECT language FROM users WHERE telegram_id = %s',
                               (withdrawal_data[1],))
                language = cursor.fetchone()[0]

                if language == 'en':
                    mess = f'Your withdrawal request has been approved. ${withdrawal_data[2]} successfully' \
                           f' withdrawn from the bot to your wallet:' \
                           f'\n<code>{applicant_data[3]}</code>'
                else:
                    mess = f'Ваша заявка на вывод одобрена. {withdrawal_data[2]}$ успешно выведены' \
                           f' с бота на ваш кошелёк:' \
                           f'\n<code>{applicant_data[3]}</code>'

                bot.send_message(withdrawal_data[1], mess, parse_mode='html')

        except Exception as _ex:
            print("[INFO] Error wile with PostgreSQL", _ex)
        finally:
            if connection:
                connection.close()
                print("[INFO] PostgreSQL connection closed")

        mess = 'Заявка одобрена и убрана в архив.'

        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
        button1 = types.KeyboardButton('Вернуться к заявкам на вывод')
        button2 = types.KeyboardButton('Вернуться в админ панель')
        markup.add(button1, button2)

        bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

        return bot.register_next_step_handler(message, menu_selection_ru, user_data)

    elif message.text == 'Отклонить заявку':

        connection = None
        try:
            connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)
            connection.autocommit = True

            with connection.cursor() as cursor:
                cursor.execute(
                    """UPDATE money_withdrawal
                    SET request_status = 'unsuccessful'
                    WHERE id = %s;""",
                    (withdrawal_data[0],)
                )

                cursor.execute('UPDATE users SET balance = balance + %s WHERE telegram_id = %s',
                               (withdrawal_data[2], withdrawal_data[1]))

                print("[INFO] Money withdrawal status and balance has been successfully updated")

                # Определяем какой язык сейчас выбран у пользователя в боте
                cursor.execute('SELECT language FROM users WHERE telegram_id = %s',
                               (withdrawal_data[1],))
                language = cursor.fetchone()[0]

                if language == 'en':
                    mess = f'Your withdrawal request has been rejected. ${withdrawal_data[2]}' \
                           f' has been returned to your balance.' \
                           f'\n\nCheck the data and create a new request. Or write to technical' \
                           f' support using the /help command to find out the reasons for the rejection'
                else:
                    mess = f'Ваша заявка на вывод отклонена. Вам на баланс возвращено {withdrawal_data[2]}$.' \
                           f'\n\nПроверьте данные и создайте новую заявку. Либо напишите в тех поддержку' \
                           f' по команде /help, что бы узнать причины отклонения'

                bot.send_message(withdrawal_data[1], mess, parse_mode='html')

        except Exception as _ex:
            print("[INFO] Error wile with PostgreSQL", _ex)
        finally:
            if connection:
                connection.close()
                print("[INFO] PostgreSQL connection closed")

        mess = f'Заявка отклонена и убрана в архив.' \
               f'\nНа баланс заявителя @{applicant_data[0]} возвращено {withdrawal_data[2]}$'

        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
        button1 = types.KeyboardButton('Вернуться к заявкам на вывод')
        button2 = types.KeyboardButton('Вернуться в админ панель')
        markup.add(button1, button2)

        bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

        return bot.register_next_step_handler(message, menu_selection_ru, user_data)

    else:
        return menu_selection_ru(message, None)


def send_all_database_ru(message, user_data):

    if message.from_user.id in admins:

        connection = None
        try:
            connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)

            connection.autocommit = True

            with connection.cursor() as cursor:

                with open('all_users_data.csv', 'w') as f:

                    cursor.execute("""SELECT COLUMN_NAME 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = N'users'""")
                    for i, x in enumerate(cursor):
                        if i != 0:
                            f.write(',')
                        f.write(str(x[0]))
                    f.write('\n')

                    cursor.execute('SELECT * FROM users ORDER BY id')
                    for i, row in enumerate(cursor):
                        if i != 0:
                            f.write('\n')
                        for n, x in enumerate(row):
                            if n != 0:
                                f.write(',')
                            f.write(str(x))

                with open('investment.csv', 'w') as f:

                    cursor.execute("""SELECT COLUMN_NAME 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = N'investment'""")
                    for i, x in enumerate(cursor):
                        if i != 0:
                            f.write(',')
                        f.write(str(x[0]))
                    f.write('\n')

                    cursor.execute('SELECT * FROM investment ORDER BY id')
                    for i, row in enumerate(cursor):
                        if i != 0:
                            f.write('\n')
                        for n, x in enumerate(row):
                            if n != 0:
                                f.write(',')
                            f.write(str(x))

            all_users_data = open(r'all_users_data.csv', 'rb')
            bot.send_document(message.chat.id, all_users_data, caption='Это все данные пользователей')
            investment_data = open(r'investment.csv', 'rb')
            markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
            button1 = types.KeyboardButton('Заявки на пополнение баланса')
            button2 = types.KeyboardButton('Заявки на вывод денег')
            button3 = types.KeyboardButton('Скачать всю базу пользователей')
            button4 = types.KeyboardButton('В личный кабинет')
            markup.add(button1, button2, button3, button4)

            bot.send_document(message.chat.id, investment_data, caption='Это данные по инвестициям пользователей',
                              reply_markup=markup)

            return bot.register_next_step_handler(message, menu_selection_ru, user_data)

        except Exception as _ex:
            print("[INFO] Error wile with PostgreSQL", _ex)
        finally:
            if connection:
                connection.close()
                print("[INFO] PostgreSQL connection closed")
                
                



# Ниже начинается группа функций с английским переводом
def for_unregistered_users_en(message):

    mess = f'Greetings, {message.from_user.first_name}!' \
           f'\nRegister by clicking the button below and start earning on investments.' \
           f'\nOr learn more about the company'

    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    button1 = types.KeyboardButton('Register')
    button2 = types.KeyboardButton('About us')
    button3 = types.KeyboardButton('Смена языка/Language change')
    markup.add(button1, button2, button3)

    bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

    return bot.register_next_step_handler(message, menu_selection_en, None)


def registration_en(message):

    user_data = database_check_id(message)
    if user_data:
        return to_personal_account_en(message)
    else:
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
        button1 = types.KeyboardButton('Back')
        markup.add(button1)
        bot.send_message(message.chat.id, 'Enter your Email:', reply_markup=markup)

        return bot.register_next_step_handler(message, database_email_registration_en)


def invalid_referral_link_en(message):

    if message.text == 'Yes':
        return registration_en(message)

    elif message.text == 'No':

        mess = f'Then check your referral link for damage or request a new link from a partner.' \
               f'\n\nCorrect referral link format: ' \
               f'\n<code>https://t.me/GlobalFinancialInvestorBot?start=000000000</code>' \
               f'\n\n, where instead of 000000000 should be the telegram ID of your referrer'

        bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=types.ReplyKeyboardRemove())

        return


def database_email_registration_en(message):
    # Сюда может попасть человек, только если у него нет емейла в базе данных

    if message.text == 'Back':
        return for_unregistered_users_en(message)

    elif not re.match(r"^[-\w.]+@([-\w]+\.)+[-\w]{2,4}$", message.text):

        if '/start' in message.text:
            return start(message)
        else:

            markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
            button1 = types.KeyboardButton('Back')
            markup.add(button1)
            bot.send_message(message.chat.id, 'You entered an invalid email format. '
                                              'Check your details and try again', reply_markup=markup)

            return registration_en(message)

    connection = None
    try:
        connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)
        connection.autocommit = True

        with connection.cursor() as cursor:
            cursor.execute(
                """UPDATE users
                SET email = %s
                WHERE telegram_id = %s;""",
                (message.text, message.from_user.id)
            )

            print("[INFO] Email has been successfully updated")

            markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
            button1 = types.KeyboardButton('To personal account')
            markup.add(button1)
            bot.send_message(message.chat.id, 'You have successfully registered!'
                                              '\n\nNow you can enter your personal account', reply_markup=markup)

            # Добавляем всем вышестоящим (до 10 вверх) реферерам по +1 в количество партнёров
            cursor.execute('SELECT invited_by FROM users WHERE telegram_id = %s', (message.from_user.id,))
            invited_id = cursor.fetchone()[0]
            cursor.execute('UPDATE users SET ref_count = ref_count + 1 WHERE telegram_id = %s', (invited_id,))

            for i in range(0, 9):
                cursor.execute('SELECT invited_by FROM users WHERE telegram_id = %s', (invited_id,))
                invited_id = cursor.fetchone()
                if invited_id:
                    invited_id = invited_id[0]
                    cursor.execute('UPDATE users SET ref_count = ref_count + 1 WHERE telegram_id = %s', (invited_id,))
                else:
                    break

            return bot.register_next_step_handler(message, to_personal_account_en)

    except Exception as _ex:
        print("[INFO] Error wile with PostgreSQL", _ex)
    finally:
        if connection:
            connection.close()
            print("[INFO] PostgreSQL connection closed")


def to_personal_account_en(message, referrer_id=None, only_buttons=False, mess_for_only_buttons=None):

    bot.clear_step_handler(message)

    user_data = database_check_id(message, referrer_id)

    # Если человек есть в базе данных и у него подтверждён email
    if user_data:

        # Если он есть в списке админов
        if user_data[1] in admins:

            if only_buttons:
                mess = mess_for_only_buttons
            else:
                mess = f'Your balance: {user_data[7]}$' \
                       f'\nYour investment income: {user_data[8]}$' \
                       f'\nYour referral income: {user_data[9]}$' \
                       f'\nYour partner turnover: {user_data[11]}$' \
                       f'\nTotal partners: {user_data[10]}'

            markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
            button1 = types.KeyboardButton('Top up your balance')
            button2 = types.KeyboardButton('Invest')
            button3 = types.KeyboardButton('My Open Investments')
            button4 = types.KeyboardButton('Withdrawal of money')
            button5 = types.KeyboardButton('Referral program')
            button6 = types.KeyboardButton('About us')
            button7 = types.KeyboardButton('Admin panel')
            markup.add(button1, button2, button3, button4, button5, button6, button7)

            bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

            return bot.register_next_step_handler(message, menu_selection_en, user_data)

        # Если это не админ, но зарегистрированный в базе данных пользователь
        else:

            if only_buttons:
                mess = mess_for_only_buttons
            else:
                mess = f'Your balance: {user_data[7]}$' \
                       f'\nYour investment income: {user_data[8]}$' \
                       f'\nYour referral income: {user_data[9]}$' \
                       f'\nYour partner turnover: {user_data[11]}$' \
                       f'\nTotal partners: {user_data[10]}'

            markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
            button1 = types.KeyboardButton('Top up your balance')
            button2 = types.KeyboardButton('Invest')
            button3 = types.KeyboardButton('My Open Investments')
            button4 = types.KeyboardButton('Withdrawal of money')
            button5 = types.KeyboardButton('Referral program')
            button6 = types.KeyboardButton('About us')
            markup.add(button1, button2, button3, button4, button5, button6)

            bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

            return bot.register_next_step_handler(message, menu_selection_en, user_data)

    else:
        return for_unregistered_users_en(message)


def menu_selection_en(message, user_data):

    bot.clear_step_handler(message)

    if message.text == 'Login to your account' or message.text == 'To personal account'\
            or message.text == 'Back to personal account':
        return to_personal_account_en(message)

    # Возможные команды для зарегистрированных
    elif user_data:
        if message.text == 'Top up your balance':

            mess = 'Enter the amount you want to top up your wallet balance, in dollars:'
            markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
            button1 = types.KeyboardButton('Back')
            markup.add(button1)

            bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

            return bot.register_next_step_handler(message, balance_replenishment_step_1_en, user_data)

        elif message.text == 'Invest' or message.text == 'Back to tariff selection':

            mess = f'Choose an investment plan:' \
                   f'\n\nAviation, railway, automobile, maritime. The minimum investment amount is 50USDT. ' \
                   f'Minimum withdrawal amount 50USDT. Payments are made daily, from Monday to Friday. ' \
                   f'Profit is accrued the next day after the activation of the investment plan at 10.00 + GMT. ' \
                   f'Profit withdrawal is available once every three days. Return of the investment body' \
                   f' at the end of the term. Advanced return on investment -50%. For each 100 USDT invested,' \
                   f' you receive 10  tokens GFT for withdrawal to your wallet, they will be available' \
                   f' in advance of public ICO' \
                   f'\n\n<b>Aviation</b> min. amount is $50, max. amount is $1000; time of deposit is 28 days, daily' \
                   f' profit is 0.7%; refund of the deposit at the end of the accrual period from Monday to Friday.' \
                   f'\n\n<b>Railway</b> min. amount is 500$, max. amount is $ 5000; time of deposit is 42 days; daily' \
                   f' profit is 1%; refund of the deposit at the end of the accrual period from Monday to Friday.' \
                   f'\n\n<b>Automobile</b> min. amount is 3000$, max is 50000$; time of deposit is 70 days; daily' \
                   f' profit is 1.3%; refund of the deposit at the end of the accrual period from Monday to Friday.' \
                   f'\n\n<b>Maritime</b> min. amount is 5000$, max. amount is $100,000; time of deposit is 91 days; ' \
                   f'daily profit is 1.6%; refund of the deposit at the end of the accrual period from Monday to Friday.'

            markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
            button1 = types.KeyboardButton('Aviation')
            button2 = types.KeyboardButton('Railway')
            button3 = types.KeyboardButton('Automobile')
            button4 = types.KeyboardButton('Maritime')
            button5 = types.KeyboardButton('Back')
            markup.add(button1, button2, button3, button4, button5)

            bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

            return bot.register_next_step_handler(message, investment_en, user_data)

        elif message.text == 'My Open Investments':
            return investment_status_en(message, user_data)

        elif message.text == 'Withdrawal of money':
            return withdrawal_history_en(message, user_data)

        elif message.text == 'Referral program':
            return referral_program_en(message, user_data)

        elif message.text == 'About us':
            return about_us_en(message, user_data)

        # Отдельно админские команды
        elif user_data[1] in admins:

            if message.text == 'Admin panel' or message.text == 'Back to admin panel':

                return admin_panel_en(message, user_data)

            elif message.text == 'Applications for balance replenishment' or message.text == 'Back to replenishment' \
                                                                                             ' requests':

                return admin_replenishment_step_1_en(message, user_data)

            elif message.text == 'Applications for withdrawal of money' or message.text == 'Back to withdrawal' \
                                                                                           ' requests':

                return admin_withdrawal_step_1_en(message, user_data)

            elif message.text == 'Download the entire user base':
                mess = 'The file is being created, it may take a few minutes'
                bot.send_message(message.chat.id, mess, parse_mode='html')

                return send_all_database_en(message, user_data)

            else:
                return menu_selection_en(message, None)

        else:
            return menu_selection_en(message, None)

    # Команды для незарегистрированных
    else:

        if message.text == 'Register':
            return registration_en(message)
        elif message.text == 'About us':
            return about_us_en(message, user_data)

        elif message.text == '/help':
            mess = 'If you have any problems with the bot, then write to the admin @Helper13'
            markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
            button1 = types.KeyboardButton('Login to your account')
            button2 = types.KeyboardButton('Register')
            markup.add(button1, button2)

            bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

            return bot.register_next_step_handler(message, menu_selection_en, None)

        elif message.text == '/language' or message.text == 'Смена языка/Language change':
            return language_selection_step_1(message)

        elif '/start' in message.text:
            return start(message)

        else:
            mess = 'The command is not recognized, the session may have been updated or you did not use the buttons' \
                   ' below. Try it all over again' \
                   '\n\nIf you have problems with the bot, then use the /help command'
            bot.send_message(message.chat.id, mess, parse_mode='html')

            return to_personal_account_en(message)


def balance_replenishment_step_1_en(message, user_data):

    if message.text == 'Back':
        return to_personal_account_en(message)

    elif message.text.isdigit():
        replenishment_amount = int(message.text)

        if replenishment_amount < 50:
            markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
            button1 = types.KeyboardButton('Back')
            markup.add(button1)
            bot.send_message(message.chat.id, '<b>Minimum deposit amount 50$</b>',
                             parse_mode='html', reply_markup=markup)
            # Отправляем на повторение шага
            message.text = 'Top up your balance'
            return menu_selection_en(message, user_data)

    else:
        bot.send_message(message.chat.id, 'The number must be an integer,'
                                          ' without dots, commas or other'
                                          ' characters. Try Again', parse_mode='html')
        # Отправляем на повторение шага
        message.text = 'Top up your balance'
        return menu_selection_en(message, user_data)

    mess = f'To replenish your account in the bot, send <b>{replenishment_amount} USDT</b> to this wallet:' \
           f'\n\n<code>1q2w3e4r5t6y7u8i9o0</code>' \
           f'\n\nAfter transfer, copy/save your Transaction Hash (hash id) and click' \
           f' <b>Confirm Transaction</b> to confirm your transfer' \
           f'\n\nWithin 48 hours, your payment will be confirmed and the balance in the bot will be replenished.'
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
    button1 = types.KeyboardButton('Confirm transaction')
    button2 = types.KeyboardButton('Back')
    markup.add(button1, button2)

    bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

    return bot.register_next_step_handler(message, balance_replenishment_step_2_en, user_data, replenishment_amount)


def balance_replenishment_step_2_en(message, user_data, replenishment_amount):

    if message.text == 'Back':
        message.text = 'Top up your balance'
        return menu_selection_en(message, user_data)

    elif message.text == 'Confirm transaction':

        mess = '<b>Enter Transaction Hash (hash id) in chat to confirm your transfer:</b>'
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
        button1 = types.KeyboardButton('Back')
        markup.add(button1)

        bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

        return bot.register_next_step_handler(message, balance_replenishment_step_3_en, user_data, replenishment_amount)

    else:
        return balance_replenishment_step_3_en(message, user_data, replenishment_amount)


def balance_replenishment_step_3_en(message, user_data, replenishment_amount):

    if message.text == 'Back':
        message.text = replenishment_amount
        return balance_replenishment_step_1_en(message, user_data)

    connection = None
    try:
        connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)
        connection.autocommit = True

        current_date = int(datetime.datetime.now().timestamp())

        with connection.cursor() as cursor:
            cursor.execute(
                """INSERT INTO balance_replenishment (telegram_id, user_name, 
                replenishment_amount, transaction_hash, request_date, request_status)
                VALUES (%s, %s, %s, %s, %s, %s);""",
                (user_data[1], user_data[2], replenishment_amount, message.text, current_date, 'in processing')
            )

            print("[INFO] The balance replenishment was successfully inserted")

            mess = 'Your application has been successfully sent! ' \
                   '\n\nExpect confirmation of payment and crediting of funds within 48 hours'
            markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
            button1 = types.KeyboardButton('Back to personal account')
            markup.add(button1)

            bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

            tz = pytz.timezone('Europe/Moscow')
            current_date = datetime.datetime.now(tz)
            current_date_format = current_date.strftime("%H:%M, %d.%m.%Y")

            mess_for_admins_ru = f'Новая заявка на пополнение баланса:' \
                                 f'\n\nTransaction Hash: <code>{message.text}</code>' \
                                 f'\nСумма для пополнения: <b>{replenishment_amount}</b>$' \
                                 f'\nТелеграм id: <code>{user_data[1]}</code>' \
                                 f'\nТелеграм ник: @{user_data[2]}' \
                                 f'\nЕго текущий баланс: {user_data[7]}' \
                                 f'\nВремя и дата заявки: {current_date_format}'

            mess_for_admins_en = f'New application for balance replenishment:' \
                                 f'\n\nTransaction Hash: <code>{message.text}</code>' \
                                 f'\nAmount to top up: $<b>{replenishment_amount}</b>' \
                                 f'\nTelegram id: <code>{user_data[1]}</code>' \
                                 f'\nTelegram username: @{user_data[2]}' \
                                 f'\nHis current balance: {user_data[7]}' \
                                 f'\nTime and date of application: {current_date_format}'

            # Отправка оповещения всем админам
            for i in admins:
                # Определяем какой язык сейчас выбран у админа в боте
                cursor.execute('SELECT language FROM users WHERE telegram_id = %s', (i,))
                language = cursor.fetchone()[0]

                if language == 'en':
                    bot.send_message(i, mess_for_admins_en, parse_mode='html')
                else:
                    bot.send_message(i, mess_for_admins_ru, parse_mode='html')

            return bot.register_next_step_handler(message, menu_selection_en, user_data)

    except Exception as _ex:
        print("[INFO] Error wile with PostgreSQL", _ex)
    finally:
        if connection:
            connection.close()
            print("[INFO] PostgreSQL connection closed")


def investment_en(message, user_data):

    if message.text == 'Back':
        return to_personal_account_en(message)

    elif message.text == 'Aviation':

        mess = 'You have selected the Aviation Investment Plan. min. amount is $50, max. amount is $1000;' \
               ' time of deposit is 28 days, daily profit is 0.7%; refund of the deposit at the end of the accrual' \
               ' period from Monday to Friday.' \
               f'\n\n<b>Enter the investment amount</b>:'
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
        button1 = types.KeyboardButton('Back')
        markup.add(button1)

        bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)
        tariff = 1

    elif message.text == 'Railway':

        mess = 'You have selected the Railway Investment Plan. min. amount is 500$, max. amount is $ 5000;' \
               ' time of deposit is 42 days; daily profit is 1%; refund of the deposit at the end of the accrual' \
               ' period from Monday to Friday.' \
               '\n\n<b>Enter the investment amount</b>:'
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
        button1 = types.KeyboardButton('Back')
        markup.add(button1)

        bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)
        tariff = 2

    elif message.text == 'Automobile':

        mess = 'You have selected the Automobile Investment Plan. min. amount is 3000$, max is 50000$; time of' \
               ' deposit is 70 days; daily profit is 1.3%; refund of the deposit at the end of the accrual period' \
               ' from Monday to Friday.' \
               '\n\n<b>Enter the investment amount</b>:'
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
        button1 = types.KeyboardButton('Back')
        markup.add(button1)

        bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)
        tariff = 3

    elif message.text == 'Maritime':

        mess = 'You have chosen the Maritime Investment Plan. min. amount is 5000$, max. amount is $100,000; time' \
               ' of deposit is 91 days; daily profit is 1.6%; refund of the deposit at the end of the accrual period' \
               ' from Monday to Friday.' \
               '\n\n<b>Enter the investment amount</b>:'
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
        button1 = types.KeyboardButton('Back')
        markup.add(button1)

        bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)
        tariff = 4

    else:
        return menu_selection_en(message, None)

    return bot.register_next_step_handler(message, successful_investment_en, tariff, user_data)


def successful_investment_en(message, tariff, user_data):

    if message.text == 'Back':

        # Отправляем на шаг назад
        message.text = 'Invest'
        return menu_selection_en(message, user_data)

    elif message.text.isdigit():
        amount = int(message.text)

        # Проверяем есть ли введённая сумма на счету у клиента
        if user_data[7] >= amount:

            # Проверяем входит ли введённая сумма в рамки тарифа
            if tariff == 1 and (amount < 50 or amount > 1000):

                markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
                button1 = types.KeyboardButton('Back to tariff selection')
                markup.add(button1)

                bot.send_message(message.chat.id, 'In the Aviation plan, you can invest'
                                                  ' only from $50 to $1000', parse_mode='html', reply_markup=markup)

                return bot.register_next_step_handler(message, menu_selection_en, user_data)

            elif tariff == 2 and (amount < 500 or amount > 5000):

                markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
                button1 = types.KeyboardButton('Back to tariff selection')
                markup.add(button1)

                bot.send_message(message.chat.id, 'In the Railway plan, you can only'
                                                  ' invest from $500 to $5000', parse_mode='html', reply_markup=markup)

                return bot.register_next_step_handler(message, menu_selection_en, user_data)

            elif tariff == 3 and (amount < 3000 or amount > 50000):

                markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
                button1 = types.KeyboardButton('Back to tariff selection')
                markup.add(button1)

                bot.send_message(message.chat.id, 'In the Automobile plan, you can only invest'
                                                  ' from $3000 to $50000', parse_mode='html', reply_markup=markup)

                return bot.register_next_step_handler(message, menu_selection_en, user_data)

            elif tariff == 4 and (amount < 5000 or amount > 100000):

                markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
                button1 = types.KeyboardButton('Back to tariff selection')
                markup.add(button1)

                bot.send_message(message.chat.id, 'In the Maritime plan, you can only invest'
                                                  ' from $5000 to $100000', parse_mode='html', reply_markup=markup)

                return bot.register_next_step_handler(message, menu_selection_en, user_data)

            # Если сумма входит в рамки тарифа
            else:

                connection = None
                try:
                    connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)
                    connection.autocommit = True

                    with connection.cursor() as cursor:
                        cursor.execute(
                            """UPDATE users
                            SET balance = balance - %s
                            WHERE telegram_id = %s;""",
                            (amount, user_data[1])
                        )

                        print("[INFO] Balance has been successfully updated")

                        current_date = int(datetime.datetime.now().timestamp())

                        if tariff == 1:
                            # В первом тарифе длительность депозита 28 дней
                            expiration_of_investment = current_date + (604800 * 4)
                        elif tariff == 2:
                            # Во втором тарифе длительность депозита 42 дня
                            expiration_of_investment = current_date + (604800 * 6)
                        elif tariff == 3:
                            # В третьем тарифе длительность депозита 70 дней
                            expiration_of_investment = current_date + (604800 * 10)
                        elif tariff == 4:
                            # В четвертом тарифе длительность депозита 91 дней
                            expiration_of_investment = current_date + (604800 * 13)

                        cursor.execute(
                            """INSERT INTO investment (telegram_id, tariff, investment_amount, 
                            investment_start_date, investment_end_date, deposit_is_active)
                            VALUES (%s, %s, %s, %s, %s, %s);""",
                            (user_data[1], tariff, amount, current_date, expiration_of_investment, True)
                        )

                        print("[INFO] The investment was successfully inserted")

                        referral_matrix = [
                            [0.05, 0.06, 0.07, 0.08, 0.1, 0.12],
                            [0.03, 0.03, 0.04, 0.05, 0.06, 0.06],
                            [0.02, 0.02, 0.02, 0.02, 0.03, 0.04],
                            [0.01, 0.01, 0.01, 0.01, 0.01, 0.01],
                            [0.005, 0.005, 0.01, 0.01, 0.01, 0.01],
                            [0, 0.005, 0.005, 0.005, 0.005, 0.01],
                            [0, 0, 0.005, 0.005, 0.005, 0.005],
                            [0, 0, 0, 0.005, 0.005, 0.005],
                            [0, 0, 0, 0, 0.005, 0.005],
                            [0, 0, 0, 0, 0, 0.005]
                        ]

                        cursor.execute('SELECT invited_by FROM users WHERE telegram_id = %s', (user_data[1],))
                        # Запоминаем id вышестоящего человека
                        invited_id = cursor.fetchone()[0]

                        # Добавляем реферальные проценты всем до 10й линии, в зависимости от их оборота и линии,
                        #  относительно первого человека
                        for i in range(0, 10):

                            # Если вышестоящий человек есть
                            if invited_id:
                                # То определяем его оборот и смотрим кто его пригласил (для будущего цикла)
                                cursor.execute('SELECT turnover, invited_by FROM users WHERE telegram_id = %s',
                                               (invited_id,))

                                temp_data = cursor.fetchone()
                                turnover = temp_data[0]

                                if turnover < 50000:
                                    current_rank = 0
                                elif turnover < 100000:
                                    current_rank = 1
                                elif turnover < 300000:
                                    current_rank = 2
                                elif turnover < 500000:
                                    current_rank = 3
                                elif turnover < 1000000:
                                    current_rank = 4
                                else:
                                    current_rank = 5

                                referral_percentage = referral_matrix[i][current_rank]
                                ref_bonus = amount * referral_percentage

                                if ref_bonus != 0:
                                    cursor.execute(
                                        """UPDATE users 
                                        SET balance = balance + %s, ref_balance = ref_balance + %s 
                                        WHERE telegram_id = %s""",
                                        (ref_bonus, ref_bonus, invited_id)
                                    )

                                # Теперь увеличиваем оборот этого человека, в зависимости от линии
                                # Оборот увеличивается только для верхних 5 линий
                                if i < 5:
                                    if i == 0:
                                        line_coefficient = 1
                                    elif i == 1:
                                        line_coefficient = 0.8
                                    elif i == 2:
                                        line_coefficient = 0.6
                                    elif i == 3:
                                        line_coefficient = 0.4
                                    elif i == 4:
                                        line_coefficient = 0.2
                                    else:
                                        line_coefficient = 0

                                    turnover_increment = amount * line_coefficient

                                    cursor.execute(
                                        """UPDATE users 
                                        SET turnover = turnover + %s
                                        WHERE telegram_id = %s""",
                                        (turnover_increment, invited_id)
                                    )

                                    # Проверяем не перешёл ли этот человек на новый ранг и на какой
                                    turnover_new = turnover + turnover_increment

                                    if turnover_new < 50000:
                                        new_rank = 0
                                    elif turnover_new < 100000:
                                        new_rank = 1
                                    elif turnover_new < 300000:
                                        new_rank = 2
                                    elif turnover_new < 500000:
                                        new_rank = 3
                                    elif turnover_new < 1000000:
                                        new_rank = 4
                                    else:
                                        new_rank = 5

                                    rank_difference = new_rank - current_rank
                                    # Если было повышение хотя бы на один ранг
                                    if rank_difference > 0:

                                        # Определяем какой язык сейчас выбран у этого человека в боте
                                        cursor.execute('SELECT language FROM users WHERE telegram_id = %s',
                                                       (invited_id,))
                                        language = cursor.fetchone()[0]

                                        rank_up_bonus = 0
                                        for n in range(0, rank_difference):

                                            if current_rank == 0:

                                                if language == 'en':
                                                    mess = 'Your referral turnover has exceeded $50,000, so you have' \
                                                           ' moved to the 2nd rank of the referral program.' \
                                                           ' Congratulations!' \
                                                           '\n\nNow you get more percentage of your lines,' \
                                                           ' and the number' \
                                                           ' of referral levels has increased to 6 in depth'
                                                else:
                                                    mess = 'Ваш реферальный оборот превысил 50000$, поэтому' \
                                                           ' вы перешли на 2й ранг реферальной программы.' \
                                                           ' Поздравляем!' \
                                                           '\n\nТеперь вы получаете больше процентов от ваших линий,' \
                                                           ' а количество реферальных уровней повысилось до 6 в глубину'

                                                bot.send_message(invited_id, mess, parse_mode='html')

                                            elif current_rank == 1:

                                                if language == 'en':
                                                    mess = 'Your referral turnover has exceeded $100,000, so you have' \
                                                           ' moved to the 3rd rank of the referral program.' \
                                                           ' Congratulations!' \
                                                           '\n\nYou get a 1% bonus from $100,000 to your balance.' \
                                                           '\nYou also get more percentage of your lines, and the' \
                                                           ' number of referral levels has increased to 7 in depth.'
                                                else:
                                                    mess = 'Ваш реферальный оборот превысил 100000$, поэтому' \
                                                           ' вы перешли на 3й ранг реферальной программы.' \
                                                           ' Поздравляем!' \
                                                           '\n\nВы получаете бонус 1% от 100000$ на баланс.' \
                                                           '\nТак же вы получаете больше процентов от ваших линий,' \
                                                           ' а количество реферальных уровней повысилось до 7 в глубину'

                                                bot.send_message(invited_id, mess, parse_mode='html')

                                                rank_up_bonus = rank_up_bonus + 1000

                                            elif current_rank == 2:

                                                if language == 'en':
                                                    mess = 'Your referral turnover has exceeded $300,000, so you have' \
                                                           ' moved to the 4rd rank of the referral program.' \
                                                           ' Congratulations!' \
                                                           '\n\nYou get a 1% bonus from $300,000 to your balance.' \
                                                           '\nYou also get more percentage of your lines, and the' \
                                                           ' number of referral levels has increased to 8 in depth.'
                                                else:
                                                    mess = 'Ваш реферальный оборот превысил 300000$, поэтому' \
                                                           ' вы перешли на 4й ранг реферальной программы.' \
                                                           ' Поздравляем!' \
                                                           '\n\nВы получаете бонус 1% от 300000$ на баланс.' \
                                                           '\nТак же вы получаете больше процентов от ваших линий,' \
                                                           ' а количество реферальных уровней повысилось до 8 в глубину'

                                                bot.send_message(invited_id, mess, parse_mode='html')

                                                rank_up_bonus = rank_up_bonus + 3000

                                            elif current_rank == 3:

                                                if language == 'en':
                                                    mess = 'Your referral turnover has exceeded $500,000, so you have' \
                                                           ' moved to the 5rd rank of the referral program.' \
                                                           ' Congratulations!' \
                                                           '\n\nYou get a 1% bonus from $500,000 to your balance.' \
                                                           '\nYou also get more percentage of your lines, and the' \
                                                           ' number of referral levels has increased to 9 in depth.'
                                                else:
                                                    mess = 'Ваш реферальный оборот превысил 500000$, поэтому' \
                                                           ' вы перешли на 5й ранг реферальной программы.' \
                                                           ' Поздравляем!' \
                                                           '\n\nВы получаете бонус 1% от 500000$ на баланс.' \
                                                           '\nТак же вы получаете больше процентов от ваших линий,' \
                                                           ' а количество реферальных уровней повысилось до 9 в глубину'

                                                bot.send_message(invited_id, mess, parse_mode='html')

                                                rank_up_bonus = rank_up_bonus + 5000

                                            elif current_rank == 4:

                                                if language == 'en':
                                                    mess = 'Your referral turnover has exceeded $1,000,000, so you' \
                                                           ' have moved to the 6rd rank of the referral program.' \
                                                           ' Congratulations!' \
                                                           '\n\nYou get a 1% bonus from $1,000,000 to your balance.' \
                                                           '\nYou also get more percentage of your lines, and the' \
                                                           ' number of referral levels has increased to 10 in depth.'
                                                else:
                                                    mess = 'Ваш реферальный оборот превысил 1000000$, поэтому' \
                                                           ' вы перешли на максимальный 6й ранг реферальной' \
                                                           ' программы. Поздравляем!' \
                                                           '\n\nВы получаете бонус 1% от 1000000$ на баланс.' \
                                                           '\nТак же вы получаете больше процентов от ваших линий, ' \
                                                           'а количество реферальных уровней повысилось до 10 в глубину'

                                                bot.send_message(invited_id, mess, parse_mode='html')

                                                rank_up_bonus = rank_up_bonus + 10000

                                            current_rank = current_rank + 1

                                        # Если перешёл на новые ранги, то добавляем ему на баланс суммарно бонусы от
                                        # каждого ранга, которого он достиг
                                        if rank_up_bonus:
                                            cursor.execute(
                                                """UPDATE users 
                                                SET balance = balance + %s 
                                                WHERE telegram_id = %s""",
                                                (rank_up_bonus, invited_id)
                                            )

                                # Берём id следующего человека, который пригласил текущего
                                invited_id = temp_data[1]

                            else:
                                # Если поле пригласившего человека пустое, то выходим из цикла. Цепочка оборвалась
                                break

                        print("[INFO] Referral balance and turnover has been successfully updated")

                        mess = f'You have successfully invested <b>${amount}</b> on the {tariff} tariff plan! ' \
                               f'\nExpect your profit starting next day at 10 GMT' \
                               f'\n\nThe deposit will be active until ' \
                               f'{datetime.datetime.fromtimestamp(expiration_of_investment).strftime("%d.%m.%Y")}'

                        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
                        button1 = types.KeyboardButton('Back to personal account')
                        markup.add(button1)

                        bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

                        return bot.register_next_step_handler(message, to_personal_account_en)

                except Exception as _ex:
                    print("[INFO] Error wile with PostgreSQL", _ex)
                finally:
                    if connection:
                        connection.close()
                        print("[INFO] PostgreSQL connection closed")

        else:

            markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
            button1 = types.KeyboardButton('Top up your balance')
            button2 = types.KeyboardButton('Back to personal account')
            markup.add(button1, button2)

            bot.send_message(message.chat.id, 'You don\'t have enough money in your account',
                             parse_mode='html', reply_markup=markup)

            return bot.register_next_step_handler(message, menu_selection_en, user_data)

    else:
        bot.send_message(message.chat.id, 'The number must be an integer,'
                                          ' without dots, commas or other'
                                          ' characters. Try Again', parse_mode='html')

        # Определяем на каком шаге был неверный ввод
        if tariff == 1:
            message.text = 'Aviation'
        elif tariff == 2:
            message.text = 'Railway'
        elif tariff == 3:
            message.text = 'Automobile'
        elif tariff == 4:
            message.text = 'Maritime'
        # Отправляем на повторение шага
        return investment_en(message, user_data)


def investment_status_en(message, user_data):

    connection = None
    try:
        connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)
        connection.autocommit = True

        # Выбираем только активные депозиты по id человека
        with connection.cursor() as cursor:
            cursor.execute(
                """SELECT id, tariff, investment_amount, investment_start_date, investment_end_date 
                FROM investment 
                WHERE telegram_id = %s and deposit_is_active = True
                ORDER BY investment_start_date;""",
                (user_data[1],)
            )
            all_investment_data = cursor.fetchall()

    except Exception as _ex:
        print("[INFO] Error wile with PostgreSQL", _ex)
    finally:
        if connection:
            connection.close()
            print("[INFO] PostgreSQL connection closed")

    if all_investment_data:

        mess = 'List of active investments:\n\n1. '
        for i, investment_data in enumerate(all_investment_data):

            if i != 0:
                mess = mess + f'\n\n\n{i + 1}. '

            if investment_data[1] == 1:
                mess = mess + f'Tariff: Aviation' \
                              f'\nTariff rate: 0.7%/day' \
                              f'\nAmount of investment: {investment_data[2]}' \
                              f'\nDeposit start date: ' \
                              f'{datetime.datetime.fromtimestamp(investment_data[3]).strftime("%d.%m.%Y")}' \
                              f'\nExpiration date: ' \
                              f'{datetime.datetime.fromtimestamp(investment_data[4]).strftime("%d.%m.%Y")}'

            elif investment_data[1] == 2:
                mess = mess + f'Tariff: Railway' \
                              f'\nTariff rate: 1%/day' \
                              f'\nAmount of investment: {investment_data[2]}' \
                              f'\nDeposit start date: ' \
                              f'{datetime.datetime.fromtimestamp(investment_data[3]).strftime("%d.%m.%Y")}' \
                              f'\nExpiration date: ' \
                              f'{datetime.datetime.fromtimestamp(investment_data[4]).strftime("%d.%m.%Y")}'

            elif investment_data[1] == 3:
                mess = mess + f'Tariff: Automobile' \
                              f'\nTariff rate: 1.3%/day' \
                              f'\nAmount of investment: {investment_data[2]}' \
                              f'\nDeposit start date: ' \
                              f'{datetime.datetime.fromtimestamp(investment_data[3]).strftime("%d.%m.%Y")}' \
                              f'\nExpiration date: ' \
                              f'{datetime.datetime.fromtimestamp(investment_data[4]).strftime("%d.%m.%Y")}'

            elif investment_data[1] == 4:
                mess = mess + f'Tariff: Maritime' \
                              f'\nTariff rate: 1.6%/day' \
                              f'\nAmount of investment: {investment_data[2]}' \
                              f'\nDeposit start date: ' \
                              f'{datetime.datetime.fromtimestamp(investment_data[3]).strftime("%d.%m.%Y")}' \
                              f'\nExpiration date: ' \
                              f'{datetime.datetime.fromtimestamp(investment_data[4]).strftime("%d.%m.%Y")}'

        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
        button1 = types.KeyboardButton('Early return of selected investments')
        button2 = types.KeyboardButton('Back')
        markup.add(button1, button2)

        bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

        return bot.register_next_step_handler(message, early_refund_step_1_en, user_data, all_investment_data)

    else:

        mess = 'You have no active deposits'

        return to_personal_account_en(message, only_buttons=True, mess_for_only_buttons=mess)


def early_refund_step_1_en(message, user_data, all_investment_data):

    if message.text == 'Back':

        # Отправляем на шаг назад
        message.text = 'Back to personal account'
        return menu_selection_en(message, user_data)

    elif message.text == 'Early return of selected investments':

        mess = f'In case of early return of the investment, only 50% is returned to you. The rest 50% you lose' \
               f'\n\nYour investment list is numbered above.' \
               f'\n\n\n<b>Enter the number of the investment you want to return ahead of schedule:</b>'

        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
        button1 = types.KeyboardButton('Back to personal account')
        markup.add(button1)

        bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

        return bot.register_next_step_handler(message, early_refund_step_2_en, user_data, all_investment_data)

    else:
        return menu_selection_en(message, None)


def early_refund_step_2_en(message, user_data, all_investment_data):

    if message.text == 'Back to personal account':
        # Отправляем в личный кабинет
        return menu_selection_en(message, user_data)

    elif message.text.isdigit():
        number_of_the_investment = int(message.text)

    else:
        mess = 'The investment number must contain only numbers greater than zero, without additional characters.' \
               '\nCheck the data and try again.'
        bot.send_message(message.chat.id, mess, parse_mode='html')
        # Отправляем на повторение шага
        return early_refund_step_1_en(message, user_data, all_investment_data)

    len_investment_data = len(all_investment_data)

    if 0 < number_of_the_investment <= len_investment_data:

        investment_data = all_investment_data[number_of_the_investment - 1]

        mess = f'<b>Do you want to return this deposit early and get ${investment_data[2] / 2} back?</b>\n\n'

        if investment_data[1] == 1:
            mess = mess + f'Tariff: Aviation' \
                          f'\nTariff rate: 0.7%/day' \
                          f'\nAmount of investment: {investment_data[2]}' \
                          f'\nDeposit start date: ' \
                          f'{datetime.datetime.fromtimestamp(investment_data[3]).strftime("%d.%m.%Y")}' \
                          f'\nExpiration date: ' \
                          f'{datetime.datetime.fromtimestamp(investment_data[4]).strftime("%d.%m.%Y")}'

        elif investment_data[1] == 2:
            mess = mess + f'Tariff: Railway' \
                          f'\nTariff rate: 1%/day' \
                          f'\nAmount of investment: {investment_data[2]}' \
                          f'\nDeposit start date: ' \
                          f'{datetime.datetime.fromtimestamp(investment_data[3]).strftime("%d.%m.%Y")}' \
                          f'\nExpiration date: ' \
                          f'{datetime.datetime.fromtimestamp(investment_data[4]).strftime("%d.%m.%Y")}'

        elif investment_data[1] == 3:
            mess = mess + f'Tariff: Automobile' \
                          f'\nTariff rate: 1.3%/day' \
                          f'\nAmount of investment: {investment_data[2]}' \
                          f'\nDeposit start date: ' \
                          f'{datetime.datetime.fromtimestamp(investment_data[3]).strftime("%d.%m.%Y")}' \
                          f'\nExpiration date: ' \
                          f'{datetime.datetime.fromtimestamp(investment_data[4]).strftime("%d.%m.%Y")}'

        elif investment_data[1] == 4:
            mess = mess + f'Tariff: Maritime' \
                          f'\nTariff rate: 1.6%/day' \
                          f'\nAmount of investment: {investment_data[2]}' \
                          f'\nDeposit start date: ' \
                          f'{datetime.datetime.fromtimestamp(investment_data[3]).strftime("%d.%m.%Y")}' \
                          f'\nExpiration date: ' \
                          f'{datetime.datetime.fromtimestamp(investment_data[4]).strftime("%d.%m.%Y")}'

        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
        button1 = types.KeyboardButton('Yes')
        button2 = types.KeyboardButton('No')
        markup.add(button1, button2)

        bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

        return bot.register_next_step_handler(message, early_refund_step_3_en, user_data, all_investment_data,
                                              investment_data)

    else:

        if len_investment_data == 1:
            mess = f'You do not have a deposit under this number' \
                   f'\n\nYou only have 1 active contribution. Therefore, enter the number "1" in the chat'
        else:
            mess = f'You do not have a deposit under this number' \
                   f'\n\nYou have {len_investment_data} active deposits' \
                   f'\n\nEnter your deposit number from <b>1 to {len_investment_data}</b>'

        bot.send_message(message.chat.id, mess, parse_mode='html')

        message.text = 'Early return of selected investments'
        return early_refund_step_1_en(message, user_data, all_investment_data)


def early_refund_step_3_en(message, user_data, all_investment_data, investment_data):

    if message.text == 'No':

        # Отправляем на шаг назад
        message.text = 'Early return of selected investments'
        return early_refund_step_1_en(message, user_data, all_investment_data)

    elif message.text == 'Yes':

        connection = None
        try:
            connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)
            connection.autocommit = True

            current_date = int(datetime.datetime.now().timestamp())

            # Закрываем инвестицию и ставим в дату окончания текущую дату
            with connection.cursor() as cursor:
                cursor.execute(
                    """UPDATE investment
                    SET investment_end_date = %s, deposit_is_active = False
                    WHERE id = %s;""",
                    (current_date, investment_data[0])
                )

                # Добавляем к балансу человека половину от суммы инвестиций
                refund = investment_data[2] / 2
                cursor.execute('UPDATE users SET balance = balance + %s WHERE telegram_id = %s',
                               (refund, user_data[1]))

                print("[INFO] Investment and balance has been successfully updated")

        except Exception as _ex:
            print("[INFO] Error wile with PostgreSQL", _ex)
        finally:
            if connection:
                connection.close()
                print("[INFO] PostgreSQL connection closed")

        mess = f'Investment successfully cancelled. Half of the investment amount returned to your balance' \
               f'\n\nNow your balance is: {float(user_data[7]) + refund}$'

        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
        button1 = types.KeyboardButton('Back to personal account')
        markup.add(button1)

        bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

        return bot.register_next_step_handler(message, menu_selection_en, user_data)

    else:
        return menu_selection_en(message, None)


def withdrawal_history_en(message, user_data):

    connection = None
    try:
        connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)
        connection.autocommit = True

        with connection.cursor() as cursor:
            cursor.execute(
                """SELECT id, withdrawal_amount, request_date, request_status 
                FROM money_withdrawal 
                WHERE telegram_id = %s
                ORDER BY request_date;""",
                (user_data[1],)
            )
            all_withdrawal_data = cursor.fetchall()

            if all_withdrawal_data:

                mess = 'Your past withdrawals:\n\n1. '
                for i, withdrawal_data in enumerate(all_withdrawal_data):

                    if i != 0:
                        mess = mess + f'\n\n\n{i + 1}. '

                    if withdrawal_data[3] == 'successful':
                        mess = mess + f'Withdrawal amount: {withdrawal_data[1]}' \
                                      f'\nDate of application for withdrawal: ' \
                        f'{datetime.datetime.fromtimestamp(withdrawal_data[2]).strftime("%H:%M GMT, %d.%m.%Y")}' \
                                      f'\nStatus: Successful withdrawal'

                    elif withdrawal_data[3] == 'in processing':
                        mess = mess + f'Withdrawal amount: {withdrawal_data[1]}' \
                                      f'\nDate of application for withdrawal: ' \
                        f'{datetime.datetime.fromtimestamp(withdrawal_data[2]).strftime("%H:%M GMT, %d.%m.%Y")}' \
                                      f'\nStatus: In processing'

                    elif withdrawal_data[3] == 'unsuccessful':
                        mess = mess + f'Withdrawal amount: {withdrawal_data[1]}' \
                                      f'\nDate of application for withdrawal: ' \
                            f'{datetime.datetime.fromtimestamp(withdrawal_data[2]).strftime("%H:%M GMT, %d.%m.%Y")}' \
                                      f'\nStatus: Not approved'

                markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
                button1 = types.KeyboardButton('Apply for money withdrawal')
                button2 = types.KeyboardButton('Back')
                markup.add(button1, button2)

                bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

            else:

                mess = 'You didn\'t have any withdrawals'
                markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
                button1 = types.KeyboardButton('Apply for money withdrawal')
                button2 = types.KeyboardButton('Back')
                markup.add(button1, button2)

                bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

    except Exception as _ex:
        print("[INFO] Error wile with PostgreSQL", _ex)
    finally:
        if connection:
            connection.close()
            print("[INFO] PostgreSQL connection closed")

    return bot.register_next_step_handler(message, withdraw_money_step_1_en, user_data, all_withdrawal_data)


def withdraw_money_step_1_en(message, user_data, all_withdrawal_data):

    if message.text == 'Back':

        # Отправляем на шаг назад
        message.text = 'Back to personal account'
        return menu_selection_en(message, user_data)

    elif message.text == 'Apply for money withdrawal':

        # Если была хотя бы одна заявка на вывод
        if all_withdrawal_data:

            current_date = int(datetime.datetime.now().timestamp())

            # Если от даты последней заявки уже прошло 3 дня
            if all_withdrawal_data[-1][2] + 259200 < current_date:

                mess = f'Withdrawal occurs within 48 hours' \
                       f'\n\n<b>Enter the amount you want to withdraw:</b>'

                markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
                button1 = types.KeyboardButton('Back to personal account')
                markup.add(button1)

                bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

                return bot.register_next_step_handler(message, withdraw_money_step_2_en, user_data, all_withdrawal_data)

            else:

                mess = 'Withdrawal request can be sent no more than once every 3 days' \
                       '\n\n3 days have not passed since the date of the last application, please wait'

                markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
                button1 = types.KeyboardButton('Back to personal account')
                markup.add(button1)

                bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

                return bot.register_next_step_handler(message, menu_selection_en, user_data)

        # Если не было ни одной заявки на вывод
        else:

            # То берём все инвестиции этого человека
            connection = None
            try:
                connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)
                connection.autocommit = True

                with connection.cursor() as cursor:
                    cursor.execute(
                        """SELECT investment_start_date
                        FROM investment 
                        WHERE telegram_id = %s
                        ORDER BY investment_start_date;""",
                        (user_data[1],)
                    )
                    user_investment_data = cursor.fetchall()

            except Exception as _ex:
                print("[INFO] Error wile with PostgreSQL", _ex)
            finally:
                if connection:
                    connection.close()
                    print("[INFO] PostgreSQL connection closed")

            # Проверяем была ли хоть одна инвестиция у этого человека
            if user_investment_data:

                current_date = int(datetime.datetime.now().timestamp())

                # Проверяем прошло 3 дня с даты первой инвестиции
                if user_investment_data[0][0] + 259200 < current_date:

                    mess = f'Withdrawal occurs within 48 hours' \
                           f'\n\n<b>Enter the amount you want to withdraw:</b>'

                    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
                    button1 = types.KeyboardButton('Back to personal account')
                    markup.add(button1)
                    bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

                    return bot.register_next_step_handler(message, withdraw_money_step_2_en,
                                                          user_data, all_withdrawal_data)

                # Инвестиции есть, но не прошло 3 дня с первой инвестиции
                else:

                    mess = 'Money can be withdrawn only if 3 days have passed since the opening of' \
                           ' the first investment.' \
                           '\n\nYou haven\'t had 3 days yet, please wait'
                    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
                    button1 = types.KeyboardButton('Back to personal account')
                    markup.add(button1)
                    bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

                    return bot.register_next_step_handler(message, menu_selection_en, user_data)

            # Тут у человека нет ни выводов, ни инвестиций
            else:

                mess = 'Money can be withdrawn only if 3 days have passed since the opening of the first investment.' \
                       '\n\nYou need to open at least one investment to be able to withdraw money'
                markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
                button1 = types.KeyboardButton('Back to personal account')
                markup.add(button1)
                bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

                return bot.register_next_step_handler(message, menu_selection_en, user_data)

    else:
        return menu_selection_en(message, None)


def withdraw_money_step_2_en(message, user_data, all_withdrawal_data):

    if message.text == 'Back to personal account':
        return to_personal_account_en(message)

    try:
        withdrawal_amount = int(message.text)
        if not message.text.isdigit():
            raise ValueError

        if withdrawal_amount >= 50:

            if user_data[7] >= withdrawal_amount:

                connection = None
                try:
                    connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)
                    connection.autocommit = True

                    with connection.cursor() as cursor:
                        cursor.execute('SELECT wallet_number FROM users WHERE telegram_id = %s',
                                       (user_data[1],))
                        wallet_number = cursor.fetchone()[0]

                        # Уже есть кошелёк в базе данных
                        if wallet_number:

                            mess = f'If you want to receive a withdrawal to this old wallet:' \
                                   f'\n<code>{wallet_number}</code>' \
                                   f'\n, then click the button <b>Make a withdrawal to the old wallet</b>' \
                                   f'\n\nIf you want to update the wallet number,' \
                                   f' <b>then enter the new number in the chat</b> ' \
                                   f'(the wallet must be in USDT currency and in the TRC20 network): '

                            markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
                            button1 = types.KeyboardButton('Make a withdrawal to the old wallet')
                            button2 = types.KeyboardButton('Back to personal account')
                            markup.add(button1, button2)

                            bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

                            return bot.register_next_step_handler(message, withdraw_money_step_3_en, user_data,
                                                                  withdrawal_amount, wallet_number)

                        # Кошелька нет в базе данных
                        else:

                            mess = 'Enter the number of your crypto wallet where you want to receive the withdrawal. ' \
                                   'The wallet must be in USDT currency and on the TRC20 network:'

                            markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
                            button1 = types.KeyboardButton('Back to personal account')
                            markup.add(button1)

                            bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

                            return bot.register_next_step_handler(message, withdraw_money_step_3_en, user_data,
                                                                  withdrawal_amount)

                except Exception as _ex:
                    print("[INFO] Error wile with PostgreSQL", _ex)
                finally:
                    if connection:
                        connection.close()
                        print("[INFO] PostgreSQL connection closed")

            else:
                bot.send_message(message.chat.id, '<b>You don\'t have enough money on'
                                                  ' your balance.</b>', parse_mode='html')
                # Отправляем на повторение шага
                message.text = 'Apply for money withdrawal'
                return withdraw_money_step_1_en(message, user_data, all_withdrawal_data)

        # Если сумма меньше 50
        else:

            bot.send_message(message.chat.id, '<b>Minimum withdrawal amount 50$</b>', parse_mode='html')
            # Отправляем на повторение шага
            message.text = 'Apply for money withdrawal'
            return withdraw_money_step_1_en(message, user_data, all_withdrawal_data)

    except ValueError:
        bot.send_message(message.chat.id, 'The number must be an integer,'
                                          ' without dots, commas or other'
                                          ' characters. Try Again', parse_mode='html')
        # Отправляем на повтор шага
        message.text = 'Apply for money withdrawal'
        return withdraw_money_step_1_en(message, user_data, all_withdrawal_data)


def withdraw_money_step_3_en(message, user_data, withdrawal_amount, wallet_number=None):

    if message.text == 'Back to personal account':
        return to_personal_account_en(message)

    elif message.text == 'Make a withdrawal to the old wallet':
        # Значит у нас уже есть номер кошелька
        pass

    else:
        # Берём номер кошелька и обновляем его в таблице пользователей
        wallet_number = message.text

        connection = None
        try:
            connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)
            connection.autocommit = True

            with connection.cursor() as cursor:
                cursor.execute('UPDATE users SET wallet_number = %s WHERE telegram_id = %s',
                               (wallet_number, user_data[1]))

                print("[INFO] Wallet number has been successfully updated")

        except Exception as _ex:
            print("[INFO] Error wile with PostgreSQL", _ex)
        finally:
            if connection:
                connection.close()
                print("[INFO] PostgreSQL connection closed")

    connection = None
    try:
        connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)
        connection.autocommit = True

        current_date = int(datetime.datetime.now().timestamp())

        with connection.cursor() as cursor:
            cursor.execute(
                """UPDATE users
                SET balance = balance - %s
                WHERE telegram_id = %s;""",
                (withdrawal_amount, user_data[1])
            )

            print("[INFO] Balance has been successfully updated")

            cursor.execute(
                """INSERT INTO money_withdrawal (telegram_id, withdrawal_amount, request_date, request_status)
                VALUES (%s, %s, %s, %s);""",
                (user_data[1], withdrawal_amount, current_date, 'in processing')
            )

            print("[INFO] The money withdrawal was successfully inserted")

            mess = f'Your request to withdraw ${withdrawal_amount} to this wallet {wallet_number}' \
                   f' has been successfully sent! ' \
                   '\n\nExpect confirmation of payment and crediting of money to the entered wallet within 48 hours'
            markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
            button1 = types.KeyboardButton('Back to personal account')
            markup.add(button1)
            bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

            tz = pytz.timezone('Europe/Moscow')
            current_date_msk = datetime.datetime.fromtimestamp(current_date).astimezone(tz)

            mess_for_admins_ru = f'Новая заявка на вывод денег:' \
                                 f'\n\nСумма для вывода: <b>{withdrawal_amount}</b>$' \
                                 f'\nУказанный кошелёк для вывода: ' \
                                 f'\n<code>{wallet_number}</code>' \
                                 f'\nТелеграм id: <code>{user_data[1]}</code>' \
                                 f'\nТелеграм ник: @{user_data[2]}' \
                                 f'\nЕго баланс до вывода: {user_data[7]}' \
                                 f'\nЕго баланс после вывода: {user_data[7] - withdrawal_amount}' \
                                 f'\nВремя и дата заявки: {current_date_msk.strftime("%H:%M, %d.%m.%Y")}'

            mess_for_admins_en = f'New withdrawal request:' \
                                 f'\n\nAmount to withdraw: $<b>{withdrawal_amount}</b>' \
                                 f'\nSpecified wallet for withdrawal: ' \
                                 f'\n<code>{wallet_number}</code>' \
                                 f'\nTelegram id: <code>{user_data[1]}</code>' \
                                 f'\nTelegram username: @{user_data[2]}' \
                                 f'\nHis balance before withdrawal: {user_data[7]}' \
                                 f'\nHis balance after withdrawal: {user_data[7] - withdrawal_amount}' \
                                 f'\nTime and date of application: {current_date_msk.strftime("%H:%M, %d.%m.%Y")}'

            # Отправка оповещения всем админам
            for i in admins:
                # Определяем какой язык сейчас выбран у админа в боте
                cursor.execute('SELECT language FROM users WHERE telegram_id = %s', (i,))
                language = cursor.fetchone()[0]

                if language == 'en':
                    bot.send_message(i, mess_for_admins_en, parse_mode='html')
                else:
                    bot.send_message(i, mess_for_admins_ru, parse_mode='html')

            return bot.register_next_step_handler(message, menu_selection_en, user_data)

    except Exception as _ex:
        print("[INFO] Error wile with PostgreSQL", _ex)
    finally:
        if connection:
            connection.close()
            print("[INFO] PostgreSQL connection closed")


def referral_program_en(message, user_data):

    mess = f'Invite friends and earn interest on their investments!' \
           f'\nHere is your referral link:' \
           f'\n\nhttps://t.me/GlobalFinancialInvestorBot?start={user_data[1]}' \
           f'\n\nCopy it and send it to your friends, acquaintances'

    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
    button1 = types.KeyboardButton('Back')
    markup.add(button1)

    bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

    return bot.register_next_step_handler(message, to_personal_account_en)


def about_us_en(message, user_data):

    doc_en = open(r'about_company_EN.docx', 'rb')
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    button = types.KeyboardButton('Back')
    markup.add(button)

    bot.send_document(message.chat.id, doc_en, caption='Here everything is described in detail in English',
                      reply_markup=markup)

    return bot.register_next_step_handler(message, to_personal_account_en)


def admin_panel_en(message, user_data):

    admin_information = database_general_information()

    mess = f'Greetings, {message.from_user.first_name}' \
           f'\nTotal people entered the bot: {admin_information[0]}' \
           f'\nTotal registered people: {admin_information[1]}' \
           f'\nRegistered people through the referral program: {admin_information[2]}' \
           f'\nEveryone\'s total score: {admin_information[3]}' \
           f'\nTotal earnings through the referral program: {admin_information[4]}'

    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    button1 = types.KeyboardButton('Applications for balance replenishment')
    button2 = types.KeyboardButton('Applications for withdrawal of money')
    button3 = types.KeyboardButton('Download the entire user base')
    button4 = types.KeyboardButton('To personal account')
    markup.add(button1, button2, button3, button4)

    bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)
    return bot.register_next_step_handler(message, menu_selection_en, user_data)


def admin_replenishment_step_1_en(message, user_data, only_buttons=False):

    connection = None
    try:
        connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)
        connection.autocommit = True

        with connection.cursor() as cursor:
            cursor.execute(
                """SELECT id, telegram_id, user_name, replenishment_amount, transaction_hash, request_date 
                FROM balance_replenishment 
                WHERE request_status = 'in processing'
                ORDER BY request_date;"""
            )
            all_replenishment_data = cursor.fetchall()

    except Exception as _ex:
        print("[INFO] Error wile with PostgreSQL", _ex)
    finally:
        if connection:
            connection.close()
            print("[INFO] PostgreSQL connection closed")

    if all_replenishment_data:

        # Если нет режима "только клавиши"
        if not only_buttons:
            tz = pytz.timezone('Europe/Moscow')
            mess = 'Pending applications for replenishment:\n\n1. '

            for i, replenishment_data in enumerate(all_replenishment_data):

                request_date_msk = datetime.datetime.fromtimestamp(replenishment_data[5]).astimezone(tz)

                if i != 0:
                    mess = mess + f'\n\n\n{i + 1}. '

                mess = mess + f'Transaction Hash: <code>{replenishment_data[4]}</code>' \
                              f'\nAmount to top up: $<b>{replenishment_data[3]}</b>' \
                              f'\nTelegram id: <code>{replenishment_data[1]}</code>' \
                              f'\nTelegram username: @{replenishment_data[2]}' \
                              f'\nTime and date of application: {request_date_msk.strftime("%H:%M MSK, %d.%m.%Y")}' \
                              f'\nStatus: In processing'

            bot.send_message(message.chat.id, mess, parse_mode='html')

        mess = 'The list of applications is numbered in the message above.' \
               '\n<b>Enter the number of the request you want to process in the chat:</b>'
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
        button1 = types.KeyboardButton('Download the history of all replenishment requests')
        button2 = types.KeyboardButton('Back')
        markup.add(button1, button2)
        bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

        return bot.register_next_step_handler(message, admin_replenishment_step_2_en, user_data, all_replenishment_data)

    else:

        mess = ''
        # Если нет режима "только клавиши"
        if not only_buttons:
            mess = 'There are no pending requests for replenishment'

        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
        button1 = types.KeyboardButton('Download the history of all replenishment requests')
        button2 = types.KeyboardButton('Back')
        markup.add(button1, button2)
        bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

        return bot.register_next_step_handler(message, admin_replenishment_step_2_en, user_data, all_replenishment_data)


def admin_replenishment_step_2_en(message, user_data, all_replenishment_data):

    if message.text == 'Back':
        # Отправляем в на шаг назад
        return admin_panel_en(message, user_data)

    elif message.text == 'Download the history of all replenishment requests':

        connection = None
        try:
            connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)
            connection.autocommit = True

            with connection.cursor() as cursor:

                with open('balance_replenishment.csv', 'w') as f:

                    cursor.execute("""SELECT COLUMN_NAME 
                            FROM INFORMATION_SCHEMA.COLUMNS 
                            WHERE TABLE_NAME = N'balance_replenishment'""")
                    for i, x in enumerate(cursor):
                        if i != 0:
                            f.write(',')
                        f.write(str(x[0]))
                    f.write('\n')

                    cursor.execute('SELECT * FROM balance_replenishment ORDER BY id')
                    for i, row in enumerate(cursor):
                        if i != 0:
                            f.write('\n')
                        for n, x in enumerate(row):
                            if n != 0:
                                f.write(',')
                            f.write(str(x))

            balance_replenishment_data = open(r'balance_replenishment.csv', 'rb')
            bot.send_document(message.chat.id, balance_replenishment_data,
                              caption='These are all user requests for balance replenishment')

        except Exception as _ex:
            print("[INFO] Error wile with PostgreSQL", _ex)
        finally:
            if connection:
                connection.close()
                print("[INFO] PostgreSQL connection closed")

        return admin_replenishment_step_1_en(message, user_data, only_buttons=True)

    elif all_replenishment_data:

        if message.text.isdigit():
            request_number = int(message.text)

        else:
            mess = 'The application number must contain only numbers greater than zero,' \
                   ' without additional characters.' \
                   '\nCheck the data and try again.'
            bot.send_message(message.chat.id, mess, parse_mode='html')
            # Отправляем на повторение шага
            return admin_replenishment_step_1_en(message, user_data, only_buttons=True)

        len_replenishment_data = len(all_replenishment_data)

        if 0 < request_number <= len_replenishment_data:

            replenishment_data = all_replenishment_data[request_number - 1]

            tz = pytz.timezone('Europe/Moscow')
            request_date_msk = datetime.datetime.fromtimestamp(replenishment_data[5]).astimezone(tz)

            mess = 'Check the data and approve the application if the person transferred the required amount. ' \
                   'Or reject the application. ' \
                   '\nWhen the application is approved, the applicant\'s balance in the' \
                   ' bot will be replenished automatically\n\n'

            mess = mess + f'{request_number}. Transaction Hash: <code>{replenishment_data[4]}</code>' \
                          f'\nAmount to top up: $<b>{replenishment_data[3]}</b>' \
                          f'\nTelegram id: <code>{replenishment_data[1]}</code>' \
                          f'\nTelegram username: @{replenishment_data[2]}' \
                          f'\nTime and date of application: {request_date_msk.strftime("%H:%M MSK, %d.%m.%Y")}' \
                          f'\nStatus: In processing'

            markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
            button1 = types.KeyboardButton('Approve application')
            button2 = types.KeyboardButton('Reject application')
            button3 = types.KeyboardButton('Back')
            markup.add(button1, button2, button3)

            bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

            return bot.register_next_step_handler(message, admin_replenishment_step_3_en, user_data, replenishment_data)

        else:

            if len_replenishment_data == 1:
                mess = f'You do not have an application under this number' \
                       f'\n\nYou only have 1 pending application. Therefore, enter the number "1" in the chat'
            else:
                mess = f'You do not have an application under this number' \
                       f'\n\nYou have {len_replenishment_data} pending applications' \
                       f'\n\nEnter the application number from <b>1 to {len_replenishment_data}</b>'

            bot.send_message(message.chat.id, mess, parse_mode='html')

            return admin_replenishment_step_1_en(message, user_data, only_buttons=True)

    else:
        return menu_selection_en(message, None)


def admin_replenishment_step_3_en(message, user_data, replenishment_data):

    if message.text == 'Back':

        # Отправляем на шаг назад
        return admin_replenishment_step_1_en(message, user_data)

    elif message.text == 'Approve application':

        connection = None
        try:
            connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)
            connection.autocommit = True

            with connection.cursor() as cursor:
                cursor.execute(
                    """UPDATE balance_replenishment
                    SET request_status = 'successful'
                    WHERE id = %s;""",
                    (replenishment_data[0],)
                )

                cursor.execute('UPDATE users SET balance = balance + %s WHERE telegram_id = %s',
                               (replenishment_data[3], replenishment_data[1]))

                print("[INFO] Balance replenishment and balance has been successfully updated")

                # Определяем какой язык сейчас выбран у пользователя в боте
                cursor.execute('SELECT language FROM users WHERE telegram_id = %s',
                               (replenishment_data[1],))
                language = cursor.fetchone()[0]

                if language == 'en':
                    mess = f'Your replenishment request has been approved. ${replenishment_data[3]}' \
                           f' credited to your balance'
                else:
                    mess = f'Ваша заявка на пополнение одобрена. Вам на баланс начислено {replenishment_data[3]}$'

                bot.send_message(replenishment_data[1], mess, parse_mode='html')

        except Exception as _ex:
            print("[INFO] Error wile with PostgreSQL", _ex)
        finally:
            if connection:
                connection.close()
                print("[INFO] PostgreSQL connection closed")

        mess = f'Application approved and archived. Applicant\'s @{replenishment_data[2]}' \
               f' balance topped up by ${replenishment_data[3]}' \

        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
        button1 = types.KeyboardButton('Back to replenishment requests')
        button2 = types.KeyboardButton('Back to admin panel')
        markup.add(button1, button2)

        bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

        return bot.register_next_step_handler(message, menu_selection_en, user_data)

    elif message.text == 'Reject application':

        connection = None
        try:
            connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)
            connection.autocommit = True

            with connection.cursor() as cursor:
                cursor.execute(
                    """UPDATE balance_replenishment
                    SET request_status = 'unsuccessful'
                    WHERE id = %s;""",
                    (replenishment_data[0],)
                )

                print("[INFO] Balance replenishment has been successfully updated")

                # Определяем какой язык сейчас выбран у пользователя в боте
                cursor.execute('SELECT language FROM users WHERE telegram_id = %s',
                               (replenishment_data[1],))
                language = cursor.fetchone()[0]

                if language == 'en':
                    mess = f'Your replenishment request has been rejected. ' \
                           f'\n\nCheck the data and create a new request. ' \
                           f'Or write to technical support using the /help command to find out the' \
                           f' reasons for the rejection'
                else:
                    mess = f'Ваша заявка на пополнение отклонена. ' \
                           f'\n\nПроверьте данные и создайте новую заявку. ' \
                           f'Либо напишите в тех поддержку по команде /help, что бы узнать причины отклонения'

                bot.send_message(replenishment_data[1], mess, parse_mode='html')

        except Exception as _ex:
            print("[INFO] Error wile with PostgreSQL", _ex)
        finally:
            if connection:
                connection.close()
                print("[INFO] PostgreSQL connection closed")

        mess = f'Application rejected and archived.' \
               f'\nApplicant\'s balance has not changed'

        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
        button1 = types.KeyboardButton('Back to replenishment requests')
        button2 = types.KeyboardButton('Back to admin panel')
        markup.add(button1, button2)

        bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

        return bot.register_next_step_handler(message, menu_selection_en, user_data)

    else:
        return menu_selection_en(message, None)


def admin_withdrawal_step_1_en(message, user_data, only_buttons=False):

    connection = None
    try:
        connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)
        connection.autocommit = True

        with connection.cursor() as cursor:
            cursor.execute(
                """SELECT id, telegram_id, withdrawal_amount, request_date 
                FROM money_withdrawal 
                WHERE request_status = 'in processing'
                ORDER BY request_date;"""
            )
            all_withdrawal_data = cursor.fetchall()

            if all_withdrawal_data:

                # Если нет режима "только клавиши"
                if not only_buttons:
                    tz = pytz.timezone('Europe/Moscow')
                    mess = 'Unprocessed withdrawal requests:\n\n1. '

                    for i, withdrawal_data in enumerate(all_withdrawal_data):

                        cursor.execute(
                            """SELECT user_name, email, balance, wallet_number
                            FROM users 
                            WHERE telegram_id = %s;""",
                            (withdrawal_data[1],)
                        )
                        applicant_data = cursor.fetchone()

                        request_date_msk = datetime.datetime.fromtimestamp(withdrawal_data[3]).astimezone(tz)

                        if i != 0:
                            mess = mess + f'\n\n\n{i + 1}. '

                        # Если пользователя нет в бд
                        if not applicant_data:
                            mess = mess + f'THE USER WHO SUBMITTED THIS APPLICATION DOES NOT EXIST NO LONGER' \
                                          f'\nAmount to withdraw: $<b>{withdrawal_data[2]}</b>' \
                                          f'\nTelegram id: <code>{withdrawal_data[1]}</code>' \
                                          f'\nTime and date of application:' \
                                          f' {request_date_msk.strftime("%H:%M, %d.%m.%Y")}' \
                                          f'\nStatus: In processing'
                            continue

                        mess = mess + f'Amount to withdraw: $<b>{withdrawal_data[2]}</b>' \
                                      f'\nSpecified wallet for withdrawal: ' \
                                      f'\n<code>{applicant_data[3]}</code>' \
                                      f'\nTelegram id: <code>{withdrawal_data[1]}</code>' \
                                      f'\nTelegram username: @{applicant_data[0]}' \
                                      f'\nEmail: {applicant_data[1]}' \
                                      f'\nHis current balance (after withdrawal): ${applicant_data[2]}' \
                                      f'\nTime and date of application:' \
                                      f' {request_date_msk.strftime("%H:%M, %d.%m.%Y")}' \
                                      f'\nStatus: In processing'

                    bot.send_message(message.chat.id, mess, parse_mode='html')

                mess = 'The list of applications is numbered in the message above.' \
                       '\n<b>Enter the number of the request you want to process in the chat:</b>'
                markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
                button1 = types.KeyboardButton('Download the history of all withdrawal requests')
                button2 = types.KeyboardButton('Back')
                markup.add(button1, button2)
                bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

                return bot.register_next_step_handler(message, admin_withdrawal_step_2_en,
                                                      user_data, all_withdrawal_data)

            else:

                mess = ''
                # Если нет режима "только клавиши"
                if not only_buttons:
                    mess = 'No pending withdrawal requests'

                markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
                button1 = types.KeyboardButton('Download the history of all withdrawal requests')
                button2 = types.KeyboardButton('Back')
                markup.add(button1, button2)
                bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

                return bot.register_next_step_handler(message, admin_withdrawal_step_2_en,
                                                      user_data, all_withdrawal_data)

    except Exception as _ex:
        print("[INFO] Error wile with PostgreSQL", _ex)
    finally:
        if connection:
            connection.close()
            print("[INFO] PostgreSQL connection closed")


def admin_withdrawal_step_2_en(message, user_data, all_withdrawal_data):

    if message.text == 'Back':
        # Отправляем в на шаг назад
        return admin_panel_en(message, user_data)

    elif message.text == 'Download the history of all withdrawal requests':

        connection = None
        try:
            connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)
            connection.autocommit = True

            with connection.cursor() as cursor:

                with open('money_withdrawal.csv', 'w') as f:

                    cursor.execute("""SELECT COLUMN_NAME 
                            FROM INFORMATION_SCHEMA.COLUMNS 
                            WHERE TABLE_NAME = N'money_withdrawal'""")
                    for i, x in enumerate(cursor):
                        if i != 0:
                            f.write(',')
                        f.write(str(x[0]))
                    f.write('\n')

                    cursor.execute('SELECT * FROM money_withdrawal ORDER BY id')
                    for i, row in enumerate(cursor):
                        if i != 0:
                            f.write('\n')
                        for n, x in enumerate(row):
                            if n != 0:
                                f.write(',')
                            f.write(str(x))

            balance_withdrawal_data = open(r'money_withdrawal.csv', 'rb')
            bot.send_document(message.chat.id, balance_withdrawal_data,
                              caption='These are all user requests for withdrawal of money')

            return admin_withdrawal_step_1_en(message, user_data, only_buttons=True)

        except Exception as _ex:
            print("[INFO] Error wile with PostgreSQL", _ex)
        finally:
            if connection:
                connection.close()
                print("[INFO] PostgreSQL connection closed")

    elif all_withdrawal_data:

        if message.text.isdigit():
            request_number = int(message.text)

        else:
            mess = 'The application number must contain only numbers greater than zero and no additional characters' \
                   '\nCheck the data and try again.'
            bot.send_message(message.chat.id, mess, parse_mode='html')
            # Отправляем на повторение шага
            return admin_withdrawal_step_1_en(message, user_data, only_buttons=True)

        len_withdrawal_data = len(all_withdrawal_data)

        if 0 < request_number <= len_withdrawal_data:

            withdrawal_data = all_withdrawal_data[request_number - 1]

            tz = pytz.timezone('Europe/Moscow')
            request_date_msk = datetime.datetime.fromtimestamp(withdrawal_data[3]).astimezone(tz)

            connection = None
            try:
                connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)
                connection.autocommit = True

                with connection.cursor() as cursor:
                    cursor.execute(
                        """SELECT user_name, email, balance, wallet_number
                        FROM users 
                        WHERE telegram_id = %s;""",
                        (withdrawal_data[1],)
                    )
                    applicant_data = cursor.fetchone()

            except Exception as _ex:
                print("[INFO] Error wile with PostgreSQL", _ex)
            finally:
                if connection:
                    connection.close()
                    print("[INFO] PostgreSQL connection closed")

            mess = 'Check the data and approve the application if you have already transferred' \
                   ' the required amount to the client\'s wallet. ' \
                   'Or reject the application. ' \
                   '\nIf the application is rejected, the money will automatically return to the' \
                   ' applicant\'s balance in the bot\n\n'

            mess = mess + f'{request_number}. Amount to withdraw: $<b>{withdrawal_data[2]}</b>' \
                          f'\nSpecified wallet for withdrawal: ' \
                          f'\n<code>{applicant_data[3]}</code>' \
                          f'\nTelegram id: <code>{withdrawal_data[1]}</code>' \
                          f'\nTelegram username: @{applicant_data[0]}' \
                          f'\nEmail: {applicant_data[1]}' \
                          f'\nHis current balance (after withdrawal): ${applicant_data[2]}' \
                          f'\nTime and date of application:' \
                          f' {request_date_msk.strftime("%H:%M, %d.%m.%Y")}' \
                          f'\nStatus: In processing'

            markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
            button1 = types.KeyboardButton('Approve application')
            button2 = types.KeyboardButton('Reject application')
            button3 = types.KeyboardButton('Back')
            markup.add(button1, button2, button3)

            bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

            return bot.register_next_step_handler(message, admin_withdrawal_step_3_en, user_data, withdrawal_data,
                                                  applicant_data)

        else:

            if len_withdrawal_data == 1:
                mess = f'You do not have an application under this number' \
                       f'\n\nYou only have 1 pending application. Therefore, enter the number "1" in the chat'
            else:
                mess = f'You do not have an application under this number' \
                       f'\n\nYou have {len_withdrawal_data} pending applications' \
                       f'\n\nEnter the application number from <b>1 to {len_withdrawal_data}</b>'

            bot.send_message(message.chat.id, mess, parse_mode='html')

            return admin_withdrawal_step_1_en(message, user_data, only_buttons=True)

    else:
        return menu_selection_en(message, None)


def admin_withdrawal_step_3_en(message, user_data, withdrawal_data, applicant_data):

    if message.text == 'Back':

        # Отправляем на шаг назад
        return admin_withdrawal_step_1_en(message, user_data)

    elif message.text == 'Approve application':

        connection = None
        try:
            connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)
            connection.autocommit = True

            with connection.cursor() as cursor:
                cursor.execute(
                    """UPDATE money_withdrawal
                    SET request_status = 'successful'
                    WHERE id = %s;""",
                    (withdrawal_data[0],)
                )
                print("[INFO] Money withdrawal status has been successfully updated")

                # Определяем какой язык сейчас выбран у пользователя в боте
                cursor.execute('SELECT language FROM users WHERE telegram_id = %s',
                               (withdrawal_data[1],))
                language = cursor.fetchone()[0]

                if language == 'en':
                    mess = f'Your withdrawal request has been approved. ${withdrawal_data[2]} successfully' \
                           f' withdrawn from the bot to your wallet:' \
                           f'\n<code>{applicant_data[3]}</code>'
                else:
                    mess = f'Ваша заявка на вывод одобрена. {withdrawal_data[2]}$ успешно выведены' \
                           f' с бота на ваш кошелёк:' \
                           f'\n<code>{applicant_data[3]}</code>'

                bot.send_message(withdrawal_data[1], mess, parse_mode='html')

        except Exception as _ex:
            print("[INFO] Error wile with PostgreSQL", _ex)
        finally:
            if connection:
                connection.close()
                print("[INFO] PostgreSQL connection closed")

        mess = 'Application approved and archived.'

        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
        button1 = types.KeyboardButton('Back to withdrawal requests')
        button2 = types.KeyboardButton('Back to admin panel')
        markup.add(button1, button2)

        bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

        return bot.register_next_step_handler(message, menu_selection_en, user_data)

    elif message.text == 'Reject application':

        connection = None
        try:
            connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)
            connection.autocommit = True

            with connection.cursor() as cursor:
                cursor.execute(
                    """UPDATE money_withdrawal
                    SET request_status = 'unsuccessful'
                    WHERE id = %s;""",
                    (withdrawal_data[0],)
                )

                cursor.execute('UPDATE users SET balance = balance + %s WHERE telegram_id = %s',
                               (withdrawal_data[2], withdrawal_data[1]))

                print("[INFO] Money withdrawal status and balance has been successfully updated")

                # Определяем какой язык сейчас выбран у пользователя в боте
                cursor.execute('SELECT language FROM users WHERE telegram_id = %s',
                               (withdrawal_data[1],))
                language = cursor.fetchone()[0]

                if language == 'en':
                    mess = f'Your withdrawal request has been rejected. ${withdrawal_data[2]}' \
                           f' has been returned to your balance.' \
                           f'\n\nCheck the data and create a new request. Or write to technical' \
                           f' support using the /help command to find out the reasons for the rejection'
                else:
                    mess = f'Ваша заявка на вывод отклонена. Вам на баланс возвращено {withdrawal_data[2]}$.' \
                           f'\n\nПроверьте данные и создайте новую заявку. Либо напишите в тех поддержку' \
                           f' по команде /help, что бы узнать причины отклонения'

                bot.send_message(withdrawal_data[1], mess, parse_mode='html')

        except Exception as _ex:
            print("[INFO] Error wile with PostgreSQL", _ex)
        finally:
            if connection:
                connection.close()
                print("[INFO] PostgreSQL connection closed")

        mess = f'Application rejected and archived.' \
               f'\n${withdrawal_data[2]} returned to @{applicant_data[0]} applicant\'s balance.'

        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
        button1 = types.KeyboardButton('Back to withdrawal requests')
        button2 = types.KeyboardButton('Back to admin panel')
        markup.add(button1, button2)

        bot.send_message(message.chat.id, mess, parse_mode='html', reply_markup=markup)

        return bot.register_next_step_handler(message, menu_selection_en, user_data)

    else:
        return menu_selection_en(message, None)


def send_all_database_en(message, user_data):

    if message.from_user.id in admins:

        connection = None
        try:
            connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)

            connection.autocommit = True

            with connection.cursor() as cursor:

                with open('all_users_data.csv', 'w') as f:

                    cursor.execute("""SELECT COLUMN_NAME 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = N'users'""")
                    for i, x in enumerate(cursor):
                        if i != 0:
                            f.write(',')
                        f.write(str(x[0]))
                    f.write('\n')

                    cursor.execute('SELECT * FROM users ORDER BY id')
                    for i, row in enumerate(cursor):
                        if i != 0:
                            f.write('\n')
                        for n, x in enumerate(row):
                            if n != 0:
                                f.write(',')
                            f.write(str(x))

                with open('investment.csv', 'w') as f:

                    cursor.execute("""SELECT COLUMN_NAME 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = N'investment'""")
                    for i, x in enumerate(cursor):
                        if i != 0:
                            f.write(',')
                        f.write(str(x[0]))
                    f.write('\n')

                    cursor.execute('SELECT * FROM investment ORDER BY id')
                    for i, row in enumerate(cursor):
                        if i != 0:
                            f.write('\n')
                        for n, x in enumerate(row):
                            if n != 0:
                                f.write(',')
                            f.write(str(x))

            all_users_data = open(r'all_users_data.csv', 'rb')
            bot.send_document(message.chat.id, all_users_data, caption='This is all user data')
            investment_data = open(r'investment.csv', 'rb')
            markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
            button1 = types.KeyboardButton('Applications for balance replenishment')
            button2 = types.KeyboardButton('Applications for withdrawal of money')
            button3 = types.KeyboardButton('Download the entire user base')
            button4 = types.KeyboardButton('To personal account')
            markup.add(button1, button2, button3, button4)

            bot.send_document(message.chat.id, investment_data, caption='This is user investment data',
                              reply_markup=markup)

            return bot.register_next_step_handler(message, menu_selection_en, user_data)

        except Exception as _ex:
            print("[INFO] Error wile with PostgreSQL", _ex)
        finally:
            if connection:
                connection.close()
                print("[INFO] PostgreSQL connection closed")


@bot.message_handler(commands=['start'])
def start(message):

    referrer_id = None
    start_label = 'on start'

    # Если есть дополнительные параметры у команды start (возможно это реферальная ссылка с id реферера)
    if "/start " in message.text:
        # Берём эти доп параметры (возможный id)
        referrer_candidate = message.text.split()[1]

        # Является ли этот доп параметр положительным числом
        if referrer_candidate.isdigit():
            referrer_candidate = int(referrer_candidate)

            connection = None
            try:
                connection = psycopg2.connect(DATABASE_URL, sslmode=sslmode)
                connection.autocommit = True

                # Ищем зарегистрирован ли у нас человек с таким id и с не пустым емейлом
                with connection.cursor() as cursor:
                    cursor.execute(
                        """SELECT telegram_id FROM users WHERE telegram_id = %s and email IS NOT NULL;""",
                        (referrer_candidate,)
                    )
                    referrer_id = cursor.fetchone()

                    # Запоминаем id приглашающего, если он найден в базе с емейлом
                    if referrer_id:
                        referrer_id = referrer_id[0]

                    # Если никого не найдено с таким id и присутствующим емейлом
                    else:
                        start_label = 'invalid referral'

            except Exception as _ex:
                print("[INFO] Error wile with PostgreSQL", _ex)
            finally:
                if connection:
                    connection.close()
                    print("[INFO] PostgreSQL connection closed")

        # Если доп параметр не является положительным числом
        else:
            start_label = 'invalid referral'

    # Идём в обязательный шаг для выбора языка, запоминая стартовые параметры
    return language_selection_step_1(message, referrer_id, start_label)


@bot.message_handler(content_types=['text'])
def get_user_text(message):

    return language_selection_step_1(message)


if __name__ == '__main__':

    threading.Thread(target=database_daily_update).start()
    bot.infinity_polling(skip_pending=True)
