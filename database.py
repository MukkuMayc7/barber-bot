# database.py
import os
import logging
import psycopg
from psycopg.rows import dict_row
from datetime import datetime, timedelta
import config
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

class Database:
    def __init__(self):
        self.database_url = config.DATABASE_URL
        self.conn = self.get_connection()
        self.create_tables()
        self.create_admin_tables()
        self.setup_default_notifications()
        self.setup_default_schedule()

    def get_connection(self):
        """Создает соединение с PostgreSQL"""
        try:
            if self.database_url.startswith('postgres://'):
                self.database_url = self.database_url.replace('postgres://', 'postgresql://')
            
            conn = psycopg.connect(self.database_url)
            logger.info("Успешное подключение к PostgreSQL")
            return conn
        except Exception as e:
            logger.error(f"Ошибка подключения к PostgreSQL: {e}")
            raise

    def create_tables(self):
        """Создает все необходимые таблицы"""
        cursor = self.conn.cursor()
        
        # Таблица appointments
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS appointments (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                user_name TEXT,
                user_username TEXT,
                phone TEXT,
                service TEXT,
                appointment_date TEXT,
                appointment_time TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                reminder_sent BOOLEAN DEFAULT FALSE
            )
        ''')
        
        # Таблица schedule
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS schedule (
                id SERIAL PRIMARY KEY,
                date TEXT,
                time TEXT,
                available BOOLEAN DEFAULT TRUE
            )
        ''')
        
        # Уникальный индекс для schedule (решает ошибку ON CONFLICT)
        cursor.execute('''
            CREATE UNIQUE INDEX IF NOT EXISTS idx_schedule_date_time 
            ON schedule(date, time)
        ''')
        
        # Таблица admin_settings
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS admin_settings (
                admin_id BIGINT PRIMARY KEY,
                notification_chat_id BIGINT
            )
        ''')
        
        # Таблица work_schedule
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS work_schedule (
                id SERIAL PRIMARY KEY,
                weekday INTEGER UNIQUE,
                start_time TEXT,
                end_time TEXT,
                is_working BOOLEAN DEFAULT TRUE
            )
        ''')
        
        # Таблица bot_users
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS bot_users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        self.conn.commit()
        logger.info("Таблицы успешно созданы/проверены")

    def create_admin_tables(self):
        """Создает таблицу для администраторов"""
        cursor = self.conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS bot_admins (
                admin_id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                added_by BIGINT
            )
        ''')
        
        # Добавляем начальных администраторов из config
        for admin_id in config.ADMIN_IDS:
            cursor.execute('''
                INSERT INTO bot_admins (admin_id, username, first_name, last_name, added_by)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (admin_id) DO NOTHING
            ''', (admin_id, 'system', 'Система', 'Администратор', 0))
        
        self.conn.commit()
        logger.info("Таблица администраторов создана/проверена")

    def setup_default_notifications(self):
        """Настраивает уведомления по умолчанию для администраторов"""
        cursor = self.conn.cursor()
        for admin_id in config.ADMIN_IDS:
            cursor.execute('''
                INSERT INTO admin_settings (admin_id, notification_chat_id)
                VALUES (%s, %s)
                ON CONFLICT (admin_id) DO NOTHING
            ''', (admin_id, admin_id))
        self.conn.commit()
        logger.info("Настроены уведомления по умолчанию для администраторов")

    def setup_default_schedule(self):
        """Устанавливает график работы по умолчанию"""
        cursor = self.conn.cursor()
        
        cursor.execute('SELECT COUNT(*) FROM work_schedule')
        count = cursor.fetchone()[0]
        
        if count == 0:
            default_schedule = [
                (0, "10:00", "20:00", True),
                (1, "10:00", "20:00", True),
                (2, "10:00", "20:00", True),
                (3, "10:00", "20:00", True),
                (4, "10:00", "20:00", True),
                (5, "10:00", "20:00", False),
                (6, "10:00", "20:00", False)
            ]
            
            for weekday, start_time, end_time, is_working in default_schedule:
                cursor.execute('''
                    INSERT INTO work_schedule (weekday, start_time, end_time, is_working)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (weekday) DO UPDATE SET
                    start_time = EXCLUDED.start_time,
                    end_time = EXCLUDED.end_time,
                    is_working = EXCLUDED.is_working
                ''', (weekday, start_time, end_time, is_working))
            
            self.conn.commit()
            logger.info("Установлен график работы по умолчанию")
        else:
            logger.info(f"В таблице work_schedule уже есть {count} записей")

    def add_appointment(self, user_id, user_name, user_username, phone, service, date, time):
        """Добавляет новую запись"""
        cursor = self.conn.cursor()
        
        # Проверяем, не занято ли время
        cursor.execute('''
            SELECT COUNT(*) FROM appointments 
            WHERE appointment_date = %s AND appointment_time = %s
        ''', (date, time))
        
        if cursor.fetchone()[0] > 0:
            raise Exception("Это время уже занято другим клиентом")
        
        # Добавляем запись
        cursor.execute('''
            INSERT INTO appointments (user_id, user_name, user_username, phone, service, appointment_date, appointment_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        ''', (user_id, user_name, user_username, phone, service, date, time))
        
        appointment_id = cursor.fetchone()[0]
        
        # Обновляем расписание (ИСПРАВЛЕННЫЙ КОД)
        cursor.execute('''
            INSERT INTO schedule (date, time, available)
            VALUES (%s, %s, FALSE)
            ON CONFLICT (date, time) DO UPDATE SET 
            available = EXCLUDED.available
        ''', (date, time))
        
        self.conn.commit()
        return appointment_id

    def check_duplicate_appointments(self):
        """Проверяет дублирующиеся записи"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT appointment_date, appointment_time, COUNT(*) as count
            FROM appointments
            GROUP BY appointment_date, appointment_time
            HAVING COUNT(*) > 1
        ''')
        return cursor.fetchall()

    def get_appointments_by_datetime(self, date, time):
        """Получает записи по дате и времени"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT id, user_name, phone, service
            FROM appointments
            WHERE appointment_date = %s AND appointment_time = %s
            ORDER BY id
        ''', (date, time))
        return cursor.fetchall()

    def cancel_appointment(self, appointment_id, user_id=None):
        """Отменяет запись"""
        cursor = self.conn.cursor()
        
        # Получаем информацию о записи
        cursor.execute('''
            SELECT user_id, user_name, phone, service, appointment_date, appointment_time 
            FROM appointments WHERE id = %s
        ''', (appointment_id,))
        appointment = cursor.fetchone()
        
        if not appointment:
            return None
        
        # Удаляем запись
        if user_id:
            cursor.execute('''
                DELETE FROM appointments 
                WHERE id = %s AND user_id = %s
            ''', (appointment_id, user_id))
        else:
            cursor.execute('''
                DELETE FROM appointments WHERE id = %s
            ''', (appointment_id,))
        
        if cursor.rowcount > 0:
            user_id, user_name, phone, service, date, time = appointment
            # Освобождаем время в расписании
            cursor.execute('''
                DELETE FROM schedule WHERE date = %s AND time = %s
            ''', (date, time))
            
            self.conn.commit()
            return appointment
        return None

    def get_available_slots(self, date):
        """Получает доступные временные слоты"""
        cursor = self.conn.cursor()
        
        # Получаем занятые времена
        cursor.execute('''
            SELECT time FROM schedule 
            WHERE date = %s AND available = FALSE
        ''', (date,))
        booked_times = [row[0] for row in cursor.fetchall()]
        
        # Получаем график работы
        # ИСПРАВЛЕНО: правильное определение дня недели
        date_obj = datetime.strptime(date, "%Y-%m-%d").date()
        weekday = date_obj.weekday()
        cursor.execute('''
            SELECT start_time, end_time, is_working FROM work_schedule 
            WHERE weekday = %s
        ''', (weekday,))
        
        work_hours = cursor.fetchone()
        
        if not work_hours or not work_hours[2]:
            return []
        
        start_time, end_time = work_hours[0], work_hours[1]
        all_slots = self.generate_time_slots(start_time, end_time)
        
        return [slot for slot in all_slots if slot not in booked_times]

    def generate_time_slots(self, start_time, end_time):
        """Генерирует временные слоты"""
        slots = []
        current = datetime.strptime(start_time, "%H:%M")
        end = datetime.strptime(end_time, "%H:%M")
        
        while current < end:
            slots.append(current.strftime("%H:%M"))
            current += timedelta(minutes=30)
        
        return slots

    def set_work_schedule(self, weekday, start_time, end_time, is_working=True):
        """Устанавливает график работы"""
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO work_schedule (weekday, start_time, end_time, is_working)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (weekday) DO UPDATE SET
            start_time = EXCLUDED.start_time,
            end_time = EXCLUDED.end_time,
            is_working = EXCLUDED.is_working
        ''', (weekday, start_time, end_time, is_working))
        
        self.conn.commit()
        logger.info(f"Установлен график для дня {weekday}: {start_time}-{end_time}, рабочий: {is_working}")

    def get_work_schedule(self, weekday=None):
        """Получает график работы"""
        cursor = self.conn.cursor()
        
        if weekday is not None:
            cursor.execute('''
                SELECT id, weekday, start_time, end_time, is_working 
                FROM work_schedule WHERE weekday = %s
            ''', (weekday,))
        else:
            cursor.execute('''
                SELECT id, weekday, start_time, end_time, is_working 
                FROM work_schedule ORDER BY weekday
            ''')
        
        return cursor.fetchall()

    def get_week_schedule(self):
        """Получает график на неделю"""
        schedule = {}
        for weekday in range(7):
            result = self.get_work_schedule(weekday)
            if result and len(result) > 0:
                schedule[weekday] = result[0]
            else:
                schedule[weekday] = (0, weekday, "10:00", "20:00", True)
        return schedule

    def get_user_appointments(self, user_id):
        """Получает записи пользователя"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT id, service, appointment_date, appointment_time 
            FROM appointments 
            WHERE user_id = %s
            ORDER BY appointment_date, appointment_time
        ''', (user_id,))
        return cursor.fetchall()

    def get_all_appointments(self):
        """Получает все записи"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT id, user_name, user_username, phone, service, appointment_date, appointment_time 
            FROM appointments 
            ORDER BY appointment_date, appointment_time
        ''')
        return cursor.fetchall()

    def get_appointments_for_reminder(self):
        """Получает записи для напоминания"""
        cursor = self.conn.cursor()
        tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        
        cursor.execute('''
            SELECT id, user_id, user_name, phone, service, appointment_date, appointment_time 
            FROM appointments 
            WHERE appointment_date = %s AND reminder_sent = FALSE
        ''', (tomorrow,))
        
        return cursor.fetchall()

    def mark_reminder_sent(self, appointment_id):
        """Отмечает напоминание как отправленное"""
        cursor = self.conn.cursor()
        cursor.execute(''>
            UPDATE appointments 
            SET reminder_sent = TRUE 
            WHERE id = %s
        ''', (appointment_id,))
        self.conn.commit()

    def get_today_appointments(self):
        """Получает записи на сегодня"""
        cursor = self.conn.cursor()
        today = datetime.now().strftime("%Y-%m-%d")
        
        cursor.execute('''
            SELECT user_name, phone, service, appointment_time 
            FROM appointments 
            WHERE appointment_date = %s
            ORDER BY appointment_time
        ''', (today,))
        
        return cursor.fetchall()

    def set_notification_chat(self, admin_id, chat_id):
        """Устанавливает чат для уведомлений"""
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO admin_settings (admin_id, notification_chat_id)
            VALUES (%s, %s)
            ON CONFLICT (admin_id) DO UPDATE SET
            notification_chat_id = EXCLUDED.notification_chat_id
        ''', (admin_id, chat_id))
        self.conn.commit()

    def get_notification_chats(self):
        """Получает все чаты для уведомлений"""
        cursor = self.conn.cursor()
        cursor.execute('SELECT DISTINCT notification_chat_id FROM admin_settings')
        return [row[0] for row in cursor.fetchall() if row[0] is not None]

    def add_or_update_user(self, user_id, username, first_name, last_name):
        """Добавляет или обновляет пользователя"""
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO bot_users (user_id, username, first_name, last_name, last_seen)
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (user_id) DO UPDATE SET
            username = EXCLUDED.username,
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            last_seen = EXCLUDED.last_seen
        ''', (user_id, username, first_name, last_name))
        self.conn.commit()

    def get_total_users_count(self):
        """Получает общее количество пользователей"""
        cursor = self.conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM bot_users')
        return cursor.fetchone()[0]

    def get_active_users_count(self, days=30):
        """Получает количество активных пользователей"""
        cursor = self.conn.cursor()
        cutoff_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute('''
            SELECT COUNT(*) FROM bot_users 
            WHERE last_seen >= %s
        ''', (cutoff_date,))
        return cursor.fetchone()[0]

    def cleanup_completed_appointments(self):
        """Очищает прошедшие записи"""
        cursor = self.conn.cursor()
        now = datetime.now()
        current_date = now.strftime("%Y-%m-%d")
        current_time = now.strftime("%H:%M")
        
        # Удаляем записи за прошлые даты
        cursor.execute('''
            DELETE FROM appointments 
            WHERE appointment_date < %s
        ''', (current_date,))
        
        deleted_past_dates = cursor.rowcount
        
        # Удаляем прошедшие записи за сегодня
        cursor.execute('''
            DELETE FROM appointments 
            WHERE appointment_date = %s 
            AND appointment_time < %s
        ''', (current_date, current_time))
        
        deleted_today = cursor.rowcount
        
        # Очищаем расписание
        cursor.execute('''
            DELETE FROM schedule 
            WHERE date < %s
        ''', (current_date,))
        
        cursor.execute('''
            DELETE FROM schedule 
            WHERE date = %s AND time < %s
        ''', (current_date, current_time))
        
        self.conn.commit()
        
        total_deleted = deleted_past_dates + deleted_today
        
        if total_deleted > 0:
            logger.info(f"Автоочистка: удалено {total_deleted} прошедших записей")
        
        return {
            'deleted_past_dates': deleted_past_dates,
            'deleted_today': deleted_today,
            'total_deleted': total_deleted
        }

    # НОВЫЕ ФУНКЦИИ ДЛЯ ПРОВЕРКИ КОНФЛИКТОВ ПРИ ИЗМЕНЕНИИ ГРАФИКА

    def get_conflicting_appointments(self, weekday, new_start_time, new_end_time, new_is_working):
        """Находит конфликтующие записи при изменении графика"""
        cursor = self.conn.cursor()
        
        if not new_is_working:
            # Если день стал выходным - находим все будущие записи на этот день недели
            cursor.execute('''
                SELECT a.id, a.user_id, a.user_name, a.phone, a.service, a.appointment_date, a.appointment_time
                FROM appointments a
                WHERE EXTRACT(DOW FROM TO_DATE(a.appointment_date, 'YYYY-MM-DD')) = %s
                AND TO_DATE(a.appointment_date, 'YYYY-MM-DD') >= CURRENT_DATE
                ORDER BY a.appointment_date, a.appointment_time
            ''', (weekday,))
        else:
            # Если изменилось время - находим записи вне нового графика
            cursor.execute('''
                SELECT a.id, a.user_id, a.user_name, a.phone, a.service, a.appointment_date, a.appointment_time
                FROM appointments a
                WHERE EXTRACT(DOW FROM TO_DATE(a.appointment_date, 'YYYY-MM-DD')) = %s
                AND TO_DATE(a.appointment_date, 'YYYY-MM-DD') >= CURRENT_DATE
                AND (
                    a.appointment_time < %s OR a.appointment_time >= %s
                )
                ORDER BY a.appointment_date, a.appointment_time
            ''', (weekday, new_start_time, new_end_time))
        
        return cursor.fetchall()

    def cancel_appointments_by_ids(self, appointment_ids):
        """Массово отменяет записи по списку ID"""
        cursor = self.conn.cursor()
        canceled_appointments = []
        
        for appt_id in appointment_ids:
            cursor.execute('''
                SELECT user_id, user_name, phone, service, appointment_date, appointment_time 
                FROM appointments WHERE id = %s
            ''', (appt_id,))
            appointment = cursor.fetchone()
            
            if appointment:
                cursor.execute('DELETE FROM appointments WHERE id = %s', (appt_id,))
                cursor.execute('DELETE FROM schedule WHERE date = %s AND time = %s', 
                              (appointment[4], appointment[5]))
                canceled_appointments.append(appointment)
        
        self.conn.commit()
        return canceled_appointments

    # НОВЫЕ ФУНКЦИИ ДЛЯ УПРАВЛЕНИЯ АДМИНИСТРАТОРАМИ

    def add_admin(self, admin_id, username, first_name, last_name, added_by):
        """Добавляет администратора"""
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO bot_admins (admin_id, username, first_name, last_name, added_by)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (admin_id) DO UPDATE SET
            username = EXCLUDED.username,
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name
        ''', (admin_id, username, first_name, last_name, added_by))
        self.conn.commit()
        logger.info(f"Добавлен администратор {admin_id}")

    def remove_admin(self, admin_id):
        """Удаляет администратора"""
        cursor = self.conn.cursor()
        cursor.execute('DELETE FROM bot_admins WHERE admin_id = %s', (admin_id,))
        deleted = cursor.rowcount > 0
        self.conn.commit()
        
        if deleted:
            logger.info(f"Удален администратор {admin_id}")
        else:
            logger.info(f"Администратор {admin_id} не найден")
        
        return deleted

    def get_all_admins(self):
        """Получает список всех администраторов"""
        cursor = self.conn.cursor()
        cursor.execute(''>
            SELECT admin_id, username, first_name, last_name, added_at, added_by
            FROM bot_admins 
            ORDER BY added_at
        ''')
        return cursor.fetchall()

    def is_admin(self, user_id):
        """Проверяет, является ли пользователь администратором"""
        cursor = self.conn.cursor()
        cursor.execute('SELECT 1 FROM bot_admins WHERE admin_id = %s', (user_id,))
        return cursor.fetchone() is not None

    def get_admin_info(self, admin_id):
        """Получает информацию об администраторе"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT admin_id, username, first_name, last_name, added_at, added_by
            FROM bot_admins WHERE admin_id = %s
        ''', (admin_id,))
        return cursor.fetchone()

    def __del__(self):
        """Закрывает соединение при удалении объекта"""
        if hasattr(self, 'conn'):
            self.conn.close()