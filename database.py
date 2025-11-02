# database.py
import os
import logging
import psycopg
from psycopg.rows import dict_row
from datetime import datetime, timedelta
import config
from urllib.parse import urlparse
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

class Database:
    def __init__(self):
        self.database_url = config.DATABASE_URL
        self.reconnect()
    
    def reconnect(self):
        """Переподключается к базе данных"""
        if hasattr(self, 'conn') and self.conn:
            try:
                self.conn.close()
            except:
                pass
        self.conn = self.get_connection()
        self.create_tables()
        self.update_database_structure()
        self.create_admin_tables()
        self.setup_default_notifications()
        self.setup_default_schedule()

    def get_connection(self):
        """Создает соединение с PostgreSQL"""
        try:
            if self.database_url.startswith('postgres://'):
                self.database_url = self.database_url.replace('postgres://', 'postgresql://')
            
            conn = psycopg.connect(self.database_url)
            logger.info("✅ Успешное подключение к PostgreSQL")
            return conn
        except Exception as e:
            logger.error(f"❌ Ошибка подключения к PostgreSQL: {e}")
            raise

    def create_tables(self):
        """Создает все необходимые таблицы с новой структурой"""
        cursor = self.conn.cursor()
        
        # Таблица appointments - ОБНОВЛЕННАЯ ВЕРСИЯ
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
                reminder_24h_sent BOOLEAN DEFAULT FALSE,
                reminder_1h_sent BOOLEAN DEFAULT FALSE
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

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS scheduled_reminders (
                id SERIAL PRIMARY KEY,
                appointment_id INTEGER,
                reminder_type TEXT,
                scheduled_time TIMESTAMP,
                sent BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        self.conn.commit()
        logger.info("✅ Таблицы успешно созданы/проверены с новой структурой")

    def update_database_structure(self):
        """Обновляет структуру базы данных для новых функций"""
        cursor = self.conn.cursor()
        
        try:
            # Проверяем существование колонок по одной и добавляем если нужно
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'appointments' AND column_name = 'reminder_24h_sent'
            """)
            if not cursor.fetchone():
                cursor.execute('ALTER TABLE appointments ADD COLUMN reminder_24h_sent BOOLEAN DEFAULT FALSE')
                logger.info("✅ Добавлена колонка reminder_24h_sent")
            
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'appointments' AND column_name = 'reminder_1h_sent'
            """)
            if not cursor.fetchone():
                cursor.execute('ALTER TABLE appointments ADD COLUMN reminder_1h_sent BOOLEAN DEFAULT FALSE')
                logger.info("✅ Добавлена колонка reminder_1h_sent")
            
            # Проверяем есть ли старая колонка reminder_sent и переименовываем если нужно
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'appointments' AND column_name = 'reminder_sent'
            """)
            if cursor.fetchone():
                # Проверяем что reminder_24h_sent еще не существует чтобы избежать конфликта
                cursor.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'appointments' AND column_name = 'reminder_24h_sent'
                """)
                if not cursor.fetchone():
                    cursor.execute('ALTER TABLE appointments RENAME COLUMN reminder_sent TO reminder_24h_sent')
                    logger.info("✅ Переименована колонка reminder_sent в reminder_24h_sent")
                else:
                    # Если обе колонки существуют, удаляем старую
                    cursor.execute('ALTER TABLE appointments DROP COLUMN reminder_sent')
                    logger.info("✅ Удалена устаревшая колонка reminder_sent")
            
            self.conn.commit()
            logger.info("✅ Структура базы данных успешно обновлена для новых напоминаний")
            
        except Exception as e:
            logger.error(f"❌ Ошибка при обновлении структуры БД: {e}")
            self.conn.rollback()
            # Пытаемся добавить колонки по отдельности если основной запрос не сработал
            self.add_columns_individually()

    def add_columns_individually(self):
        """Добавляет недостающие колонки по отдельности (резервный метод)"""
        cursor = self.conn.cursor()
        
        try:
            # Пробуем добавить reminder_24h_sent
            try:
                cursor.execute('ALTER TABLE appointments ADD COLUMN reminder_24h_sent BOOLEAN DEFAULT FALSE')
                logger.info("✅ Колонка reminder_24h_sent добавлена (индивидуально)")
            except Exception as e:
                logger.info(f"ℹ️ Колонка reminder_24h_sent уже существует: {e}")
            
            # Пробуем добавить reminder_1h_sent
            try:
                cursor.execute('ALTER TABLE appointments ADD COLUMN reminder_1h_sent BOOLEAN DEFAULT FALSE')
                logger.info("✅ Колонка reminder_1h_sent добавлена (индивидуально)")
            except Exception as e:
                logger.info(f"ℹ️ Колонка reminder_1h_sent уже существует: {e}")
            
            # Пробуем удалить старую колонку если она мешает
            try:
                cursor.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'appointments' AND column_name = 'reminder_sent'
                """)
                if cursor.fetchone():
                    cursor.execute('ALTER TABLE appointments DROP COLUMN reminder_sent')
                    logger.info("✅ Устаревшая колонка reminder_sent удалена")
            except Exception as e:
                logger.info(f"ℹ️ Не удалось удалить reminder_sent: {e}")
            
            self.conn.commit()
            logger.info("✅ Резервное обновление структуры БД завершено")
            
        except Exception as e:
            logger.error(f"❌ Ошибка в резервном обновлении БД: {e}")
            self.conn.rollback()

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
        logger.info("✅ Таблица администраторов создана/проверена")

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
        logger.info("✅ Настроены уведомления по умолчанию для администраторов")

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
            logger.info("✅ Установлен график работы по умолчанию")
        else:
            logger.info(f"ℹ️ В таблице work_schedule уже есть {count} записей")

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
        
        # Обновляем расписание
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
        logger.info(f"✅ Установлен график для дня {weekday}: {start_time}-{end_time}, рабочий: {is_working}")

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
        """Получает только будущие записи пользователя"""
        cursor = self.conn.cursor()
        # Текущие дата и время (в московском времени)
        now = datetime.now()
        moscow_tz = timezone(timedelta(hours=3))
        moscow_time = now.astimezone(moscow_tz)
    
        current_date = moscow_time.strftime("%Y-%m-%d")
        current_time = moscow_time.strftime("%H:%M")
    
        cursor.execute('''
            SELECT id, service, appointment_date, appointment_time 
            FROM appointments 
            WHERE user_id = %s AND (
                appointment_date > %s OR 
                (appointment_date = %s AND appointment_time >= %s)
            )
            ORDER BY appointment_date, appointment_time
        ''', (user_id, current_date, current_date, current_time))
    
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

    def get_appointments_for_24h_reminder(self):
        """Получает записи для 24-часового напоминания (устаревший метод)"""
        # Этот метод больше не используется, логика перенесена в send_24h_reminders
        logger.warning("⚠️ Используется устаревший метод get_appointments_for_24h_reminder")
        return []

    def get_appointments_for_1h_reminder(self):
        """Получает записи для 1-часового напоминания (устаревший метод)"""
        # Этот метод больше не используется, логика перенесена в send_1h_reminders
        logger.warning("⚠️ Используется устаревший метод get_appointments_for_1h_reminder")
        return []

    def mark_24h_reminder_sent(self, appointment_id):
        """Отмечает 24-часовое напоминание как отправленное"""
        cursor = self.conn.cursor()
        cursor.execute('''
            UPDATE appointments 
            SET reminder_24h_sent = TRUE 
            WHERE id = %s
        ''', (appointment_id,))
        self.conn.commit()

    def mark_1h_reminder_sent(self, appointment_id):
        """Отмечает 1-часовое напоминание как отправленное"""
        cursor = self.conn.cursor()
        cursor.execute('''
            UPDATE appointments 
            SET reminder_1h_sent = TRUE 
            WHERE id = %s
        ''', (appointment_id,))
        self.conn.commit()

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
            logger.info(f"✅ Автоочистка: удалено {total_deleted} прошедших записей")
        
        return {
            'deleted_past_dates': deleted_past_dates,
            'deleted_today': deleted_today,
            'total_deleted': total_deleted
        }

    def cleanup_old_data(self):
        """Очистка только неактивных пользователей старше 40 дней"""
        cursor = self.conn.cursor()
    
        # ТОЛЬКО очистка неактивных пользователей старше 40 дней
        forty_days_ago = (datetime.now() - timedelta(days=40)).strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute('''
            DELETE FROM bot_users 
            WHERE last_seen < %s 
            AND user_id NOT IN (
                SELECT DISTINCT user_id FROM appointments 
                WHERE user_id IS NOT NULL
            )
        ''', (forty_days_ago,))
    
        deleted_users = cursor.rowcount  # ← ИСПРАВИЛ: было fetchone()[0]
    
        self.conn.commit()
    
        logger.info(f"✅ Очистка БД: удалено {deleted_users} неактивных пользователей (>40 дней)")
    
        return {
            'deleted_users': deleted_users
        }

    def get_weekly_stats(self):
        """Собирает статистику за прошедшую неделю (только завершенные записи)"""
        cursor = self.conn.cursor()
        
        # Определяем период: последние 7 дней (исключая сегодня)
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=7)
        
        # 1. Общее количество завершенных записей
        cursor.execute('''
            SELECT COUNT(*) 
            FROM appointments 
            WHERE appointment_date >= %s AND appointment_date < %s
        ''', (start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")))
        total_appointments = cursor.fetchone()[0]
        
        # 2. Пиковое время (самое популярное время записи)
        cursor.execute('''
            SELECT appointment_time, COUNT(*) as count
            FROM appointments 
            WHERE appointment_date >= %s AND appointment_date < %s
            GROUP BY appointment_time 
            ORDER BY count DESC 
            LIMIT 1
        ''', (start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")))
        peak_time_result = cursor.fetchone()
        peak_time = peak_time_result[0] if peak_time_result else "Нет данных"
        peak_time_count = peak_time_result[1] if peak_time_result else 0
        
        # 3. Новые клиенты (впервые записавшиеся за период)
        cursor.execute('''
            SELECT COUNT(DISTINCT user_id) 
            FROM appointments 
            WHERE appointment_date >= %s AND appointment_date < %s
            AND user_id IS NOT NULL 
            AND user_id NOT IN (
                SELECT DISTINCT user_id 
                FROM appointments 
                WHERE appointment_date < %s
            )
        ''', (start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"), start_date.strftime("%Y-%m-%d")))
        new_clients = cursor.fetchone()[0]
        
        # 4. Постоянные клиенты (уже записывавшиеся ранее)
        cursor.execute('''
            SELECT COUNT(DISTINCT user_id) 
            FROM appointments 
            WHERE appointment_date >= %s AND appointment_date < %s
            AND user_id IS NOT NULL 
            AND user_id IN (
                SELECT DISTINCT user_id 
                FROM appointments 
                WHERE appointment_date < %s
            )
        ''', (start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"), start_date.strftime("%Y-%m-%d")))
        regular_clients = cursor.fetchone()[0]
        
        return {
            'start_date': start_date.strftime("%d.%m.%Y"),
            'end_date': (end_date - timedelta(days=1)).strftime("%d.%m.%Y"),
            'total_appointments': total_appointments,
            'peak_time': peak_time,
            'peak_time_count': peak_time_count,
            'new_clients': new_clients,
            'regular_clients': regular_clients
        }

    def get_conflicting_appointments(self, weekday, new_start_time, new_end_time, new_is_working):
        """Находит конфликтующие записи при изменении графика"""
        cursor = self.conn.cursor()
        
        # Получаем все будущие записи
        cursor.execute('''
            SELECT id, user_id, user_name, phone, service, appointment_date, appointment_time
            FROM appointments 
            WHERE TO_DATE(appointment_date, 'YYYY-MM-DD') >= CURRENT_DATE
            ORDER BY appointment_date, appointment_time
        ''')
        
        all_future_appointments = cursor.fetchall()
        
        # Фильтруем в Python, чтобы избежать проблем с DOW в PostgreSQL
        conflicting_appointments = []
        
        for appointment in all_future_appointments:
            appt_id, user_id, user_name, phone, service, date, time = appointment
            
            # Определяем день недели записи в Python
            try:
                appointment_date = datetime.strptime(date, "%Y-%m-%d").date()
                appointment_weekday = appointment_date.weekday()
            except ValueError:
                logger.error(f"Неверный формат даты в записи {appt_id}: {date}")
                continue
            
            # Проверяем, относится ли запись к изменяемому дню недели
            if appointment_weekday == weekday:
                if not new_is_working:
                    # Если день становится выходным - все записи на этот день конфликтующие
                    conflicting_appointments.append(appointment)
                    logger.info(f"Найдена конфликтующая запись (выходной): {date} {time} - {user_name}")
                else:
                    # Если изменяется время - проверяем попадает ли время записи в новый график
                    try:
                        appointment_time = datetime.strptime(time, "%H:%M").time()
                        new_start = datetime.strptime(new_start_time, "%H:%M").time()
                        new_end = datetime.strptime(new_end_time, "%H:%M").time()
                        
                        # Запись конфликтует, если она вне нового графика
                        if appointment_time < new_start or appointment_time >= new_end:
                            conflicting_appointments.append(appointment)
                            logger.info(f"Найдена конфликтующая запись (время): {date} {time} - {user_name}")
                    except ValueError:
                        logger.error(f"Неверный формат времени в записи {appt_id}: {time}")
        
        logger.info(f"Всего найдено конфликтующих записей для дня {weekday}: {len(conflicting_appointments)}")
        return conflicting_appointments

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
                # 🔥 УДАЛЯЕМ напоминания (нужно передать context)
                # cancel_scheduled_reminders(context, appt_id)  # ← нужно добавить context
            
                cursor.execute('DELETE FROM appointments WHERE id = %s', (appt_id,))
                cursor.execute('DELETE FROM schedule WHERE date = %s AND time = %s', 
                          (appointment[4], appointment[5]))
                canceled_appointments.append(appointment)
                logger.info(f"Отменена запись #{appt_id} для {appointment[1]}")
    
        self.conn.commit()
        logger.info(f"Всего отменено записей: {len(canceled_appointments)}")
        return canceled_appointments

    def is_admin(self, user_id):
        """Проверяет, является ли пользователь администратором"""
        try:
            cursor = self.conn.cursor()
            cursor.execute('SELECT 1 FROM bot_admins WHERE admin_id = %s', (user_id,))
            result = cursor.fetchone() is not None
            # УБИРАЕМ логирование для обычных пользователей - только для отладки реальных админов
            if result:
                logger.info(f"🔐 Admin access granted for user_id: {user_id}")
            # Убираем warning для обычных пользователей - это нормально!
            return result
        except Exception as e:
            logger.error(f"❌ Ошибка при проверке прав администратора для {user_id}: {e}")
            return False

    def add_admin(self, admin_id, username, first_name, last_name, added_by):
        """Добавляет администратора"""
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                INSERT INTO bot_admins (admin_id, username, first_name, last_name, added_by)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (admin_id) DO NOTHING
            ''', (admin_id, username, first_name, last_name, added_by))
            self.conn.commit()
            
            added = cursor.rowcount > 0
            if added:
                logger.info(f"✅ Добавлен администратор {admin_id}")
            else:
                logger.info(f"⚠️ Администратор {admin_id} уже существует")
                
            return added
            
        except Exception as e:
            logger.error(f"❌ Ошибка при добавлении администратора {admin_id}: {e}")
            self.conn.rollback()
            return False

    def remove_admin(self, admin_id):
        """Удаляет администратора, если он не защищен"""
        try:
            # Проверяем, не защищен ли администратор
            if hasattr(config, 'PROTECTED_ADMINS') and admin_id in config.PROTECTED_ADMINS:
                logger.warning(f"🚫 Попытка удалить защищенного администратора {admin_id}")
                return False
                
            cursor = self.conn.cursor()
            cursor.execute('DELETE FROM bot_admins WHERE admin_id = %s', (admin_id,))
            self.conn.commit()
            
            deleted = cursor.rowcount > 0
            if deleted:
                logger.info(f"✅ Администратор {admin_id} удален из БД")
            else:
                logger.warning(f"⚠️ Администратор {admin_id} не найден в БД")
                
            return deleted
            
        except Exception as e:
            logger.error(f"❌ Ошибка при удалении администратора {admin_id}: {e}")
            self.conn.rollback()
            return False

    def get_all_admins(self):
        """Возвращает список всех администраторов"""
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                SELECT admin_id, username, first_name, last_name, added_at, added_by 
                FROM bot_admins 
                ORDER BY added_at DESC
            ''')
            admins = cursor.fetchall()
            logger.info(f"📊 Загружено {len(admins)} администраторов из БД")
            return admins
        except Exception as e:
            logger.error(f"❌ Ошибка при получении списка администраторов: {e}")
            return []

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