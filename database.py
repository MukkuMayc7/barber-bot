# database.py
import os
import logging
import sqlite3
import time
from datetime import datetime, timedelta, timezone
import config

logger = logging.getLogger(__name__)

def get_moscow_time():
    """Возвращает текущее московское время (UTC+3)"""
    return datetime.now(timezone(timedelta(hours=3)))

class Database:
    def __init__(self):
        self.database_url = config.DATABASE_URL
        self.max_retries = 3
        self.retry_delay = 0.1
        self.reconnect()
    
    def reconnect(self):
        """Переподключается к базе данных с повторными попытками"""
        for attempt in range(self.max_retries):
            try:
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
                logger.info("✅ Успешное подключение к SQLite")
                return
                
            except sqlite3.OperationalError as e:
                if "locked" in str(e) and attempt < self.max_retries - 1:
                    logger.warning(f"⚠️ База данных заблокирована, попытка {attempt + 1}/{self.max_retries}")
                    time.sleep(self.retry_delay)
                    continue
                raise
            except Exception as e:
                logger.error(f"❌ Ошибка подключения к SQLite: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                    continue
                raise

    def get_connection(self):
        """Создает соединение с SQLite с оптимизациями"""
        try:
            if self.database_url.startswith('sqlite:///'):
                db_path = self.database_url[10:]
            else:
                db_path = 'barbershop.db'
            
            # Оптимизации для SQLite
            conn = sqlite3.connect(db_path, check_same_thread=False, timeout=10.0)
            conn.row_factory = sqlite3.Row
            
            # Включаем WAL mode для лучшей производительности
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA synchronous=NORMAL')
            conn.execute('PRAGMA cache_size=-64000')  # 64MB кэш
            conn.execute('PRAGMA foreign_keys=ON')
            
            return conn
        except Exception as e:
            logger.error(f"❌ Ошибка подключения к SQLite: {e}")
            raise

    def execute_with_retry(self, query, params=()):
        """Выполняет запрос с повторными попытками при блокировке"""
        for attempt in range(self.max_retries):
            try:
                cursor = self.conn.cursor()
                cursor.execute(query, params)
                return cursor
            except sqlite3.OperationalError as e:
                if "locked" in str(e) and attempt < self.max_retries - 1:
                    logger.warning(f"⚠️ База заблокирована, повторная попытка {attempt + 1}")
                    time.sleep(self.retry_delay)
                    continue
                raise
            except Exception as e:
                logger.error(f"❌ Ошибка выполнения запроса: {e}")
                self.check_connection()  # Переподключаемся при других ошибках
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                    continue
                raise

    def create_tables(self):
        """Создает все необходимые таблицы с новой структурой"""
        cursor = self.conn.cursor()
        
        # Таблица appointments - ОБНОВЛЕННАЯ ВЕРСИЯ
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS appointments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
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
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT,
                time TEXT,
                available BOOLEAN DEFAULT TRUE,
                UNIQUE(date, time)
            )
        ''')

        # Таблица scheduled_reminders с исправленной структурой
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS scheduled_reminders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                appointment_id INTEGER,
                reminder_type TEXT,
                scheduled_time TIMESTAMP,
                sent BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(appointment_id, reminder_type)
            )
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
                id INTEGER PRIMARY KEY AUTOINCREMENT,
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
            CREATE TABLE IF NOT EXISTS bot_admins (
                admin_id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                added_by BIGINT
            )
        ''')
        
        self.conn.commit()
        logger.info("✅ Таблицы успешно созданы/проверены с новой структурой")

    def update_database_structure(self):
        """Обновляет структуру базы данных для новых функций"""
        cursor = self.conn.cursor()
        
        try:
            # Проверяем существование колонок и добавляем если нужно
            cursor.execute("PRAGMA table_info(appointments)")
            columns = [column[1] for column in cursor.fetchall()]
            
            if 'reminder_24h_sent' not in columns:
                cursor.execute('ALTER TABLE appointments ADD COLUMN reminder_24h_sent BOOLEAN DEFAULT FALSE')
                logger.info("✅ Добавлена колонка reminder_24h_sent")
            
            if 'reminder_1h_sent' not in columns:
                cursor.execute('ALTER TABLE appointments ADD COLUMN reminder_1h_sent BOOLEAN DEFAULT FALSE')
                logger.info("✅ Добавлена колонка reminder_1h_sent")
            
            # Удаляем старую колонку если она существует
            if 'reminder_sent' in columns:
                # В SQLite нельзя удалять колонки напрямую, создаем новую таблицу
                self._recreate_appointments_table()
                
            self.conn.commit()
            logger.info("✅ Структура базы данных успешно обновлена для новых напоминаний")
            
        except Exception as e:
            logger.error(f"❌ Ошибка при обновлении структуры БД: {e}")
            self.conn.rollback()

    def _recreate_appointments_table(self):
        """Пересоздает таблицу appointments для удаления устаревших колонок"""
        try:
            cursor = self.conn.cursor()
            
            # Создаем временную таблицу с новой структурой
            cursor.execute('''
                CREATE TABLE appointments_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
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
            
            # Копируем данные из старой таблицы
            cursor.execute('''
                INSERT INTO appointments_new 
                (id, user_id, user_name, user_username, phone, service, appointment_date, appointment_time, created_at, reminder_24h_sent)
                SELECT id, user_id, user_name, user_username, phone, service, appointment_date, appointment_time, created_at, reminder_sent
                FROM appointments
            ''')
            
            # Удаляем старую таблицу и переименовываем новую
            cursor.execute('DROP TABLE appointments')
            cursor.execute('ALTER TABLE appointments_new RENAME TO appointments')
            
            logger.info("✅ Таблица appointments пересоздана с новой структурой")
            
        except Exception as e:
            logger.error(f"❌ Ошибка при пересоздании таблицы appointments: {e}")
            raise

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
                INSERT OR IGNORE INTO bot_admins (admin_id, username, first_name, last_name, added_by)
                VALUES (?, ?, ?, ?, ?)
            ''', (admin_id, 'system', 'Система', 'Администратор', 0))
        
        self.conn.commit()
        logger.info("✅ Таблица администраторов создана/проверена")

    def setup_default_notifications(self):
        """Настраивает уведомления по умолчанию для администраторов"""
        cursor = self.conn.cursor()
        for admin_id in config.ADMIN_IDS:
            cursor.execute('''
                INSERT OR IGNORE INTO admin_settings (admin_id, notification_chat_id)
                VALUES (?, ?)
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
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(weekday) DO UPDATE SET
                    start_time = excluded.start_time,
                    end_time = excluded.end_time,
                    is_working = excluded.is_working
                ''', (weekday, start_time, end_time, is_working))
            
            self.conn.commit()
            logger.info("✅ Установлен график работы по умолчанию")
        else:
            logger.info(f"ℹ️ В таблице work_schedule уже есть {count} записей")

    def check_connection(self):
        """Проверяет соединение с БД и переподключается при необходимости"""
        try:
            cursor = self.execute_with_retry('SELECT 1')
            return True
        except Exception as e:
            logger.warning(f"⚠️ Соединение с БД разорвано, переподключаемся: {e}")
            self.reconnect()
            return False

    def add_appointment(self, user_id, user_name, user_username, phone, service, date, time):
        """Добавляет новую запись с переподключением"""
        try:
            self.check_connection()
            
            # Проверяем, не занято ли время
            cursor = self.execute_with_retry('''
                SELECT COUNT(*) FROM appointments 
                WHERE appointment_date = ? AND appointment_time = ?
            ''', (date, time))
            
            if cursor.fetchone()[0] > 0:
                raise Exception("Это время уже занято другим клиентом")
            
            # Добавляем запись
            cursor = self.execute_with_retry('''
                INSERT INTO appointments (user_id, user_name, user_username, phone, service, appointment_date, appointment_time)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (user_id, user_name, user_username, phone, service, date, time))
            
            appointment_id = cursor.lastrowid
            
            # Обновляем расписание
            self.execute_with_retry('''
                INSERT INTO schedule (date, time, available)
                VALUES (?, ?, ?)
                ON CONFLICT(date, time) DO UPDATE SET 
                available = excluded.available
            ''', (date, time, False))
            
            self.conn.commit()
            return appointment_id
            
        except Exception as e:
            logger.error(f"❌ Ошибка БД в add_appointment: {e}")
            self.reconnect()
            raise

    def add_or_update_user(self, user_id, username, first_name, last_name):
        """Добавляет или обновляет пользователя с переподключением"""
        try:
            self.check_connection()
            
            self.execute_with_retry('''
                INSERT INTO bot_users (user_id, username, first_name, last_name, last_seen)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(user_id) DO UPDATE SET
                username = excluded.username,
                first_name = excluded.first_name,
                last_name = excluded.last_name,
                last_seen = excluded.last_seen
            ''', (user_id, username, first_name, last_name))
            
            self.conn.commit()
        except Exception as e:
            logger.error(f"❌ Ошибка БД в add_or_update_user: {e}")
            self.reconnect()
            # Повторяем после переподключения
            try:
                self.execute_with_retry('''
                    INSERT INTO bot_users (user_id, username, first_name, last_name, last_seen)
                    VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(user_id) DO UPDATE SET
                    username = excluded.username,
                    first_name = excluded.first_name,
                    last_name = excluded.last_name,
                    last_seen = excluded.last_seen
                ''', (user_id, username, first_name, last_name))
                self.conn.commit()
            except Exception as e2:
                logger.error(f"❌ Критическая ошибка после переподключения: {e2}")

    def is_admin(self, user_id):
        """Проверяет, является ли пользователь администратором"""
        try:
            self.check_connection()
            
            cursor = self.execute_with_retry('SELECT 1 FROM bot_admins WHERE admin_id = ?', (user_id,))
            result = cursor.fetchone() is not None
            if result:
                logger.info(f"🔐 Admin access granted for user_id: {user_id}")
            return result
        except Exception as e:
            logger.error(f"❌ Ошибка при проверке прав администратора для {user_id}: {e}")
            return False

    def get_available_slots(self, date):
        """Получает доступные временные слоты"""
        cursor = self.execute_with_retry('''
            SELECT time FROM schedule 
            WHERE date = ? AND available = FALSE
        ''', (date,))
        booked_times = [row[0] for row in cursor.fetchall()]
        
        # Получаем график работы
        date_obj = datetime.strptime(date, "%Y-%m-%d").date()
        weekday = date_obj.weekday()
        cursor = self.execute_with_retry('''
            SELECT start_time, end_time, is_working FROM work_schedule 
            WHERE weekday = ?
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
        cursor = self.execute_with_retry('''
            INSERT INTO work_schedule (weekday, start_time, end_time, is_working)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(weekday) DO UPDATE SET
            start_time = excluded.start_time,
            end_time = excluded.end_time,
            is_working = excluded.is_working
        ''', (weekday, start_time, end_time, is_working))
        
        self.conn.commit()
        logger.info(f"✅ Установлен график для дня {weekday}: {start_time}-{end_time}, рабочий: {is_working}")

    def get_work_schedule(self, weekday=None):
        """Получает график работы"""
        if weekday is not None:
            cursor = self.execute_with_retry('''
                SELECT id, weekday, start_time, end_time, is_working 
                FROM work_schedule WHERE weekday = ?
            ''', (weekday,))
        else:
            cursor = self.execute_with_retry('''
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
        moscow_time = get_moscow_time()
        current_date = moscow_time.strftime("%Y-%m-%d")
        current_time = moscow_time.strftime("%H:%M")
    
        cursor = self.execute_with_retry('''
            SELECT id, service, appointment_date, appointment_time 
            FROM appointments 
            WHERE user_id = ? AND (
                appointment_date > ? OR 
                (appointment_date = ? AND appointment_time >= ?)
            )
            ORDER BY appointment_date, appointment_time
        ''', (user_id, current_date, current_date, current_time))
    
        return cursor.fetchall()

    def get_all_appointments(self):
        """Получает только БУДУЩИЕ записи"""
        moscow_time = get_moscow_time()
        current_date = moscow_time.strftime("%Y-%m-%d")
        current_time = moscow_time.strftime("%H:%M")
    
        cursor = self.execute_with_retry('''
            SELECT id, user_name, user_username, phone, service, appointment_date, appointment_time 
            FROM appointments 
            WHERE appointment_date > ? OR 
                  (appointment_date = ? AND appointment_time >= ?)
            ORDER BY appointment_date, appointment_time
        ''', (current_date, current_date, current_time))
    
        return cursor.fetchall()

    def get_today_appointments(self):
        """Получает записи на сегодня"""
        moscow_time = get_moscow_time()
        today = moscow_time.strftime("%Y-%m-%d")
        
        cursor = self.execute_with_retry('''
            SELECT user_name, phone, service, appointment_time 
            FROM appointments 
            WHERE appointment_date = ?
            ORDER BY appointment_time
        ''', (today,))
        
        return cursor.fetchall()

    def cancel_appointment(self, appointment_id, user_id=None):
        """Отменяет запись"""
        # Получаем информацию о записи
        cursor = self.execute_with_retry('''
            SELECT user_id, user_name, phone, service, appointment_date, appointment_time 
            FROM appointments WHERE id = ?
        ''', (appointment_id,))
        appointment = cursor.fetchone()
        
        if not appointment:
            return None
        
        # Удаляем запись
        if user_id:
            cursor = self.execute_with_retry('''
                DELETE FROM appointments 
                WHERE id = ? AND user_id = ?
            ''', (appointment_id, user_id))
        else:
            cursor = self.execute_with_retry('''
                DELETE FROM appointments WHERE id = ?
            ''', (appointment_id,))
        
        if cursor.rowcount > 0:
            user_id, user_name, phone, service, date, time = appointment
            # Освобождаем время в расписании
            self.execute_with_retry('''
                DELETE FROM schedule WHERE date = ? AND time = ?
            ''', (date, time))
            
            self.conn.commit()
            return appointment
        return None

    def mark_24h_reminder_sent(self, appointment_id):
        """Отмечает 24-часовое напоминание как отправленное"""
        cursor = self.execute_with_retry('''
            UPDATE appointments 
            SET reminder_24h_sent = TRUE 
            WHERE id = ?
        ''', (appointment_id,))
        self.conn.commit()

    def mark_1h_reminder_sent(self, appointment_id):
        """Отмечает 1-часовое напоминание как отправленное"""
        cursor = self.execute_with_retry('''
            UPDATE appointments 
            SET reminder_1h_sent = TRUE 
            WHERE id = ?
        ''', (appointment_id,))
        self.conn.commit()

    def set_notification_chat(self, admin_id, chat_id):
        """Устанавливает чат для уведомлений"""
        cursor = self.execute_with_retry('''
            INSERT INTO admin_settings (admin_id, notification_chat_id)
            VALUES (?, ?)
            ON CONFLICT(admin_id) DO UPDATE SET
            notification_chat_id = excluded.notification_chat_id
        ''', (admin_id, chat_id))
        self.conn.commit()

    def get_notification_chats(self):
        """Получает все чаты для уведомлений"""
        cursor = self.execute_with_retry('SELECT DISTINCT notification_chat_id FROM admin_settings')
        return [row[0] for row in cursor.fetchall() if row[0] is not None]

    def get_total_users_count(self):
        """Получает общее количество пользователей"""
        cursor = self.execute_with_retry('SELECT COUNT(*) FROM bot_users')
        return cursor.fetchone()[0]

    def get_active_users_count(self, days=30):
        """Получает количество активных пользователей"""
        cutoff_date = (get_moscow_time() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        cursor = self.execute_with_retry('''
            SELECT COUNT(*) FROM bot_users 
            WHERE last_seen >= ?
        ''', (cutoff_date,))
        return cursor.fetchone()[0]

    def cleanup_completed_appointments(self):
        """Очищает прошедшие записи в московском времени"""
        moscow_time = get_moscow_time()
        current_date = moscow_time.strftime("%Y-%m-%d")
        current_time = moscow_time.strftime("%H:%M")
        
        # Удаляем записи за прошлые даты
        cursor = self.execute_with_retry('''
            DELETE FROM appointments 
            WHERE appointment_date < ?
        ''', (current_date,))
        
        deleted_past_dates = cursor.rowcount
        
        # Удаляем прошедшие записи за сегодня
        cursor = self.execute_with_retry('''
            DELETE FROM appointments 
            WHERE appointment_date = ? 
            AND appointment_time < ?
        ''', (current_date, current_time))
        
        deleted_today = cursor.rowcount
        
        # Очищаем расписание
        self.execute_with_retry('''
            DELETE FROM schedule 
            WHERE date < ?
        ''', (current_date,))
        
        self.execute_with_retry('''
            DELETE FROM schedule 
            WHERE date = ? AND time < ?
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
        forty_days_ago = (get_moscow_time() - timedelta(days=40)).strftime("%Y-%m-%d %H:%M:%S")
        cursor = self.execute_with_retry('''
            DELETE FROM bot_users 
            WHERE last_seen < ? 
            AND user_id NOT IN (
                SELECT DISTINCT user_id FROM appointments 
                WHERE user_id IS NOT NULL
            )
        ''', (forty_days_ago,))

        deleted_users = cursor.rowcount
        self.conn.commit()

        logger.info(f"✅ Очистка БД: удалено {deleted_users} неактивных пользователей (>40 дней)")

        return {
            'deleted_users': deleted_users
        }

    def get_weekly_stats(self):
        """Собирает статистику за прошедшую неделю (только завершенные записи)"""
        end_date = get_moscow_time().date()
        start_date = end_date - timedelta(days=7)
        
        # 1. Общее количество завершенных записей
        cursor = self.execute_with_retry('''
            SELECT COUNT(*) 
            FROM appointments 
            WHERE appointment_date >= ? AND appointment_date < ?
        ''', (start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")))
        total_appointments = cursor.fetchone()[0]
        
        # 2. Пиковое время (самое популярное время записи)
        cursor = self.execute_with_retry('''
            SELECT appointment_time, COUNT(*) as count
            FROM appointments 
            WHERE appointment_date >= ? AND appointment_date < ?
            GROUP BY appointment_time 
            ORDER BY count DESC 
            LIMIT 1
        ''', (start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")))
        peak_time_result = cursor.fetchone()
        peak_time = peak_time_result[0] if peak_time_result else "Нет данных"
        peak_time_count = peak_time_result[1] if peak_time_result else 0
        
        # 3. Новые клиенты (впервые записавшиеся за период)
        cursor = self.execute_with_retry('''
            SELECT COUNT(DISTINCT user_id) 
            FROM appointments 
            WHERE appointment_date >= ? AND appointment_date < ?
            AND user_id IS NOT NULL 
            AND user_id NOT IN (
                SELECT DISTINCT user_id 
                FROM appointments 
                WHERE appointment_date < ?
            )
        ''', (start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"), start_date.strftime("%Y-%m-%d")))
        new_clients = cursor.fetchone()[0]
        
        # 4. Постоянные клиенты (уже записывавшиеся ранее)
        cursor = self.execute_with_retry('''
            SELECT COUNT(DISTINCT user_id) 
            FROM appointments 
            WHERE appointment_date >= ? AND appointment_date < ?
            AND user_id IS NOT NULL 
            AND user_id IN (
                SELECT DISTINCT user_id 
                FROM appointments 
                WHERE appointment_date < ?
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
        cursor = self.execute_with_retry('''
            SELECT id, user_id, user_name, phone, service, appointment_date, appointment_time
            FROM appointments 
            WHERE DATE(appointment_date) >= DATE('now')
            ORDER BY appointment_date, appointment_time
        ''')
        
        all_future_appointments = cursor.fetchall()
        
        conflicting_appointments = []
        
        for appointment in all_future_appointments:
            appt_id, user_id, user_name, phone, service, date, time = appointment
            
            try:
                appointment_date = datetime.strptime(date, "%Y-%m-%d").date()
                appointment_weekday = appointment_date.weekday()
            except ValueError:
                logger.error(f"Неверный формат даты в записи {appt_id}: {date}")
                continue
            
            if appointment_weekday == weekday:
                if not new_is_working:
                    conflicting_appointments.append(appointment)
                    logger.info(f"Найдена конфликтующая запись (выходной): {date} {time} - {user_name}")
                else:
                    try:
                        appointment_time = datetime.strptime(time, "%H:%M").time()
                        new_start = datetime.strptime(new_start_time, "%H:%M").time()
                        new_end = datetime.strptime(new_end_time, "%H:%M").time()
                        
                        if appointment_time < new_start or appointment_time >= new_end:
                            conflicting_appointments.append(appointment)
                            logger.info(f"Найдена конфликтующая запись (время): {date} {time} - {user_name}")
                    except ValueError:
                        logger.error(f"Неверный формат времени в записи {appt_id}: {time}")
        
        logger.info(f"Всего найдено конфликтующих записей для дня {weekday}: {len(conflicting_appointments)}")
        return conflicting_appointments

    def cancel_appointments_by_ids(self, appointment_ids):
        """Массово отменяет записи по списку ID"""
        canceled_appointments = []
    
        for appt_id in appointment_ids:
            cursor = self.execute_with_retry('''
                SELECT user_id, user_name, phone, service, appointment_date, appointment_time 
                FROM appointments WHERE id = ?
            ''', (appt_id,))
            appointment = cursor.fetchone()
        
            if appointment:
                self.execute_with_retry('DELETE FROM appointments WHERE id = ?', (appt_id,))
                self.execute_with_retry('DELETE FROM schedule WHERE date = ? AND time = ?', 
                          (appointment[4], appointment[5]))
                canceled_appointments.append(appointment)
                logger.info(f"Отменена запись #{appt_id} для {appointment[1]}")
    
        self.conn.commit()
        logger.info(f"Всего отменено записей: {len(canceled_appointments)}")
        return canceled_appointments

    def add_admin(self, admin_id, username, first_name, last_name, added_by):
        """Добавляет администратора"""
        try:
            cursor = self.execute_with_retry('''
                INSERT INTO bot_admins (admin_id, username, first_name, last_name, added_by)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(admin_id) DO NOTHING
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
            if hasattr(config, 'PROTECTED_ADMINS') and admin_id in config.PROTECTED_ADMINS:
                logger.warning(f"🚫 Попытка удалить защищенного администратора {admin_id}")
                return False
                
            cursor = self.execute_with_retry('DELETE FROM bot_admins WHERE admin_id = ?', (admin_id,))
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
            cursor = self.execute_with_retry('''
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
        cursor = self.execute_with_retry('''
            SELECT admin_id, username, first_name, last_name, added_at, added_by
            FROM bot_admins WHERE admin_id = ?
        ''', (admin_id,))
        return cursor.fetchone()

    def check_duplicate_appointments(self):
        """Проверяет дублирующиеся записи"""
        cursor = self.execute_with_retry('''
            SELECT appointment_date, appointment_time, COUNT(*) as count
            FROM appointments 
            WHERE appointment_date >= DATE('now')
            GROUP BY appointment_date, appointment_time
            HAVING COUNT(*) > 1
            ORDER BY appointment_date, appointment_time
        ''')
        return cursor.fetchall()

    def get_appointments_by_datetime(self, date, time):
        """Получает все записи на указанные дату и время"""
        cursor = self.execute_with_retry('''
            SELECT id, user_name, phone, service
            FROM appointments 
            WHERE appointment_date = ? AND appointment_time = ?
            ORDER BY id
        ''', (date, time))
        return cursor.fetchall()

    def __del__(self):
        """Закрывает соединение при удалении объекта"""
        if hasattr(self, 'conn'):
            self.conn.close()