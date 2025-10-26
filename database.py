# database.py - FIXED VERSION
import sqlite3
from datetime import datetime, timedelta
import logging
import config

logger = logging.getLogger(__name__)

class Database:
    def __init__(self, setup_notifications=True):
        self.conn = sqlite3.connect('barbershop.db', check_same_thread=False, timeout=30.0)
        self.create_tables()
        self.migrate_database()
        if setup_notifications:
            self.setup_default_notifications()
        self.setup_default_schedule()
        self.load_admins_from_db()

    def create_tables(self):
        cursor = self.conn.cursor()
        
        # Существующие таблицы
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS appointments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
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
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS schedule (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT,
                time TEXT,
                available BOOLEAN DEFAULT TRUE
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS admin_settings (
                admin_id INTEGER PRIMARY KEY,
                notification_chat_id INTEGER
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS work_schedule (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                weekday INTEGER UNIQUE,
                start_time TEXT,
                end_time TEXT,
                is_working BOOLEAN DEFAULT TRUE
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS bot_users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Новая таблица для администраторов
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS bot_admins (
                admin_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                added_by INTEGER,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE
            )
        ''')
        
        self.conn.commit()

    def load_admins_from_db(self):
        """Загружает список администраторов из базы данных"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT admin_id FROM bot_admins WHERE is_active = TRUE
        ''')
        db_admins = [row[0] for row in cursor.fetchall()]
        
        # Объединяем администраторов из .env и из базы данных
        all_admins = list(set(config.BASE_ADMIN_IDS + db_admins))
        config.update_admin_ids(all_admins)
        
        logger.info(f"📋 Loaded {len(all_admins)} admins from database: {all_admins}")

    def add_admin(self, admin_id, username, first_name, last_name, added_by):
        """Добавляет нового администратора"""
        cursor = self.conn.cursor()
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO bot_admins 
                (admin_id, username, first_name, last_name, added_by, is_active)
                VALUES (?, ?, ?, ?, ?, TRUE)
            ''', (admin_id, username, first_name, last_name, added_by))
            
            self.conn.commit()
            self.load_admins_from_db()  # Обновляем кэш
            logger.info(f"✅ Admin {admin_id} added by {added_by}")
            return True
        except Exception as e:
            logger.error(f"❌ Error adding admin: {e}")
            return False

    def remove_admin(self, admin_id, removed_by):
        """Удаляет администратора (деактивирует)"""
        cursor = self.conn.cursor()
        try:
            # Не позволяем удалить самого себя
            if admin_id == removed_by:
                return False, "Нельзя удалить самого себя"
            
            # Не позволяем удалить базовых администраторов из .env
            if admin_id in config.BASE_ADMIN_IDS:
                return False, "Этот администратор защищен (указан в настройках)"
            
            cursor.execute('''
                UPDATE bot_admins SET is_active = FALSE WHERE admin_id = ?
            ''', (admin_id,))
            
            self.conn.commit()
            self.load_admins_from_db()  # Обновляем кэш
            logger.info(f"✅ Admin {admin_id} removed by {removed_by}")
            return True, "Администратор успешно удален"
        except Exception as e:
            logger.error(f"❌ Error removing admin: {e}")
            return False, f"Ошибка при удалении: {e}"

    def get_all_admins(self):
        """Возвращает список всех активных администраторов"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT admin_id, username, first_name, last_name, added_by, added_at
            FROM bot_admins 
            WHERE is_active = TRUE
            ORDER BY added_at
        ''')
        return cursor.fetchall()

    def get_admin_info(self, admin_id):
        """Возвращает информацию об администраторе"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT admin_id, username, first_name, last_name, added_by, added_at
            FROM bot_admins 
            WHERE admin_id = ? AND is_active = TRUE
        ''', (admin_id,))
        return cursor.fetchone()

    def migrate_database(self):
        cursor = self.conn.cursor()
        try:
            cursor.execute("SELECT phone FROM appointments LIMIT 1")
        except sqlite3.OperationalError:
            cursor.execute('ALTER TABLE appointments ADD COLUMN phone TEXT')
            self.conn.commit()
            logger.info("Колонка phone успешно добавлена")
        
        try:
            cursor.execute('''
                CREATE UNIQUE INDEX IF NOT EXISTS idx_work_schedule_weekday 
                ON work_schedule(weekday)
            ''')
            self.conn.commit()
            logger.info("UNIQUE constraint добавлен к work_schedule.weekday")
        except Exception as e:
            logger.warning(f"Не удалось добавить UNIQUE constraint: {e}")

    def setup_default_notifications(self):
        try:
            cursor = self.conn.cursor()
            for admin_id in config.ADMIN_IDS:
                cursor.execute('''
                    INSERT OR IGNORE INTO admin_settings (admin_id, notification_chat_id)
                    VALUES (?, ?)
                ''', (admin_id, admin_id))
            self.conn.commit()
            logger.info("Настроены уведомления по умолчанию для администраторов")
        except Exception as e:
            logger.warning(f"Не удалось настроить уведомления: {e}")
    
    def setup_default_schedule(self):
        cursor = self.conn.cursor()
        
        cursor.execute('''
            DELETE FROM work_schedule 
            WHERE id NOT IN (
                SELECT MIN(id) 
                FROM work_schedule 
                GROUP BY weekday
            )
        ''')
        
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
                    INSERT OR REPLACE INTO work_schedule (weekday, start_time, end_time, is_working)
                    VALUES (?, ?, ?, ?)
                ''', (weekday, start_time, end_time, is_working))
            
            self.conn.commit()
            logger.info("Установлен график работы по умолчанию")
        else:
            logger.info(f"В таблице work_schedule уже есть {count} записей")
    
    def add_appointment(self, user_id, user_name, user_username, phone, service, date, time):
        cursor = self.conn.cursor()
        
        cursor.execute('''
            SELECT COUNT(*) FROM appointments 
            WHERE appointment_date = ? AND appointment_time = ?
        ''', (date, time))
        
        if cursor.fetchone()[0] > 0:
            raise Exception("Это время уже занято другим клиентом")
        
        cursor.execute('''
            INSERT INTO appointments (user_id, user_name, user_username, phone, service, appointment_date, appointment_time)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (user_id, user_name, user_username, phone, service, date, time))
        
        appointment_id = cursor.lastrowid
        
        cursor.execute('''
            INSERT OR REPLACE INTO schedule (date, time, available)
            VALUES (?, ?, FALSE)
        ''', (date, time))
        
        self.conn.commit()
        return appointment_id
    
    def check_duplicate_appointments(self):
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT appointment_date, appointment_time, COUNT(*) as count
            FROM appointments
            GROUP BY appointment_date, appointment_time
            HAVING COUNT(*) > 1
        ''')
        return cursor.fetchall()
    
    def get_appointments_by_datetime(self, date, time):
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT id, user_name, phone, service
            FROM appointments
            WHERE appointment_date = ? AND appointment_time = ?
            ORDER BY id
        ''', (date, time))
        return cursor.fetchall()
    
    def cancel_appointment(self, appointment_id, user_id=None):
        cursor = self.conn.cursor()
        
        cursor.execute('''
            SELECT user_id, user_name, phone, service, appointment_date, appointment_time 
            FROM appointments WHERE id = ?
        ''', (appointment_id,))
        appointment = cursor.fetchone()
        
        if not appointment:
            return None
        
        if user_id:
            cursor.execute('''
                DELETE FROM appointments 
                WHERE id = ? AND user_id = ?
            ''', (appointment_id, user_id))
        else:
            cursor.execute('''
                DELETE FROM appointments WHERE id = ?
            ''', (appointment_id,))
        
        if cursor.rowcount > 0:
            user_id, user_name, phone, service, date, time = appointment
            cursor.execute('''
                DELETE FROM schedule WHERE date = ? AND time = ?
            ''', (date, time))
            
            self.conn.commit()
            return appointment
        return None
    
    def get_available_slots(self, date):
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT time FROM schedule 
            WHERE date = ? AND available = FALSE
        ''', (date,))
        booked_times = [row[0] for row in cursor.fetchall()]
        
        weekday = datetime.strptime(date, "%Y-%m-%d").weekday()
        
        cursor.execute('''
            SELECT id, weekday, start_time, end_time, is_working FROM work_schedule 
            WHERE weekday = ?
        ''', (weekday,))
        
        work_hours = cursor.fetchone()
        
        if not work_hours or not work_hours[4]:
            return []
        
        start_time, end_time = work_hours[2], work_hours[3]
        all_slots = self.generate_time_slots(start_time, end_time)
        
        return [slot for slot in all_slots if slot not in booked_times]
    
    def generate_time_slots(self, start_time, end_time):
        slots = []
        current = datetime.strptime(start_time, "%H:%M")
        end = datetime.strptime(end_time, "%H:%M")
        
        while current < end:
            slots.append(current.strftime("%H:%M"))
            current += timedelta(minutes=30)
        
        return slots
    
    def set_work_schedule(self, weekday, start_time, end_time, is_working=True):
        cursor = self.conn.cursor()
        
        cursor.execute('DELETE FROM work_schedule WHERE weekday = ?', (weekday,))
        
        cursor.execute('''
            INSERT INTO work_schedule (weekday, start_time, end_time, is_working)
            VALUES (?, ?, ?, ?)
        ''', (weekday, start_time, end_time, is_working))
        
        self.conn.commit()
        logger.info(f"Установлен график для дня {weekday}: {start_time}-{end_time}, рабочий: {is_working}")
    
    def get_work_schedule(self, weekday=None):
        cursor = self.conn.cursor()
        
        if weekday is not None:
            cursor.execute('''
                SELECT id, weekday, start_time, end_time, is_working 
                FROM work_schedule WHERE weekday = ?
            ''', (weekday,))
        else:
            cursor.execute('''
                SELECT id, weekday, start_time, end_time, is_working 
                FROM work_schedule ORDER BY weekday
            ''')
        
        result = cursor.fetchall()
        logger.info(f"Получен график для дня {weekday}: {result}")
        return result
    
    def get_week_schedule(self):
        schedule = {}
        for weekday in range(7):
            result = self.get_work_schedule(weekday)
            if result and len(result) > 0:
                schedule[weekday] = result[0]
            else:
                schedule[weekday] = (0, weekday, "10:00", "20:00", True)
        return schedule
    
    def get_user_appointments(self, user_id):
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT id, service, appointment_date, appointment_time 
            FROM appointments 
            WHERE user_id = ?
            ORDER BY appointment_date, appointment_time
        ''', (user_id,))
        return cursor.fetchall()
    
    def get_all_appointments(self):
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT id, user_name, user_username, phone, service, appointment_date, appointment_time 
            FROM appointments 
            ORDER BY appointment_date, appointment_time
        ''')
        return cursor.fetchall()
    
    def get_appointments_for_reminder(self):
        cursor = self.conn.cursor()
        tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        
        cursor.execute('''
            SELECT id, user_id, user_name, phone, service, appointment_date, appointment_time 
            FROM appointments 
            WHERE appointment_date = ? AND reminder_sent = FALSE
        ''', (tomorrow,))
        
        return cursor.fetchall()
    
    def mark_reminder_sent(self, appointment_id):
        cursor = self.conn.cursor()
        cursor.execute('''
            UPDATE appointments 
            SET reminder_sent = TRUE 
            WHERE id = ?
        ''', (appointment_id,))
        self.conn.commit()
    
    def get_today_appointments(self):
        cursor = self.conn.cursor()
        today = datetime.now().strftime("%Y-%m-%d")
        
        cursor.execute('''
            SELECT user_name, phone, service, appointment_time 
            FROM appointments 
            WHERE appointment_date = ?
            ORDER BY appointment_time
        ''', (today,))
        
        return cursor.fetchall()
    
    def set_notification_chat(self, admin_id, chat_id):
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO admin_settings (admin_id, notification_chat_id)
            VALUES (?, ?)
        ''', (admin_id, chat_id))
        self.conn.commit()
    
    def get_notification_chats(self):
        cursor = self.conn.cursor()
        cursor.execute('SELECT DISTINCT notification_chat_id FROM admin_settings')
        return [row[0] for row in cursor.fetchall() if row[0] is not None]
    
    def add_or_update_user(self, user_id, username, first_name, last_name):
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO bot_users (user_id, username, first_name, last_name, last_seen)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (user_id, username, first_name, last_name))
        self.conn.commit()
    
    def get_total_users_count(self):
        cursor = self.conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM bot_users')
        return cursor.fetchone()[0]
    
    def get_active_users_count(self, days=30):
        cursor = self.conn.cursor()
        cutoff_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute('''
            SELECT COUNT(*) FROM bot_users 
            WHERE last_seen >= ?
        ''', (cutoff_date,))
        return cursor.fetchone()[0]
    
    def cleanup_completed_appointments(self):
        """Удаляет записи, время которых уже прошло"""
        cursor = self.conn.cursor()
        now = datetime.now()
        current_date = now.strftime("%Y-%m-%d")
        current_time = now.strftime("%H:%M")
        
        # Удаляем записи за прошлые даты
        cursor.execute('''
            DELETE FROM appointments 
            WHERE appointment_date < ?
        ''', (current_date,))
        
        deleted_past_dates = cursor.rowcount
        
        # ПРАВИЛЬНОЕ УДАЛЕНИЕ: преобразуем время в минуты для корректного сравнения
        cursor.execute('''
            DELETE FROM appointments 
            WHERE appointment_date = ? 
            AND (
                (substr(appointment_time, 1, 2) * 60 + substr(appointment_time, 4, 2)) < 
                (substr(?, 1, 2) * 60 + substr(?, 4, 2))
            )
        ''', (current_date, current_time, current_time))
        
        deleted_today = cursor.rowcount
        
        # Очищаем расписание для удаленных записей
        cursor.execute('''
            DELETE FROM schedule 
            WHERE date < ?
        ''', (current_date,))
        
        # Очищаем расписание для прошедших записей сегодня
        cursor.execute('''
            DELETE FROM schedule 
            WHERE date = ? 
            AND (
                (substr(time, 1, 2) * 60 + substr(time, 4, 2)) < 
                (substr(?, 1, 2) * 60 + substr(?, 4, 2))
            )
        ''', (current_date, current_time, current_time))
        
        self.conn.commit()
        
        total_deleted = deleted_past_dates + deleted_today
        
        if total_deleted > 0:
            logger.info(f"Автоочистка: удалено {total_deleted} прошедших записей ({deleted_past_dates} за прошлые даты, {deleted_today} за сегодня)")
        
        return {
            'deleted_past_dates': deleted_past_dates,
            'deleted_today': deleted_today,
            'total_deleted': total_deleted
        }
    
    def cleanup_duplicate_schedules(self):
        cursor = self.conn.cursor()
        
        cursor.execute('''
            DELETE FROM work_schedule 
            WHERE id NOT IN (
                SELECT MAX(id) 
                FROM work_schedule 
                GROUP BY weekday
            )
        ''')
        
        deleted_count = cursor.rowcount
        self.conn.commit()
        
        if deleted_count > 0:
            logger.info(f"Очищено {deleted_count} дублирующихся записей расписания")
        
        return deleted_count