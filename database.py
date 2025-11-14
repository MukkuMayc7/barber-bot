# database.py
import os
import logging
import sqlite3
import time
import shutil
import glob
from datetime import datetime, timedelta, timezone
import config

logger = logging.getLogger(__name__)

def get_moscow_time():
    """Возвращает текущее московское время (UTC+3)"""
    return datetime.now(timezone(timedelta(hours=3)))

def get_database_path():
    """🎯 ОПТИМИЗИРОВАННЫЙ ПУТЬ ДЛЯ RENDER"""
    db_path = '/tmp/barbershop.db'
    db_exists = os.path.exists(db_path)
    
    logger.info(f"📁 ПУТЬ К БД: {db_path}")
    logger.info(f"📊 БД СУЩЕСТВУЕТ: {db_exists}")
    
    if db_exists:
        size = os.path.getsize(db_path) / 1024  # KB
        logger.info(f"📏 РАЗМЕР БД: {size:.1f} KB")
    else:
        logger.info("🆕 БД не найдена, будет создана новая")
    
    return db_path

class Database:
    def __init__(self):
        self.database_url = config.DATABASE_URL
        self.max_retries = 3
        self.retry_delay = 0.1
        self.conn = None
        self.db_path = get_database_path()
        self.last_backup_time = None
        self.backup_enabled = True
        self.reconnect()
    
    def reconnect(self):
        """Переподключается к базе данных с повторными попытками"""
        for attempt in range(self.max_retries):
            try:
                if self.conn:
                    try:
                        self.conn.close()
                    except:
                        pass
                
                logger.info(f"📁 Подключаемся к БД по пути: {self.db_path}")
                
                self.conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=10.0)
                self.conn.row_factory = sqlite3.Row
                
                # Оптимизации для SQLite
                self.conn.execute('PRAGMA journal_mode=WAL')
                self.conn.execute('PRAGMA synchronous=NORMAL')
                self.conn.execute('PRAGMA cache_size=-64000')
                self.conn.execute('PRAGMA foreign_keys=ON')
                self.conn.execute('PRAGMA auto_vacuum=INCREMENTAL')
                
                self.create_tables()
                self.update_database_structure()
                self.create_admin_tables()
                self.setup_default_notifications()
                self.setup_default_schedule()
                
                # 🎯 ВОССТАНОВЛЕНИЕ ИЗ BACKUP ПРИ ПЕРВОМ ЗАПУСКЕ
                if not self.has_data() and self.backup_enabled:
                    self.restore_from_backup()
                
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

    def has_data(self):
        """Проверяет, есть ли данные в БД"""
        try:
            cursor = self.execute_with_retry('SELECT COUNT(*) FROM appointments')
            appointments_count = cursor.fetchone()[0]
            
            cursor = self.execute_with_retry('SELECT COUNT(*) FROM bot_users')
            users_count = cursor.fetchone()[0]
            
            return appointments_count > 0 or users_count > 0
        except:
            return False

    def check_connection(self):
        """🎯 ПРОСТАЯ ПРОВЕРКА СОЕДИНЕНИЯ БЕЗ РЕКУРСИИ"""
        try:
            if not self.conn:
                self.reconnect()
                return False
                
            cursor = self.conn.cursor()
            cursor.execute('SELECT 1')
            cursor.fetchone()
            return True
            
        except Exception as e:
            logger.warning(f"⚠️ Проверка соединения провалилась: {e}")
            try:
                self.reconnect()
                return True
            except:
                return False

    def execute_with_retry(self, query, params=()):
        """Выполняет запрос с повторными попытками при блокировке"""
        for attempt in range(self.max_retries):
            try:
                if not self.conn:
                    self.reconnect()
                    
                cursor = self.conn.cursor()
                cursor.execute(query, params)
                return cursor
            except sqlite3.OperationalError as e:
                if "locked" in str(e) and attempt < self.max_retries - 1:
                    logger.warning(f"⚠️ База заблокирована, повторная попытка {attempt + 1}")
                    time.sleep(self.retry_delay * (attempt + 1))
                    continue
                raise
            except sqlite3.DatabaseError as e:
                logger.error(f"❌ Ошибка базы данных, переподключаемся: {e}")
                self.reconnect()
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                    continue
                raise

    def create_tables(self):
        """Создает все необходимые таблицы"""
        cursor = self.conn.cursor()
        
        # Таблица appointments
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

        # Таблица scheduled_reminders
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
        
        # 🎯 ТАБЛИЦА ДЛЯ BACKUP МЕТАДАННЫХ
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS backup_metadata (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                backup_type TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                size_kb INTEGER,
                success BOOLEAN,
                backup_path TEXT,
                error_message TEXT
            )
        ''')
        
        self.conn.commit()
        logger.info("✅ Таблицы успешно созданы/проверены")

    def update_database_structure(self):
        """Обновляет структуру базы данных"""
        cursor = self.conn.cursor()
        
        try:
            cursor.execute("PRAGMA table_info(appointments)")
            columns = [column[1] for column in cursor.fetchall()]
            
            if 'reminder_24h_sent' not in columns:
                cursor.execute('ALTER TABLE appointments ADD COLUMN reminder_24h_sent BOOLEAN DEFAULT FALSE')
                logger.info("✅ Добавлена колонка reminder_24h_sent")
            
            if 'reminder_1h_sent' not in columns:
                cursor.execute('ALTER TABLE appointments ADD COLUMN reminder_1h_sent BOOLEAN DEFAULT FALSE')
                logger.info("✅ Добавлена колонка reminder_1h_sent")
                
            self.conn.commit()
            
        except Exception as e:
            logger.error(f"❌ Ошибка при обновлении структуры БД: {e}")
            self.conn.rollback()

    def create_backup(self):
        """🎯 СОЗДАЕТ ЛОКАЛЬНЫЙ BACKUP (ИСПРАВЛЕННАЯ)"""
        try:
            if not self.backup_enabled:
                logger.info("⏩ Backup отключен")
                return None
        
            # 🎯 ПРОВЕРЯЕМ ЧТО В БД ЕСТЬ ДАННЫЕ ДЛЯ БЭКАПА
            cursor = self.execute_with_retry('SELECT COUNT(*) FROM appointments')
            appointments_count = cursor.fetchone()[0]
        
            cursor = self.execute_with_retry('SELECT COUNT(*) FROM bot_users')
            users_count = cursor.fetchone()[0]
        
            if appointments_count == 0 and users_count == 0:
                logger.info("⏩ Нет данных для бэкапа")
                return None
        
            logger.info(f"💾 Создание backup (записей: {appointments_count}, пользователей: {users_count})...")
        
            backup_path = "/tmp/barbershop_latest_backup.db"
        
            # 🎯 ВАЖНО: ДЕЛАЕМ COMMIT ПЕРЕД БЭКАПОМ
            self.conn.commit()
        
            # Копируем файл БД
            import shutil
            shutil.copy2(self.db_path, backup_path)
        
            # Проверяем что бэкап создался и не пустой
            if os.path.exists(backup_path):
                backup_size = os.path.getsize(backup_path)
                if backup_size == 0:
                    logger.error("❌ Бэкап файл пустой!")
                    return None
            else:
                logger.error("❌ Бэкап файл не создался!")
                return None
        
            # Сохраняем информацию о backup в БД
            cursor = self.execute_with_retry('''
                INSERT INTO backup_metadata 
                (backup_type, size_kb, success, backup_path) 
                VALUES (?, ?, ?, ?)
            ''', ('latest_backup', backup_size // 1024, True, backup_path))
        
            self.conn.commit()
        
            self.last_backup_time = get_moscow_time()
            logger.info(f"✅ Локальный backup создан: {backup_path} ({backup_size} bytes)")
            return backup_path
        
        except Exception as e:
            logger.error(f"❌ Ошибка создания локального backup: {e}")
        
            try:
                cursor = self.execute_with_retry('''
                    INSERT INTO backup_metadata 
                    (backup_type, success, error_message) 
                    VALUES (?, ?, ?)
                ''', ('latest_backup', False, str(e)))
                self.conn.commit()
            except:
                pass
        
            return None

    def restore_from_backup(self):
        """🎯 ВОССТАНОВЛЕНИЕ ИЗ ЛОКАЛЬНОГО BACKUP (ИСПРАВЛЕННАЯ)"""
        try:
            if not self.backup_enabled:
                logger.info("⏩ Восстановление отключено")
                return False
        
            backup_path = "/tmp/barbershop_latest_backup.db"
        
            if not os.path.exists(backup_path):
                logger.info("⏩ Нет backup файла для восстановления")
                return False
        
            # 🎯 ПРОВЕРЯЕМ ЧТО В БЭКАПЕ ЕСТЬ ДАННЫЕ
            import sqlite3
            conn_check = sqlite3.connect(backup_path)
            cursor_check = conn_check.cursor()
        
            try:
                cursor_check.execute('SELECT COUNT(*) FROM appointments')
                backup_appointments = cursor_check.fetchone()[0]
            
                cursor_check.execute('SELECT COUNT(*) FROM bot_users')
                backup_users = cursor_check.fetchone()[0]
            except:
                backup_appointments = 0
                backup_users = 0
        
            conn_check.close()
        
            if backup_appointments == 0 and backup_users == 0:
                logger.info("⏩ Бэкап файл пустой, восстановление не требуется")
                return False
        
            logger.info(f"🔄 Восстанавливаем из backup: {backup_path} (записей: {backup_appointments}, пользователей: {backup_users})")
        
            # 🎯 ЗАКРЫВАЕМ СОЕДИНЕНИЕ ПРАВИЛЬНО
            if self.conn:
                try:
                    self.conn.close()
                except:
                    pass
        
            # 🎯 УДАЛЯЕМ СТАРУЮ БД И КОПИРУЕМ БЭКАП
            try:
                if os.path.exists(self.db_path):
                    os.remove(self.db_path)
                    logger.info("✅ Старая БД удалена")
            except Exception as e:
                logger.error(f"❌ Ошибка удаления старой БД: {e}")
        
            # Копируем backup
            import shutil
            shutil.copy2(backup_path, self.db_path)
            logger.info("✅ Бэкап скопирован")
        
            # 🎯 ПЕРЕПОДКЛЮЧАЕМСЯ И ПРОВЕРЯЕМ
            self.reconnect()
        
            # Проверяем что данные восстановились
            cursor = self.execute_with_retry('SELECT COUNT(*) FROM appointments')
            restored_appointments = cursor.fetchone()[0]
        
            cursor = self.execute_with_retry('SELECT COUNT(*) FROM bot_users')
            restored_users = cursor.fetchone()[0]
        
            logger.info(f"✅ Восстановление завершено! Записей: {restored_appointments}, пользователей: {restored_users}")
        
            return True
        
        except Exception as e:
            logger.error(f"❌ Ошибка при восстановлении из backup: {e}")
            # Пытаемся переподключиться к оригинальной БД
            try:
                self.reconnect()
            except:
                pass
            return False

    def get_backup_status(self):
        """Получает статус последних backup"""
        try:
            cursor = self.execute_with_retry('''
                SELECT timestamp, size_kb, success, backup_path, error_message
                FROM backup_metadata 
                ORDER BY timestamp DESC 
                LIMIT 5
            ''')
            
            return cursor.fetchall()
        except Exception as e:
            logger.error(f"❌ Ошибка получения статуса backup: {e}")
            return []

    def get_backup_files_info(self):
        """Получает информацию о backup файле"""
        try:
            backup_path = "/tmp/barbershop_latest_backup.db"
            files_info = []
        
            if os.path.exists(backup_path):
                file_size = os.path.getsize(backup_path) / 1024  # KB
                file_time = os.path.getmtime(backup_path)
                file_date = datetime.fromtimestamp(file_time).strftime("%d.%m.%Y %H:%M")
            
                files_info.append({
                    'path': backup_path,
                    'size_kb': round(file_size, 1),
                    'date': file_date
                })
        
            return files_info
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения информации о backup файле: {e}")
            return []

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

    def automatic_cleanup(self):
        """🎯 АВТОМАТИЧЕСКАЯ ОЧИСТКА ДЛЯ RENDER"""
        try:
            # Очищаем прошедшие записи
            cleanup_result = self.cleanup_completed_appointments()
            
            # Дополнительно: очищаем очень старые записи (> 14 дней) для экономии места
            moscow_time = get_moscow_time()
            cutoff_date_14_days = (moscow_time - timedelta(days=14)).strftime("%Y-%m-%d")
            
            cursor = self.execute_with_retry('''
                DELETE FROM appointments 
                WHERE appointment_date < ?
            ''', (cutoff_date_14_days,))
            deleted_old = cursor.rowcount
            
            # Очищаем старые напоминания
            cutoff_datetime = (moscow_time - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
            cursor = self.execute_with_retry('''
                DELETE FROM scheduled_reminders 
                WHERE sent = TRUE AND scheduled_time < ?
            ''', (cutoff_datetime,))
            deleted_reminders = cursor.rowcount
            
            # Очищаем неактивных пользователей (не заходили 60 дней)
            cutoff_users = (moscow_time - timedelta(days=60)).strftime("%Y-%m-%d %H:%M:%S")
            cursor = self.execute_with_retry('''
                DELETE FROM bot_users 
                WHERE last_seen < ?
            ''', (cutoff_users,))
            deleted_users = cursor.rowcount
            
            self.conn.commit()
            
            total_deleted = (cleanup_result['total_deleted'] + deleted_old + 
                            deleted_reminders + deleted_users)
            
            logger.info(f"🧹 Автоочистка: удалено {total_deleted} записей "
                       f"(прошлые: {cleanup_result['total_deleted']}, "
                       f"старые: {deleted_old}, "
                       f"напоминания: {deleted_reminders}, "
                       f"пользователи: {deleted_users})")
            
            return {
                'total_deleted': total_deleted,
                'deleted_past_appointments': cleanup_result['total_deleted'],
                'deleted_old_appointments': deleted_old,
                'deleted_reminders': deleted_reminders,
                'deleted_users': deleted_users
            }
            
        except Exception as e:
            logger.error(f"❌ Ошибка автоматической очистки: {e}")
            return {'total_deleted': 0}

    def emergency_cleanup(self):
        """Экстренная очистка при нехватке памяти БЕЗ УВЕДОМЛЕНИЙ"""
        try:
            logger.warning("🚨 ВЫПОЛНЯЕТСЯ ЭКСТРЕННАЯ ОЧИСТКА!")
        
            # Создаем backup перед очисткой (без уведомлений)
            self.create_backup()
        
            # Удаляем очень старые записи (> 7 дней)
            moscow_time = get_moscow_time()
            cutoff_date = (moscow_time - timedelta(days=7)).strftime("%Y-%m-%d")
        
            cursor = self.execute_with_retry('''
                DELETE FROM appointments 
                WHERE appointment_date < ?
            ''', (cutoff_date,))
            deleted_appointments = cursor.rowcount
        
            # Очищаем старые backup метаданные
            cursor = self.execute_with_retry('''
                DELETE FROM backup_metadata 
                WHERE timestamp < DATE('now', '-30 days')
            ''')
            deleted_backup_meta = cursor.rowcount
        
            self.conn.commit()
        
            logger.info(f"🚨 Экстренная очистка: удалено {deleted_appointments} записей, {deleted_backup_meta} backup метаданных")
        
            return deleted_appointments + deleted_backup_meta
        
        except Exception as e:
            logger.error(f"❌ Ошибка экстренной очистки: {e}")
            return 0
    
    def add_appointment(self, user_id, user_name, user_username, phone, service, date, time):
        """Добавляет новую запись"""
        try:
            # Проверяем, не занято ли время
            cursor = self.execute_with_retry('''
                SELECT COUNT(*) FROM appointments 
                WHERE appointment_date = ? AND appointment_time = ?
            ''', (date, time))
            
            if cursor.fetchone()[0] > 0:
                raise Exception("Это время уже занято другим клиентом")
            
            cursor = self.execute_with_retry('''
                INSERT INTO appointments (user_id, user_name, user_username, phone, service, appointment_date, appointment_time)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (user_id, user_name, user_username, phone, service, date, time))
            
            appointment_id = cursor.lastrowid
            
            self.execute_with_retry('''
                INSERT OR REPLACE INTO schedule (date, time, available)
                VALUES (?, ?, ?)
            ''', (date, time, False))
            
            self.conn.commit()
            
            # 🎯 АВТОМАТИЧЕСКИЙ BACKUP ПОСЛЕ КАЖДОЙ НОВОЙ ЗАПИСИ
            if self.backup_enabled:
                logger.info(f"💾 Автоматический backup после создания записи #{appointment_id}")
                self.create_backup()  # 🎯 ЭТА СТРОЧКА ДОЛЖНА БЫТЬ!

            return appointment_id
            
        except Exception as e:
            logger.error(f"❌ Ошибка БД в add_appointment: {e}")
            raise

    def add_or_update_user(self, user_id, username, first_name, last_name):
        """Добавляет или обновляет пользователя"""
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
        except Exception as e:
            logger.error(f"❌ Ошибка БД в add_or_update_user: {e}")

    def is_admin(self, user_id):
        """Проверяет, является ли пользователь администратором"""
        try:
            cursor = self.execute_with_retry('SELECT 1 FROM bot_admins WHERE admin_id = ?', (user_id,))
            result = cursor.fetchone() is not None
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
        cursor = self.execute_with_retry('''
            SELECT user_id, user_name, phone, service, appointment_date, appointment_time 
            FROM appointments WHERE id = ?
        ''', (appointment_id,))
        appointment = cursor.fetchone()
        
        if not appointment:
            return None
        
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
            self.execute_with_retry('''
                DELETE FROM schedule WHERE date = ? AND time = ?
            ''', (date, time))
            
            self.conn.commit()

            # 🎯 АВТОМАТИЧЕСКИЙ BACKUP ПОСЛЕ ОТМЕНЫ ЗАПИСИ
            if self.backup_enabled:
                logger.info(f"💾 Автоматический backup после отмены записи #{appointment_id}")
                self.create_backup()

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
        """Очищает прошедшие записи"""
        moscow_time = get_moscow_time()
        current_date = moscow_time.strftime("%Y-%m-%d")
        current_time = moscow_time.strftime("%H:%M")
        
        cursor = self.execute_with_retry('''
            DELETE FROM appointments 
            WHERE appointment_date < ?
        ''', (current_date,))
        
        deleted_past_dates = cursor.rowcount
        
        cursor = self.execute_with_retry('''
            DELETE FROM appointments 
            WHERE appointment_date = ? 
            AND appointment_time < ?
        ''', (current_date, current_time))
        
        deleted_today = cursor.rowcount
        
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

    def check_duplicate_appointments(self):
        """Проверяет дублирующиеся записи"""
        try:
            cursor = self.execute_with_retry('''
                SELECT appointment_date, appointment_time, COUNT(*) as count
                FROM appointments 
                WHERE appointment_date >= DATE('now')
                GROUP BY appointment_date, appointment_time
                HAVING COUNT(*) > 1
                ORDER BY appointment_date, appointment_time
            ''')
            return cursor.fetchall()
        except Exception as e:
            logger.error(f"❌ Ошибка при проверке дубликатов: {e}")
            return []
    
    def get_appointments_by_datetime(self, date, time):
        """Получает все записи на указанные дату и время"""
        try:
            cursor = self.execute_with_retry('''
                SELECT id, user_name, phone, service
                FROM appointments 
                WHERE appointment_date = ? AND appointment_time = ?
                ORDER BY id
            ''', (date, time))
            return cursor.fetchall()
        except Exception as e:
            logger.error(f"❌ Ошибка при получении записей: {e}")
            return []

    def get_conflicting_appointments(self, weekday, new_start_time, new_end_time, new_is_working):
        """Находит конфликтующие записи при изменении графика"""
        try:
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
                    continue
                
                if appointment_weekday == weekday:
                    if not new_is_working:
                        conflicting_appointments.append(appointment)
                    else:
                        try:
                            appointment_time = datetime.strptime(time, "%H:%M").time()
                            new_start = datetime.strptime(new_start_time, "%H:%M").time()
                            new_end = datetime.strptime(new_end_time, "%H:%M").time()
                            
                            if appointment_time < new_start or appointment_time >= new_end:
                                conflicting_appointments.append(appointment)
                        except ValueError:
                            continue
            
            return conflicting_appointments
            
        except Exception as e:
            logger.error(f"❌ Ошибка при поиске конфликтующих записей: {e}")
            return []

    def cancel_appointments_by_ids(self, appointment_ids):
        """Массово отменяет записи по списку ID"""
        try:
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
            
            self.conn.commit()
            return canceled_appointments
            
        except Exception as e:
            logger.error(f"❌ Ошибка при массовой отмене записей: {e}")
            return []

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
            return added
            
        except Exception as e:
            logger.error(f"❌ Ошибка при добавлении администратора: {e}")
            self.conn.rollback()
            return False

    def remove_admin(self, admin_id):
        """Удаляет администратора"""
        try:
            if hasattr(config, 'PROTECTED_ADMINS') and admin_id in config.PROTECTED_ADMINS:
                return False
                
            cursor = self.execute_with_retry('DELETE FROM bot_admins WHERE admin_id = ?', (admin_id,))
            self.conn.commit()
            
            deleted = cursor.rowcount > 0
            return deleted
            
        except Exception as e:
            logger.error(f"❌ Ошибка при удалении администратора: {e}")
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
            return cursor.fetchall()
        except Exception as e:
            logger.error(f"❌ Ошибка при получении списка администраторов: {e}")
            return []

    def get_admin_info(self, admin_id):
        """Получает информацию об администраторе"""
        try:
            cursor = self.execute_with_retry('''
                SELECT admin_id, username, first_name, last_name, added_at, added_by
                FROM bot_admins WHERE admin_id = ?
            ''', (admin_id,))
            return cursor.fetchone()
        except Exception as e:
            logger.error(f"❌ Ошибка при получении информации об администраторе: {e}")
            return None

    def get_weekly_stats(self):
        """Собирает статистику за прошедшую неделю"""
        try:
            end_date = get_moscow_time().date()
            start_date = end_date - timedelta(days=7)
            
            cursor = self.execute_with_retry('''
                SELECT COUNT(*) 
                FROM appointments 
                WHERE appointment_date >= ? AND appointment_date < ?
            ''', (start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")))
            total_appointments = cursor.fetchone()[0]
            
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
            
            return {
                'start_date': start_date.strftime("%d.%m.%Y"),
                'end_date': (end_date - timedelta(days=1)).strftime("%d.%m.%Y"),
                'total_appointments': total_appointments,
                'peak_time': peak_time,
                'peak_time_count': peak_time_count,
                'new_clients': 0,
                'regular_clients': 0
            }
            
        except Exception as e:
            logger.error(f"❌ Ошибка при сборе статистики: {e}")
            return {
                'start_date': '',
                'end_date': '',
                'total_appointments': 0,
                'peak_time': "Нет данных",
                'peak_time_count': 0,
                'new_clients': 0,
                'regular_clients': 0
            }

    def __del__(self):
        """Закрывает соединение при удалении объекта"""
        if self.conn:
            self.conn.close()