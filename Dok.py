import asyncio
import logging
import os
import sqlite3
from datetime import datetime, timedelta
import yt_dlp
import aiofiles
import tempfile
import shutil
from functools import lru_cache
import re
import urllib.parse
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from aiogram import Bot, Dispatcher, types, F
from aiogram.enums import ParseMode, ChatMemberStatus
from aiogram.filters import CommandStart, Command
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, FSInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiohttp import web
from aiohttp.web_app import Application

# Logging sozlamalari
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Bot sozlamalari
BOT_TOKEN = "7626749090:AAFL--dyGniYyUVQ-U0sErxtwOL0qbrytX"
WEBHOOK_URL = "https://video-br8o.onrender.com"
WEBHOOK_PATH = "/webhook"
PORT = int(os.environ.get("PORT", 8080))
ADMIN_IDS = [6852738257]
DATABASE_PATH = "bot_database.db"

# Bot va dispatcher yaratish
bot = Bot(token=BOT_TOKEN, parse_mode=ParseMode.HTML)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Thread pool for downloading
executor = ThreadPoolExecutor(max_workers=3)

# Tillar
LANGUAGES = {
    'uz': {
        'welcome': """🎬 Video Downloader Bot ga xush kelibsiz!

📱 Quyidagi platformalardan video yuklay olasiz:
• TikTok
• YouTube (Video va Shorts)
• Facebook (Reels va Video)
• Instagram (Reels, Story, Post)

📝 Foydalanish:
Video linkini yuboring va kerakli sifatni tanlang!

👨‍💼 Admin: /admin - Admin panel
📊 Statistika: /stats""",
        'choose_language': '🌐 Tilni tanlang / Choose language / Выберите язык:',
        'language_changed': '✅ Til o\'zgartirildi!',
        'subscription_required': '🔒 Botdan foydalanish uchun quyidagi kanallarga obuna bo\'ling:',
        'subscription_check': '✅ Tekshirish',
        'subscription_success': '✅ Tabriklaymiz! Endi botdan foydalanishingiz mumkin.',
        'subscription_failed': '❌ Siz hali barcha kanallarga obuna bo\'lmadingiz!',
        'send_link': '❌ Iltimos, to\'g\'ri video linkini yuboring!\n\n📱 Qo\'llab-quvvatlanadigan platformalar:\n• TikTok • YouTube • Facebook • Instagram',
        'unknown_platform': '❌ Noma\'lum platforma! Qo\'llab-quvvatlanadigan:\n• TikTok • YouTube • Facebook • Instagram',
        'choose_quality': 'video aniqlandi!\n🎬 Kerakli sifatni tanlang:',
        'downloading': '⏳ Video yuklanmoqda... (3-5 soniya)',
        'file_too_large': '❌ Fayl hajmi juda katta (50MB dan ortiq)',
        'download_error': '❌ Xatolik:',
        'send_error': '❌ Fayl yuborishda xatolik:',
        'admin_only': '❌ Sizda admin huquqi yo\'q!',
        'admin_panel': '👨‍💼 Admin Panel',
        'stats': '📊 Statistika',
        'broadcast': '📢 Xabar yuborish',
        'channels': '⚙️ Kanal sozlamalari',
        'users': '👥 Foydalanuvchilar',
        'no_permission': '❌ Ruxsat yo\'q!',
        'quality_720': '🔥 720p',
        'quality_480': '📱 480p',
        'quality_360': '💾 360p',
        'quality_audio': '🎵 Audio',
        'quality_high': '🔥 Yuqori',
        'quality_medium': '📱 O\'rta',
        'quality_low': '💾 Past'
    },
    'ru': {
        'welcome': """🎬 Добро пожаловать в Video Downloader Bot!

📱 Вы можете скачивать видео с следующих платформ:
• TikTok
• YouTube (Видео и Shorts)
• Facebook (Reels и Видео)
• Instagram (Reels, Story, Post)

📝 Использование:
Отправьте ссылку на видео и выберите нужное качество!

👨‍💼 Админ: /admin - Админ панель
📊 Статистика: /stats""",
        'choose_language': '🌐 Tilni tanlang / Choose language / Выберите язык:',
        'language_changed': '✅ Язык изменен!',
        'subscription_required': '🔒 Для использования бота подпишитесь на следующие каналы:',
        'subscription_check': '✅ Проверить',
        'subscription_success': '✅ Поздравляем! Теперь вы можете пользоваться ботом.',
        'subscription_failed': '❌ Вы еще не подписались на все каналы!',
        'send_link': '❌ Пожалуйста, отправьте правильную ссылку на видео!\n\n📱 Поддерживаемые платформы:\n• TikTok • YouTube • Facebook • Instagram',
        'unknown_platform': '❌ Неизвестная платформа! Поддерживаемые:\n• TikTok • YouTube • Facebook • Instagram',
        'choose_quality': 'видео найдено!\n🎬 Выберите нужное качество:',
        'downloading': '⏳ Загружается видео... (3-5 секунд)',
        'file_too_large': '❌ Размер файла слишком большой (более 50MB)',
        'download_error': '❌ Ошибка:',
        'send_error': '❌ Ошибка отправки файла:',
        'admin_only': '❌ У вас нет прав администратора!',
        'admin_panel': '👨‍💼 Админ Панель',
        'stats': '📊 Статистика',
        'broadcast': '📢 Отправить сообщение',
        'channels': '⚙️ Настройки каналов',
        'users': '👥 Пользователи',
        'no_permission': '❌ Нет разрешения!',
        'quality_720': '🔥 720p',
        'quality_480': '📱 480p',
        'quality_360': '💾 360p',
        'quality_audio': '🎵 Аудио',
        'quality_high': '🔥 Высокое',
        'quality_medium': '📱 Среднее',
        'quality_low': '💾 Низкое'
    },
    'en': {
        'welcome': """🎬 Welcome to Video Downloader Bot!

📱 You can download videos from the following platforms:
• TikTok
• YouTube (Videos and Shorts)
• Facebook (Reels and Videos)
• Instagram (Reels, Stories, Posts)

📝 Usage:
Send a video link and choose the desired quality!

👨‍💼 Admin: /admin - Admin panel
📊 Statistics: /stats""",
        'choose_language': '🌐 Tilni tanlang / Choose language / Выберите язык:',
        'language_changed': '✅ Language changed!',
        'subscription_required': '🔒 To use the bot, subscribe to the following channels:',
        'subscription_check': '✅ Check',
        'subscription_success': '✅ Congratulations! Now you can use the bot.',
        'subscription_failed': '❌ You haven\'t subscribed to all channels yet!',
        'send_link': '❌ Please send a valid video link!\n\n📱 Supported platforms:\n• TikTok • YouTube • Facebook • Instagram',
        'unknown_platform': '❌ Unknown platform! Supported:\n• TikTok • YouTube • Facebook • Instagram',
        'choose_quality': 'video detected!\n🎬 Choose the desired quality:',
        'downloading': '⏳ Downloading video... (3-5 seconds)',
        'file_too_large': '❌ File size too large (over 50MB)',
        'download_error': '❌ Error:',
        'send_error': '❌ File sending error:',
        'admin_only': '❌ You don\'t have admin rights!',
        'admin_panel': '👨‍💼 Admin Panel',
        'stats': '📊 Statistics',
        'broadcast': '📢 Send message',
        'channels': '⚙️ Channel settings',
        'users': '👥 Users',
        'no_permission': '❌ No permission!',
        'quality_720': '🔥 720p',
        'quality_480': '📱 480p',
        'quality_360': '💾 360p',
        'quality_audio': '🎵 Audio',
        'quality_high': '🔥 High',
        'quality_medium': '📱 Medium',
        'quality_low': '💾 Low'
    }
}

# State'lar
class UserStates(StatesGroup):
    waiting_broadcast = State()

# Ma'lumotlar bazasini yaratish
def init_database():
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    
    # Foydalanuvchilar jadvali
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            join_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_active BOOLEAN DEFAULT TRUE,
            language TEXT DEFAULT 'uz'
        )
    ''')
    
    # Majburiy kanallar jadvali
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS required_channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id TEXT UNIQUE,
            channel_name TEXT,
            added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Statistika jadvali
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS daily_stats (
            date DATE PRIMARY KEY,
            new_users INTEGER DEFAULT 0,
            active_users INTEGER DEFAULT 0,
            downloads INTEGER DEFAULT 0
        )
    ''')
    
    conn.commit()
    conn.close()

# Cache uchun
@lru_cache(maxsize=100)
def get_required_channels():
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    cursor.execute('SELECT channel_id, channel_name FROM required_channels')
    channels = cursor.fetchall()
    conn.close()
    return channels

# Foydalanuvchi tilini olish
async def get_user_language(user_id):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _get_user_language_sync, user_id)

def _get_user_language_sync(user_id):
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    cursor.execute('SELECT language FROM users WHERE user_id = ?', (user_id,))
    result = cursor.fetchone()
    conn.close()
    return result[0] if result else 'uz'

# Foydalanuvchi tilini saqlash
async def set_user_language(user_id, language):
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _set_user_language_sync, user_id, language)

def _set_user_language_sync(user_id, language):
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    cursor.execute('UPDATE users SET language = ? WHERE user_id = ?', (language, user_id))
    conn.commit()
    conn.close()

# Til tanlash tugmalari
def get_language_keyboard():
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="🇺🇿 O'zbek", callback_data="lang_uz"),
        InlineKeyboardButton(text="🇷🇺 Русский", callback_data="lang_ru")
    )
    builder.row(
        InlineKeyboardButton(text="🇺🇸 English", callback_data="lang_en")
    )
    return builder.as_markup()

# Matnni olish
async def get_text(user_id, key):
    language = await get_user_language(user_id)
    return LANGUAGES.get(language, LANGUAGES['uz']).get(key, LANGUAGES['uz'][key])

# Foydalanuvchini ma'lumotlar bazasiga qo'shish
async def add_user(user_id, username=None, first_name=None):
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _add_user_sync, user_id, username, first_name)

def _add_user_sync(user_id, username, first_name):
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT OR IGNORE INTO users (user_id, username, first_name, language)
        VALUES (?, ?, ?, ?)
    ''', (user_id, username, first_name, 'uz'))
    
    today = datetime.now().date()
    if cursor.rowcount > 0:
        # Yangi foydalanuvchi
        cursor.execute('''
            INSERT OR REPLACE INTO daily_stats (date, new_users, active_users, downloads)
            VALUES (?,
                COALESCE((SELECT new_users FROM daily_stats WHERE date = ?), 0) + 1,
                COALESCE((SELECT active_users FROM daily_stats WHERE date = ?), 0) + 1,
                COALESCE((SELECT downloads FROM daily_stats WHERE date = ?), 0)
            )
        ''', (today, today, today, today))
    else:
        # Mavjud foydalanuvchi
        cursor.execute('''
            INSERT OR REPLACE INTO daily_stats (date, new_users, active_users, downloads)
            VALUES (?,
                COALESCE((SELECT new_users FROM daily_stats WHERE date = ?), 0),
                COALESCE((SELECT active_users FROM daily_stats WHERE date = ?), 0) + 1,
                COALESCE((SELECT downloads FROM daily_stats WHERE date = ?), 0)
            )
        ''', (today, today, today, today))
    
    conn.commit()
    conn.close()

# Majburiy kanallarni tekshirish
async def check_user_subscription(user_id: int) -> bool:
    channels = get_required_channels()
    
    if not channels:
        return True
    
    # Parallel ravishda barcha kanallarni tekshirish
    tasks = []
    for channel_id, _ in channels:
        task = _check_single_channel(user_id, channel_id)
        tasks.append(task)
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Agar biron bir kanal False qaytarsa, False qaytaramiz
    for result in results:
        if isinstance(result, bool) and not result:
            return False
    
    return True

async def _check_single_channel(user_id, channel_id):
    try:
        member = await bot.get_chat_member(channel_id, user_id)
        return member.status in [ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR]
    except Exception:
        return True  # Xato bo'lsa, true qaytaramiz

# Obuna tugmalarini yaratish
async def get_subscription_keyboard(user_id):
    channels = get_required_channels()
    
    if not channels:
        return None
    
    builder = InlineKeyboardBuilder()
    for channel_id, channel_name in channels:
        builder.row(
            InlineKeyboardButton(
                text=f"📢 {channel_name}", 
                url=f"https://t.me/{channel_id.replace('@', '')}"
            )
        )
    
    check_text = await get_text(user_id, 'subscription_check')
    builder.row(
        InlineKeyboardButton(text=check_text, callback_data="check_subscription")
    )
    
    return builder.as_markup()

# Video linkini aniqlash
@lru_cache(maxsize=50)
def detect_platform(url):
    patterns = {
        'tiktok': r'(?:tiktok.com|vm.tiktok.com)',
        'youtube': r'(?:youtube.com|youtu.be)',
        'facebook': r'(?:facebook.com|fb.watch)',
        'instagram': r'instagram.com'
    }
    
    for platform, pattern in patterns.items():
        if re.search(pattern, url, re.IGNORECASE):
            return platform
    return None

# Video sifat tugmalarini yaratish
async def get_quality_keyboard(platform, url, user_id):
    # URL ni base64 qilib encode qilamiz uzunlik muammosini oldini olish uchun
    encoded_url = urllib.parse.quote(url, safe='')
    
    builder = InlineKeyboardBuilder()
    
    if platform == 'youtube':
        quality_720 = await get_text(user_id, 'quality_720')
        quality_480 = await get_text(user_id, 'quality_480')
        quality_360 = await get_text(user_id, 'quality_360')
        quality_audio = await get_text(user_id, 'quality_audio')
        
        builder.row(InlineKeyboardButton(text=quality_720, callback_data=f"dl_720_{encoded_url}"))
        builder.row(InlineKeyboardButton(text=quality_480, callback_data=f"dl_480_{encoded_url}"))
        builder.row(InlineKeyboardButton(text=quality_360, callback_data=f"dl_360_{encoded_url}"))
        builder.row(InlineKeyboardButton(text=quality_audio, callback_data=f"dl_audio_{encoded_url}"))
    else:
        quality_high = await get_text(user_id, 'quality_high')
        quality_medium = await get_text(user_id, 'quality_medium')
        quality_low = await get_text(user_id, 'quality_low')
        
        builder.row(InlineKeyboardButton(text=quality_high, callback_data=f"dl_high_{encoded_url}"))
        builder.row(InlineKeyboardButton(text=quality_medium, callback_data=f"dl_medium_{encoded_url}"))
        builder.row(InlineKeyboardButton(text=quality_low, callback_data=f"dl_low_{encoded_url}"))
    
    return builder.as_markup()

# Video yuklab olish
async def download_video(url, quality='best'):
    def _download():
        try:
            temp_dir = tempfile.mkdtemp()
            
            ydl_opts = {
                'outtmpl': f'{temp_dir}/%(title)s.%(ext)s',
                'writesubtitles': False,
                'writeautomaticsub': False,
                'noplaylist': True,
                'extract_flat': False,
                'ignoreerrors': False,
                'no_warnings': True,
                'quiet': True,
                'extractaudio': False,
                'audioformat': 'mp3',
                'embed_subs': False,
                'writeinfojson': False,
                'writethumbnail': False,
            }
            
            # Sifat sozlamalari
            format_map = {
                '720': 'best[height<=720]/best[width<=1280]/best',
                '480': 'best[height<=480]/best[width<=854]/best',
                '360': 'best[height<=360]/best[width<=640]/best',
                'audio': 'bestaudio[ext=m4a]/bestaudio/best',
                'high': 'best[filesize<50M]/best',
                'medium': 'best[height<=720][filesize<30M]/best[height<=480]',
                'low': 'worst[height>=240]/worst'
            }
            
            ydl_opts['format'] = format_map.get(quality, 'best[filesize<100M]/best')
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=True)
                filename = ydl.prepare_filename(info)
                
                return {
                    'success': True,
                    'filename': filename,
                    'title': info.get('title', 'Unknown')[:50] + '...' if len(info.get('title', '')) > 50 else info.get('title', 'Unknown'),
                    'duration': info.get('duration', 0),
                    'temp_dir': temp_dir
                }
                
        except Exception as e:
            return {
                'success': False,
                'error': str(e)[:100] + '...' if len(str(e)) > 100 else str(e)
            }
    
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(executor, _download)
    return result

# Statistikani yangilash
async def update_download_stats():
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _update_download_stats_sync)

def _update_download_stats_sync():
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    today = datetime.now().date()
    cursor.execute('''
        INSERT OR REPLACE INTO daily_stats (date, new_users, active_users, downloads)
        VALUES (?,
            COALESCE((SELECT new_users FROM daily_stats WHERE date = ?), 0),
            COALESCE((SELECT active_users FROM daily_stats WHERE date = ?), 0),
            COALESCE((SELECT downloads FROM daily_stats WHERE date = ?), 0) + 1
        )
    ''', (today, today, today, today))
    conn.commit()
    conn.close()

# Yangi foydalanuvchini tekshirish
async def _check_new_user(user_id):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _check_new_user_sync, user_id)

def _check_new_user_sync(user_id):
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    cursor.execute('SELECT join_date FROM users WHERE user_id = ?', (user_id,))
    result = cursor.fetchone()
    conn.close()
    
    if result:
        join_date = datetime.fromisoformat(result[0])
        now = datetime.now()
        # Agar 1 daqiqadan kam vaqt o'tgan bo'lsa, yangi foydalanuvchi
        return (now - join_date).total_seconds() < 60
    return True

# Handlerlar
@dp.message(CommandStart())
async def start_handler(message: types.Message):
    user = message.from_user
    await add_user(user.id, user.username, user.first_name)
    
    # Foydalanuvchi tilini tekshirish
    user_language = await get_user_language(user.id)
    
    # Agar til tanlanmagan bo'lsa, til tanlash tugmalarini ko'rsatish
    if not user_language or user_language == 'uz':
        # Yangi foydalanuvchi uchun til tanlash
        new_user_check = await _check_new_user(user.id)
        if new_user_check:
            choose_lang_text = LANGUAGES['uz']['choose_language']
            keyboard = get_language_keyboard()
            await message.answer(choose_lang_text, reply_markup=keyboard)
            return
    
    # Obunani tekshirish
    if not await check_user_subscription(user.id):
        keyboard = await get_subscription_keyboard(user.id)
        if keyboard:
            subscription_text = await get_text(user.id, 'subscription_required')
            await message.answer(subscription_text, reply_markup=keyboard)
            return
    
    welcome_text = await get_text(user.id, 'welcome')
    await message.answer(welcome_text)

@dp.message(Command("admin"))
async def admin_panel(message: types.Message):
    user_id = message.from_user.id
    
    if user_id not in ADMIN_IDS:
        admin_only_text = await get_text(user_id, 'admin_only')
        await message.answer(admin_only_text)
        return
    
    stats_text = await get_text(user_id, 'stats')
    broadcast_text = await get_text(user_id, 'broadcast')
    channels_text = await get_text(user_id, 'channels')
    users_text = await get_text(user_id, 'users')
    
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text=stats_text, callback_data="admin_stats"))
    builder.row(InlineKeyboardButton(text=broadcast_text, callback_data="admin_broadcast"))
    builder.row(InlineKeyboardButton(text=channels_text, callback_data="admin_channels"))
    builder.row(InlineKeyboardButton(text=users_text, callback_data="admin_users"))
    
    admin_panel_text = await get_text(user_id, 'admin_panel')
    await message.answer(admin_panel_text, reply_markup=builder.as_markup())

@dp.message(Command("stats"))
async def show_stats(message: types.Message):
    user_id = message.from_user.id
    
    if user_id not in ADMIN_IDS:
        return
    
    loop = asyncio.get_event_loop()
    stats = await loop.run_in_executor(None, _get_stats_sync)
    
    stats_message = f"""📊 Bot Statistikasi:

👥 Jami foydalanuvchilar: {stats['total_users']}
📅 Bugungi yangi foydalanuvchilar: {stats['today_new']}
🔥 Bugungi faol foydalanuvchilar: {stats['today_active']}
⬇️ Bugungi yuklab olishlar: {stats['today_downloads']}

📈 Oxirgi 7 kun:
👥 Yangi foydalanuvchilar: {stats['week_new']}
⬇️ Yuklab olishlar: {stats['week_downloads']}"""

    await message.answer(stats_message)

def _get_stats_sync():
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    
    # Jami foydalanuvchilar
    cursor.execute('SELECT COUNT(*) FROM users')
    total_users = cursor.fetchone()[0]
    
    # Bugungi statistika
    today = datetime.now().date()
    cursor.execute('SELECT new_users, active_users, downloads FROM daily_stats WHERE date = ?', (today,))
    today_stats = cursor.fetchone()
    
    if today_stats:
        today_new, today_active, today_downloads = today_stats
    else:
        today_new = today_active = today_downloads = 0
    
    # Oxirgi 7 kun statistikasi
    week_ago = today - timedelta(days=7)
    cursor.execute('SELECT SUM(new_users), SUM(downloads) FROM daily_stats WHERE date >= ?', (week_ago,))
    week_stats = cursor.fetchone()
    
    week_new = week_stats[0] if week_stats[0] else 0
    week_downloads = week_stats[1] if week_stats[1] else 0
    
    conn.close()
    
    return {
        'total_users': total_users,
        'today_new': today_new,
        'today_active': today_active,
        'today_downloads': today_downloads,
        'week_new': week_new,
        'week_downloads': week_downloads
    }

@dp.message(F.text)
async def handle_message(message: types.Message):
    user = message.from_user
    message_text = message.text
    
    # Obunani tekshirish
    if not await check_user_subscription(user.id):
        keyboard = await get_subscription_keyboard(user.id)
        if keyboard:
            subscription_text = await get_text(user.id, 'subscription_required')
            await message.answer(subscription_text, reply_markup=keyboard)
            return
    
    # URL tekshirish
    if not (message_text.startswith('http://') or message_text.startswith('https://')):
        send_link_text = await get_text(user.id, 'send_link')
        await message.answer(send_link_text)
        return
    
    platform = detect_platform(message_text)
    
    if not platform:
        unknown_platform_text = await get_text(user.id, 'unknown_platform')
        await message.answer(unknown_platform_text)
        return
    
    # Sifat tanlash tugmalari
    keyboard = await get_quality_keyboard(platform, message_text, user.id)
    
    platform_names = {
        'tiktok': 'TikTok',
        'youtube': 'YouTube',
        'facebook': 'Facebook',
        'instagram': 'Instagram'
    }
    
    choose_quality_text = await get_text(user.id, 'choose_quality')
    await message.answer(
        f"📱 {platform_names[platform]} {choose_quality_text}",
        reply_markup=keyboard
    )

@dp.callback_query(F.data.startswith("lang_"))
async def language_callback(callback: types.CallbackQuery):
    language = callback.data.split("_")[1]
    await set_user_language(callback.from_user.id, language)
    
    language_changed_text = await get_text(callback.from_user.id, 'language_changed')
    await callback.message.edit_text(language_changed_text)
    
    # Welcome xabarini yuborish
    welcome_text = await get_text(callback.from_user.id, 'welcome')
    await callback.message.answer(welcome_text)
    await callback.answer()

@dp.callback_query(F.data == "check_subscription")
async def check_subscription_callback(callback: types.CallbackQuery):
    if await check_user_subscription(callback.from_user.id):
        success_text = await get_text(callback.from_user.id, 'subscription_success')
        await callback.message.edit_text(success_text)
        
        # Welcome xabarini yuborish
        welcome_text = await get_text(callback.from_user.id, 'welcome')
        await callback.message.answer(welcome_text)
    else:
        failed_text = await get_text(callback.from_user.id, 'subscription_failed')
        await callback.answer(failed_text, show_alert=True)

@dp.callback_query(F.data.startswith("dl_"))
async def download_callback(callback: types.CallbackQuery):
    parts = callback.data.split("_", 2)
    quality = parts[1]
    encoded_url = parts[2]
    url = urllib.parse.unquote(encoded_url)
    
    # Yuklanish jarayoni haqida xabar
    downloading_text = await get_text(callback.from_user.id, 'downloading')
    await callback.message.edit_text(downloading_text)
    
    try:
        # Video yuklab olish
        result = await download_video(url, quality)
        
        if result['success']:
            # Fayl yuborish
            try:
                # Fayl hajmini tekshirish
                file_size = os.path.getsize(result['filename'])
                if file_size > 50 * 1024 * 1024:  # 50MB
                    large_file_text = await get_text(callback.from_user.id, 'file_too_large')
                    await callback.message.edit_text(large_file_text)
                else:
                    video_file = FSInputFile(result['filename'])
                    caption = f"🎬 {result['title']}\n📤 @{(await bot.get_me()).username}"
                    
                    await bot.send_video(
                        chat_id=callback.message.chat.id,
                        video=video_file,
                        caption=caption,
                        supports_streaming=True
                    )
                    await callback.message.delete()
            except Exception as e:
                send_error_text = await get_text(callback.from_user.id, 'send_error')
                await callback.message.edit_text(f"{send_error_text} {str(e)[:100]}")
            
            # Vaqtinchalik fayllarni tozalash
            shutil.rmtree(result['temp_dir'], ignore_errors=True)
            
            # Statistikani yangilash
            await update_download_stats()
            
        else:
            error_text = await get_text(callback.from_user.id, 'download_error')
            await callback.message.edit_text(f"{error_text} {result['error']}")
            
    except Exception as e:
        send_error_text = await get_text(callback.from_user.id, 'send_error')
        await callback.message.edit_text(f"{send_error_text} {str(e)[:100]}")
    
    await callback.answer()

@dp.callback_query(F.data.startswith("admin_"))
async def admin_callback(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    
    if user_id not in ADMIN_IDS:
        await callback.answer("❌ Ruxsat yo'q!", show_alert=True)
        return
    
    action = callback.data.replace("admin_", "")
    
    if action == "stats":
        loop = asyncio.get_event_loop()
        stats = await loop.run_in_executor(None, _get_stats_sync)
        
        stats_message = f"""📊 Bot Statistikasi:

👥 Jami foydalanuvchilar: {stats['total_users']}
📅 Bugungi yangi foydalanuvchilar: {stats['today_new']}
🔥 Bugungi faol foydalanuvchilar: {stats['today_active']}
⬇️ Bugungi yuklab olishlar: {stats['today_downloads']}

📈 Oxirgi 7 kun:
👥 Yangi foydalanuvchilar: {stats['week_new']}
⬇️ Yuklab olishlar: {stats['week_downloads']}"""

        await callback.message.edit_text(stats_message)
    
    elif action == "broadcast":
        await callback.message.edit_text("📢 Barcha foydalanuvchilarga yuborish uchun xabarni yozing:")
        await state.set_state(UserStates.waiting_broadcast)
    
    elif action == "channels":
        channels = get_required_channels()
        if channels:
            channel_list = "\n".join([f"• {name} (@{channel_id})" for channel_id, name in channels])
            await callback.message.edit_text(f"📢 Majburiy kanallar:\n\n{channel_list}")
        else:
            await callback.message.edit_text("📢 Hozircha majburiy kanallar yo'q")
    
    elif action == "users":
        loop = asyncio.get_event_loop()
        user_count = await loop.run_in_executor(None, _get_user_count_sync)
        await callback.message.edit_text(f"👥 Jami foydalanuvchilar: {user_count}")
    
    await callback.answer()

def _get_user_count_sync():
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM users WHERE is_active = TRUE')
    count = cursor.fetchone()[0]
    conn.close()
    return count

@dp.message(UserStates.waiting_broadcast)
async def broadcast_message(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    broadcast_text = message.text
    
    # Barcha foydalanuvchilarni olish
    loop = asyncio.get_event_loop()
    users = await loop.run_in_executor(None, _get_all_users_sync)
    
    sent_count = 0
    failed_count = 0
    
    status_message = await message.answer(f"📢 Xabar yuborilmoqda...\n✅ Yuborildi: {sent_count}\n❌ Xato: {failed_count}")
    
    for user_id in users:
        try:
            await bot.send_message(user_id, broadcast_text)
            sent_count += 1
        except Exception:
            failed_count += 1
        
        # Har 10 ta xabardan keyin statusni yangilash
        if (sent_count + failed_count) % 10 == 0:
            try:
                await status_message.edit_text(
                    f"📢 Xabar yuborilmoqda...\n✅ Yuborildi: {sent_count}\n❌ Xato: {failed_count}"
                )
            except:
                pass
    
    # Yakuniy natija
    await status_message.edit_text(
        f"📢 Xabar yuborish tugadi!\n✅ Yuborildi: {sent_count}\n❌ Xato: {failed_count}"
    )
    
    await state.clear()

def _get_all_users_sync():
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    cursor.execute('SELECT user_id FROM users WHERE is_active = TRUE')
    users = [row[0] for row in cursor.fetchall()]
    conn.close()
    return users

# Webhook setup
async def on_startup(app: Application):
    """Startup event handler"""
    webhook_url = f"{WEBHOOK_URL}{WEBHOOK_PATH}"
    await bot.set_webhook(webhook_url)
    logger.info(f"Webhook set to {webhook_url}")

async def on_shutdown(app: Application):
    """Shutdown event handler"""
    await bot.delete_webhook()
    logger.info("Webhook deleted")

def create_app():
    """Create aiohttp application"""
    app = web.Application()
    
    # Webhook handler
    webhook_requests_handler = SimpleRequestHandler(
        dispatcher=dp,
        bot=bot,
    )
    webhook_requests_handler.register(app, path=WEBHOOK_PATH)
    
    # Startup/shutdown events
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    
    # Health check endpoint
    async def health_check(request):
        return web.json_response({"status": "healthy", "timestamp": datetime.now().isoformat()})
    
    app.router.add_get('/health', health_check)
    app.router.add_get('/', lambda r: web.json_response({"status": "Bot is running!"}))
    
    return app

async def main():
    """Main function"""
    # Ma'lumotlar bazasini ishga tushirish
    init_database()
    
    # App yaratish
    app = create_app()
    
    # Bot application ni initialize qilish
    await dp.start_polling(bot)

if __name__ == '__main__':
    try:
        # Webhook mode uchun
        if WEBHOOK_URL:
            app = create_app()
            web.run_app(app, host="0.0.0.0", port=PORT)
        else:
            # Polling mode uchun
            asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Critical error: {e}")
        time.sleep(5)
        # Bot ni qayta ishga tushirishga harakat qilish
        if WEBHOOK_URL:
            app = create_app()
            web.run_app(app, host="0.0.0.0", port=PORT)
        else:
            asyncio.run(main())
