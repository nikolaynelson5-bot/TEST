import discord
import requests
import csv
import io
import asyncio
import json
import os
from datetime import datetime, date, timedelta
import re

# ⚠️ ВАЖНО: Токен нужно заменить! Этот токен скомпрометирован
TOKEN = os.getenv("MTQ3MzAyODQ0NzA3NjA5NDAxMw.GvPL4r.1QhQ1d21Ol0rM4MO6Rs-YzKjg-ZPWg79QDjypM")  # Получите новый токен в Discord Developer Portal

CSV_URL = "https://docs.google.com/spreadsheets/d/18LXeD2pB0YpsfsPWPhe0PXgNKVL4L35gXykD3eNcqV8/export?format=csv"

# ID канала для уведомлений с тегами (ваши уведомления)
NOTIFICATION_CHANNEL_ID = 1480301687402008696
# ID канала для лога модерации (ВСЕ ИЗМЕНЕНИЯ)
MODERATION_LOG_CHANNEL_ID = 1474337639137153077

# ID канала для напоминаний
REMINDER_CHANNEL_ID = 1480618232611344476

# Кто будет получать уведомления (ваш тег)
NOTIFY_USERS = [
    1364992552616329349,  # Ваш Discord ID
]

NOTIFY_ROLE_ID = None
PREVIOUS_STATE_FILE = "previous_state.json"
REMINDERS_FILE = "reminders.json"

intents = discord.Intents.default()
intents.message_content = True
bot = discord.Client(intents=intents)

notification_channel = None
moderation_channel = None
reminder_channel = None
first_run = True

COLUMN_LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

# Загружаем напоминания при старте
reminders = []

def load_reminders():
    """Загружает напоминания из файла"""
    global reminders
    if not os.path.exists(REMINDERS_FILE):
        reminders = []
        return
    try:
        with open(REMINDERS_FILE, 'r', encoding='utf-8') as f:
            reminders = json.load(f)
        print(f"📅 Загружено {len(reminders)} напоминаний")
    except:
        reminders = []

def save_reminders():
    """Сохраняет напоминания в файл"""
    try:
        with open(REMINDERS_FILE, 'w', encoding='utf-8') as f:
            json.dump(reminders, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"❌ Ошибка сохранения напоминаний: {e}")

@bot.event
async def on_ready():
    global notification_channel, moderation_channel, reminder_channel, first_run
    print(f"✅ Бот {bot.user} запущен!")
    
    notification_channel = bot.get_channel(NOTIFICATION_CHANNEL_ID)
    moderation_channel = bot.get_channel(MODERATION_LOG_CHANNEL_ID)
    reminder_channel = bot.get_channel(REMINDER_CHANNEL_ID)
    
    if notification_channel:
        print(f"📢 Канал уведомлений: #{notification_channel.name}")
    else:
        print(f"❌ Канал уведомлений с ID {NOTIFICATION_CHANNEL_ID} не найден!")
    
    if moderation_channel:
        print(f"📝 Канал модерации: #{moderation_channel.name}")
    else:
        print(f"❌ Канал модерации с ID {MODERATION_LOG_CHANNEL_ID} не найден!")
    
    if reminder_channel:
        print(f"⏰ Канал напоминаний: #{reminder_channel.name}")
    else:
        print(f"❌ Канал напоминаний с ID {REMINDER_CHANNEL_ID} не найден!")
    
    load_reminders()
    
    bot.loop.create_task(check_changes_periodically())
    bot.loop.create_task(check_reminders_periodically())

async def check_reminders_periodically():
    """Проверяет напоминания каждую минуту"""
    await bot.wait_until_ready()
    
    while not bot.is_closed():
        try:
            await check_reminders()
            await asyncio.sleep(60)
        except Exception as e:
            print(f"Ошибка в check_reminders_periodically: {e}")
            await asyncio.sleep(60)

async def check_reminders():
    """Проверяет и отправляет просроченные напоминания"""
    global reminders
    
    if not reminders or not reminder_channel:
        return
    
    now = datetime.now()
    to_remove = []
    
    for i, reminder in enumerate(reminders):
        try:
            reminder_date = datetime.strptime(reminder['datetime'], "%d.%m.%Y %H:%M")
            
            if reminder_date <= now:
                mentions = " ".join([f"<@{user_id}>" for user_id in reminder.get('users', NOTIFY_USERS)])
                
                message = f"{mentions}\n```\n⏰ НАПОМИНАНИЕ\n"
                message += f"📅 {reminder['datetime']}\n"
                message += f"📝 {reminder['text']}\n"
                if 'author' in reminder:
                    message += f"👤 от {reminder['author']}\n"
                message += f"```"
                
                await reminder_channel.send(message)
                print(f"✅ Отправлено напоминание: {reminder['text']}")
                
                to_remove.append(i)
                
        except Exception as e:
            print(f"❌ Ошибка проверки напоминания: {e}")
    
    if to_remove:
        for i in sorted(to_remove, reverse=True):
            reminders.pop(i)
        save_reminders()

async def check_changes_periodically():
    """Проверяет изменения каждые 30 секунд"""
    await bot.wait_until_ready()
    
    while not bot.is_closed():
        try:
            await check_all_changes()
            await asyncio.sleep(30)
        except Exception as e:
            print(f"Ошибка: {e}")
            await asyncio.sleep(30)

async def get_current_blacklist_with_details():
    """Получает текущие данные из таблицы"""
    try:
        response = requests.get(CSV_URL, timeout=10)
        response.encoding = 'utf-8'
        
        if response.status_code != 200:
            return []
        
        csv_content = response.text
        reader = csv.DictReader(io.StringIO(csv_content))
        current_rows = list(reader)
        
        current_players = []
        
        for i, row in enumerate(current_rows, start=2):
            игровой_ник = row.get("Игровой ник", "")
            if not игровой_ник:
                continue
            
            актуальность = row.get("Актуальность", "").lower()
            дата_снятия = row.get("Дата снятия", "").strip()
            
            is_in_blacklist = True
            
            if "навсегда" in актуальность:
                is_in_blacklist = False
            elif "вынесен из чс" in дата_снятия.lower() or "амнистия" in дата_снятия.lower():
                is_in_blacklist = False
            
            player_data = {
                "ник": игровой_ник,
                "дискорд": row.get("Дискорд юз", "-"),
                "причина": row.get("Причина", "-"),
                "дата_снятия": row.get("Дата снятия", "-"),
                "кто_выдал": row.get("Кто выдал", "-"),
                "актуальность": row.get("Актуальность", "-"),
                "организация": row.get("Организация", "-"),
                "строка": i,
                "is_in_blacklist": is_in_blacklist
            }
            
            current_players.append(player_data)
        
        return current_players
        
    except Exception as e:
        print(f"Ошибка загрузки: {e}")
        return []

async def check_all_changes():
    """Проверяет изменения и отправляет в канал"""
    global first_run
    
    try:
        current_players = await get_current_blacklist_with_details()
        
        if not current_players:
            return
        
        previous_state = load_previous_state()
        
        if not previous_state:
            print("📝 Первый запуск - сохраняю состояние без уведомлений")
            save_current_state(current_players)
            return
        
        prev_by_nick = {p["ник"].lower(): p for p in previous_state}
        curr_by_nick = {p["ник"].lower(): p for p in current_players}
        
        added = []
        removed = []
        changed = []
        exited = []
        
        for nick, curr in curr_by_nick.items():
            if nick not in prev_by_nick:
                added.append(curr)
                print(f"➕ ДОБАВЛЕН: {curr['ник']}")
            else:
                prev = prev_by_nick[nick]
                
                if prev["is_in_blacklist"] and not curr["is_in_blacklist"]:
                    exited.append(curr)
                    print(f"👤 ВЫШЕЛ: {curr['ник']}")
                
                changes = []
                for field in ["причина", "дата_снятия", "кто_выдал", "актуальность", "организация", "дискорд"]:
                    if curr.get(field) != prev.get(field):
                        changes.append({
                            "поле": field,
                            "было": prev.get(field, "-"),
                            "стало": curr.get(field, "-")
                        })
                
                if changes:
                    changed.append({
                        "ник": curr['ник'],
                        "строка": curr['строка'],
                        "изменения": changes
                    })
                    print(f"✏️ ИЗМЕНЕНО: {curr['ник']}")
        
        for nick, prev in prev_by_nick.items():
            if nick not in curr_by_nick:
                removed.append(prev)
                print(f"➖ УДАЛЕН: {prev['ник']}")
        
        save_current_state(current_players)
        
        if added or removed or changed or exited:
            print(f"📢 ОБНАРУЖЕНЫ ИЗМЕНЕНИЯ! Отправляю в канал...")
            
            if moderation_channel:
                await send_all_changes_one_message(added, removed, changed, exited)
            
            if exited and notification_channel:
                await send_to_notification_channel(exited)
        
    except Exception as e:
        print(f"Ошибка: {e}")

def load_previous_state():
    """Загружает предыдущее состояние из файла"""
    if not os.path.exists(PREVIOUS_STATE_FILE):
        return []
    try:
        with open(PREVIOUS_STATE_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except:
        return []

def save_current_state(players):
    """Сохраняет текущее состояние в файл"""
    try:
        with open(PREVIOUS_STATE_FILE, 'w', encoding='utf-8') as f:
            json.dump(players, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Ошибка сохранения: {e}")

async def send_all_changes_one_message(added, removed, changed, exited):
    """Отправляет ВСЕ изменения ОДНИМ сообщением в канал модерации"""
    if not moderation_channel:
        return
    
    try:
        message = "```\n"
        message += f"📊 ИЗМЕНЕНИЯ В ТАБЛИЦЕ\n"
        message += f"🕐 {datetime.now().strftime('%H:%M:%S')}\n"
        
        stats_parts = []
        if added: stats_parts.append(f"✅ +{len(added)}")
        if removed: stats_parts.append(f"❌ -{len(removed)}")
        if changed: stats_parts.append(f"✏️ {len(changed)}")
        if exited: stats_parts.append(f"👤 {len(exited)}")
        
        if stats_parts:
            message += f"{' | '.join(stats_parts)}\n"
        
        message += f"\n"
        
        for player in changed:
            message += f"✏️ ИЗМЕНЕН: {player['ник']} (стр.{player['строка']})\n"
            for change in player['изменения']:
                message += f"{change['поле']}: {change['было']} → {change['стало']}\n"
            message += f"\n"
        
        for player in added:
            message += f"✅ ДОБАВЛЕН: {player['ник']} (стр.{player['строка']})\n"
            message += f"  Discord: {player['дискорд']}\n"
            message += f"  Причина: {player['причина'][:100]}\n"
            message += f"  Дата снятия: {player['дата_снятия']}\n"
            message += f"  Выдал: {player['кто_выдал']}\n"
            message += f"  Актуальность: {player['актуальность']}\n\n"
        
        for player in removed:
            message += f"❌ УДАЛЕН: {player['ник']} (стр.{player['строка']})\n\n"
        
        for player in exited:
            message += f"👤 ВЫШЕЛ ИЗ ЧС: {player['ник']} (стр.{player['строка']})\n"
            message += f"  Discord: {player['дискорд']}\n"
            message += f"  Дата снятия: {player['дата_снятия']}\n\n"
        
        message += "```"
        
        if len(message) <= 2000:
            await moderation_channel.send(message)
        else:
            header = f"```\n📊 ИЗМЕНЕНИЯ В ТАБЛИЦЕ\n🕐 {datetime.now().strftime('%H:%M:%S')}\n"
            if stats_parts:
                header += f"{' | '.join(stats_parts)}\n"
            header += "```"
            await moderation_channel.send(header)
            
            changes_message = "```\n"
            for player in changed:
                part = f"✏️ ИЗМЕНЕН: {player['ник']} (стр.{player['строка']})\n"
                for change in player['изменения']:
                    part += f"{change['поле']}: {change['было']} → {change['стало']}\n"
                part += f"\n"
                
                if len(changes_message + part) > 1900:
                    changes_message += "```"
                    await moderation_channel.send(changes_message)
                    await asyncio.sleep(1)
                    changes_message = "```\n" + part
                else:
                    changes_message += part
            
            if changes_message != "```\n":
                changes_message += "```"
                await moderation_channel.send(changes_message)
        
    except Exception as e:
        print(f"❌ Ошибка отправки: {e}")

async def send_to_notification_channel(exited_players):
    """Отправляет в канал с тегами"""
    if not notification_channel:
        return
    
    try:
        mentions = " ".join([f"<@{user_id}>" for user_id in NOTIFY_USERS])
        
        message = f"{mentions}\n```\n"
        message += f"👤 ВЫХОД ИЗ ЧС\n"
        message += f"Количество: {len(exited_players)}\n\n"
        
        for player in exited_players:
            message += f"• {player['ник']} (стр.{player['строка']})\n"
            message += f"  Discord: {player['дискорд']}\n"
            message += f"  Дата: {player['дата_снятия']}\n"
            message += f"  Причина: {player['причина'][:50]}\n\n"
        
        message += f"```"
        
        await notification_channel.send(message)
        
    except Exception as e:
        print(f"Ошибка: {e}")

# =============== ВСЕ КОМАНДЫ ===============

@bot.event
async def on_message(message):
    if message.author == bot.user:
        return
    
    # Тестовая команда для проверки канала напоминаний
    if message.content == "!test_remind":
        if reminder_channel:
            test_msg = "```\n🧪 ТЕСТ КАНАЛА НАПОМИНАНИЙ\nКанал работает!\n```"
            await reminder_channel.send(test_msg)
            await message.channel.send(f"✅ Тест отправлен в канал напоминаний!")
        else:
            await message.channel.send("❌ Канал напоминаний не найден!")
        return
    
    # Тестовая команда для проверки канала модерации
    if message.content == "!test_mod":
        if moderation_channel:
            test_msg = "```\n📊 ИЗМЕНЕНИЯ В ТАБЛИЦЕ\n🕐 18:15:22\n✏️ ИЗМЕНЕН: тест (стр.696)\nактуальность: В ЧС → Вынесен из ЧС\n```"
            await moderation_channel.send(test_msg)
            await message.channel.send("✅ Тест отправлен в канал модерации!")
        else:
            await message.channel.send("❌ Канал модерации не найден!")
        return
    
    # ⏰ КОМАНДА ДЛЯ НАПОМИНАНИЙ
    if message.content.startswith("!remind"):
        try:
            text = message.content[7:].strip()
            
            date_match = re.search(r'(\d{2}\.\d{2}\.\d{4})', text)
            time_match = re.search(r'(\d{2}:\d{2})', text)
            quote_match = re.search(r'"([^"]*)"', text)
            
            if not date_match:
                await message.channel.send("❌ Неправильный формат! Используйте: !remind ДД.ММ.ГГГГ ЧЧ:ММ \"текст\"")
                return
            
            date_str = date_match.group(1)
            
            if time_match:
                time_str = time_match.group(1)
                datetime_str = f"{date_str} {time_str}"
            else:
                datetime_str = f"{date_str} 00:00"
            
            if quote_match:
                reminder_text = quote_match.group(1)
            else:
                parts = text.split()
                if time_match:
                    reminder_text = ' '.join(parts[2:])
                else:
                    reminder_text = ' '.join(parts[1:])
            
            if not reminder_text:
                reminder_text = "Напоминание"
            
            try:
                reminder_datetime = datetime.strptime(datetime_str, "%d.%m.%Y %H:%M")
                if reminder_datetime < datetime.now():
                    await message.channel.send("❌ Нельзя установить напоминание на прошлое!")
                    return
            except ValueError:
                await message.channel.send("❌ Неправильный формат даты или времени!")
                return
            
            reminder = {
                "datetime": datetime_str,
                "text": reminder_text,
                "author": str(message.author),
                "author_id": message.author.id,
                "users": NOTIFY_USERS,
                "created": datetime.now().strftime("%d.%m.%Y %H:%M:%S")
            }
            
            reminders.append(reminder)
            save_reminders()
            
            response = f"```\n✅ НАПОМИНАНИЕ УСТАНОВЛЕНО\n"
            response += f"📅 {datetime_str}\n"
            response += f"📝 {reminder_text}\n"
            response += f"```"
            
            await message.channel.send(response)
            print(f"✅ Установлено напоминание: {datetime_str} - {reminder_text}")
            
        except Exception as e:
            await message.channel.send(f"❌ Ошибка: {e}")
            print(f"Ошибка в !remind: {e}")
        
        return
    
    # Команда для просмотра напоминаний
    if message.content == "!reminders":
        if not reminders:
            await message.channel.send("📭 Нет активных напоминаний")
            return
        
        response = "```\n📅 АКТИВНЫЕ НАПОМИНАНИЯ\n\n"
        for i, r in enumerate(reminders, 1):
            response += f"{i}. {r['datetime']}\n"
            response += f"   {r['text']}\n"
            response += f"   от {r['author']}\n\n"
        response += f"Всего: {len(reminders)}\n```"
        
        if len(response) <= 2000:
            await message.channel.send(response)
        else:
            parts = [response[i:i+1990] for i in range(0, len(response), 1990)]
            for part in parts:
                await message.channel.send(part)
        
        return
    
    # Команда для удаления напоминания
    if message.content.startswith("!remind_remove"):
        try:
            num = int(message.content[14:].strip())
            if 1 <= num <= len(reminders):
                removed = reminders.pop(num - 1)
                save_reminders()
                await message.channel.send(f"✅ Напоминание удалено: {removed['datetime']} - {removed['text']}")
            else:
                await message.channel.send(f"❌ Напоминание с номером {num} не найдено")
        except:
            await message.channel.send("❌ Используйте: !remind_remove НОМЕР")
        
        return
    
    # Команда для очистки всех напоминаний
    if message.content == "!remind_clear" and message.author.id in NOTIFY_USERS:
        reminders.clear()
        save_reminders()
        await message.channel.send("✅ Все напоминания удалены")
        return
    
    # =============== ИСПРАВЛЕННАЯ КОМАНДА !next ===============
    if message.content.startswith("!next"):
        await message.channel.send("📅 Проверяю записи, которые должны выйти из черного списка сегодня...")
        
        try:
            response = requests.get(CSV_URL, timeout=10)
            response.encoding = 'utf-8'
            response.raise_for_status()
            
            csv_content = response.text
            reader = csv.DictReader(io.StringIO(csv_content))
            all_rows = list(reader)
            
            if not all_rows:
                await message.channel.send("❌ Таблица пуста")
                return
            
            today = date.today()
            today_str = today.strftime("%d.%m.%Y")
            
            exiting_today = []
            
            for i, row in enumerate(all_rows, start=2):
                игровой_ник = row.get("Игровой ник", "").strip()
                if not игровой_ник:
                    continue
                
                # Получаем значение из колонки K (Актуальность)
                актуальность = row.get("Актуальность", "").strip()
                
                # Проверяем, что в колонке K ТОЧНО "В ЧС" (без учета регистра)
                if актуальность.lower() != "в чс":
                    continue
                
                # Получаем дату снятия из колонки I
                дата_снятия_str = row.get("Дата снятия", "").strip()
                
                # Проверяем, что дата снятия содержит сегодняшнюю дату
                if today_str not in дата_снятия_str:
                    continue
                
                exiting_today.append({
                    "ник": игровой_ник,
                    "строка": i,
                    "discord": row.get("Дискорд юз", "-").strip(),
                    "дата_снятия": дата_снятия_str,
                    "причина": row.get("Причина", "-").strip()[:50],
                    "актуальность": актуальность
                })
            
            if exiting_today:
                exiting_today.sort(key=lambda x: x["ник"])
                
                embed = discord.Embed(
                    title="📅 ДОЛЖНЫ ВЫЙТИ СЕГОДНЯ",
                    description=f"**Дата:** {today_str}\n"
                               f"**Количество:** {len(exiting_today)} игроков",
                    color=0xff9900
                )
                
                for player in exiting_today[:10]:
                    player_info = (
                        f"┌ **Ник:** {player['ник']}\n"
                        f"├ **Discord:** {player['discord']}\n"
                        f"├ **Дата снятия:** {player['дата_снятия']}\n"
                        f"├ **Причина:** {player['причина']}\n"
                        f"└ **Строка:** #{player['строка']}"
                    )
                    
                    embed.add_field(
                        name=f"⏳ {player['ник']}",
                        value=player_info,
                        inline=False
                    )
                
                if len(exiting_today) > 10:
                    embed.set_footer(text=f"Показаны первые 10 из {len(exiting_today)}")
                
                mentions = " ".join([f"<@{user_id}>" for user_id in NOTIFY_USERS])
                await message.channel.send(content=mentions, embed=embed)
            else:
                embed = discord.Embed(
                    title="📅 НЕТ ЗАПЛАНИРОВАННЫХ ВЫХОДОВ",
                    description=f"На **{today_str}** нет игроков, которые должны выйти из черного списка.",
                    color=0x808080
                )
                await message.channel.send(embed=embed)
            
        except Exception as e:
            await message.channel.send(f"❌ Ошибка: {str(e)}")
        
        return
    
    # Команда !check_blacklist
    if message.content.startswith("!check_blacklist"):
        await message.channel.send("🔍 Проверяю изменения...")
        await check_all_changes()
        await message.channel.send("✅ Проверка завершена!")
        return
    
    # Команда !changes
    if message.content.startswith("!changes"):
        try:
            current = await get_current_blacklist_with_details()
            previous = load_previous_state()
            
            if not previous:
                await message.channel.send("📭 Нет данных о предыдущих состояниях. Используйте !check_blacklist для первой проверки.")
                return
            
            prev_in_blacklist = [p for p in previous if p["is_in_blacklist"]]
            curr_in_blacklist = [p for p in current if p["is_in_blacklist"]]
            
            embed = discord.Embed(
                title="📊 Статистика черного списка",
                color=0x3498db
            )
            
            embed.add_field(name="Было в ЧС", value=str(len(prev_in_blacklist)), inline=True)
            embed.add_field(name="Стало в ЧС", value=str(len(curr_in_blacklist)), inline=True)
            embed.add_field(name="Вышло из ЧС", value=str(len(prev_in_blacklist) - len(curr_in_blacklist)), inline=True)
            embed.add_field(name="Всего записей", value=str(len(current)), inline=True)
            
            await message.channel.send(embed=embed)
            
        except Exception as e:
            await message.channel.send(f"❌ Ошибка: {str(e)}")
        
        return
    
    # =============== КОМАНДА !a ===============
    if message.content.startswith("!a"):
        await message.channel.send("🔍 Проверяю просроченные записи в черном списке...")
        
        try:
            response = requests.get(CSV_URL, timeout=10)
            response.encoding = 'utf-8'
            response.raise_for_status()
            
            csv_content = response.text
            reader = csv.DictReader(io.StringIO(csv_content))
            all_rows = list(reader)
            
            if not all_rows:
                await message.channel.send("❌ Таблица пуста")
                return
            
            today = date.today()
            current_date = today.strftime("%d.%m.%Y")
            
            просроченные_в_чс = []  # Только те, кто должен выйти из ЧС
            
            for i, row in enumerate(all_rows, start=2):
                игровой_ник = row.get("Игровой ник", "").strip()
                if not игровой_ник:
                    continue
                
                # Столбец I - Дата снятия
                дата_снятия_str = row.get("Дата снятия", "").strip()
                
                # Столбец K - Актуальность
                актуальность = row.get("Актуальность", "").strip()
                актуальность_lower = актуальность.lower()
                
                # Пропускаем если:
                # 1. Нет даты снятия
                if not дата_снятия_str or дата_снятия_str == "-":
                    continue
                
                # 2. В столбце K "навсегда"
                if "навсегда" in актуальность_lower:
                    continue
                
                # 3. В столбце K "вынесен из чс" или "амнистия"
                if "вынесен из чс" in актуальность_lower or "амнистия" in актуальность_lower:
                    continue
                
                # 4. Статус не содержит "в чс"
                if "в чс" not in актуальность_lower:
                    continue
                
                # Пытаемся распарсить дату
                дата_снятия = None
                try:
                    # Пробуем разные форматы даты
                    for fmt in ["%d.%m.%Y", "%d.%m.%y", "%Y-%m-%d"]:
                        try:
                            # Берем только первую часть даты (без времени)
                            date_part = дата_снятия_str.split()[0]
                            дата_снятия = datetime.strptime(date_part, fmt).date()
                            break
                        except:
                            continue
                    
                    if дата_снятия is None:
                        continue
                        
                except Exception as e:
                    print(f"Ошибка парсинга даты '{дата_снятия_str}': {e}")
                    continue
                
                # Проверяем просрочена ли дата (дата в прошлом)
                if дата_снятия < today:
                    # Это просроченная запись, которая все еще в ЧС
                    просроченные_в_чс.append({
                        "ник": игровой_ник,
                        "строка": i,
                        "дата": дата_снятия_str,
                        "статус": актуальность,
                        "discord": row.get("Дискорд юз", "-").strip(),
                        "причина": row.get("Причина", "-").strip()[:50]
                    })
            
            # Формируем ответ
            if просроченные_в_чс:
                # Сортируем по дате (от самых старых)
                просроченные_в_чс.sort(key=lambda x: x['дата'])
                
                response_text = "```\n"
                response_text += "⚠️ ПРОСРОЧЕННЫЕ ЗАПИСИ (ДОЛЖНЫ БЫЛИ ВЫЙТИ ИЗ ЧС)\n"
                response_text += f"📅 Сегодня: {current_date}\n"
                response_text += f"🔴 Найдено: {len(просроченные_в_чс)}\n\n"
                
                for player in просроченные_в_чс:
                    response_text += f"👤 {player['ник']} (стр.{player['строка']})\n"
                    response_text += f"  📅 Должны были снять: {player['дата']}\n"
                    response_text += f"  📌 Текущий статус: {player['статус']}\n"
                    if player['discord'] and player['discord'] != "-":
                        response_text += f"  💬 Discord: {player['discord']}\n"
                    response_text += f"  📝 Причина: {player['причина']}\n\n"
                
                response_text += "```"
                
                # Проверяем длину сообщения
                if len(response_text) <= 2000:
                    await message.channel.send(response_text)
                else:
                    # Если слишком длинное, отправляем частями
                    await message.channel.send(f"```\n⚠️ ПРОСРОЧЕННЫЕ ЗАПИСИ (В ЧС)\n📅 {current_date}\n🔴 Найдено: {len(просроченные_в_чс)}\n```")
                    
                    # Отправляем по частям
                    chunk = "```\n"
                    for player in просроченные_в_чс:
                        player_text = f"👤 {player['ник']} (стр.{player['строка']})\n  📅 {player['дата']}\n  📌 {player['статус']}\n\n"
                        if len(chunk + player_text) > 1900:
                            chunk += "```"
                            await message.channel.send(chunk)
                            await asyncio.sleep(1)
                            chunk = "```\n" + player_text
                        else:
                            chunk += player_text
                    
                    if chunk != "```\n":
                        chunk += "```"
                        await message.channel.send(chunk)
                
            else:
                response_text = "```\n"
                response_text += "✅ НЕТ ПРОСРОЧЕННЫХ ЗАПИСЕЙ\n"
                response_text += f"📅 Сегодня: {current_date}\n\n"
                response_text += "Все записи актуальны или уже обработаны\n"
                response_text += "```"
                await message.channel.send(response_text)
            
        except Exception as e:
            await message.channel.send(f"❌ Ошибка: {str(e)}")
            print(f"Ошибка в !a: {e}")
        
        return
    
    # =============== КОМАНДА !r ===============
    if message.content.startswith("!r"):
        await message.channel.send("🔍 Получаю Discord'ы просроченных записей...")
        
        try:
            response = requests.get(CSV_URL, timeout=10)
            response.encoding = 'utf-8'
            response.raise_for_status()
            
            csv_content = response.text
            reader = csv.DictReader(io.StringIO(csv_content))
            all_rows = list(reader)
            
            if not all_rows:
                await message.channel.send("❌ Таблица пуста")
                return
            
            today = date.today()
            просроченные_дискорды = []
            
            for i, row in enumerate(all_rows, start=2):
                игровой_ник = row.get("Игровой ник", "")
                if not игровой_ник:
                    continue
                
                дата_снятия_str = row.get("Дата снятия", "").strip()
                актуальность = row.get("Актуальность", "").strip()
                актуальность_lower = актуальность.lower()
                
                if not дата_снятия_str or дата_снятия_str == "-":
                    continue
                
                # Пропускаем если "навсегда"
                if "навсегда" in актуальность_lower:
                    continue
                
                # Пропускаем если уже обработано
                if "вынесен из чс" in актуальность_lower or "амнистия" in актуальность_lower:
                    continue
                
                try:
                    for fmt in ["%d.%m.%Y", "%d.%m.%y", "%Y-%m-%d"]:
                        try:
                            дата_снятия = datetime.strptime(дата_снятия_str.split()[0], fmt).date()
                            break
                        except:
                            continue
                    else:
                        continue
                except:
                    continue
                
                # Если дата прошла и статус В ЧС
                if дата_снятия < today:
                    discord_value = row.get("Дискорд юз", "").strip()
                    if discord_value and discord_value != "-":
                        просроченные_дискорды.append(discord_value)
            
            # Формируем ответ
            if просроченные_дискорды:
                # Убираем дубликаты
                уникальные_дискорды = list(set(просроченные_дискорды))
                
                # Сортируем
                уникальные_дискорды.sort()
                
                # Создаем сообщение с Discord'ами в столбик
                response_text = "```\n"
                response_text += "📋 DISCORD'Ы ПРОСРОЧЕННЫХ ЗАПИСЕЙ\n"
                response_text += f"📅 {today.strftime('%d.%m.%Y')}\n"
                response_text += f"🔴 Найдено: {len(уникальные_дискорды)}\n\n"
                
                for discord_name in уникальные_дискорды:
                    response_text += f"{discord_name}\n"
                
                response_text += "```"
                
                # Проверяем длину
                if len(response_text) <= 2000:
                    await message.channel.send(response_text)
                else:
                    # Если слишком длинное, разбиваем на несколько сообщений
                    parts = []
                    current_part = "```\n📋 DISCORD'Ы ПРОСРОЧЕННЫХ ЗАПИСЕЙ\n"
                    current_part += f"📅 {today.strftime('%d.%m.%Y')}\n"
                    current_part += f"🔴 Найдено: {len(уникальные_дискорды)}\n\n"
                    
                    for discord_name in уникальные_дискорды:
                        if len(current_part) + len(discord_name) + 1 > 1990:
                            current_part += "```"
                            parts.append(current_part)
                            current_part = "```\n" + discord_name + "\n"
                        else:
                            current_part += discord_name + "\n"
                    
                    if current_part != "```\n":
                        current_part += "```"
                        parts.append(current_part)
                    
                    for part in parts:
                        await message.channel.send(part)
                        await asyncio.sleep(1)
                
            else:
                response_text = "```\n"
                response_text += "✅ НЕТ АКТУАЛЬНЫХ ПРОСРОЧЕННЫХ ЗАПИСЕЙ\n"
                response_text += f"📅 {today.strftime('%d.%m.%Y')}\n\n"
                response_text += "Нет Discord'ов для отображения\n"
                response_text += "```"
                await message.channel.send(response_text)
            
        except Exception as e:
            await message.channel.send(f"❌ Ошибка: {str(e)}")
            print(f"Ошибка в !r: {e}")
        
        return

    # Команда !inf
    if message.content.startswith("!inf"):
        text = message.content.replace("!inf", "").strip()
        
        if text == "columns" or text == "столбцы":
            await show_all_columns(message)
            return
        
        names = [name.strip() for name in text.split('\n') if name.strip()]
        
        if not names:
            await message.channel.send("❌ Напиши ник или ники")
            return
        
        try:
            response = requests.get(CSV_URL, timeout=10)
            response.encoding = 'utf-8'
            response.raise_for_status()
            
            csv_content = response.text
            reader = csv.DictReader(io.StringIO(csv_content))
            all_rows = list(reader)
            
            if not all_rows:
                await message.channel.send("❌ Таблица пуста")
                return
            
            results = []
            
            for search_name in names:
                found = False
                name_lower = search_name.lower()
                
                for i, row in enumerate(all_rows, start=2):
                    игровой_ник = row.get("Игровой ник", "")
                    
                    if игровой_ник and игровой_ник.lower() == name_lower:
                        row_data = []
                        row_data.append("```")
                        row_data.append(f"📋 Информация об игроке: {игровой_ник}")
                        row_data.append(f"📍 Строка в таблице: {i}")
                        row_data.append("========================================")
                        row_data.append(f"Discord: {row.get('Дискорд юз', '-')}")
                        row_data.append(f"Организация: {row.get('Организация', '-')}")
                        row_data.append(f"Причина: {row.get('Причина', '-')}")
                        row_data.append(f"Дата снятия: {row.get('Дата снятия', '-')}")
                        row_data.append(f"Выдал: {row.get('Кто выдал', '-')}")
                        row_data.append(f"Актуальность: {row.get('Актуальность', '-')}")
                        row_data.append("========================================")
                        row_data.append("```")
                        
                        results.append("\n".join(row_data))
                        found = True
                
                if not found:
                    results.append(f"\n❌ Игрок '{search_name}' не найден\n")
            
            for result in results:
                await message.channel.send(result)
            
        except Exception as e:
            await message.channel.send(f"❌ Ошибка: {str(e)}")
        
        return
    
    # Команда !columns
    if message.content.startswith("!columns"):
        await show_all_columns(message)
        return

async def show_all_columns(message):
    """Показывает все доступные столбцы в таблице"""
    try:
        response = requests.get(CSV_URL, timeout=10)
        response.encoding = 'utf-8'
        csv_content = response.text
        reader = csv.DictReader(io.StringIO(csv_content))
        all_rows = list(reader)
        
        if all_rows:
            columns = list(all_rows[0].keys())
            text = "📊 Доступные столбцы:\n\n"
            for i, col in enumerate(columns):
                letter = COLUMN_LETTERS[i] if i < len(COLUMN_LETTERS) else f"Column{i+1}"
                text += f"**{letter}** - {col}\n"
            await message.channel.send(text)
        else:
            await message.channel.send("❌ Таблица пуста")
    except Exception as e:
        await message.channel.send(f"❌ Ошибка: {str(e)}")

if __name__ == "__main__":
    print("🚗 Запуск бота...")
    bot.run(TOKEN)