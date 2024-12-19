import logging
import json
import requests
import qrcode
import asyncio
import uuid
from io import BytesIO
from datetime import datetime, timedelta
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, ForceReply, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters, ConversationHandler, CallbackQueryHandler, PicklePersistence
from apscheduler.schedulers.background import BackgroundScheduler
import sys
import signal

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Configuration
PANEL_URL = "http://5.252.118.78:55001"
PANEL_USERNAME = "paneladmin"
PANEL_PASSWORD = "cwzGdD3ygy6u"
BOT_TOKEN = "7730268619:AAHzrMIgG0VWI4swC-5sIq6J45bRLJnm9lU"
ADMIN_USERNAME = "ooostyx"

# States for conversation handler
WAITING_FOR_TAG = 1

# Admin panel states
ADMIN_ADD_USER_EMAIL = 2
ADMIN_ADD_USER_EXPIRY = 3
ADMIN_EDIT_USER_EXPIRY = 4

# Callback data prefixes
USER_PREFIX = "user_"
EXTEND_PREFIX = "extend_"
KEY_PREFIX = "key_"
TOGGLE_PREFIX = "toggle_"

# Keyboard markup
def get_keyboard(is_authorized=False):
    """Get keyboard markup based on authorization status"""
    buttons = []
    if is_authorized:
        buttons = [
            ['📊 Статус', '🔑 Показать ключ'],
            ['🏆 Рейтинг', '❌ Удалить подключение'],
            ['❓ Помощь']
        ]
    else:
        buttons = [
            ['🔐 Авторизация'],
            ['❓ Помощь']
        ]
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def get_admin_keyboard():
    """Get admin panel keyboard markup"""
    return ReplyKeyboardMarkup([
        ['👥 Список пользователей'],
        ['❌ Выйти']
    ], resize_keyboard=True)

def get_user_list_keyboard(users):
    """Get inline keyboard markup with user list"""
    keyboard = []
    for user in users:
        email = user.get('email', '')
        if email:
            keyboard.append([
                InlineKeyboardButton(
                    email,
                    callback_data=f"{USER_PREFIX}{email}"
                )
            ])
    return InlineKeyboardMarkup(keyboard)

def get_user_actions_keyboard(email):
    """Get inline keyboard markup with user actions"""
    keyboard = [
        [
            InlineKeyboardButton("🔄 Включить/Отключить", callback_data=f"{TOGGLE_PREFIX}{email}"),
            InlineKeyboardButton("🔑 Показать ключ", callback_data=f"{KEY_PREFIX}{email}")
        ],
        [InlineKeyboardButton("« Назад", callback_data="back_to_list")]
    ]
    return InlineKeyboardMarkup(keyboard)

class VPNPanel:
    def __init__(self):
        self.session = requests.Session()
        
    def login(self):
        """Login to VPN panel and return session cookies"""
        try:
            response = self.session.post(
                f"{PANEL_URL}/login",
                data={
                    'username': PANEL_USERNAME,
                    'password': PANEL_PASSWORD
                }
            )
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Login error: {e}")
            return False

    def get_client_info(self, username: str) -> dict:
        """Get client information from VPN panel"""
        try:
            if not self.login():
                return {'found': False, 'error': 'Login failed'}

            headers = {
                'Accept': 'application/json, text/plain, */*',
                'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                'X-Requested-With': 'XMLHttpRequest'
            }

            response = self.session.post(f"{PANEL_URL}/panel/inbound/list", headers=headers)
            if response.status_code == 200:
                data = response.json()
                for inbound in data.get('obj', []):
                    # Check in clientStats
                    for client in inbound.get('clientStats', []):
                        if client.get('email', '').strip() == username.strip():
                            # Get full client info from settings
                            settings = json.loads(inbound.get('settings', '{}'))
                            stream_settings = json.loads(inbound.get('streamSettings', '{}'))
                            client_full = next(
                                (c for c in settings.get('clients', []) if c.get('email', '').strip() == username.strip()),
                                {}
                            )
                            return {
                                'found': True,
                                'enable': client.get('enable', False),
                                'up': client.get('up', 0),
                                'down': client.get('down', 0),
                                'total': client.get('total', 0),
                                'expiryTime': client.get('expiryTime', 0),
                                'client_id': client_full.get('id', ''),
                                'inbound': inbound,
                                'stream_settings': stream_settings
                            }
                    
                    # Also check in settings for additional info
                    settings = json.loads(inbound.get('settings', '{}'))
                    for client in settings.get('clients', []):
                        if client.get('email', '').strip() == username.strip():
                            stream_settings = json.loads(inbound.get('streamSettings', '{}'))
                            return {
                                'found': True,
                                'enable': client.get('enable', False),
                                'total': client.get('total', 0),
                                'expiryTime': client.get('expiryTime', 0),
                                'limitIp': client.get('limitIp', 0),
                                'client_id': client.get('id', ''),
                                'inbound': inbound,
                                'stream_settings': stream_settings
                            }
            return {'found': False}
        except Exception as e:
            logger.error(f"Error getting client info: {e}")
            return {'found': False, 'error': str(e)}

    def get_client_key(self, client_info: dict) -> str:
        """Generate vless key from client info"""
        try:
            inbound = client_info['inbound']
            stream_settings = client_info['stream_settings']
            client_id = client_info['client_id']

            public_key = stream_settings["realitySettings"]["settings"]["publicKey"]
            fingerprint = stream_settings["realitySettings"]["settings"]["fingerprint"]
            sni = stream_settings["realitySettings"]["dest"].split(":")[0]
            short_id = stream_settings["realitySettings"]["shortIds"][0]
            
            raw_key = (
                f"vless://{client_id}@5.252.118.78:{inbound['port']}"
                f"?type={stream_settings['network']}"
                f"&security={stream_settings['security']}"
                f"&pbk={public_key}"
                f"&fp={fingerprint}"
                f"&sni={sni}"
                f"&sid={short_id}"
                f"&spx=%2F"
            )
            
            return raw_key
        except Exception as e:
            logger.error(f"Error generating client key: {e}")
            return None

    def add_user(self, email: str, expiry_time: int = 0) -> bool:
        """Add new user to VPN panel"""
        try:
            if not self.login():
                return False

            # Get inbound with remark "subscribe"
            response = self.session.post(f"{PANEL_URL}/panel/inbound/list")
            if response.status_code != 200:
                return False

            data = response.json()
            subscribe_inbound = None
            for inbound in data.get('obj', []):
                if inbound.get('remark') == 'subscribe':
                    subscribe_inbound = inbound
                    break

            if not subscribe_inbound:
                logger.error("No inbound with remark 'subscribe' found")
                return False

            # Prepare new client data
            new_client = {
                "id": str(uuid.uuid4()),
                "flow": "xtls-rprx-vision",
                "email": email,
                "limitIp": 0,
                "totalGB": 0,
                "expiryTime": expiry_time,
                "enable": True
            }

            # Add client to inbound
            settings = json.loads(subscribe_inbound.get('settings', '{}'))
            if 'clients' not in settings:
                settings['clients'] = []
            settings['clients'].append(new_client)

            # Update inbound
            update_data = {
                'id': subscribe_inbound['id'],
                'settings': json.dumps(settings)
            }

            response = self.session.post(
                f"{PANEL_URL}/panel/inbound/update",
                json=update_data
            )

            return response.status_code == 200
        except Exception as e:
            logger.error(f"Error adding user: {e}")
            return False

    def update_user_expiry(self, email: str, new_expiry_time: int) -> bool:
        """Update user expiry time"""
        try:
            if not self.login():
                logger.error("Failed to login while updating expiry time")
                return False

            # Get inbound list
            response = self.session.post(f"{PANEL_URL}/panel/inbound/list")
            if response.status_code != 200:
                logger.error(f"Failed to get inbound list: {response.status_code}")
                return False

            data = response.json()
            for inbound in data.get('obj', []):
                if inbound.get('remark') != 'subscribe':  # Skip non-subscribe inbounds
                    continue
                
                try:
                    settings = json.loads(inbound.get('settings', '{}'))
                    clients = settings.get('clients', [])
                    
                    # Find and update client
                    client_updated = False
                    for i, client in enumerate(clients):
                        if client.get('email') == email:
                            clients[i]['expiryTime'] = new_expiry_time
                            client_updated = True
                            break
                    
                    if not client_updated:
                        continue
                    
                    # Update inbound with the same format as add_user
                    update_data = {
                        'id': inbound['id'],
                        'settings': json.dumps(settings)
                    }
                    
                    logger.info(f"Updating expiry for {email} to {new_expiry_time}")
                    
                    response = self.session.post(
                        f"{PANEL_URL}/panel/inbound/update",
                        json=update_data
                    )
                    
                    if response.status_code != 200:
                        logger.error(f"Failed to update inbound: {response.status_code}")
                        logger.error(f"Response: {response.text}")
                        return False
                        
                    logger.info(f"Successfully updated expiry for {email}")
                    return True

                except json.JSONDecodeError:
                    logger.error(f"Failed to parse settings JSON for inbound {inbound.get('id')}")
                    continue

            logger.error(f"User {email} not found in any inbound")
            return False
        except Exception as e:
            logger.error(f"Error updating user expiry: {e}")
            return False

    def toggle_user(self, email: str) -> bool:
        """Toggle user enable status"""
        try:
            if not self.login():
                logger.error("Failed to login while toggling user")
                return False

            # Get inbound list
            response = self.session.post(f"{PANEL_URL}/panel/inbound/list")
            if response.status_code != 200:
                logger.error(f"Failed to get inbound list: {response.status_code}")
                return False

            data = response.json()
            for inbound in data.get('obj', []):
                if inbound.get('remark') != 'subscribe':  # Skip non-subscribe inbounds
                    continue
                
                try:
                    settings = json.loads(inbound.get('settings', '{}'))
                    clients = settings.get('clients', [])
                    
                    # Find and toggle client
                    client_updated = False
                    for i, client in enumerate(clients):
                        if client.get('email') == email:
                            clients[i]['enable'] = not client.get('enable', False)
                            client_updated = True
                            break
                    
                    if not client_updated:
                        continue
                    
                    # Update inbound with the same format as add_user
                    update_data = {
                        'id': inbound['id'],
                        'settings': json.dumps(settings)
                    }
                    
                    logger.info(f"Toggling user {email}")
                    
                    response = self.session.post(
                        f"{PANEL_URL}/panel/inbound/update",
                        json=update_data
                    )
                    
                    if response.status_code != 200:
                        logger.error(f"Failed to update inbound: {response.status_code}")
                        logger.error(f"Response: {response.text}")
                        return False
                        
                    logger.info(f"Successfully toggled user {email}")
                    return True

                except json.JSONDecodeError:
                    logger.error(f"Failed to parse settings JSON for inbound {inbound.get('id')}")
                    continue

            logger.error(f"User {email} not found in any inbound")
            return False
        except Exception as e:
            logger.error(f"Error toggling user: {e}")
            return False

vpn_panel = VPNPanel()

async def generate_qr(text: str) -> BytesIO:
    """Generate QR code from text"""
    qr = qrcode.QRCode(version=1, box_size=10, border=5)
    qr.add_data(text)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    bio = BytesIO()
    bio.name = 'qr.png'
    img.save(bio, 'PNG')
    bio.seek(0)
    return bio

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start command handler"""
    user = update.effective_user
    username = user.username
    
    if not username:
        await update.message.reply_text(
            "⚠️ Пожалуйста, установите имя пользователя в Telegram для использования бота.",
            reply_markup=get_keyboard(False)
        )
        return

    subscription = await check_subscription(username)
    
    if subscription.get('found'):
        # Automatically set VPN tag if subscription is found
        context.user_data['vpn_tag'] = username
        # Also save to bot_data as backup
        if 'user_tags' not in context.bot_data:
            context.bot_data['user_tags'] = {}
        context.bot_data['user_tags'][str(update.effective_user.id)] = username
        
        await update.message.reply_text(
            f"👋 Добро пожаловать, @{username}!\n"
            f"Ваша VPN подписка {('активна' if subscription.get('enable') else 'отключена')}. "
            f"Используйте кнопки ниже для проверки статуса или получения помощи.",
            reply_markup=get_keyboard(True)
        )
    else:
        await update.message.reply_text(
            f"👋 Добро пожаловать, @{username}!\n"
            f"Не удалось найти вашу подписку.\n"
            f"Нажмите кнопку 'Авторизация' для входа с другим тегом или обратитесь в поддержку для получения тега.\n",
            reply_markup=get_keyboard(False)
        )

async def auth_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start authentication process"""
    await update.message.reply_text(
        "Пожалуйста, введите ваш тег (который был выдан вам при покупке подписки):",
        reply_markup=ForceReply()
    )
    return WAITING_FOR_TAG

async def auth_tag(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle tag input"""
    tag = update.message.text.strip()
    logger.info(f"Attempting to authenticate with tag: {tag}")
    
    client_info = vpn_panel.get_client_info(tag)
    logger.info(f"Client info response: {client_info}")
    
    if client_info.get('found'):
        logger.info(f"Authentication successful for tag: {tag}")
        # Save to persistent user_data
        context.user_data['vpn_tag'] = tag
        # Also save to bot_data as backup
        if 'user_tags' not in context.bot_data:
            context.bot_data['user_tags'] = {}
        context.bot_data['user_tags'][str(update.effective_user.id)] = tag
        
        await update.message.reply_text(
            f"✅ Авторизация успешна!\nВаш тег: {tag}",
            reply_markup=get_keyboard(True)
        )
    else:
        logger.info(f"Authentication failed for tag: {tag}")
        await update.message.reply_text(
            "❌ Тег не найден. Пожалуйста, проверьте правильность ввода или обратитесь к менеджеру @ooostyx",
            reply_markup=get_keyboard(False)
        )
    return ConversationHandler.END

async def show_key(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show VPN key and QR code"""
    user = update.effective_user
    username = context.user_data.get('vpn_tag') or user.username
    
    if not username:
        await update.message.reply_text(
            "⚠️ Пожалуйста, сначала авторизуйтесь.",
            reply_markup=get_keyboard(False)
        )
        return

    client_info = vpn_panel.get_client_info(username)
    if not client_info.get('found'):
        await update.message.reply_text(
            "❌ Подписка не найдена. Пожалуйста, проверьте статус подписки.",
            reply_markup=get_keyboard(False)
        )
        return

    key = vpn_panel.get_client_key(client_info)
    if not key:
        await update.message.reply_text(
            "❌ Ошибка при генерации ключа. Пожалуйста, обратитесь к менеджеру @ooostyx",
            reply_markup=get_keyboard(True)
        )
        return

    # Send QR code
    qr = await generate_qr(key)
    await update.message.reply_photo(
        photo=qr,
        caption=f"🔑 Ваш ключ подключения:\n`{key}`\n\nДля подключения отсканируйте QR-код или скопируйте ключ.",
        parse_mode='Markdown'
    )

async def remove_connection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Remove current connection and return to initial state"""
    if 'vpn_tag' in context.user_data:
        del context.user_data['vpn_tag']
    
    await update.message.reply_text(
        "🔄 Подключение удалено. Вы можете авторизоваться заново.",
        reply_markup=get_keyboard(False)
    )

async def check_subscription(username: str) -> dict:
    """Check subscription status for a user"""
    client_info = vpn_panel.get_client_info(username)
    if client_info.get('found'):
        return {
            'found': True,
            'enable': client_info.get('enable', False),
            'total': client_info.get('total', 0),
            'up': client_info.get('up', 0),
            'down': client_info.get('down', 0),
            'expiryTime': client_info.get('expiryTime', 0),
            'limitIp': client_info.get('limitIp', 0)
        }
    return {'found': False}

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Status command handler"""
    username = context.user_data.get('vpn_tag') or update.effective_user.username
    
    if not username:
        await update.message.reply_text(
            "⚠️ Пожалуйста, установите имя пользователя в Telegram или авторизуйтесь с помощью тега.",
            reply_markup=get_keyboard(False)
        )
        return
    
    client_info = vpn_panel.get_client_info(username)
    
    if not client_info.get('found'):
        await update.message.reply_text(
            "❌ Подписка не найдена. Пожалуйста, проверьте статус подписки или авторизуйтесь с помощью тега.",
            reply_markup=get_keyboard(False)
        )
        return

    # Format traffic values
    up = client_info.get('up', 0)
    down = client_info.get('down', 0)
    
    up_gb = up / (1024 ** 3)
    down_gb = down / (1024 ** 3)
    
    # Calculate expiry info
    expiry_time = client_info.get('expiryTime', 0)
    if expiry_time == 0:
        expiry_status = "🔄 Бессрочная подписка"
        days_left = "∞"
    else:
        expiry_date = datetime.fromtimestamp(expiry_time/1000)
        days_left = (expiry_date - datetime.now()).days
        expiry_status = f"📅 Дата окончания: {expiry_date.strftime('%d.%m.%Y')}"
    
    # Prepare status message with better formatting
    message = "📱 СТАТУС ПОДПИСКИ 📱\n"
    message += "─────────────────\n\n"
    message += f"{'🟢 Активна' if client_info.get('enable') else '🔴 Отключена'}\n"
    message += f"{expiry_status}\n"
    message += f"📆 Осталось дней: {days_left}\n\n"
    message += "📊 СТАТИСТИКА ТРАФИКА 📊\n"
    message += "─────────────────\n"
    message += f"📥 Загружено: {down_gb:.2f} GB\n"
    message += f"📤 Отправлено: {up_gb:.2f} GB\n"
    message += "─────────────────"
    
    await update.message.reply_text(message, reply_markup=get_keyboard(True))

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Help command handler"""
    help_text = (
        "❗️ Если у вас возникли проблемы с подключением или вы хотите приобрести/продлить подписку - "
        "пожалуйста, обратитесь к менеджеру @ooostyx"
    )
    await update.message.reply_text(help_text)

async def check_expiring_subscriptions(context: ContextTypes.DEFAULT_TYPE):
    """Check for expiring subscriptions and send notifications"""
    try:
        if not vpn_panel.login():
            logger.error("Failed to login to VPN panel during subscription check")
            return

        response = vpn_panel.session.post(f"{PANEL_URL}/panel/inbound/list")
        if response.status_code == 200:
            data = response.json()
            current_time = datetime.now()
            
            for inbound in data.get('obj', []):
                for client in inbound.get('clientStats', []):
                    if client.get('expiryTime', 0) > 0:  # Skip infinite subscriptions
                        email = client.get('email', '').strip()
                        expiry_time = datetime.fromtimestamp(client.get('expiryTime', 0) / 1000)
                        time_until_expiry = expiry_time - current_time
                        days_until_expiry = time_until_expiry.days
                        
                        # Check for expired subscriptions
                        if time_until_expiry.total_seconds() <= 0:
                            notification_text = (
                                "❌ Ваша подписка закончилась!\n\n"
                                "Для продления подписки обратитесь к менеджеру @ooostyx"
                            )
                            try:
                                await context.bot.send_message(
                                    chat_id=email,
                                    text=notification_text
                                )
                                logger.info(f"Sent expiration notification to {email}")
                            except Exception as e:
                                logger.error(f"Failed to send expiration notification to {email}: {e}")
                        
                        # Check for subscriptions expiring in 3 days
                        elif days_until_expiry == 3:
                            notification_text = (
                                "⚠️ Внимание! Ваша подписка истекает через 3 дня.\n\n"
                                "Для продления подписки обратитесь к менеджеру @ooostyx"
                            )
                            try:
                                await context.bot.send_message(
                                    chat_id=email,
                                    text=notification_text
                                )
                                logger.info(f"Sent expiry warning to {email}")
                            except Exception as e:
                                logger.error(f"Failed to send expiry warning to {email}: {e}")
    except Exception as e:
        logger.error(f"Error checking subscriptions: {e}")

async def admin_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start admin panel"""
    user = update.effective_user
    username = user.username
    
    if username != ADMIN_USERNAME:
        await update.message.reply_text(
            "⚠️ Доступ запрещен.",
            reply_markup=get_keyboard(False)
        )
        return
    
    await update.message.reply_text(
        "👋 Добро пожаловать в админ-панель!",
        reply_markup=get_admin_keyboard()
    )

async def admin_list_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List users in admin panel"""
    try:
        response = vpn_panel.session.post(f"{PANEL_URL}/panel/inbound/list")
        if response.status_code == 200:
            data = response.json()
            users = []
            for inbound in data.get('obj', []):
                for client in inbound.get('clientStats', []):
                    users.append({
                        'email': client.get('email', ''),
                        'expiryTime': client.get('expiryTime', 0)
                    })
            await update.message.reply_text(
                "👥 Список пользователей:",
                reply_markup=get_user_list_keyboard(users)
            )
    except Exception as e:
        logger.error(f"Error listing users: {e}")

async def admin_user_actions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle user actions in admin panel"""
    query = update.callback_query
    await query.answer()
    data = query.data
    
    if data.startswith(USER_PREFIX):
        email = data[len(USER_PREFIX):]
        client_info = vpn_panel.get_client_info(email)
        if client_info.get('found'):
            expiry_time = client_info.get('expiryTime', 0)
            expiry_text = "🔄 Бессрочная подписка" if expiry_time == 0 else f"📅 До: {datetime.fromtimestamp(expiry_time/1000).strftime('%d.%m.%Y')}"
            await query.edit_message_text(
                f"👤 Пользователь: {email}\n{expiry_text}",
                reply_markup=get_user_actions_keyboard(email)
            )
        else:
            await query.edit_message_text(
                f"❌ Информация о пользователе {email} не найдена",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Назад", callback_data="back_to_list")]])
            )
    
    elif data.startswith(TOGGLE_PREFIX):
        email = data[len(TOGGLE_PREFIX):]
        if vpn_panel.toggle_user(email):
            await query.edit_message_text(
                f"✅ Пользователь {email} успешно включен/отключен",
                reply_markup=get_user_actions_keyboard(email)
            )
        else:
            await query.edit_message_text(
                f"❌ Ошибка включения/отключения пользователя {email}",
                reply_markup=get_user_actions_keyboard(email)
            )
    
    elif data.startswith(KEY_PREFIX):
        email = data[len(KEY_PREFIX):]
        client_info = vpn_panel.get_client_info(email)
        if client_info.get('found'):
            key = vpn_panel.get_client_key(client_info)
            if key:
                qr = await generate_qr(key)
                await query.message.reply_photo(
                    photo=qr,
                    caption=f"🔑 Ключ для {email}:\n`{key}`",
                    parse_mode='Markdown'
                )
                await query.edit_message_text(
                    f"✅ Ключ для {email} отправлен",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Назад", callback_data="back_to_list")]])
                )
            else:
                await query.edit_message_text(
                    f"❌ Ошибка генерации ключа для {email}",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Назад", callback_data="back_to_list")]])
                )
        else:
            await query.edit_message_text(
                f"❌ Пользователь {email} не найден",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Назад", callback_data="back_to_list")]])
            )
    
    elif data == "back_to_list":
        await admin_list_users(update, context)

async def admin_exit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Exit from admin panel"""
    await update.message.reply_text(
        "Вы вышли из панели администратора",
        reply_markup=get_keyboard(await check_subscription(update.effective_user.username))
    )

async def show_rating(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show top-10 users by traffic usage"""
    try:
        if not vpn_panel.login():
            await update.message.reply_text(
                "❌ Ошибка подключения к панели",
                reply_markup=get_keyboard(True)
            )
            return

        # Get current user's tag from user_data
        current_user = context.user_data.get('vpn_tag')
        
        # If not in user_data, try bot_data
        if not current_user and 'user_tags' in context.bot_data:
            current_user = context.bot_data['user_tags'].get(str(update.effective_user.id))
            if current_user:
                # Restore to user_data
                context.user_data['vpn_tag'] = current_user

        if not current_user:
            await update.message.reply_text(
                "❌ Не удалось определить ваш тег. Пожалуйста, авторизуйтесь заново.",
                reply_markup=get_keyboard(False)
            )
            return

        # Get current user's tag
        current_user = context.user_data.get('vpn_tag')
        if not current_user:
            await update.message.reply_text(
                "❌ Не удалось определить ваш тег. Пожалуйста, авторизуйтесь заново.",
                reply_markup=get_keyboard(False)
            )
            return

        response = vpn_panel.session.post(f"{PANEL_URL}/panel/inbound/list")
        if response.status_code != 200:
            await update.message.reply_text(
                "❌ Ошибка получения данных",
                reply_markup=get_keyboard(True)
            )
            return

        data = response.json()
        users_traffic = []
        
        for inbound in data.get('obj', []):
            for client in inbound.get('clientStats', []):
                if client.get('email'):
                    users_traffic.append({
                        'username': client['email'],
                        'total': client.get('down', 0) + client.get('up', 0)
                    })

        # Sort users by traffic
        users_traffic.sort(key=lambda x: x['total'], reverse=True)
        
        # Get current user's position and traffic
        user_position = next((i + 1 for i, user in enumerate(users_traffic) 
                            if user['username'].strip() == current_user.strip()), None)
        user_traffic = next((user['total'] for user in users_traffic 
                           if user['username'].strip() == current_user.strip()), 0)

        # Format message
        message = "🏆 ТОП ПОЛЬЗОВАТЕЛЕЙ 🏆\n"
        message += "─────────────────\n\n"
        
        if user_position:
            position_emoji = get_position_emoji(user_position)
            message += f"📊 Ваша позиция: {position_emoji} {user_position} место\n"
            message += f"📈 Ваш трафик: {format_traffic(user_traffic)}\n\n"
        
        message += "⭐️ РЕЙТИНГ ТОП-10 ⭐️\n"
        message += "─────────────────\n"

        for i, user in enumerate(users_traffic[:10], 1):
            position_emoji = get_position_emoji(i)
            # Show full tag if it matches current user's tag
            username = user['username']
            if user['username'].strip() != current_user.strip():
                if len(username) <= 3:
                    username = username[0] + "*" * (len(username) - 1)
                else:
                    username = username[0] + "*" * (len(username) - 2) + username[-1]
            
            traffic = format_traffic(user['total'])
            
            if user['username'].strip() == current_user.strip():
                message += f"{position_emoji} {i}. 👉 {username}\n   ┗━ {traffic}\n"
            else:
                message += f"{position_emoji} {i}. {username}\n   ┗━ {traffic}\n"

        message += "─────────────────"

        await update.message.reply_text(
            message,
            reply_markup=get_keyboard(True)
        )

    except Exception as e:
        logger.error(f"Error showing rating: {e}")
        await update.message.reply_text(
            "❌ Произошла ошибка при получении рейтинга",
            reply_markup=get_keyboard(True)
        )

def format_username(username: str, show_full: bool = False) -> str:
    """Format username to show only first and last characters"""
    if show_full:
        return username
    
    if len(username) <= 3:
        return username[0] + "*" * (len(username) - 1)
    else:
        return username[0] + "*" * (len(username) - 2) + username[-1]

def format_traffic(bytes_count: int) -> str:
    """Format traffic size to human readable format"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_count < 1024:
            return f"{bytes_count:.2f} {unit}"
        bytes_count /= 1024
    return f"{bytes_count:.2f} PB"

def get_position_emoji(position: int) -> str:
    """Get emoji for position"""
    if position == 1:
        return "🥇"
    elif position == 2:
        return "🥈"
    elif position == 3:
        return "🥉"
    return "🏅"

def format_remaining_time(expiry_time: int) -> str:
    """Format remaining time until expiry"""
    try:
        if expiry_time == 0:
            return "♾️ Бессрочная"
            
        expiry = datetime.fromtimestamp(expiry_time / 1000)
        now = datetime.now()
        
        if expiry < now:
            return "❌ Истекла"
            
        delta = expiry - now
        days = delta.days
        hours = delta.seconds // 3600
        minutes = (delta.seconds % 3600) // 60
        
        if days > 0:
            return f"⏳ {days}д {hours}ч"
        elif hours > 0:
            return f"⏳ {hours}ч {minutes}м"
        else:
            return f"⏳ {minutes}м"
    except:
        return "❓ Неизвестно"

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    logger.info("Received shutdown signal. Cleaning up...")
    scheduler.shutdown()
    sys.exit(0)

def main():
    """Main function to run the bot"""
    try:
        # Set up signal handlers
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Initialize persistence
        persistence = PicklePersistence(filepath="bot_data.pickle")
        
        # Create application with persistence
        application = Application.builder().token(BOT_TOKEN).persistence(persistence).build()

        # Add handlers
        application.add_handler(CommandHandler("start", start))
        application.add_handler(MessageHandler(filters.Regex('^📊 Статус$'), status))
        application.add_handler(MessageHandler(filters.Regex('^🔑 Показать ключ$'), show_key))
        application.add_handler(MessageHandler(filters.Regex('^❌ Удалить подключение$'), remove_connection))
        application.add_handler(MessageHandler(filters.Regex('^🏆 Рейтинг$'), show_rating))
        
        # Add conversation handler for authentication
        auth_handler = ConversationHandler(
            entry_points=[MessageHandler(filters.Regex('^🔐 Авторизация$'), auth_start)],
            states={
                WAITING_FOR_TAG: [MessageHandler(filters.TEXT & ~filters.COMMAND, auth_tag)]
            },
            fallbacks=[CommandHandler('cancel', start)],
            persistent=True,
            name='auth_conversation'
        )
        application.add_handler(auth_handler)
        
        application.add_handler(MessageHandler(filters.Regex('^❓ Помощь$'), help_command))

        # Add admin panel handlers
        application.add_handler(CommandHandler("admin", admin_start))
        application.add_handler(MessageHandler(filters.Regex('^👥 Список пользователей$'), admin_list_users))
        application.add_handler(MessageHandler(filters.Regex('^❌ Выйти$'), admin_exit))
        application.add_handler(CallbackQueryHandler(admin_user_actions))

        # Initialize scheduler with error handling
        global scheduler
        scheduler = BackgroundScheduler()
        scheduler.add_job(
            lambda: asyncio.run(check_expiring_subscriptions(application)),
            'interval',
            minutes=1,
            next_run_time=datetime.now(),
            max_instances=1,
            coalesce=True,
            misfire_grace_time=None
        )
        
        # Start scheduler with error handling
        try:
            scheduler.start()
            logger.info("Scheduler started successfully")
        except Exception as e:
            logger.error(f"Failed to start scheduler: {e}")
            sys.exit(1)

        # Log successful startup
        logger.info("Bot started successfully")
        
        # Start the bot
        application.run_polling(allowed_updates=Update.ALL_TYPES)

    except Exception as e:
        logger.error(f"Critical error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
