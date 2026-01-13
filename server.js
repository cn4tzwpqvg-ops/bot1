require("dotenv").config();
const TelegramBot = require("node-telegram-bot-api");
const mysql = require("mysql2/promise");
// Подключаем зависимости
const express = require("express");
const cors = require("cors");
const http = require("http");
const WebSocket = require("ws");

// ================= Новая функция: рассылка и обновление с лимитом =================
const pLimit = require("p-limit").default; // убедиться, что установлен npm install p-limit



// ================= Настройки =================
const TOKEN = process.env.TELEGRAM_TOKEN;
const ADMIN_USERNAME = process.env.ADMIN_USERNAME || "admin";
const ADMIN_ID = parseInt(process.env.ADMIN_ID) || 7664644901;
const PORT = 3000;
const HOST = "0.0.0.0";

// ================= Состояние =================
const adminWaitingOrdersCourier = new Map();
const waitingReview = new Map();

// ================= Глобальные переменные =================
let db;
let COURIERS = {};
const bot = new TelegramBot(TOKEN);
bot.deleteWebHook().catch(() => {});
bot.on("polling_error", (err) => console.error("Polling error:", err));


// ================= Инициализация БД =================
async function initDB() {
  db = await mysql.createConnection({
    host: process.env.MYSQLHOST,
    user: process.env.MYSQLUSER,
    password: process.env.MYSQL_ROOT_PASSWORD,
   database: process.env.MYSQLDATABASE,
    port: parseInt(process.env.MYSQLPORT) || 3306
  });

  console.log("MySQL connected");

  // ===== Создание таблиц =====
  await db.execute(`
    CREATE TABLE IF NOT EXISTS clients (
      id INT AUTO_INCREMENT PRIMARY KEY,
      username VARCHAR(255) UNIQUE,
      first_name VARCHAR(255),
      chat_id BIGINT,
      subscribed TINYINT DEFAULT 1,
      city VARCHAR(255),
      created_at DATETIME,
      last_active DATETIME
    )
  `);

  await db.execute(`
    CREATE TABLE IF NOT EXISTS orders (
      id VARCHAR(255) PRIMARY KEY,
      tgNick VARCHAR(255),
      city VARCHAR(255),
      delivery VARCHAR(255),
      payment VARCHAR(255),
      orderText TEXT,
      date DATE,
      time TIME,
      status VARCHAR(50) DEFAULT 'new',
      courier_username VARCHAR(255),
      taken_at DATETIME,
      delivered_at DATETIME,
      created_at DATETIME,
      client_chat_id BIGINT
    )
  `);

  await db.execute(`
    CREATE TABLE IF NOT EXISTS couriers (
      username VARCHAR(255) PRIMARY KEY,
      chat_id BIGINT
    )
  `);

  await db.execute(`
    CREATE TABLE IF NOT EXISTS order_messages (
      order_id VARCHAR(255),
      chat_id BIGINT,
      message_id BIGINT,
      PRIMARY KEY (order_id, chat_id)
    )
  `);

  await db.execute(`
    CREATE TABLE IF NOT EXISTS reviews (
      id INT AUTO_INCREMENT PRIMARY KEY,
      order_id VARCHAR(255),
      client_username VARCHAR(255),
      courier_username VARCHAR(255),
      rating INT,
      review_text TEXT,
      created_at DATETIME
    )
  `);

  // ===== Индексы =====
  const indexes = [
    ["orders", "idx_orders_status", "status"],
    ["orders", "idx_orders_courier", "courier_username"],
    ["clients", "idx_clients_username", "username"],
    ["order_messages", "idx_order_messages_order_id", "order_id"],
    ["reviews", "idx_reviews_order_id", "order_id"],
    ["reviews", "idx_reviews_courier_username", "courier_username"]
  ];

  for (const [table, index, column] of indexes) {
    await db.execute(`CREATE INDEX IF NOT EXISTS ${index} ON ${table}(${column})`).catch(() => {});
  }

  console.log("База данных и таблицы готовы");
}

function escapeMarkdown(text) {
  if (!text) return "";
  return text.replace(/([*_`[\]])/g, "\\$1");
}

function withAt(username) {
  if (!username) return "—";
  return username.startsWith("@") ? username : `@${username}`;
}



// ================= Курьеры =================
async function getCouriers() {
  const [rows] = await db.execute("SELECT username, chat_id FROM couriers");
  const map = {};
  rows.forEach(r => { if (r.username && r.chat_id) map[r.username] = r.chat_id; });
  return map;
}

async function addCourier(username, chatId = null) {
  if (!username) return false;
  await db.execute(`
    INSERT INTO couriers (username, chat_id)
    VALUES (?, ?)
    ON DUPLICATE KEY UPDATE chat_id=VALUES(chat_id)
  `, [username, chatId]);
  COURIERS = await getCouriers();
  console.log(`Курьер добавлен/обновлён: @${username}, chat_id: ${chatId}`);
  return true;
}


async function removeCourier(username) {
  await db.execute("DELETE FROM couriers WHERE username=?", [username]);
  COURIERS = await getCouriers();
  console.log(`Курьер удалён: @${username}`);
}

function isCourier(username) { return !!COURIERS[username]; }

// ================= Клиенты =================
async function addOrUpdateClient(username, first_name, chat_id) {
  const now = new Date().toISOString().slice(0, 19).replace('T', ' ');

  await db.execute(`
    INSERT INTO clients (username, first_name, subscribed, created_at, last_active, chat_id)
    VALUES (?, ?, 1, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      first_name = VALUES(first_name),
      last_active = VALUES(last_active),
      chat_id = VALUES(chat_id),
      subscribed = 1
  `, [username, first_name, now, now, chat_id]);
}

async function getClient(username) {
  const [rows] = await db.execute("SELECT * FROM clients WHERE username=?", [username]);
  return rows[0];
}

// ================= Заказы =================
// ================= Вспомогательные функции =================

// Преобразует дату в формат MySQL DATETIME: YYYY-MM-DD HH:MM:SS
function formatMySQLDateTime(date = new Date()) {
  const pad = n => n.toString().padStart(2, '0');
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ` +
         `${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
}

// Преобразует дату в формат MySQL DATE: YYYY-MM-DD
function formatMySQLDate(date = new Date()) {
  const pad = n => n.toString().padStart(2, '0');
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}`;
}

// Парсинг даты из dd.mm.yyyy в MySQL формат
function parseDateForMySQL(dateStr) {
  if (!dateStr) return formatMySQLDate(new Date());
  const parts = dateStr.split('.');
  if (parts.length === 3) {
    const [day, month, year] = parts;
    return `${year}-${month.padStart(2,'0')}-${day.padStart(2,'0')}`;
  }
  return dateStr; // если уже в формате YYYY-MM-DD
}

// ================= Заказы =================

async function addOrder(order) {
  // Получаем chat_id клиента по tgNick, если отсутствует
  if (!order.client_chat_id) {
    const cleanNick = order.tgNick.replace(/^@+/, "");
    const client = await getClient(cleanNick);
    if (client?.chat_id) order.client_chat_id = client.chat_id;
  }

  const now = new Date();
  const mysqlDate = order.date ? parseDateForMySQL(order.date) : formatMySQLDate(now);
  const mysqlTime = order.time ? order.time : `${now.getHours()}:${now.getMinutes()}:${now.getSeconds()}`;
  const createdAt = formatMySQLDateTime(now);

  // Вставляем или обновляем заказ
  await db.execute(`
    INSERT INTO orders
      (id, tgNick, city, delivery, payment, orderText, date, time, status, created_at, client_chat_id)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      tgNick = VALUES(tgNick),
      city = VALUES(city),
      delivery = VALUES(delivery),
      payment = VALUES(payment),
      orderText = VALUES(orderText),
      date = VALUES(date),
      time = VALUES(time),
      status = VALUES(status),
      client_chat_id = VALUES(client_chat_id)
  `, [
    order.id,
    order.tgNick,
    order.city,
    order.delivery,
    order.payment,
    order.orderText,
    mysqlDate,
    mysqlTime,
    order.status || "new",
    createdAt,
    order.client_chat_id || null
  ]);

  // Проверяем, было ли уже сообщение клиенту
  const messages = await getOrderMessages(order.id);
  const clientAlreadyNotified = messages.some(m => m.chat_id === order.client_chat_id);

  if (!clientAlreadyNotified) {
    const updatedOrder = await getOrderById(order.id);
    await sendOrUpdateOrder(updatedOrder);
  }
}


async function getOrderById(id) {
  const [rows] = await db.execute("SELECT * FROM orders WHERE id=?", [id]);
  return rows[0];
}

async function updateOrderStatus(id, status, courier_username = null) {
  const now = formatMySQLDateTime();

  switch (status) {
    case "taken":
      await db.execute(
        "UPDATE orders SET status = ?, courier_username = ?, taken_at = ? WHERE id = ?",
        [status, courier_username, now, id]
      );
      break;

    case "delivered":
      await db.execute(
        "UPDATE orders SET status = ?, delivered_at = ?, courier_username = ? WHERE id = ?",
        [status, now, courier_username, id]
      );
      break;

    case "new":
      await db.execute(
        "UPDATE orders SET status = ?, courier_username = NULL, taken_at = NULL, delivered_at = NULL WHERE id = ?",
        [status, id]
      );
      break;

    default:
      throw new Error(`Unknown status: ${status}`);
  }
}

async function takeOrderAtomic(orderId, username) {
  if (!username) return false;
  const now = formatMySQLDateTime();
  const [res] = await db.execute(
    "UPDATE orders SET status='taken', courier_username=?, taken_at=? WHERE id=? AND status='new'",
    [username, now, orderId]
  );
  return res.affectedRows === 1;
}

// ================= Order Messages =================
async function getOrderMessages(orderId) {
  const [rows] = await db.execute("SELECT * FROM order_messages WHERE order_id=?", [orderId]);
  return rows;
}

async function saveOrderMessage(orderId, chatId, messageId) {
  await db.execute(`
    INSERT INTO order_messages (order_id, chat_id, message_id)
    VALUES (?, ?, ?)
    ON DUPLICATE KEY UPDATE message_id=VALUES(message_id)
  `, [orderId, chatId, messageId]);
}

async function clearOrderMessage(orderId, chatId) {
  await db.execute("DELETE FROM order_messages WHERE order_id=? AND chat_id=?", [orderId, chatId]);
}
// =================== Вспомогательная функция ===================
function escapeMarkdownV2(text) {
  if (text == null) return "";
  return String(text).replace(/([\\_*[\]()~`>#+\-=|{}.!])/g, "\\$1");
}


// =================== Восстановление заказов для клиентов ===================
async function restoreOrdersForClients() {
  console.log("[INFO] Восстановление заказов для клиентов...");
  const [clients] = await db.execute("SELECT username, chat_id FROM clients WHERE chat_id IS NOT NULL");

  const limit = pLimit(5);

  for (const client of clients) {
    const [orders] = await db.execute(
      `SELECT * FROM orders WHERE REPLACE(tgNick,'@','') = ? AND status IN ('new','taken') ORDER BY created_at DESC`,
      [client.username]
    );

    const tasks = orders.map(order =>
      limit(async () => {
        try {
          // Проверяем, есть ли уже сообщение клиенту
          const messages = await getOrderMessages(order.id);
          const alreadySent = messages.some(m => m.chat_id === client.chat_id);
          if (alreadySent) return;

          const text = escapeMarkdownV2(buildOrderMessage(order));
          const sent = await bot.sendMessage(client.chat_id, text, { parse_mode: "MarkdownV2" });

          await saveOrderMessage(order.id, client.chat_id, sent.message_id);
          console.log(`[INFO] Отправлен заказ №${order.id} клиенту @${client.username}`);
        } catch (err) {
          console.error(`[ERROR] Ошибка отправки заказа №${order.id} клиенту @${client.username}:`, err.message);
        }
      })
    );

    await Promise.all(tasks);
  }

  console.log("[INFO] Восстановление заказов для клиентов завершено");
}





// =================== Восстановление заказов для курьеров ===================
async function restoreOrdersForCouriers() {
  console.log("[INFO] Восстановление заказов для курьеров...");

  const [orders] = await db.execute("SELECT * FROM orders WHERE status IN ('new','taken') ORDER BY created_at ASC");
  const limit = pLimit(5);

  const tasks = orders.map(order =>
    limit(async () => {
      try {
        const messages = await getOrderMessages(order.id);
        if (messages.some(m => !!COURIERS[m.username])) return; // если курьеры уже получили, пропускаем

        const text = escapeMarkdownV2(buildOrderMessage(order));
        await sendOrUpdateOrder(order, text);
      } catch (err) {
        console.error(`[ERROR] Ошибка восстановления заказа №${order.id}:`, err.message);
      }
    })
  );

  await Promise.all(tasks);
  console.log("[INFO] Восстановление заказов для курьеров завершено");
}




// ==================== Основной блок ====================
(async function main() {
  await initDB();
  COURIERS = await getCouriers();
  await addCourier(ADMIN_USERNAME, ADMIN_ID);

  await restoreOrdersForClients();   // безопасно
  await restoreOrdersForCouriers();  // безопасно

  bot.startPolling();
  console.log("Бот и сервер запущены");
})();

// ================= Транзакция для отмены заказа =================
async function releaseOrderTx(orderId) {
  const conn = await db.getConnection();
  try {
    await conn.beginTransaction();
    await updateOrderStatus(orderId, "new");
    await conn.commit();
  } catch (err) {
    await conn.rollback();
    throw err;
  } finally {
    conn.release();
  }
}

// =================== Построение сообщения =================
const deliveryMap = { "DHL": "DHL", "Курьер": "Курьер" };
const paymentMap = {
  "Наличные": "Наличные",
  "Карта": "Банковская карта",
  "Криптовалюта": "Крипто"
};

function buildOrderMessage(order) {
  const statusMap = {
    new: "Новый",
    taken: "Взято",
    delivered: "Доставлен"
  };

  const courierName = order.courier_username ? '@' + order.courier_username.replace(/^@/, '') : "—";

  return [
    `*Заказ №${escapeMarkdownV2(String(order.id))}*`,
    `*Клиент:* ${escapeMarkdownV2(withAt(order.tgNick))}`,
    `*Город:* ${escapeMarkdownV2(order.city || "—")}`,
    `*Доставка:* ${escapeMarkdownV2(deliveryMap[order.delivery] || order.delivery || "—")}`,
    `*Оплата:* ${escapeMarkdownV2(paymentMap[order.payment] || order.payment || "—")}`,
    `*Дата:* ${escapeMarkdownV2(order.date || "—")}`,
    `*Время:* ${escapeMarkdownV2(order.time || "—")}`,
    "",
    `*Состав заказа:*`,
    `${escapeMarkdownV2(order.orderText || "")}`,
    "",
    `Статус: *${escapeMarkdownV2(statusMap[order.status] || "—")}*`,
    `Курьер: ${escapeMarkdownV2(courierName)}`
  ].join("\n");
}

async function askForReview(order) {
  if (!order.client_chat_id) {
    console.log("НЕТ client_chat_id — отзыв невозможен");
    return;
  }

 waitingReview.set(order.client_chat_id, {
  orderId: order.id,
  courier: order.courier_username
    ? order.courier_username.replace(/^@/, "")
    : "",
  client: order.tgNick.replace(/^@/, ""),
  rating: null
});

  const courierEscaped = order.courier_username 
    ? '@' + escapeMarkdownV2(order.courier_username.replace(/^@/, '')) 
    : '—';
  const orderIdEscaped = escapeMarkdownV2(String(order.id));

  await bot.sendMessage(
    order.client_chat_id,
    `Заказ №${orderIdEscaped} доставлен\n\nКурьер: ${courierEscaped}\n\nПоставьте оценку курьеру:`,
    {
      reply_markup: {
        inline_keyboard: [
          [
            { text: "⭐1", callback_data: `rate_${order.id}_1` },
            { text: "⭐2", callback_data: `rate_${order.id}_2` },
            { text: "⭐3", callback_data: `rate_${order.id}_3` },
            { text: "⭐4", callback_data: `rate_${order.id}_4` },
            { text: "⭐5", callback_data: `rate_${order.id}_5` }
          ]
        ]
      },
      parse_mode: "MarkdownV2"
    }
  );

  console.log(`Запрос отзыва отправлен клиенту @${order.tgNick}`);
}

async function sendOrUpdateOrder(order, text = null) {
  console.log(`[INFO] Начало отправки/обновления заказа №${order.id}, статус: ${order.status}`);

  // Получаем список курьеров с chat_id
  const [courierRows] = await db.execute(
    "SELECT username, chat_id FROM couriers WHERE chat_id IS NOT NULL"
  );

  // Формируем уникальный список получателей
  const recipientsMap = new Map();

  // Админ
  if (ADMIN_ID && ADMIN_USERNAME) {
    recipientsMap.set(ADMIN_ID, { username: ADMIN_USERNAME, chatId: ADMIN_ID });
  }

  // Курьеры
  courierRows.forEach(r => {
    if (r.chat_id) recipientsMap.set(r.chat_id, { username: r.username, chatId: r.chat_id });
  });

  // Клиент
  if (order.client_chat_id) {
    recipientsMap.set(order.client_chat_id, {
      username: order.tgNick.replace(/^@/, ""),
      chatId: order.client_chat_id
    });
  }

  const recipients = Array.from(recipientsMap.values());
  const limit = pLimit(5); // ограничение параллельных отправок

  const tasks = recipients.map(recipient =>
    limit(async () => {
      if (!recipient.chatId) return;

      const isClient = recipient.chatId === order.client_chat_id;
      const isAdmin = recipient.chatId === ADMIN_ID;
      const isCourier = !!COURIERS[recipient.username];
      const isOwnerCourier = order.courier_username?.replace(/^@/, "") === recipient.username;

     // ================== Кнопки ==================
let keyboard = [];

// Курьеры и админ
const canSeeButtons = !isClient && (isCourier || isAdmin);

if (canSeeButtons) {
  if (order.status === "new") {
    keyboard.push([{ text: "🚚 Взять заказ", callback_data: `take_${order.id}` }]);
  } 
  else if (order.status === "taken" && isOwnerCourier) {
    keyboard.push([
      { text: "❌ Отказаться", callback_data: `release_${order.id}` },
      { text: "✅ Доставлено", callback_data: `delivered_${order.id}` }
    ]);
  }
}

// ===== Кнопка для клиента =====
if (isClient) {
  const orderAge = Date.now() - new Date(order.created_at).getTime();

  // ❗ ТОЛЬКО подтверждение отмены
  if (order.status === "new" && orderAge <= 20 * 60 * 1000) {
    keyboard.push([
      { text: "❌ Отменить заказ", callback_data: `confirm_cancel_${order.id}` }
    ]);
  }
}

// ❗ Если заказ отменён — НИКАКИХ кнопок ни у кого
if (order.status === "canceled") {
  keyboard = [];
}


     // Формируем текст
let msgText = text || buildOrderMessage({
  ...order,
  courier_username: order.courier_username || "—"
});

// ❗ Если заказ отменён — добавляем уведомление прямо в том же сообщении
if (order.status === "canceled") {
  msgText += "\n\n❌ Заказ был отменён покупателем";
  keyboard = []; // убираем кнопки полностью
}


      try {
        // Проверяем существующие сообщения
        const messages = await getOrderMessages(order.id);
        const existingMsg = messages.find(m => m.chat_id === recipient.chatId);

        if (existingMsg) {
          await bot.editMessageText(msgText, {
            chat_id: recipient.chatId,
            message_id: existingMsg.message_id,
            parse_mode: "MarkdownV2",
            reply_markup: keyboard.length ? { inline_keyboard: keyboard } : undefined
          });
        } else {
          const sent = await bot.sendMessage(recipient.chatId, msgText, {
            parse_mode: "MarkdownV2",
            reply_markup: keyboard.length ? { inline_keyboard: keyboard } : undefined
          });
          await saveOrderMessage(order.id, recipient.chatId, sent.message_id);
        }

        if (keyboard.length) {
          console.log(`[INFO] @${recipient.username} видит кнопки: ${keyboard.map(k => k.map(b => b.text).join(",")).join(" | ")}`);
        }
      } catch (err) {
        if (!err.message.includes("message is not modified") &&
            !err.message.includes("chat not found")) {
          console.error(`[ERROR] Ошибка отправки заказа №${order.id} для @${recipient.username}:`, err.message);
        }
      }
    })
  );

  await Promise.all(tasks);
  console.log(`[INFO] Завершена отправка/обновление заказа №${order.id}`);
}

// ============== Telegram: callback =================
bot.on("callback_query", async (q) => {
  const data = q.data || "";
  const fromId = q.from.id;
  const username = q.from.username;

  console.log(`[CALLBACK] Пользователь @${username} (${fromId}) нажал: ${data}`);

  if (!username) {
    console.log("У пользователя нет username");
    return bot.answerCallbackQuery(q.id, {
      text: "У вас нет username",
      show_alert: true
    });
  }

  // ================== Рейтинг / отзыв ==================
  if (data.startsWith("rate_")) {
    const [, orderId, rating] = data.split("_");
    const review = waitingReview.get(fromId);

    if (!review || review.orderId !== orderId) {
      return bot.answerCallbackQuery(q.id, {
        text: "Отзыв уже отправлен или устарел",
        show_alert: true
      });
    }

    review.rating = Number(rating);
    waitingReview.set(fromId, review);

    await bot.sendMessage(
      fromId,
      "Отлично! Теперь напишите текст отзыва одним сообщением."
    );

    return bot.answerCallbackQuery(q.id, {
      text: `Оценка ${rating} сохранена`
    });
  }

// ================== Просмотр отзывов курьера ==================
if (data.startsWith("reviews_") && fromId === ADMIN_ID) {

  // username курьера БЕЗ @ (так как в БД без собачки)
  const courierUsername = data.replace("reviews_", "").replace(/^@/, "");

  try {
    const [reviews] = await db.execute(
      `SELECT order_id, client_username, courier_username, rating, review_text, created_at
       FROM reviews
       WHERE courier_username = ?
       ORDER BY created_at DESC`,
      [courierUsername]
    );

   if (!reviews || reviews.length === 0) {
  return bot.sendMessage(
    fromId,
    `❌ У пользователя @${escapeMarkdownV2(courierUsername)} пока нет отзывов`,
    { parse_mode: "MarkdownV2" }
  );
}


    const msg = reviews.map(r =>
  `*Заказ №${escapeMarkdownV2(r.order_id)}*\n` +
  `👤 Клиент: @${escapeMarkdownV2(r.client_username)}\n` +
  `🚚 Курьер: @${escapeMarkdownV2(r.courier_username)}\n` +
  `⭐ Оценка: ${r.rating}/5\n` +  // число экранировать не нужно
  `📝 Отзыв: ${escapeMarkdownV2(r.review_text || "—")}\n` +
  `📅 Дата: ${escapeMarkdownV2(new Date(r.created_at).toLocaleString("ru-RU"))}`
).join("\n\n--------------------\n\n");


    await bot.sendMessage(
      fromId,
      msg.length > 4000 ? msg.slice(0, 4000) + "\n…и ещё отзывы" : msg,
      { parse_mode: "MarkdownV2" }
    );

  } catch (err) {
    console.error("Отзывы курьера:", err);
    await bot.sendMessage(fromId, "Ошибка при получении отзывов");
  }

  return bot.answerCallbackQuery(q.id, { text: "Отзывы загружены" });
}





// ================== Основная часть (заказы) ==================
let orderId = null;
let order = null;

// Определяем orderId ПРАВИЛЬНО
if (
  data.startsWith("take_") ||
  data.startsWith("release_") ||
  data.startsWith("cancel_") ||
  data.startsWith("delivered_")
) {
  orderId = data.split("_")[1];
}
else if (
  data.startsWith("confirm_cancel_") ||
  data.startsWith("no_cancel_")
) {
  orderId = data.split("_")[2];
}

// Если orderId определён — загружаем заказ
if (orderId) {
  order = await getOrderById(orderId);

  if (!order) {
    console.log(`Заказ ${orderId} не найден`);
    return bot.answerCallbackQuery(q.id, {
      text: "Заказ не найден",
      show_alert: true
    });
  }
}

// ================== TAKE ==================
if (data.startsWith("take_")) {
  console.log(`TAKE заказ ${orderId} пользователем @${username}`);

  // Проверка: только курьер или админ
  const canTake = (await isCourier(username)) || fromId === ADMIN_ID;
  if (!canTake) {
    return bot.answerCallbackQuery(q.id, {
      text: "Только курьеры могут брать заказ",
      show_alert: true
    });
  }

  // Пытаемся взять заказ атомарно
  const success = await takeOrderAtomic(orderId, username);
  console.log(`Результат попытки взять заказ ${orderId}: ${success ? "успешно" : "не удалось"}`);

  if (!success) {
    return bot.answerCallbackQuery(q.id, {
      text: "Заказ уже взят другим курьером!",
      show_alert: true
    });
  }

  // Получаем обновлённый заказ
  const updatedOrder = await getOrderById(orderId);

  // Отправляем/обновляем сообщение всем получателям
  await sendOrUpdateOrder(updatedOrder);

  return bot.answerCallbackQuery(q.id, { text: "Заказ взят" });
}

// ================== RELEASE ==================
if (data.startsWith("release_")) {
  console.log(`RELEASE заказ ${orderId} пользователем @${username}`);

  // Проверка: заказ должен быть в статусе 'taken'
  if (order.status !== "taken") {
    return bot.answerCallbackQuery(q.id, {
      text: "От этого заказа уже отказались",
      show_alert: true
    });
  }

  // Проверка: только курьер, который взял заказ, или админ
  const isOwnerOrAdmin = order.courier_username?.replace(/^@/, "") === username || fromId === ADMIN_ID;
  if (!isOwnerOrAdmin) {
    return bot.answerCallbackQuery(q.id, {
      text: "Вы не можете отказаться от этого заказа",
      show_alert: true
    });
  }

  const oldCourier = order.courier_username;

  try {
    // Сбрасываем заказ в статус 'new' и убираем курьера
    await db.execute(
      "UPDATE orders SET status='new', courier_username=NULL WHERE id=?",
      [orderId]
    );

    console.log(`Заказ ${orderId} сброшен на 'new'`);

    const updatedOrder = await getOrderById(orderId);

    // Обновляем/отправляем всем
    await sendOrUpdateOrder(updatedOrder);

    // Уведомление админа, если отказался курьер
    if (ADMIN_ID && oldCourier && oldCourier !== ADMIN_USERNAME) {
      await bot.sendMessage(
        ADMIN_ID,
        `Курьер @${oldCourier} отказался от заказа №${orderId}`
      );
    }

    return bot.answerCallbackQuery(q.id, {
      text: "Вы отказались от заказа"
    });

  } catch (err) {
    console.error(`Ошибка при отказе от заказа ${orderId}:`, err.message);
    return bot.answerCallbackQuery(q.id, {
      text: "Ошибка при отказе",
      show_alert: true
    });
  }
}

// 1️⃣ CONFIRM CANCEL
if (data.startsWith("confirm_cancel_")) {
  const orderId = data.split("_")[2];
  const order = await getOrderById(orderId);
  if (!order) return bot.answerCallbackQuery(q.id, { text: "Заказ не найден", show_alert: true });

  const orderAge = Date.now() - new Date(order.created_at).getTime();
  if (orderAge > 20 * 60 * 1000 || order.status !== "new") {
    return bot.answerCallbackQuery(q.id, { text: "Заказ не отменяем", show_alert: true });
  }

  await bot.editMessageText(
    `Вы точно хотите отменить заказ #${order.id}?`,
    {
      chat_id: fromId,
      message_id: q.message.message_id,
      reply_markup: {
        inline_keyboard: [
          [
            { text: "✅ Да, отменить", callback_data: `cancel_${order.id}` },
            { text: "❌ Нет", callback_data: `no_cancel_${order.id}` }
          ]
        ]
      }
    }
  );

  return bot.answerCallbackQuery(q.id);
}

// 2️⃣ NO CANCEL
if (data.startsWith("no_cancel_")) {
  const orderId = data.split("_")[2];
  const order = await getOrderById(orderId);
  if (!order) return bot.answerCallbackQuery(q.id, { text: "Заказ не найден", show_alert: true });

  await sendOrUpdateOrder(order); // возвращаем обычные кнопки
  return bot.answerCallbackQuery(q.id, { text: "Отмена отменена" });
}

// 3️⃣ FINAL CANCEL
if (data.startsWith("cancel_")) {
  const orderId = data.split("_")[1];
  const order = await getOrderById(orderId);
  if (!order) return bot.answerCallbackQuery(q.id, { text: "Заказ не найден", show_alert: true });

  if (order.client_chat_id !== fromId) {
    return bot.answerCallbackQuery(q.id, { text: "Вы не можете отменить этот заказ", show_alert: true });
  }

  try {
    // Ставим статус canceled и убираем курьера
    await db.execute("UPDATE orders SET status='canceled', courier_username=NULL WHERE id=?", [orderId]);

    // Обновляем сообщения для всех (клиент + курьеры)
    const updatedOrder = await getOrderById(orderId);
    await sendOrUpdateOrder(updatedOrder);


    broadcastStock();

    return bot.answerCallbackQuery(q.id, { text: "Заказ успешно отменен" });
  } catch (err) {
    console.error(err);
    return bot.answerCallbackQuery(q.id, { text: "Ошибка при отмене", show_alert: true });
  }
}



  // ================== DELIVERED ==================
if (data.startsWith("delivered_")) {                  // ← открытие DELIVERED
  console.log(`DELIVERED заказ ${orderId} пользователем @${username}`);

  if (order.courier_username !== username && fromId !== ADMIN_ID) {
    console.log(`Пользователь @${username} не может отметить заказ ${orderId} как доставленный`);
    return bot.answerCallbackQuery(q.id, {
      text: "Нельзя отметить",
      show_alert: true
    });
  }

  await updateOrderStatus(orderId, "delivered", username);

  const updatedOrder = await getOrderById(orderId);
  await sendOrUpdateOrder(updatedOrder);

  if (updatedOrder.client_chat_id && !waitingReview.has(updatedOrder.client_chat_id)) {
    await askForReview(updatedOrder);
  }

  console.log(`Заказ ${orderId} помечен как доставленный`);

  return bot.answerCallbackQuery(q.id, {
    text: "Заказ доставлен"
  });
}
                                                    
});                                                   


// ================== /start и меню =================
// ... остальной код меню, панель курьера, админка, рассылки и API без изменений


// ================== /start ==================
bot.onText(/\/start/, async (msg) => {
  const id = msg.from.id;
  const username = msg.from.username || `id${id}`;
  const first_name = msg.from.first_name || "";

  console.log(` /start от @${username} (id: ${id}), имя: ${first_name}`);

  try {
    // Проверяем, новый ли пользователь
    const [existing] = await db.execute("SELECT id FROM clients WHERE username=?", [username]);
    const isNew = existing.length === 0;

    // Сохраняем или обновляем клиента
    await addOrUpdateClient(username, first_name, id);
    console.log(`Клиент @${username} добавлен/обновлён в базе`);

    // Если курьер, сохраняем в таблицу couriers и обновляем COURIERS
    if (await isCourier(username)) {
      await db.execute(
        `INSERT INTO couriers (username, chat_id)
         VALUES (?, ?)
         ON DUPLICATE KEY UPDATE chat_id = VALUES(chat_id)`,
        [username, id]
      );
      COURIERS = await getCouriers();
      console.log(`Курьер @${username} добавлен/обновлён, chat_id: ${id}`);
    }

    // Формируем приветственное сообщение и клавиатуру
    let welcomeText = "Добро пожаловать! Чтобы оформить заказ нажмите кнопку снизу открыть магазин.";
    let keyboard = [];

    if (username === ADMIN_USERNAME) {
      welcomeText += "\nПанель администратора и Панель курьера доступны через текстовые кнопки ниже.";
      keyboard = [
        [{ text: "Статистика" }, { text: "Курьеры" }],
        [{ text: "Добавить курьера" }, { text: "Удалить курьера" }],
        [{ text: "Список курьеров" }, { text: "Все пользователи" }],
        [{ text: "Рассылка" }, { text: "Выполненные заказы" }],
        [{ text: "Назад" }]
      ];
      console.log(`Админ @${username} видит админ меню`);
    } else if (await isCourier(username)) {
      welcomeText += "\nПанель курьера доступна через текстовые кнопки ниже.";
      keyboard = [
        [{ text: "Личный кабинет" }, { text: "Поддержка" }],
        [{ text: "Панель курьера" }]
      ];
      console.log(`Курьер @${username} видит курьерское меню`);
    } else {
      keyboard = [
        [{ text: "Личный кабинет" }, { text: "Поддержка" }],
        [{ text: "Мои заказы" }]
      ];
      console.log(`Пользователь @${username} видит обычное меню с кнопкой "Мои заказы"`);
    }

 // Отправляем сообщение пользователю
await bot.sendMessage(id, welcomeText, {
  reply_markup: { keyboard, resize_keyboard: true }
});

// ===== Уведомление админу о новом пользователе =====
if (isNew && ADMIN_ID) {
  const login = msg.from.username ? `@${escapeMarkdown(msg.from.username)}` : "—";

  try {
    await bot.sendMessage(
      ADMIN_ID,
      `🆕 *Новый пользователь*\n\nИмя: *${escapeMarkdown(first_name) || "—"}*
\nЛогин: ${login}\nChat ID: \`${id}\``,
      { parse_mode: "Markdown" }
    );
    console.log(`Админу отправлено уведомление о новом пользователе @${username}`);
  } catch (err) {
    console.error("Не удалось отправить уведомление админу:", err.message);
  }
}

console.log(`Приветственное сообщение отправлено @${username}`);

  } catch (err) {
    console.error(`Ошибка обработки /start для @${username}:`, err.message);
  }
});

// ================== Панель курьера и админка ==================
const adminWaitingCourier = new Map(); 
const adminWaitingBroadcast = new Map(); 

(async () => {
  try {
    await db.execute("ALTER TABLE clients ADD COLUMN banned TINYINT(1) DEFAULT 0");
    console.log("Колонка banned добавлена в clients");
  } catch (e) {
    console.log("Колонка banned уже существует");
  }
})();


// ===== Основной обработчик сообщений =====
bot.on("message", async (msg) => {
  const id = msg.from.id;
  const username = msg.from.username; // username должен быть для курьеров
  const first_name = msg.from.first_name || "";

  if (!msg.text) return;
  const text = msg.text.trim();

  // Проверка username
  if (!username) {
    console.log(`[WARN] Пользователь с chat_id ${id} не имеет username`);
    return bot.sendMessage(id, "У вас нет username, бот не сможет идентифицировать вас как курьера.");
  }

  // ===== Логирование всех сообщений =====
  console.log(" MESSAGE", {
    from: id,
    username,
    text,
    waitingReview: waitingReview.has(id)
  });


// ===== Проверка бана =====
try {
  const [userRows] = await db.execute(
    "SELECT banned FROM clients WHERE username = ?",
    [username]
  );
  if (userRows[0] && userRows[0].banned) {
    // Пользователь забанен — ничего не может писать
    return bot.sendMessage(id, "Вы заблокированы и не можете использовать бота.");
  }
} catch (err) {
  console.error(`Ошибка проверки бана для @${username}:`, err.message);
}



 console.log(
    " MESSAGE",
    {
      from: id,
      username,
      text: msg.text,
      waitingReview: waitingReview.has(id)
    }
  );




  
    // ===== Прием отзыва от клиента =====
// ===== Прием отзыва от клиента =====
if (waitingReview.has(id)) {
  const review = waitingReview.get(id);

  //  ПРОВЕРКА №2 — запрет текста без оценки
  if (review.rating === null) {
    return bot.sendMessage(
      id,
      "Пожалуйста, сначала выберите оценку кнопкой выше"
    );
  }

  //  запрет служебных сообщений
  const forbidden = [
    "Назад",
    "Панель курьера",
    "Панель администратора",
    "/start"
  ];

  if (forbidden.includes(text)) {
    return bot.sendMessage(
      id,
      "Пожалуйста, напишите именно текст отзыва"
    );
  }

  // Валидация текста отзыва
  const reviewText = text.trim();
  if (!reviewText) {
    return bot.sendMessage(id, "Пожалуйста, напишите текст отзыва (не пустой)");
  }
  if (reviewText.length < 3) {
    return bot.sendMessage(id, "Слишком короткий отзыв, напишите хотя бы несколько слов");
  }

// ===== добавляем колонки rating и review_text в reviews, если ещё нет =====
try {
  await db.execute("ALTER TABLE reviews ADD COLUMN rating INT");
  console.log("rating добавлен в reviews");
} catch (e) {
  console.log("rating уже существует в reviews");
}

try {
  await db.execute("ALTER TABLE reviews ADD COLUMN review_text TEXT");
  console.log("review_text добавлен в reviews");
} catch (e) {
  console.log("review_text уже существует в reviews");
}

// ===== сохраняем отзыв + рейтинг =====
const now = new Date().toISOString().slice(0, 19).replace("T", " "); // MySQL DATETIME

// Убираем @ перед сохранением в БД
const courierNick = review.courier.replace(/^@/, "");
const clientNick = review.client.replace(/^@/, "");

// Сохраняем в БД (не меняем Markdown, БД спокойно хранит спецсимволы)
await db.execute(
  `INSERT INTO reviews (
     order_id,
     client_username,
     courier_username,
     rating,
     review_text,
     created_at
   ) VALUES (?, ?, ?, ?, ?, ?)`,
  [review.orderId, clientNick, courierNick, review.rating, reviewText, now]
);

console.log(
  `Отзыв сохранён: заказ ${review.orderId}, рейтинг ${review.rating}, клиент @${clientNick}, курьер @${courierNick}`
);

// ===== Экранируем MarkdownV2 перед отправкой =====
function escapeMarkdownV2(text) {
  if (text == null) return "";
  return String(text).replace(/([\\_*[\]()~`>#+\-=|{}.!])/g, "\\$1");
}

// отправляем админу
await bot.sendMessage(
  ADMIN_ID,
  `Новый отзыв

Заказ: №${review.orderId}
Клиент: @${escapeMarkdownV2(clientNick)}
Курьер: @${escapeMarkdownV2(courierNick)}
Оценка: ${review.rating}/5

Отзыв:
${escapeMarkdownV2(reviewText)}`,
  { parse_mode: "MarkdownV2" }
);

waitingReview.delete(id);

return bot.sendMessage(
  id,
  "Спасибо за отзыв! Он отправлен администратору."
);
}


// ===== Обработка выбора курьера для просмотра его заказов =====
if (adminWaitingOrdersCourier.has(username)) {
  if (text === "Назад") {
    // Пользователь отменил выбор курьера, возвращаем панель админа
    adminWaitingOrdersCourier.delete(username);
    return bot.sendMessage(id, "Панель администратора", {
      reply_markup: {
        keyboard: [
          [{ text: "Статистика" }, { text: "Курьеры" }],
          [{ text: "Добавить курьера" }, { text: "Удалить курьера" }],
          [{ text: "Список курьеров" }, { text: "Рассылка" }],
          [{ text: "Выполненные заказы" }, { text: "Назад" }]
        ],
        resize_keyboard: true
      }
    });
  }

  const selectedCourier = text.replace(/^@/, "").trim();
  if (!selectedCourier) {
    return bot.sendMessage(id, "Пожалуйста, введите ник курьера, например @username");
  }

 // Проверка существования курьера
const [rows] = await db.execute("SELECT 1 FROM couriers WHERE username = ?", [selectedCourier]);

if (rows.length === 0) {
  return bot.sendMessage(id, `Курьер @${selectedCourier} не найден`);
}

  // Получаем состояние просмотра: "active" или "done"
  const state = adminWaitingOrdersCourier.get(username);
  const showDone = state.type === "done";
  
// Получаем заказы в зависимости от типа
const query = showDone
  ? "SELECT * FROM orders WHERE status='delivered' AND courier_username=?"
  : "SELECT * FROM orders WHERE status IN ('new','taken') AND courier_username=?";

const [orders] = await db.execute(query, [selectedCourier]);

if (!orders || orders.length === 0) {
  return bot.sendMessage(
    id,
    `Курьер @${selectedCourier} пока не имеет ${showDone ? "выполненных" : "активных"} заказов`
  );
}

// Отправка заголовка
await bot.sendMessage(
  id,
  `${showDone ? "Выполненные" : "Активные"} заказы курьера @${selectedCourier}:`
);

// Отправка заказов параллельно с гарантией строк
await Promise.all(
  orders.map(async (o) => {
    // Приводим все важные поля к строкам
    o.orderText = o.orderText || "—";
    o.tgNick = o.tgNick || "—";
    o.city = o.city || "—";
    o.delivery = o.delivery || "—";
    o.payment = o.payment || "—";
    o.date = o.date || "—";
    o.time = o.time || "—";

    try {
      const text = String(buildOrderMessage(o)); // гарантируем строку
      await bot.sendMessage(id, text, { parse_mode: "MarkdownV2" });
    } catch (err) {
      console.error(`Ошибка отправки заказа №${o.id} @${selectedCourier}:`, err.message);
    }
  })
);

  // Состояние оставляем, чтобы админ мог выбрать следующего курьера
  return;
}

// Если админ в состоянии ожидания ввода ника, но нажал кнопку меню
const menuCommands = ["Список курьеров", "Назад", "Панель администратора"];
if (adminWaitingCourier.has(username) && menuCommands.includes(text)) {
  adminWaitingCourier.delete(username); // сброс ожидания
  console.log(`Состояние ожидания ника сброшено для @${username} из-за меню`);
}

// ===== Просмотр всех курьеров (кнопка 📈 Курьеры) =====
if (text === "Курьеры" && id === ADMIN_ID) {
  // Получаем список курьеров из MySQL
  const [couriers] = await db.execute("SELECT username, chat_id FROM couriers");
  
  if (couriers.length === 0) return bot.sendMessage(id, "Нет курьеров");
  
  const list = couriers
    .map(c => `@${c.username} — chat_id: ${c.chat_id || "неизвестно"}`)
    .join("\n");

  console.log(`Админ @${username} запросил список курьеров`);
  return bot.sendMessage(id, "Список курьеров:\n" + list);
}



// 👉 Проверяем — новый ли пользователь
const [rows] = await db.execute(
  "SELECT id FROM clients WHERE chat_id=?",
  [id]
);
const isNew = rows.length === 0;



  // Добавляем или обновляем клиента
await addOrUpdateClient(username, first_name, id);
const client = await getClient(username);

 // ===== Главное меню =====
if (text === "Назад") {
  if (id === ADMIN_ID) {
    return bot.sendMessage(id, "Главное меню админа", {
      reply_markup: {
        keyboard: [
          [{ text: "Панель администратора" }, { text: "Панель курьера" }]
        ],
        resize_keyboard: true
      }
    });
  }

  if (COURIERS[username]) {
    return bot.sendMessage(id, "Главное меню курьера", {
      reply_markup: {
        keyboard: [
          [{ text: "Панель курьера" }]
        ],
        resize_keyboard: true
      }
    });
  }

  // Главное меню для обычного пользователя
  return bot.sendMessage(id, "Главное меню", {
    reply_markup: {
      keyboard: [
        [{ text: "Личный кабинет" }, { text: "Поддержка" }],
        [{ text: "Мои заказы" }]
      ],
      resize_keyboard: true
    }
  });
}


// ===== Команды бан/разбан =====
if (text.startsWith("/ban ") && id === ADMIN_ID) {
  const uname = text.replace("/ban ", "").replace(/^@/, "").trim();
  await db.execute("UPDATE clients SET banned = 1 WHERE username = ?", [uname]);
  return bot.sendMessage(ADMIN_ID, `Пользователь @${uname} забанен`);
}

if (text.startsWith("/unban ") && id === ADMIN_ID) {
  const uname = text.replace("/unban ", "").replace(/^@/, "").trim();
  await db.execute("UPDATE clients SET banned = 0 WHERE username = ?", [uname]);
  return bot.sendMessage(ADMIN_ID, `Пользователь @${uname} разбанен`);
}

if (text === "/banned" && id === ADMIN_ID) {
  const [rows] = await db.execute("SELECT username FROM clients WHERE banned = 1");
  if (rows.length === 0) return bot.sendMessage(ADMIN_ID, "Нет забаненных пользователей");
  const list = rows.map(r => `@${r.username}`).join("\n");
  return bot.sendMessage(ADMIN_ID, "Забаненные пользователи:\n" + list);
}


// ===== Личный кабинет =====
if (text === "Личный кабинет") {
  try {
    // Получаем количество заказов пользователя
    const [[{ cnt: totalOrders }]] = await db.execute(
      "SELECT COUNT(*) AS cnt FROM orders WHERE tgNick = ?",
      [username]
    );

    const info = [
      `Имя: ${client.first_name || "—"}`,
      `Город: ${client.city || "—"}`,
      `Последняя активность: ${client.last_active || "—"}`,
      `Всего заказов: ${totalOrders || 0}`
    ].join("\n");

    return bot.sendMessage(id, info);
  } catch (err) {
    console.error(`Ошибка получения данных личного кабинета для @${username}:`, err.message);
    return bot.sendMessage(id, "Ошибка при получении информации о личном кабинете.");
  }
}


  // ===== Поддержка =====
  if (text === "Поддержка") {
    return bot.sendMessage(id, "Свяжитесь с поддержкой через @crazycloud_manager.");
  }
// ===== Менюшка =====
if (text === "Мои заказы") {
  return bot.sendMessage(id, "Что показать?", {
    reply_markup: {
      keyboard: [
        [{ text: "Активные заказы" }],
        [{ text: "Выполненные заказы" }],
        [{ text: "Назад" }]
      ],
      resize_keyboard: true
    }
  });
}

if (text === "Назад") {
  return bot.sendMessage(id, "Главное меню", {  
    reply_markup: {
      keyboard: [
        [{ text: "Личный кабинет" }, { text: "Поддержка" }, { text: "Мои заказы" }]
      ],
      resize_keyboard: true
    }
  });
}

//
// ---------- АКТИВНЫЕ ЗАКАЗЫ --------------
//
if (text === "Активные заказы") {

  const [orders] = await db.query(
    "SELECT * FROM orders WHERE client_chat_id = ? AND status != 'delivered' ORDER BY created_at DESC",
    [id]
  );

  if (!orders.length) {
    return bot.sendMessage(id, "Активных заказов пока нет 🙂", {
      reply_markup: {
        keyboard: [
          [{ text: "Активные заказы" }],
          [{ text: "Выполненные заказы" }],
          [{ text: "Назад" }]
        ],
        resize_keyboard: true
      }
    });
  }

  const msg = orders
    .map(o => `#${o.id} — статус: ${o.status}\n${o.orderText || "—"}`)
    .join("\n\n");

  return bot.sendMessage(id, msg, {
    reply_markup: {
      keyboard: [
        [{ text: "Активные заказы" }],
        [{ text: "Выполненные заказы" }],
        [{ text: "Назад" }]
      ],
      resize_keyboard: true
    }
  });
}

//
// ---------- ВЫПОЛНЕННЫЕ ЗАКАЗЫ --------------
//
if (text === "Выполненные заказы") {

  const [orders] = await db.query(
    "SELECT * FROM orders WHERE client_chat_id = ? AND status = 'delivered' ORDER BY delivered_at DESC",
    [id]
  );

  if (!orders.length) {
    return bot.sendMessage(id, "Выполненных заказов пока нет.", {
      reply_markup: {
        keyboard: [
          [{ text: "Активные заказы" }],
          [{ text: "Выполненные заказы" }],
          [{ text: "Назад" }]
        ],
        resize_keyboard: true
      }
    });
  }

  const msg = orders
    .map(o => {
      const deliveredAt = o.delivered_at || o.created_at;
      const d = new Date(deliveredAt);

      return `#${o.id} — доставлен: ${d.toLocaleDateString("ru-RU")} ${d.toLocaleTimeString("ru-RU")}\n${o.orderText || "—"}`;
    })
    .join("\n\n");

  return bot.sendMessage(id, msg, {
    reply_markup: {
      keyboard: [
        [{ text: "Активные заказы" }],
        [{ text: "Выполненные заказы" }],
        [{ text: "Назад" }]
      ],
      resize_keyboard: true
    }
  });
}



  // ===== Панель администратора =====
// ===== Панель администратора =====
if (text === "Панель администратора" && id === ADMIN_ID) {
  const kb = {
    keyboard: [
      [{ text: "Статистика" }, { text: "Курьеры" }],
      [{ text: "Добавить курьера" }, { text: "Удалить курьера" }],
      [{ text: "Список курьеров" }, { text: "Все пользователи" }], // добавили кнопку
      [{ text: "Рассылка" }, { text: "Выполненные заказы" }],
      [{ text: "Назад" }]
    ],
    resize_keyboard: true
  };
  return bot.sendMessage(id, "Панель администратора", { reply_markup: kb });
}


// ===== Все пользователи =====
if (text === "Все пользователи" && id === ADMIN_ID) {
  try {
    // Получаем всех клиентов из базы
    const [clients] = await db.execute("SELECT username, chat_id FROM clients");

    if (clients.length === 0) {
      return bot.sendMessage(id, "Пользователей пока нет.");
    }

    // Формируем список
    const list = clients
      .map(c => `@${c.username} — chat_id: ${c.chat_id || "неизвестно"}`)
      .join("\n");

    return bot.sendMessage(id, "Список всех пользователей:\n" + list);
  } catch (err) {
    console.error("Ошибка получения списка пользователей:", err.message);
    return bot.sendMessage(id, "Произошла ошибка при получении списка пользователей.");
  }
}



// ===== Добавить / удалить курьера =====
if (text === "Добавить курьера" && id === ADMIN_ID) {
  adminWaitingCourier.set(username, { action: "add" });
  return bot.sendMessage(id, "Введите ник курьера, чтобы добавить (@username):");
}

if (text === "Удалить курьера" && id === ADMIN_ID) {
  adminWaitingCourier.set(username, { action: "remove" });
  return bot.sendMessage(id, "Введите ник курьера, чтобы удалить (@username):");
}

// ===== Обработка введённого ника курьера =====
if (adminWaitingCourier.has(username)) {
  const { action } = adminWaitingCourier.get(username);
  if (!text.startsWith("@")) {
    return bot.sendMessage(id, "Ник должен начинаться с @");
  }

  const uname = text.replace(/^@+/, "").trim();

  // Получаем клиента асинхронно
  const client = await getClient(uname);

  if (action === "add") {
    if (client && client.chat_id) {
      // Добавляем курьера с chat_id
      await addCourier(uname, client.chat_id);
      await bot.sendMessage(ADMIN_ID, `Курьер @${uname} добавлен`);
    } else {
      // Добавляем курьера без chat_id (ещё не писал боту)
      await addCourier(uname, null);
      await bot.sendMessage(ADMIN_ID, `Курьер @${uname} добавлен (ещё не писал боту)`);
    }
  } else if (action === "remove") {
    // Удаляем курьера
    await removeCourier(uname);
    await bot.sendMessage(ADMIN_ID, `Курьер @${uname} удалён`);
  }

  // Обновляем глобальный объект курьеров
  COURIERS = await getCouriers();

  // Сбрасываем состояние ожидания ника
  adminWaitingCourier.delete(username);

  return;
}


// ===== Список курьеров =====
if (text === "Список курьеров" && id === ADMIN_ID) {
  adminWaitingCourier.delete(username); // убираем ожидание ника

  // Получаем список курьеров из MySQL
  const [couriers] = await db.execute("SELECT username FROM couriers");

  let list = couriers.map(c => `@${c.username}`);
  if (list.length === 0) list = ["Нет курьеров"];

  return bot.sendMessage(ADMIN_ID, "Список курьеров:\n" + list.join("\n"));
}


// ===== Выбор курьера и просмотр его заказов =====
if (text === "Заказы курьера" && id === ADMIN_ID) {
  // Получаем список курьеров из MySQL
  const [couriers] = await db.execute("SELECT username FROM couriers");
  
  if (couriers.length === 0) {
    return bot.sendMessage(id, "Нет курьеров для выбора");
  }

  const keyboard = couriers.map(c => [{ text: `@${c.username}` }]);
  keyboard.push([{ text: "Назад" }]); // кнопка возврата

  await bot.sendMessage(id, "Выберите курьера, чтобы посмотреть его активные заказы:", {
    reply_markup: { keyboard, resize_keyboard: true }
  });

  // Сохраняем состояние выбора курьера и тип просмотра "active"
  adminWaitingOrdersCourier.set(username, { type: "active" });
  return;
}
// ===== Выполненные заказы (выбор курьера) =====
if (text === "Выполненные заказы" && id === ADMIN_ID) {
  const [couriers] = await db.execute("SELECT username FROM couriers");
  if (couriers.length === 0) return bot.sendMessage(id, "Нет курьеров для выбора");

  const keyboard = couriers.map(c => [{ text: `@${c.username}` }]);
  keyboard.push([{ text: "Назад" }]);

  // Сохраняем состояние выбора курьера, чтобы потом отправлять заказы
  adminWaitingOrdersCourier.set(username, { type: "done" });

  return bot.sendMessage(id, "Выберите курьера, чтобы посмотреть его выполненные заказы:", {
    reply_markup: { keyboard, resize_keyboard: true }
  });
}

if (text === "Статистика" && id === ADMIN_ID) {
  try {
    // ===== Статистика заказов =====
    const [[{ c: total }]] = await db.execute("SELECT COUNT(*) AS c FROM orders");
    const [[{ c: newO }]] = await db.execute("SELECT COUNT(*) AS c FROM orders WHERE status='new'");
    const [[{ c: taken }]] = await db.execute("SELECT COUNT(*) AS c FROM orders WHERE status='taken'");
    const [[{ c: delivered }]] = await db.execute("SELECT COUNT(*) AS c FROM orders WHERE status='delivered'");

    // ===== Список курьеров =====
    const [couriers] = await db.execute("SELECT username FROM couriers");

    // Формируем inline-кнопки для каждого курьера
    const keyboard = couriers.map(c => [{ text: `@${c.username}`, callback_data: `reviews_${c.username}` }]);
    if (keyboard.length === 0) keyboard.push([{ text: "Нет курьеров", callback_data: "none" }]);

    // ===== Отправляем сообщение =====
    await bot.sendMessage(
      id,
      `📊 *Статистика заказов*\n\nВсего: ${total}\nНовые: ${newO}\nВзяты: ${taken}\nДоставлены: ${delivered}\n\n👇 Нажмите на курьера, чтобы посмотреть отзывы`,
      {
        parse_mode: "Markdown",
        reply_markup: { inline_keyboard: keyboard }
      }
    );
  } catch (err) {
    console.error("Ошибка при показе статистики:", err);
    await bot.sendMessage(id, "Ошибка при получении статистики заказов");
  }
}


// ===== Кнопка "Рассылка" =====
if (text === "Рассылка" && id === ADMIN_ID) {
  adminWaitingBroadcast.set(username, true); // <-- устанавливаем флаг ожидания
  return bot.sendMessage(id, "Введите текст для рассылки всем подписанным клиентам:");
}


// ===== Рассылка с лимитом =====
if (adminWaitingBroadcast.has(username)) {
  const msgText = text;

  try {
    const [allClients] = await db.execute(
      "SELECT chat_id, username FROM clients WHERE subscribed=1 AND chat_id IS NOT NULL"
    );

    console.log(`Начало рассылки от @${username}, текст: "${msgText}"`);
    console.log(`Всего получателей: ${allClients.length}`);

    const limit = pLimit(5);
    let successCount = 0;

    const tasks = allClients.map(c => limit(async () => {
      try {
        const safeMsg = escapeMarkdownV2(msgText);
        await bot.sendMessage(c.chat_id, safeMsg, { parse_mode: 'MarkdownV2' });
        successCount++;
        console.log(`Отправлено пользователю chat_id: ${c.chat_id}`);
      } catch (err) {
        console.error(`Ошибка при отправке @${c.username} (chat_id: ${c.chat_id}):`, err.message);
      }
    }));

    await Promise.all(tasks);

    const safeReport = escapeMarkdownV2(`Рассылка завершена\nУспешно отправлено: ${successCount} из ${allClients.length}`);
    await bot.sendMessage(ADMIN_ID, safeReport, { parse_mode: 'MarkdownV2' });

    adminWaitingBroadcast.delete(username);
    console.log(`Рассылка от @${username} завершена`);
  } catch (err) {
    console.error(`Ошибка при рассылке от @${username}:`, err.message);
    await bot.sendMessage(ADMIN_ID, `Ошибка при рассылке: ${escapeMarkdownV2(err.message)}`, { parse_mode: 'MarkdownV2' });
  }

  return;
}


// ===== Панель курьера =====
if (text === "Панель курьера" && (COURIERS[username] || id === ADMIN_ID)) {
  const kb = {
    keyboard: [
      [{ text: "Активные заказы" }, { text: "Выполненные заказы" }],
      [{ text: "Назад" }]
    ],
    resize_keyboard: true
  };
  return bot.sendMessage(id, "Панель курьера", { reply_markup: kb });
}

// ===== Просмотр заказов курьера =====
if ((text === "Активные заказы" || text === "Выполненные заказы") && await isCourier(username)) {
  const isActive = text === "Активные заказы";

  console.log(`${isActive ? "Активные" : "Выполненные"} заказы курьера @${username} (id: ${id})`);

  // Запрос заказов для этого курьера
  const query = isActive
    ? "SELECT * FROM orders WHERE status IN ('new','taken') AND courier_username=? ORDER BY created_at DESC"
    : "SELECT * FROM orders WHERE status='delivered' AND courier_username=? ORDER BY delivered_at DESC";

  const [orders] = await db.execute(query, [username]);

  if (!orders.length) {
    console.log(`Нет ${isActive ? "активных" : "выполненных"} заказов у курьера`);
    return bot.sendMessage(id, `Нет ${isActive ? "активных" : "выполненных"} заказов`);
  }

  // Отправка заказов параллельно
  await Promise.all(
    orders.map(async (o) => {
      // Приводим все поля к строкам, чтобы escapeMarkdownV2 не падал
      const orderSafe = {
        ...o,
        orderText: o.orderText || "—",
        tgNick: o.tgNick || "—",
        city: o.city || "—",
        delivery: o.delivery || "—",
        payment: o.payment || "—",
        date: o.date || "—",
        time: o.time || "—"
      };

      // Inline-кнопки только для активных заказов
      let inlineKeyboard;
      if (isActive) {
        if (o.status === "new") {
          inlineKeyboard = [[{ text: "Взять заказ", callback_data: `take_${o.id}` }]];
        } else if (o.status === "taken") {
          inlineKeyboard = [[
            { text: "Доставлен", callback_data: `delivered_${o.id}` },
            { text: "Отказаться", callback_data: `release_${o.id}` }
          ]];
        }
      }

      try {
        const textMsg = escapeMarkdownV2(buildOrderMessage(orderSafe));
        await bot.sendMessage(id, textMsg, {
          parse_mode: "MarkdownV2",
          reply_markup: inlineKeyboard ? { inline_keyboard: inlineKeyboard } : undefined
        });
      } catch (err) {
        console.error(`Ошибка отправки заказа №${o.id} курьеру @${username}:`, err.message);
      }
    })
  );

  console.log(`Все ${isActive ? "активные" : "выполненные"} заказы отправлены курьеру @${username}`);
  return;
}
});





// ================= Express / WebSocket =================
const app = express();
app.use(cors());
app.use(express.json());
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// Функция для рассылки обновлений stock всем подключённым клиентам WebSocket
function broadcastStock() {
  const data = JSON.stringify({ type: "stock-update" });
  wss.clients.forEach(c => {
    if (c.readyState === WebSocket.OPEN) c.send(data);
  });
}

// ================= Генерация ID заказа =================
async function generateOrderId() {
  let id;
  let exists;
  do {
    id = String(Math.floor(100000 + Math.random() * 900000));
    exists = await getOrderById(id);
  } while (exists);
  return id;
}

// ================= API: отправка заказа =================
app.post("/api/send-order", async (req, res) => {
  try {
    const { tgNick, city, delivery, payment, orderText, date, time, client_chat_id } = req.body;

    // ===== ПРОВЕРКА ВХОДНЫХ ДАННЫХ =====
   if (!tgNick || !orderText) {
  console.log("❌ Ошибка: неполные данные", req.body);
  return res.status(400).json({ success: false, error: "INVALID_DATA" });
}

    const cleanUsername = tgNick.replace(/^@/, "");

    console.log(`Новый заказ через API от ${cleanUsername}`);
    console.log(`Детали: город=${city}, доставка=${delivery}, оплата=${payment}, текст заказа="${orderText}"`);

    // ===== ГАРАНТИРОВАННО РЕГИСТРИРУЕМ ПОЛЬЗОВАТЕЛЯ =====
    await db.execute(`
      INSERT INTO clients (chat_id, username, banned)
      VALUES (?, ?, 0)
      ON DUPLICATE KEY UPDATE username = VALUES(username)
    `, [client_chat_id, cleanUsername]);

 // ===== ПРОВЕРКА БАНА =====
let banned = false;

// Сначала проверяем по chat_id, если есть
if (client_chat_id) {
  const [rows] = await db.execute(
    "SELECT banned FROM clients WHERE chat_id = ? LIMIT 1",
    [client_chat_id]
  );
  if (rows.length && rows[0].banned === 1) banned = true;
}

// Если chat_id нет или не найден — проверяем по username
if (!banned) {
  const [rows2] = await db.execute(
    "SELECT banned FROM clients WHERE username = ? LIMIT 1",
    [cleanUsername]
  );
  if (rows2.length && rows2[0].banned === 1) banned = true;
}

if (banned) {
  console.log(`⛔ Заблокированный пользователь ${cleanUsername} (${client_chat_id || "no chat_id"})`);
  return res.json({
    success: false,
    error: "USER_BANNED",
    message: "Вы заблокированы и не можете создавать заказы"
  });
}


    // ===== Проверка существующего заказа =====
    const [existing] = await db.execute(
      "SELECT id FROM orders WHERE client_chat_id=? AND orderText=?",
      [client_chat_id, orderText]
    );

    let id;
    if (existing.length) {
      id = existing[0].id;
      console.log(`Заказ уже существует, используем ID: ${id}`);
    } else {
      id = await generateOrderId();
      console.log(`Присвоен новый ID заказа: ${id}`);
    }

    const order = {
      id,
      tgNick: cleanUsername,
      city,
      delivery,
      payment,
      orderText,
      date,
      time,
      status: "new",
      client_chat_id
    };

    // ===== Добавляем заказ в базу, если его ещё нет =====
    if (!existing.length) {
      await addOrder(order);
      console.log(`Заказ ${id} добавлен в базу`);
    } else {
      console.log(`Заказ ${id} уже в базе, пропускаем добавление`);
    }

    // ===== Получаем заказ из базы =====
    const updated = await getOrderById(id);

    // ===== Отправляем уведомления в Telegram =====
    await sendOrUpdateOrder(updated);
    console.log(`Уведомления отправлены для заказа ${id}`);

    // ===== WebSocket: обновление stock =====
    broadcastStock();
    console.log(`WebSocket: отправлено обновление stock`);

    return res.json({ success: true, orderId: id });

  } catch (err) {
    console.error("Ошибка при обработке /api/send-order:", err);
    return res.status(500).json({ success: false, error: "Внутренняя ошибка сервера" });
  }
});



// ================= Фикс зависших заказов =================
app.post("/fix-all-new-orders", async (req, res) => {
  try {
    // Получаем все заказы со статусом "new"
    const [orders] = await db.execute("SELECT * FROM orders WHERE status='new'");

    if (orders.length === 0) {
      console.log("Нет новых заказов для исправления.");
      return res.send("Нет новых заказов для исправления.");
    }

    let successCount = 0;

    for (const order of orders) {
      try {
        // Повторная отправка уведомлений в Telegram
        await sendOrUpdateOrder(order);
        console.log(`Заказ #${order.id} успешно обновлен`);
        successCount++;
      } catch (err) {
        console.error(`Ошибка при обновлении заказа #${order.id}:`, err.message);
      }
    }

    // Можно также отправить обновление stock после всех исправлений
    broadcastStock();
    console.log("WebSocket: обновлено состояние stock после фикса");

    res.send(`Обновлено ${successCount} из ${orders.length} заказ(ов). Кнопки теперь должны появиться.`);
  } catch (err) {
    console.error("Ошибка сервера при исправлении заказов:", err);
    res.status(500).send("Ошибка сервера при исправлении заказов");
  }
});



// ================= Запуск сервера =================
server.listen(PORT, HOST, () => {
  console.log(`Server running at port ${PORT}`);
});
