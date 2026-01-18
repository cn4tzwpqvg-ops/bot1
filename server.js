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



// ================= Настройки1 =================
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

  // время в формате HH:MM:SS
  const pad = n => String(n).padStart(2, "0");
  const mysqlTime = order.time
    ? order.time
    : `${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())}`;

  const createdAt = formatMySQLDateTime(now);

  // Вставляем или обновляем заказ
  await db.execute(
    `
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
    `,
    [
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
    ]
  );
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

async function reassignOrderCourier(orderId, newCourierUsername) {
  const now = formatMySQLDateTime();
  await db.execute(
    "UPDATE orders SET courier_username=?, status='taken', taken_at=? WHERE id=?",
    [newCourierUsername, now, orderId]
  );
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

async function getOrderMessageForChat(orderId, chatId) {
  const [rows] = await db.execute(
    "SELECT message_id FROM order_messages WHERE order_id=? AND chat_id=? LIMIT 1",
    [orderId, chatId]
  );
  return rows[0]?.message_id || null;
}

async function deleteOrderMessageForChat(orderId, chatId) {
  const messageId = await getOrderMessageForChat(orderId, chatId);
  if (!messageId) return;

  try {
    await bot.deleteMessage(chatId, messageId);
  } catch (e) {
    // иногда Telegram не даст удалить — не страшно, просто чистим запись
  }
  await clearOrderMessage(orderId, chatId);
}

function buildKeyboardForRecipient(order, { role, username }) {
  const owner = order.courier_username?.replace(/^@/, "") || null;
  const isAdmin = role === "admin";
  const isCourier = role === "courier";
  const isClient = role === "client";

  const me = (username || "").replace(/^@/, "");
  const isOwner = owner && me && owner === me;

  // По умолчанию кнопок нет
  let keyboard = [];

  // Клиент — отмена в первые 20 минут, пока заказ NEW или TAKEN
if (isClient) {
  const orderAge = Date.now() - new Date(order.created_at).getTime();
  const canCancelByTime = orderAge <= 20 * 60 * 1000;
  const canCancelByStatus = (order.status === "new" || order.status === "taken");

  if (canCancelByTime && canCancelByStatus) {
    keyboard = [[{ text: "❌ Отменить заказ", callback_data: `confirm_cancel_${order.id}` }]];
  }
  return keyboard;
}

  // Админ/курьеры
  if (order.status === "new") {
    keyboard = [[{ text: "🚚 Взять заказ", callback_data: `take_${order.id}` }]];
    return keyboard;
  }

  if (order.status === "taken") {
  if (isAdmin || (isCourier && isOwner)) {
    keyboard = [[
      { text: "❌ Отказаться", callback_data: `release_${order.id}` },
      { text: "✅ Доставлено", callback_data: `delivered_${order.id}` }
    ]];

    if (isAdmin) {
      keyboard.push([{ text: "🔁 Переназначить курьера", callback_data: `reassign_${order.id}` }]);
    }
  }
  return keyboard;
}


  // delivered / canceled — без кнопок
  return [];
}

function buildOrderMessage(order) {
  const lines = [
    `🧾 Заказ №${escapeMarkdownV2(order.id)}`,
    `👤 Клиент: ${escapeMarkdownV2(withAt(order.tgNick))}`,
    `🏙 Город: ${escapeMarkdownV2(order.city || "—")}`,
    `🚚 Доставка: ${escapeMarkdownV2(order.delivery || "—")}`,
    `💰 Оплата: ${escapeMarkdownV2(order.payment || "—")}`,
    `📝 Заказ: ${escapeMarkdownV2(order.orderText || "—")}`,
    `📅 Дата: ${escapeMarkdownV2(order.date || "—")}`,
    `⏰ Время: ${escapeMarkdownV2(order.time || "—")}`,
    `🚚 Курьер: ${escapeMarkdownV2(withAt(order.courier_username || "—"))}`,
    `📌 Статус: ${escapeMarkdownV2(order.status || "—")}`
  ];

  return lines.join("\n");
}


function buildTextForOrder(order) {
  let msgText = buildOrderMessage({
    ...order,
    courier_username: order.courier_username || "—"
  });

  if (order.status === "canceled") {
    msgText += "\n\n" + escapeMarkdownV2("❌ Заказ был отменён покупателем");
  }

  return msgText;
}

// =================== Отправка / обновление заказа в ОДИН чат ===================
async function sendOrUpdateOrderToChat(order, chatId, role, username) {
  const msgText = buildTextForOrder(order);
  const keyboard = buildKeyboardForRecipient(order, { role, username });

  const existingMsgId = await getOrderMessageForChat(order.id, chatId);

  try {
    if (existingMsgId) {
      await bot.editMessageText(msgText, {
        chat_id: chatId,
        message_id: existingMsgId,
        parse_mode: "MarkdownV2",
        reply_markup: keyboard.length ? { inline_keyboard: keyboard } : undefined
      });
    } else {
      const sent = await bot.sendMessage(chatId, msgText, {
        parse_mode: "MarkdownV2",
        reply_markup: keyboard.length ? { inline_keyboard: keyboard } : undefined
      });
      await saveOrderMessage(order.id, chatId, sent.message_id);
    }
  } catch (err) {
    const emsg = String(err?.message || "");

    // Нормальная ситуация — Telegram ругается, что текст не изменился
    if (emsg.includes("message is not modified")) return;

    // Если старое сообщение уже удалено/не найдено — чистим запись и шлём заново
    if (
      emsg.includes("message to edit not found") ||
      emsg.includes("message identifier is not specified") ||
      emsg.includes("message can't be edited") ||
      emsg.includes("MESSAGE_ID_INVALID")
    ) {
      await clearOrderMessage(order.id, chatId);
    }

    // Пытаемся отправить заново и сохранить новый message_id
    try {
      const sent = await bot.sendMessage(chatId, msgText, {
        parse_mode: "MarkdownV2",
        reply_markup: keyboard.length ? { inline_keyboard: keyboard } : undefined
      });
      await saveOrderMessage(order.id, chatId, sent.message_id);
    } catch (e2) {
      console.error(`[ERROR] sendOrUpdateOrderToChat ${order.id} -> ${chatId}:`, e2.message);
    }
  }
}

// =================== ГЛАВНОЕ: разослать/обновить всем участникам ===================
async function sendOrUpdateOrderAll(order) {
  // Собираем получателей без дублей
  const recipientsMap = new Map();

  // Админ
  if (ADMIN_ID) {
    recipientsMap.set(ADMIN_ID, {
      chatId: ADMIN_ID,
      role: "admin",
      username: ADMIN_USERNAME
    });
  }

  // Курьеры
  const [couriers] = await db.execute(
    "SELECT username, chat_id FROM couriers WHERE chat_id IS NOT NULL"
  );
  for (const c of couriers) {
    recipientsMap.set(c.chat_id, {
      chatId: c.chat_id,
      role: "courier",
      username: c.username
    });
  }

  // Клиент
  if (order.client_chat_id) {
    recipientsMap.set(order.client_chat_id, {
      chatId: order.client_chat_id,
      role: "client",
      username: order.tgNick?.replace(/^@/, "") || ""
    });
  }

  const recipients = Array.from(recipientsMap.values());
  const owner = order.courier_username?.replace(/^@/, "") || null;

  for (const r of recipients) {
    const isCourier = r.role === "courier";
    const isAdmin = r.role === "admin";

    // 1) Если заказ отменён — убираем у всех курьеров (админу/клиенту оставляем)
    if (order.status === "canceled" && isCourier && !isAdmin) {
      await deleteOrderMessageForChat(order.id, r.chatId);
      continue;
    }

    // 2) Если заказ взят — убираем у других курьеров (кроме владельца и админа)
    if (order.status === "taken" && isCourier && !isAdmin) {
      const courierUname = (r.username || "").replace(/^@/, "");
      if (owner && courierUname !== owner) {
        await deleteOrderMessageForChat(order.id, r.chatId);
        continue;
      }
    }

    // 3) В остальных случаях — обновляем/отправляем
    await sendOrUpdateOrderToChat(order, r.chatId, r.role, r.username);
  }
}




// =================== Вспомогательная функция ===================
function escapeMarkdownV2(text) {
  if (text == null) return "";
  return String(text).replace(/([\\_*[\]()~`>#+\-=|{}.!])/g, "\\$1");
}

// =================== Запрос отзыва у клиента (оценка + пропуск) ===================
async function hasReviewForOrder(orderId) {
  try {
    const [rows] = await db.execute(
      "SELECT 1 FROM reviews WHERE order_id = ? LIMIT 1",
      [String(orderId)]
    );
    return rows.length > 0;
  } catch (e) {
    console.error("hasReviewForOrder error:", e.message);
    return false;
  }
}

async function askForReview(order) {
  if (!order) return;

  // ✅ Если client_chat_id пустой — пробуем достать по tgNick из clients
  if (!order.client_chat_id && order.tgNick) {
    try {
      const cleanNick = String(order.tgNick).replace(/^@+/, "").trim();
      const client = await getClient(cleanNick);

      if (client?.chat_id) {
        order.client_chat_id = client.chat_id;

        // ✅ сохраняем в заказ (чтобы потом всегда было)
        await db.execute(
          "UPDATE orders SET client_chat_id=? WHERE id=? AND (client_chat_id IS NULL OR client_chat_id=0)",
          [client.chat_id, order.id]
        );
      }
    } catch (e) {
      console.error("[askForReview] lookup client_chat_id error:", e?.message || e);
    }
  }

  // если так и не нашли chat_id — выходим
  if (!order.client_chat_id) return;

  const orderId = String(order.id);
  const clientId = order.client_chat_id;

  // 1) не спрашиваем, если отзыв по заказу уже есть
  const already = await hasReviewForOrder(orderId);
  if (already) return;

  // 2) не спрашиваем второй раз, если уже ждём отзыв от этого клиента
// но если ждём по другому заказу — сбрасываем и спрашиваем заново
if (waitingReview.has(clientId)) {
  const cur = waitingReview.get(clientId);
  if (cur && String(cur.orderId) !== String(orderId)) {
    waitingReview.delete(clientId);
  } else {
    return;
  }
}


  // сохраняем состояние ожидания
  waitingReview.set(clientId, {
    orderId,
    courier: order.courier_username ? `@${String(order.courier_username).replace(/^@/, "")}` : "—",
    client: order.tgNick ? `@${String(order.tgNick).replace(/^@/, "")}` : "—",
    rating: null
  });

  const kb = {
    inline_keyboard: [
      [
        { text: "⭐ 1", callback_data: `rate_${orderId}_1` },
        { text: "⭐ 2", callback_data: `rate_${orderId}_2` },
        { text: "⭐ 3", callback_data: `rate_${orderId}_3` },
        { text: "⭐ 4", callback_data: `rate_${orderId}_4` },
        { text: "⭐ 5", callback_data: `rate_${orderId}_5` }
      ],
      [{ text: "⏭ Пропустить", callback_data: `skip_review_${orderId}` }]
    ]
  };

  const courier = order.courier_username ? withAt(order.courier_username) : "—";

   try {
    await bot.sendMessage(
      clientId,
      `✅ Заказ №${escapeMarkdownV2(orderId)} доставлен.\n` +
        `🚚 Курьер: ${escapeMarkdownV2(courier)}\n\n` +
        `Поставьте оценку (1–5) и (по желанию) напишите отзыв.\n` +
        `Если не хотите — нажмите «Пропустить».`,
      { parse_mode: "MarkdownV2", reply_markup: kb }
    );

    console.log("[DEBUG] review request sent to client:", clientId, "order:", orderId);
  } catch (e) {
    console.error("[ERROR] cannot send review request:", e?.message || e, {
      clientId,
      orderId,
      tgNick: order.tgNick
    });
  }
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

          const text = buildTextForOrder(order);
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

  const [orders] = await db.execute(
    "SELECT * FROM orders WHERE status IN ('new','taken') ORDER BY created_at ASC"
  );

  const limit = pLimit(5);

  const tasks = orders.map(order =>
    limit(async () => {
      try {
        await sendOrUpdateOrderAll(order);
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

// ================== ПРОПУСТИТЬ ОТЗЫВ ==================
if (data.startsWith("skip_review_")) {
  const orderId = String(data.split("_")[2] || "").trim();
  const review = waitingReview.get(fromId);

  if (!review || review.orderId !== orderId) {
    return bot.answerCallbackQuery(q.id, {
      text: "Отзыв уже обработан или устарел",
      show_alert: true
    });
  }

  // ✅ Если отзыв уже есть в БД — просто выходим (чтобы не было дублей)
  const already = await hasReviewForOrder(orderId);
  if (already) {
    waitingReview.delete(fromId);
    await bot.sendMessage(fromId, "Ок ✅ Отзыв по этому заказу уже был сохранён ранее.");
    return bot.answerCallbackQuery(q.id, { text: "Готово" });
  }

  // Если успел выбрать оценку — сохраняем только рейтинг (без текста)
  if (review.rating !== null) {
    try {
      const now = new Date().toISOString().slice(0, 19).replace("T", " ");
      const courierNick = String(review.courier || "").replace(/^@/, "");
      const clientNick = String(review.client || "").replace(/^@/, "");

      await db.execute(
        `INSERT INTO reviews (order_id, client_username, courier_username, rating, review_text, created_at)
         VALUES (?, ?, ?, ?, ?, ?)`,
        [review.orderId, clientNick, courierNick, Number(review.rating), null, now]
      );

      // админу — уведомление
      if (ADMIN_ID) {
        await bot.sendMessage(
          ADMIN_ID,
          `⚠️ Клиент @${escapeMarkdownV2(clientNick)} поставил оценку ${review.rating}/5 по заказу №${escapeMarkdownV2(review.orderId)}, но пропустил текст отзыва.`,
          { parse_mode: "MarkdownV2" }
        );
      }
    } catch (e) {
      console.error("[skip_review] save rating only error:", e.message);
    }
  }

  waitingReview.delete(fromId);

  // Если оценки не было — просто закрываем
  if (review.rating === null) {
    await bot.sendMessage(fromId, "Ок, отзыв пропущен ✅ (оценка не выбрана)");
  } else {
    await bot.sendMessage(fromId, "Ок, отзыв пропущен ✅");
  }

  return bot.answerCallbackQuery(q.id, { text: "Пропущено" });
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
  `⭐ Оценка: ${r.rating}/5\n` +
  `📝 Отзыв: ${escapeMarkdownV2(r.review_text || "—")}\n` +
  `📅 Дата: ${escapeMarkdownV2(new Date(r.created_at).toLocaleString("ru-RU"))}`
).join("\n\n\\-\\-\\-\\-\\-\\-\\-\\-\\-\\-\\-\\-\\-\\-\\-\\-\\-\\-\\-\\-\\\n\n");



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


// ================== REASSIGN (админ) ==================
if (data.startsWith("reassign_") && fromId === ADMIN_ID) {
  const orderId = data.split("_")[1];

  const [couriers] = await db.execute("SELECT username FROM couriers ORDER BY username ASC");

  const kb = {
    inline_keyboard: [
      ...couriers.map(c => ([
        { text: `@${c.username}`, callback_data: `setcourier_${orderId}_${c.username}` }
      ])),
      [{ text: "Отмена", callback_data: `reassign_cancel_${orderId}` }]
    ]
  };

  await bot.sendMessage(fromId, `Выберите курьера для заказа №${orderId}:`, { reply_markup: kb });
  return bot.answerCallbackQuery(q.id);
}

if (data.startsWith("setcourier_") && fromId === ADMIN_ID) {
  const parts = data.split("_");
  const orderId = parts[1];
  const newCourier = parts.slice(2).join("_").replace(/^@/, "");

  const [rows] = await db.execute("SELECT 1 FROM couriers WHERE username=? LIMIT 1", [newCourier]);
  if (!rows.length) {
    await bot.answerCallbackQuery(q.id, { text: "Курьер не найден", show_alert: true });
    return;
  }

  await reassignOrderCourier(orderId, newCourier);
  const updatedOrder = await getOrderById(orderId);

  await sendOrUpdateOrderAll(updatedOrder);

  await bot.answerCallbackQuery(q.id, { text: `Назначено: @${newCourier}` });
  return;
}

if (data.startsWith("reassign_cancel_") && fromId === ADMIN_ID) {
  await bot.answerCallbackQuery(q.id, { text: "Отменено" });
  return;
}



// ================== Основная часть (заказы) ==================
let orderId = null;
let order = null;

// Определяем orderId
if (
  data.startsWith("take_") ||
  data.startsWith("release_") ||
  data.startsWith("cancel_") ||
  data.startsWith("delivered_")
) {
  orderId = data.split("_")[1];
} else if (
  data.startsWith("confirm_cancel_") ||
  data.startsWith("no_cancel_")
) {
  orderId = data.split("_")[2];
}

// Загружаем заказ
if (orderId) {
  order = await getOrderById(orderId);
  if (!order) {
    console.log(`Заказ ${orderId} не найден`);
    return bot.answerCallbackQuery(q.id, { text: "Заказ не найден", show_alert: true });
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
  const success = await takeOrderAtomic(orderId, username.replace(/^@/, ""));

  // Получаем свежий заказ
  const updatedOrder = await getOrderById(orderId);

  if (!success) {
    // Заказ уже взят другим курьером — обновляем сообщение у всех участников
    await sendOrUpdateOrderAll(updatedOrder);
    return bot.answerCallbackQuery(q.id, {
      text: "Заказ уже взят другим курьером!",
      show_alert: true
    });
  }

  // Заказ успешно взят — обновляем сообщение у всех участников
  await sendOrUpdateOrderAll(updatedOrder);

  return bot.answerCallbackQuery(q.id, { text: "✅ Заказ взят" });
}



// ================== RELEASE ==================
if (data.startsWith("release_")) {
  console.log(`RELEASE заказ ${orderId} пользователем @${username}`);

  // Проверка: заказ должен быть взят
  if (order.status !== "taken") {
    return bot.answerCallbackQuery(q.id, {
      text: "От этого заказа уже отказались",
      show_alert: true
    });
  }

  // Проверка: только курьер, который взял заказ, или админ
  const isOwnerOrAdmin = order.courier_username?.replace(/^@/, "") === username.replace(/^@/, "") || fromId === ADMIN_ID;
  if (!isOwnerOrAdmin) {
    return bot.answerCallbackQuery(q.id, {
      text: "Вы не можете отказаться от этого заказа",
      show_alert: true
    });
  }

  const oldCourier = order.courier_username;

  try {
    // Сбрасываем заказ в статус 'new' и убираем курьера
    await updateOrderStatus(orderId, "new");

    // Получаем свежий заказ
    const updatedOrder = await getOrderById(orderId);

    // ✅ Обновляем сообщение у всех участников
    await sendOrUpdateOrderAll(updatedOrder);

    // Уведомляем админа, если курьер отказался
    if (ADMIN_ID && oldCourier && oldCourier !== ADMIN_USERNAME) {
      await bot.sendMessage(ADMIN_ID, `Курьер @${oldCourier} отказался от заказа №${orderId}`);
    }

    return bot.answerCallbackQuery(q.id, { text: "❌ Вы отказались от заказа" });
  } catch (err) {
    console.error(`Ошибка при отказе от заказа ${orderId}:`, err.message);
    return bot.answerCallbackQuery(q.id, { text: "Ошибка при отказе", show_alert: true });
  }
}

// ================== DELIVERED ==================
if (data.startsWith("delivered_")) {
  console.log(`DELIVERED заказ ${orderId} пользователем @${username}`);

  // Проверка: только курьер, который взял заказ, или админ
  const isOwnerOrAdmin =
    order.courier_username?.replace(/^@/, "") === username.replace(/^@/, "") ||
    fromId === ADMIN_ID;

  if (!isOwnerOrAdmin) {
    return bot.answerCallbackQuery(q.id, { text: "Нельзя отметить", show_alert: true });
  }

  try {
    // Обновляем статус на 'delivered'
    await updateOrderStatus(orderId, "delivered", username.replace(/^@/, ""));
    const updatedOrder = await getOrderById(orderId);

    // ✅ Обновляем сообщение у всех участников
    await sendOrUpdateOrderAll(updatedOrder);

    // ✅ Просим отзыв (1 раз) + лог
    try {
      console.log("[DEBUG] delivered -> askForReview", {
        orderId: updatedOrder.id,
        tgNick: updatedOrder.tgNick,
        client_chat_id: updatedOrder.client_chat_id,
        status: updatedOrder.status
      });
      await askForReview(updatedOrder);
      console.log("[DEBUG] askForReview done for order", updatedOrder.id);
    } catch (e) {
      console.error("[ERROR] askForReview failed:", e?.message || e);
    }

    return bot.answerCallbackQuery(q.id, { text: "✅ Заказ доставлен" });
  } catch (err) {
    console.error(`Ошибка при отметке заказа ${orderId} как доставленного:`, err.message);
    return bot.answerCallbackQuery(q.id, { text: "Ошибка при доставке", show_alert: true });
  }
}






// ================== CONFIRM CANCEL ==================
if (data.startsWith("confirm_cancel_")) {
  const orderId = data.split("_")[2];
  const order = await getOrderById(orderId);
  if (!order) {
    return bot.answerCallbackQuery(q.id, { text: "Заказ не найден", show_alert: true });
  }

  const orderAge = Date.now() - new Date(order.created_at).getTime();
const okStatus = (order.status === "new" || order.status === "taken");
if (orderAge > 20 * 60 * 1000 || !okStatus) {
  return bot.answerCallbackQuery(q.id, { text: "Заказ не отменяем", show_alert: true });
}


  const keyboard = [
    [
      { text: "✅ Да, отменить", callback_data: `cancel_${order.id}` },
      { text: "❌ Нет", callback_data: `no_cancel_${order.id}` }
    ]
  ];

  const msg = escapeMarkdownV2(`Вы точно хотите отменить заказ #${order.id}?`);

  try {
    if (q.message && q.message.message_id) {
      await bot.editMessageText(msg, {
        chat_id: q.message.chat.id,
        message_id: q.message.message_id,
        parse_mode: "MarkdownV2",
        reply_markup: { inline_keyboard: keyboard }
      });
    } else {
      await bot.sendMessage(fromId, msg, {
        parse_mode: "MarkdownV2",
        reply_markup: { inline_keyboard: keyboard }
      });
    }
  } catch (err) {
    console.error(`Ошибка при confirm_cancel для заказа ${orderId}:`, err.message);
  }

  return bot.answerCallbackQuery(q.id);
}

// ================== NO CANCEL ==================
if (data.startsWith("no_cancel_")) {
  const orderId = data.split("_")[2];
  const order = await getOrderById(orderId);
  if (!order) {
    return bot.answerCallbackQuery(q.id, { text: "Заказ не найден", show_alert: true });
  }

  await sendOrUpdateOrderAll(order);

  return bot.answerCallbackQuery(q.id, { text: "Отмена отменена" });
}

// ================== FINAL CANCEL ==================
if (data.startsWith("cancel_")) {
  const orderId = data.split("_")[1];
  const order = await getOrderById(orderId);
  if (!order) {
    return bot.answerCallbackQuery(q.id, { text: "Заказ не найден", show_alert: true });
  }

  if (order.client_chat_id !== fromId) {
    return bot.answerCallbackQuery(q.id, { text: "Вы не можете отменить этот заказ", show_alert: true });
  }

  try {
   await db.execute(
  "UPDATE orders SET status='canceled', courier_username=NULL, taken_at=NULL, delivered_at=NULL WHERE id=?",
  [orderId]
);

    const updatedOrder = await getOrderById(orderId);

    await sendOrUpdateOrderAll(updatedOrder);

    broadcastStock();

    return bot.answerCallbackQuery(q.id, { text: "✅ Заказ успешно отменен" });
  } catch (err) {
    console.error(`Ошибка при cancel для заказа ${orderId}:`, err.message);
    return bot.answerCallbackQuery(q.id, { text: "Ошибка при отмене", show_alert: true });
  }
}

})


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
  [{ text: "Активные по курьеру" }, { text: "Выполненные по курьеру" }],
  [{ text: "Взятые сейчас" }, { text: "Сводка курьеров" }],
  [{ text: "Добавить курьера" }, { text: "Удалить курьера" }],
  [{ text: "Список курьеров" }, { text: "Все пользователи" }],
  [{ text: "Рассылка" }],
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

  // ✅ чтобы кнопки меню не перехватывались режимами "ожидания"
if (id === ADMIN_ID) {
  const adminMenuClicks = [
    "Панель курьера",
    "Панель администратора",
    "Новые заказы",
    "Взятые заказы",
    "Выполненные заказы",
    "Взятые сейчас",
    "Сводка курьеров",
    "Активные по курьеру",
    "Выполненные по курьеру",
    "Назад"
  ];

  if (adminMenuClicks.includes(text)) {
    adminWaitingOrdersCourier.delete(username);
    adminWaitingBroadcast.delete(username);
    adminWaitingCourier.delete(username);
  }
}

// ===== Админ: Взятые сейчас (все заказы status='taken') =====
if (text === "Взятые сейчас" && id === ADMIN_ID) {
  const [orders] = await db.execute(
    "SELECT * FROM orders WHERE status='taken' ORDER BY taken_at DESC"
  );

  if (!orders.length) {
    await bot.sendMessage(id, "Сейчас нет взятых заказов");
    return;
  }

  await bot.sendMessage(id, `Взятые сейчас: ${orders.length}`);

  for (const o of orders) {
    await clearOrderMessage(o.id, id);              // ✅ чтобы прислало заново как новое
    await sendOrUpdateOrderToChat(o, id, "admin", ADMIN_USERNAME);
  }
  return;
}


// ===== Админ: Сводка курьеров =====
if (text === "Сводка курьеров" && id === ADMIN_ID) {
  const [rows] = await db.execute(`
    SELECT
      c.username,
      SUM(o.status='taken') AS taken_cnt,
      SUM(o.status='delivered' AND DATE(o.delivered_at)=CURDATE()) AS delivered_today
    FROM couriers c
    LEFT JOIN orders o ON o.courier_username = c.username
    GROUP BY c.username
    ORDER BY taken_cnt DESC, delivered_today DESC
  `);

  if (!rows.length) return bot.sendMessage(id, "Нет курьеров");

  const lines = rows.map(r =>
    `@${r.username}: взято=${r.taken_cnt || 0}, выполнено сегодня=${r.delivered_today || 0}`
  ).join("\n");

  return bot.sendMessage(id, "📌 Сводка курьеров:\n" + lines);
}



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

  // 1) Нажали "Назад" — выйти и вернуть админ-меню
  if (text === "Назад") {
    adminWaitingOrdersCourier.delete(username);
    return bot.sendMessage(id, "Панель администратора", {
      reply_markup: {
       keyboard: [
  [{ text: "Статистика" }, { text: "Курьеры" }],
  [{ text: "Активные по курьеру" }, { text: "Выполненные по курьеру" }],
  [{ text: "Взятые сейчас" }, { text: "Сводка курьеров" }],
  [{ text: "Добавить курьера" }, { text: "Удалить курьера" }],
  [{ text: "Список курьеров" }, { text: "Все пользователи" }],
  [{ text: "Рассылка" }],
  [{ text: "Назад" }]
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

  // Тип просмотра: "active" или "done"
  const state = adminWaitingOrdersCourier.get(username);
  const showDone = state.type === "done";

const query = showDone
  ? "SELECT * FROM orders WHERE status='delivered' AND courier_username=? ORDER BY delivered_at DESC"
  : "SELECT * FROM orders WHERE status='taken' AND courier_username=? ORDER BY taken_at DESC";


  const [orders] = await db.execute(query, [selectedCourier]);
  console.log("[DEBUG admin orders] courier:", selectedCourier, "showDone:", showDone, "count:", orders.length);
if (orders.length) {
  console.log("[DEBUG admin orders] first:", orders[0].id, orders[0].status, orders[0].courier_username);
}


  if (!orders || orders.length === 0) {
    // ✅ ВАЖНО: если заказов нет — тоже выходим из режима выбора
    adminWaitingOrdersCourier.delete(username);
    return bot.sendMessage(
      id,
      `Курьер @${selectedCourier} пока не имеет ${showDone ? "выполненных" : "активных"} заказов`
    );
  }

  await bot.sendMessage(
  id,
  `${showDone ? "Выполненные" : "Активные"} заказы курьера @${selectedCourier}:`
);

await bot.sendMessage(id, `Найдено: ${orders.length}`);

for (const o of orders) {
  await clearOrderMessage(o.id, id);
  await sendOrUpdateOrderToChat(o, id, "admin", ADMIN_USERNAME);
}



  // ✅ Вариант B: после просмотра — выходим из режима выбора
  adminWaitingOrdersCourier.delete(username);
  return;
}


// Если админ в состоянии ожидания ввода ника, но нажал кнопку меню
const menuCommands = ["Список курьеров", "Назад", "Панель администратора"];
if (adminWaitingCourier.has(username) && menuCommands.includes(text)) {
  adminWaitingCourier.delete(username); // сброс ожидания
  console.log(`Состояние ожидания ника сброшено для @${username} из-за меню`);
}

// ✅ ✅ ✅ ВОТ СЮДА ВСТАВЛЯЕШЬ ОБРАБОТЧИК "НАЗАД"
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

  if (isCourier(username)) {
    return bot.sendMessage(id, "Главное меню курьера", {
      reply_markup: {
        keyboard: [
          [{ text: "Личный кабинет" }, { text: "Поддержка" }],
          [{ text: "Панель курьера" }]
        ],
        resize_keyboard: true
      }
    });
  }

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


// ===== Личный кабинет (с защитой от Markdown) =====
if (text === "Личный кабинет") {
  console.log("[DEBUG] Личный кабинет нажали:", { id, username });

  try {
    const uname = (username || "").replace(/^@/, "");

    const roleLabel =
      (id === ADMIN_ID) ? "👑 Админ" :
      (isCourier(username) ? "🚚 Курьер" : "🧑 Клиент");

    const [[{ cnt: totalOrders }]] = await db.execute(
      "SELECT COUNT(*) AS cnt FROM orders WHERE REPLACE(tgNick,'@','') = ?",
      [uname]
    );

    const [[{ cnt: newCnt }]] = await db.execute(
      "SELECT COUNT(*) AS cnt FROM orders WHERE REPLACE(tgNick,'@','') = ? AND status='new'",
      [uname]
    );

    const [[{ cnt: takenCnt }]] = await db.execute(
      "SELECT COUNT(*) AS cnt FROM orders WHERE REPLACE(tgNick,'@','') = ? AND status='taken'",
      [uname]
    );

    const [[{ cnt: deliveredCnt }]] = await db.execute(
      "SELECT COUNT(*) AS cnt FROM orders WHERE REPLACE(tgNick,'@','') = ? AND status='delivered'",
      [uname]
    );

    const [lastOrders] = await db.execute(
      "SELECT id, status, created_at FROM orders WHERE REPLACE(tgNick,'@','')=? ORDER BY created_at DESC LIMIT 1",
      [uname]
    );
    const lastOrder = lastOrders[0];

    const client = await getClient(uname);

    const formatRu = (dt) => {
      if (!dt) return "—";
      return new Date(dt).toLocaleString("ru-RU", {
        timeZone: "Europe/Zaporozhye",
        day: "2-digit",
        month: "2-digit",
        year: "numeric",
        hour: "2-digit",
        minute: "2-digit"
      });
    };

    // 1) Сначала делаем красивый MarkdownV2
    const msgMarkdown =
      `👤 *Личный кабинет*\n\n` +
      `🧑 Имя: *${escapeMarkdownV2(client?.first_name || "—")}*\n` +
      `🔗 Ник: @${escapeMarkdownV2(uname)}\n` +
      `🏷 Статус: *${escapeMarkdownV2(roleLabel)}*\n\n` +
      `🧾 Всего заказов: *${totalOrders || 0}*\n` +
      `🆕 Новые: *${newCnt || 0}*\n` +
      `🚚 В пути: *${takenCnt || 0}*\n` +
      `✅ Выполнено: *${deliveredCnt || 0}*\n\n` +
      `🕒 Последняя активность: *${escapeMarkdownV2(formatRu(client?.last_active))}*\n` +
      (lastOrder
        ? `📦 Последний заказ: *№${escapeMarkdownV2(String(lastOrder.id))}* (${escapeMarkdownV2(lastOrder.status)})\n` +
          `📅 Создан: *${escapeMarkdownV2(formatRu(lastOrder.created_at))}*`
        : `📦 Последний заказ: —`);

    try {
     await bot.sendMessage(id, msgMarkdown.replace(/\*/g, ""));
      return;
    } catch (e) {
      // 2) Если Markdown сломался — логируем и отправляем обычным текстом (без parse_mode)
      console.error("[ERROR] ЛК MarkdownV2 failed:", e?.message || e);

      const msgPlain =
        `Личный кабинет\n\n` +
        `Имя: ${client?.first_name || "—"}\n` +
        `Ник: @${uname}\n` +
        `Статус: ${roleLabel}\n\n` +
        `Всего заказов: ${totalOrders || 0}\n` +
        `Новые: ${newCnt || 0}\n` +
        `В пути: ${takenCnt || 0}\n` +
        `Выполнено: ${deliveredCnt || 0}\n\n` +
        `Последняя активность: ${formatRu(client?.last_active)}\n` +
        (lastOrder
          ? `Последний заказ: №${lastOrder.id} (${lastOrder.status}), создан: ${formatRu(lastOrder.created_at)}`
          : `Последний заказ: —`);

      await bot.sendMessage(id, msgPlain);
      return;
    }

  } catch (err) {
    console.error("[ERROR] Личный кабинет общий:", err?.message || err);
    return bot.sendMessage(id, "Ошибка при открытии личного кабинета. (Смотри консоль сервера)");
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

  // ===== Панель администратора =====
if (text === "Панель администратора" && id === ADMIN_ID) {
  const kb = {
    keyboard: [
  [{ text: "Статистика" }, { text: "Курьеры" }],
  [{ text: "Активные по курьеру" }, { text: "Выполненные по курьеру" }],
  [{ text: "Взятые сейчас" }, { text: "Сводка курьеров" }],
  [{ text: "Добавить курьера" }, { text: "Удалить курьера" }],
  [{ text: "Список курьеров" }, { text: "Все пользователи" }],
  [{ text: "Рассылка" }],
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
if (text === "Активные по курьеру" && id === ADMIN_ID) {
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
if (text === "Выполненные по курьеру" && id === ADMIN_ID) {
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


// ===== Рассылка с лимитом (без дублей + отчет по никам) =====
if (adminWaitingBroadcast.has(username)) {
  const msgText = text;

  try {
    // 1) Берем уникальные chat_id (если в базе дубли — они схлопнутся)
    const [rows] = await db.execute(`
      SELECT chat_id, MAX(username) AS username
      FROM clients
      WHERE subscribed=1 AND chat_id IS NOT NULL
      GROUP BY chat_id
    `);

    console.log(`Начало рассылки от @${username}, текст: "${msgText}"`);
    console.log(`Уникальных получателей: ${rows.length}`);

    const safeMsg = escapeMarkdownV2(msgText);

    const limit = pLimit(5);

    const okUsers = [];
    const failUsers = [];

    // 2) На всякий случай еще защита от дублей в коде
    const sentSet = new Set();

    const tasks = rows.map(r => limit(async () => {
      const chatId = r.chat_id;
      const uname = r.username ? String(r.username) : "";

      if (!chatId) return;

      // если каким-то чудом chatId повторился — пропускаем
      if (sentSet.has(chatId)) return;
      sentSet.add(chatId);

      try {
        await bot.sendMessage(chatId, safeMsg, { parse_mode: "MarkdownV2" });
        okUsers.push(uname ? `@${uname.replace(/^@/, "")}` : `chat_id:${chatId}`);
        console.log(`✅ Отправлено: ${uname || chatId}`);
      } catch (err) {
        failUsers.push(uname ? `@${uname.replace(/^@/, "")}` : `chat_id:${chatId}`);
        console.error(`❌ Ошибка отправки ${uname || chatId}:`, err.message);
      }
    }));

    await Promise.all(tasks);

    adminWaitingBroadcast.delete(username);

    // 3) Отчет админу (может быть длинный — шлем частями)
    const makeChunks = (arr, maxLen = 3500) => {
      const out = [];
      let cur = "";
      for (const x of arr) {
        const add = (cur ? "\n" : "") + x;
        if ((cur + add).length > maxLen) {
          out.push(cur);
          cur = x;
        } else {
          cur += add;
        }
      }
      if (cur) out.push(cur);
      return out;
    };

    const header =
      `📣 Рассылка завершена\n` +
      `Успешно: ${okUsers.length} из ${rows.length}\n` +
      `Ошибки: ${failUsers.length}\n`;

    await bot.sendMessage(ADMIN_ID, escapeMarkdownV2(header), { parse_mode: "MarkdownV2" });

    if (okUsers.length) {
      const okChunks = makeChunks(okUsers);
      for (let i = 0; i < okChunks.length; i++) {
        await bot.sendMessage(
          ADMIN_ID,
          escapeMarkdownV2(`✅ Доставлено (часть ${i + 1}/${okChunks.length}):\n${okChunks[i]}`),
          { parse_mode: "MarkdownV2" }
        );
      }
    }

    if (failUsers.length) {
      const failChunks = makeChunks(failUsers);
      for (let i = 0; i < failChunks.length; i++) {
        await bot.sendMessage(
          ADMIN_ID,
          escapeMarkdownV2(`❌ Не доставлено (часть ${i + 1}/${failChunks.length}):\n${failChunks[i]}`),
          { parse_mode: "MarkdownV2" }
        );
      }
    }

  } catch (err) {
    console.error(`Ошибка при рассылке от @${username}:`, err.message);
    await bot.sendMessage(
      ADMIN_ID,
      `Ошибка при рассылке: ${escapeMarkdownV2(err.message)}`,
      { parse_mode: "MarkdownV2" }
    );
  }

  return;
}



// ===== Панель курьера =====
if (text === "Панель курьера" && (COURIERS[username] || id === ADMIN_ID)) {
  const kb = {
    keyboard: [
      [{ text: "Новые заказы" }, { text: "Взятые заказы" }],
      [{ text: "Выполненные заказы" }],
      [{ text: "Назад" }]
    ],
    resize_keyboard: true
  };
  return bot.sendMessage(id, "Панель курьера", { reply_markup: kb });
}


// ===== ПРОСМОТР ЗАКАЗОВ КУРЬЕРА (ЕДИНАЯ ЛОГИКА) =====
// ===== ПРОСМОТР ЗАКАЗОВ КУРЬЕРА (НОВЫЕ / ВЗЯТЫЕ / ВЫПОЛНЕННЫЕ) =====
if (
  (text === "Новые заказы" || text === "Взятые заказы" || text === "Выполненные заказы") &&
  (isCourier(username) || id === ADMIN_ID)
) {

console.log("[DEBUG] courier panel click:", text, "user:", username, "id:", id);

  // если админ — сбрасываем режимы админки, чтобы не мешали панели курьера
if (id === ADMIN_ID) {
  adminWaitingOrdersCourier.delete(username);
  adminWaitingBroadcast.delete(username);
}

  const courierName = (username || "").replace(/^@/, "");

  let query = "";
  let params = [];
  let emptyText = "";

  if (text === "Новые заказы") {
    emptyText = "Нет новых заказов";
    query = `
      SELECT * FROM orders
      WHERE status='new' AND courier_username IS NULL
      ORDER BY created_at DESC
    `;
  }

  if (text === "Взятые заказы") {
    emptyText = "Нет взятых заказов";
    query = `
      SELECT * FROM orders
      WHERE status='taken' AND courier_username=?
      ORDER BY taken_at DESC
    `;
    params = [courierName];
  }

  if (text === "Выполненные заказы") {
    emptyText = "Нет выполненных заказов";
    query = `
      SELECT * FROM orders
      WHERE status='delivered' AND courier_username=?
      ORDER BY delivered_at DESC
    `;
    params = [courierName];
  }

const [orders] = await db.execute(query, params);

if (!orders.length) {
  await bot.sendMessage(id, emptyText);
  return;
}

await bot.sendMessage(id, `Найдено заказов: ${orders.length}`);

for (const order of orders) {
  await clearOrderMessage(order.id, id); // ✅ чтобы прислало заново
  await sendOrUpdateOrderToChat(order, id, "courier", username);
}

return;
} // закрыли IF

}); // ✅ закрыли bot.on("message", async (msg) => { ... })

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
  ON DUPLICATE KEY UPDATE
    chat_id = VALUES(chat_id),
    username = VALUES(username)
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

      // ✅ СТРАХОВКА: гарантируем client_chat_id у заказа
  if (client_chat_id) {
    await db.execute(
      "UPDATE orders SET client_chat_id=? WHERE id=? AND (client_chat_id IS NULL OR client_chat_id=0)",
      [client_chat_id, id]
    );
  }
    } else {
      console.log(`Заказ ${id} уже в базе, пропускаем добавление`);
    }

    // ===== Получаем заказ из базы =====
    const updated = await getOrderById(id);

    // ===== Отправляем уведомления в Telegram =====
    await sendOrUpdateOrderAll(updated);
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
        await sendOrUpdateOrderAll(order);
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
