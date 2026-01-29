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

const MINI_APP_URL = "https://cn4tzwpqvg-ops.github.io/crazycloud/";

const referralText =
  "🎉 *Вас пригласил друг!*\n\n" +
  "Как новому клиенту вам доступна скидка *2€* на первый заказ.\n\n" +
  "Нажмите кнопку *«КУПИТЬ ЖИЖУ»* — скидка применится автоматически 👇";

const discountMenuText =
  "💸 *Скидки и приглашения*\n\n" +
  "Приглашайте друзей и получайте *2€* скидки на заказы 👇";

const discountMenuKeyboard = {
  keyboard: [
    [{ text: "🤝 Мои приглашённые" }],
    [{ text: "🔗 Моя реферальная ссылка" }],
    [{ text: "⬅️ Назад" }]
  ],
  resize_keyboard: true
};


const mainMenuKeyboard = {
  keyboard: [
    [{ text: "💸 Получить скидку" }],
    [{ text: "👤 Личный кабинет" }, { text: "🛟 Поддержка" }],
    [{ text: "🧾 Мои заказы" }]
  ],
  resize_keyboard: true
};

const courierStartKeyboard = {
  keyboard: [
    [{ text: "💸 Получить скидку" }],
    [{ text: "👤 Личный кабинет" }, { text: "🛟 Поддержка" }],
    [{ text: "🧾 Мои заказы" }],
    [{ text: "Панель курьера" }],
    [{ text: "⬅️ Назад" }]
  ],
  resize_keyboard: true
};


const adminStartKeyboard = {
  keyboard: [
    [{ text: "Статистика" }, { text: "Курьеры" }],
    [{ text: "Активные по курьеру" }, { text: "Выполненные по курьеру" }],
    [{ text: "Взятые сейчас" }, { text: "✅ Доставлено сегодня" }], // ✅ было "Сводка курьеров"
    [{ text: "🤝 Рефералы" }, { text: "🚨 Логи рефералов" }],
    [{ text: "Добавить курьера" }, { text: "Удалить курьера" }],
    [{ text: "Список курьеров" }, { text: "Все пользователи" }],
    [{ text: "Рассылка" }],
    [{ text: "⬅️ Назад" }]
  ],
  resize_keyboard: true
};


const myOrdersKeyboard = {
  keyboard: [
    [{ text: "Активные заказы" }],
    [{ text: "Выполненные заказы" }],
    [{ text: "⬅️ Назад" }]
  ],
  resize_keyboard: true
};

const courierPanelKeyboard = {
  keyboard: [
    [{ text: "Новые заказы" }, { text: "Взятые заказы" }],
    [{ text: "Выполненные заказы" }],
    [{ text: "⬅️ Назад" }]
  ],
  resize_keyboard: true
};





// ================= Настройки1 =================
const TOKEN = process.env.TELEGRAM_TOKEN;
const ADMIN_USERNAME = process.env.ADMIN_USERNAME || "admin";
const ADMIN_ID = Number(process.env.ADMIN_ID || 7664644901);

// Railway: порт задаётся платформой через env PORT
const PORT = Number(process.env.PORT || 3000);
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

async function ensureClientsChatIdUnique() {
  // 0) нормализуем "0" как NULL (чтобы не конфликтовало)
  await db.execute(`UPDATE clients SET chat_id=NULL WHERE chat_id=0`).catch(() => {});

  // 1) удаляем дубли chat_id (оставляем самую "свежую" запись)
  // оставляем ту, у которой last_active больше (если равны — id больше)
  await db.execute(`
    DELETE c1
    FROM clients c1
    JOIN clients c2
      ON c1.chat_id = c2.chat_id
     AND c1.chat_id IS NOT NULL
     AND (
          COALESCE(c1.last_active,'1970-01-01') < COALESCE(c2.last_active,'1970-01-01')
          OR (
            COALESCE(c1.last_active,'1970-01-01') = COALESCE(c2.last_active,'1970-01-01')
            AND c1.id < c2.id
          )
     )
  `).catch(() => {});

  // 2) добавляем UNIQUE на chat_id (если ещё нет)
  await db.execute(
    `ALTER TABLE clients ADD UNIQUE KEY uq_clients_chat_id (chat_id)`
  ).catch(() => {});

  // 3) РЕКОМЕНДУЮ: убрать UNIQUE с username (username может меняться/переиспользоваться)
  // если UNIQUE уже убран — просто будет catch
  try {
    const [idx] = await db.execute(`SHOW INDEX FROM clients`);
    const uniqueOnUsername = (idx || []).find(r => r.Column_name === "username" && Number(r.Non_unique) === 0);
    if (uniqueOnUsername?.Key_name) {
      await db.execute(`ALTER TABLE clients DROP INDEX \`${uniqueOnUsername.Key_name}\``);
    }
  } catch (e) {}

  // 4) и поставить обычный индекс на username (для скорости)
  await db.execute(`CREATE INDEX idx_clients_username ON clients(username)`).catch(() => {});
}


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
    username VARCHAR(255),
    first_name VARCHAR(255),
    chat_id BIGINT UNIQUE,
    banned TINYINT(1) DEFAULT 0,
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

  // ===== ЛОГИ ПОДОЗРИТЕЛЬНЫХ ДЕЙСТВИЙ =====
await db.execute(`
  CREATE TABLE IF NOT EXISTS referral_logs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    type VARCHAR(50),
    username VARCHAR(255),
    details TEXT,
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

     // ===== ДОБАВЛЯЕМ КОЛОНКИ ДЛЯ РЕФЕРАЛКИ =====

  // clients.referrer
  try {
    await db.execute(
      "ALTER TABLE clients ADD COLUMN referrer VARCHAR(255) DEFAULT NULL"
    );
    console.log("clients.referrer добавлена");
  } catch (e) {}

  // clients.referral_bonus_available
  try {
    await db.execute(
      "ALTER TABLE clients ADD COLUMN referral_bonus_available INT DEFAULT 0"
    );
    console.log("clients.referral_bonus_available добавлена");
  } catch (e) {}

  // clients.eligible_referrer (может ли человек приглашать других)
  try {
    await db.execute(
      "ALTER TABLE clients ADD COLUMN eligible_referrer TINYINT(1) DEFAULT 0"
    );
    console.log("clients.eligible_referrer добавлена");
  } catch (e) {}

  // clients.referrals_locked (если сам пришёл по рефке и ещё не сделал заказ — блок на приглашения)
  try {
    await db.execute(
      "ALTER TABLE clients ADD COLUMN referrals_locked TINYINT(1) DEFAULT 0"
    );
    console.log("clients.referrals_locked добавлена");
  } catch (e) {}

  // clients.referral_bonus_locked (резерв бонусов под активные заказы)
  try {
    await db.execute(
      "ALTER TABLE clients ADD COLUMN referral_bonus_locked INT DEFAULT 0"
    );
    console.log("clients.referral_bonus_locked добавлена");
  } catch (e) {}

  // orders.original_price
  try {
    await db.execute(
      "ALTER TABLE orders ADD COLUMN original_price DECIMAL(10,2) DEFAULT 15"
    );
    console.log("orders.original_price добавлена");
  } catch (e) {}

  // orders.final_price
  try {
    await db.execute(
      "ALTER TABLE orders ADD COLUMN final_price DECIMAL(10,2) DEFAULT 15"
    );
    console.log("orders.final_price добавлена");
  } catch (e) {}

  // orders.discount_type
  try {
    await db.execute(
      "ALTER TABLE orders ADD COLUMN discount_type VARCHAR(50) DEFAULT NULL"
    );
    console.log("orders.discount_type добавлена");
  } catch (e) {}

  // orders.referral_bonus_given
  try {
    await db.execute(
      "ALTER TABLE orders ADD COLUMN referral_bonus_given TINYINT(1) DEFAULT 0"
    );
    console.log("orders.referral_bonus_given добавлена");
  } catch (e) {}

  // orders.referral_bonus_reserved_qty (сколько бонусов зарезервировано под этот заказ)
  try {
    await db.execute(
      "ALTER TABLE orders ADD COLUMN referral_bonus_reserved_qty INT DEFAULT 0"
    );
    console.log("orders.referral_bonus_reserved_qty добавлена");
  } catch (e) {}

  // orders.referral_bonus_spent (чтобы не списать/не вернуть дважды)
  try {
    await db.execute(
      "ALTER TABLE orders ADD COLUMN referral_bonus_spent TINYINT(1) DEFAULT 0"
    );
    console.log("orders.referral_bonus_spent добавлена");
  } catch (e) {}

  await ensureClientsChatIdUnique();

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
  for (const r of rows) {
    const u = String(r.username || "").replace(/^@+/, "").trim();
    if (!u) continue;
    map[u] = (r.chat_id == null ? null : Number(r.chat_id));
  }
  return map;
}


async function addCourier(username, chatId = null) {
  const u = String(username || "").replace(/^@+/, "").trim();
  if (!u) return false;

  const cid = (chatId === null || chatId === undefined || chatId === "")
    ? null
    : Number(chatId);

  await db.execute(
    `
    INSERT INTO couriers (username, chat_id)
    VALUES (?, ?)
    ON DUPLICATE KEY UPDATE chat_id=VALUES(chat_id)
    `,
    [u, cid]
  );

  COURIERS = await getCouriers();
  console.log(`Курьер добавлен/обновлён: @${u}, chat_id: ${cid}`);
  return true;
}

async function removeCourier(username) {
  const u = String(username || "").replace(/^@+/, "").trim();
  if (!u) return;

  await db.execute("DELETE FROM couriers WHERE username=?", [u]);
  COURIERS = await getCouriers();
  console.log(`Курьер удалён: @${u}`);
}

function isCourier(username) {
  const u = String(username || "").replace(/^@+/, "").trim();
  if (!u) return false;

  // true если курьер есть в таблице couriers (даже если chat_id ещё NULL)
  return Object.prototype.hasOwnProperty.call(COURIERS, u);
}


// ================= Клиенты =================
async function addOrUpdateClient(username, first_name, chat_id) {
  const now = new Date().toISOString().slice(0, 19).replace("T", " ");
  const uname = String(username || "").replace(/^@/, "").trim();
  const fname = String(first_name || "");
  const chatId = chat_id ? Number(chat_id) : null;

  if (!uname) return;

  // ✅ основной путь — апдейт/инсерт по UNIQUE chat_id
  if (chatId) {
    await db.execute(
      `
      INSERT INTO clients (chat_id, username, first_name, subscribed, created_at, last_active)
      VALUES (?, ?, ?, 1, ?, ?)
      ON DUPLICATE KEY UPDATE
        username = VALUES(username),
        first_name = VALUES(first_name),
        last_active = VALUES(last_active),
        subscribed = 1
      `,
      [chatId, uname, fname, now, now]
    );
    return;
  }

  // запасной путь, если chat_id неизвестен
  await db.execute(
    `
    INSERT INTO clients (username, first_name, subscribed, created_at, last_active, chat_id)
    VALUES (?, ?, 1, ?, ?, NULL)
    ON DUPLICATE KEY UPDATE
      first_name = VALUES(first_name),
      last_active = VALUES(last_active),
      subscribed = 1
    `,
    [uname, fname, now, now]
  );
}



async function getClient(username) {
  const [rows] = await db.execute("SELECT * FROM clients WHERE username=?", [username]);
  return rows[0];
}

async function isEligibleReferrer(username) {
  const uname = String(username || "").replace(/^@/, "").trim();
  if (!uname) return false;

  // если уже помечен — ок
  const c = await getClient(uname);
  if (c && Number(c.eligible_referrer || 0) === 1) return true;

  // иначе проверяем: есть ли хотя бы 1 delivered
  const [[row]] = await db.execute(
    `SELECT 1 AS ok FROM orders
     WHERE REPLACE(tgNick,'@','')=? AND status='delivered'
     LIMIT 1`,
    [uname]
  );

  const ok = !!row?.ok;

  // если есть delivered — фиксируем
  if (ok) {
    await db.execute(
      "UPDATE clients SET eligible_referrer=1 WHERE username=?",
      [uname]
    );
  }

  return ok;
}


// ================= Заказы =================
// ================= Вспомогательные функции =================

// Преобразует дату в формат MySQL DATETIME: YYYY-MM-DD HH:MM:SS
function formatMySQLDateTime(date = new Date()) {
  const pad = n => n.toString().padStart(2, '0');
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ` +
         `${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
}
async function hasReferralLog(type, username, details) {
  const [rows] = await db.execute(
    "SELECT 1 FROM referral_logs WHERE type=? AND username=? AND details=? LIMIT 1",
    [type, username, details]
  );
  return rows.length > 0;
}

async function addReferralLog(type, username, details) {
  await db.execute(
    "INSERT INTO referral_logs (type, username, details, created_at) VALUES (?, ?, ?, NOW())",
    [type, username, details]
  );
}

async function refundReservedBonusIfNeeded(order) {
  try {
    if (!order) return;

    const discountType = String(order.discount_type || "");
    const reservedQty = Number(order.referral_bonus_reserved_qty || 0);
    const spent = Number(order.referral_bonus_spent || 0);

    // Возвращаем только если это бонус 3€, он был зарезервирован, и ещё не "закреплён"
    if (discountType !== "referral_bonus") return;
    if (reservedQty <= 0) return;
    if (spent === 1) return; // уже закрепили — не возвращаем

    const buyer = String(order.tgNick || "").replace(/^@+/, "").trim();
    if (!buyer) return;

    // Возврат бонуса клиенту
    await db.execute(
      "UPDATE clients SET referral_bonus_available = referral_bonus_available + ? WHERE username=?",
      [reservedQty, buyer]
    );

    // Обнуляем резерв на заказе (чтобы второй раз не вернуть)
    await db.execute(
      "UPDATE orders SET referral_bonus_reserved_qty=0 WHERE id=?",
      [order.id]
    );

    // Лог
    await db.execute(
      "INSERT INTO referral_logs (type, username, details, created_at) VALUES (?, ?, ?, NOW())",
      ["bonus_refund", buyer, `Возврат ${reservedQty} бонус(ов) 2€ за заказ №${order.id}`]
    );

    console.log(`[BONUS REFUND] +${reservedQty} для @${buyer} за заказ ${order.id}`);
  } catch (e) {
    console.error("[refundReservedBonusIfNeeded] error:", e?.message || e);
  }
}


async function notifyReferrer(referrerUsername, text) {
  const uname = String(referrerUsername || "").replace(/^@+/, "").trim();
  if (!uname) return;

  try {
    const ref = await getClient(uname);
    if (!ref || !ref.chat_id) return;

    await bot.sendMessage(ref.chat_id, String(text || ""));
  } catch (e) {
    console.error("[notifyReferrer] failed:", e?.message || e);
  }
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
    INSERT INTO orders (
      id,
      tgNick,
      city,
      delivery,
      payment,
      orderText,
      date,
      time,
      status,
      created_at,
      client_chat_id,
      original_price,
      final_price,
      discount_type,
      referral_bonus_reserved_qty,
      referral_bonus_spent

    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      tgNick = VALUES(tgNick),
      city = VALUES(city),
      delivery = VALUES(delivery),
      payment = VALUES(payment),
      orderText = VALUES(orderText),
      date = VALUES(date),
      time = VALUES(time),
      status = VALUES(status),
      client_chat_id = VALUES(client_chat_id),
      original_price = VALUES(original_price),
      final_price = VALUES(final_price),
      discount_type = VALUES(discount_type),
      referral_bonus_reserved_qty = VALUES(referral_bonus_reserved_qty),
      referral_bonus_spent = VALUES(referral_bonus_spent)
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
      order.client_chat_id || null, // ← что пришло — то и пишем
      order.original_price ?? 15,
      order.final_price ?? 15,
      order.discount_type || null,
      order.referral_bonus_reserved_qty ?? 0,
      order.referral_bonus_spent ?? 0
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
  const createdMs = order.created_at
    ? new Date(order.created_at).getTime()
    : Date.now();

  const orderAge = Date.now() - createdMs;

  const canCancelByTime = orderAge <= 20 * 60 * 1000;
  const canCancelByStatus = (order.status === "new" || order.status === "taken");

  console.log("[DEBUG cancel btn]", {
    orderId: order.id,
    status: order.status,
    created_at: order.created_at,
    orderAgeMin: Math.round(orderAge / 60000),
    canCancelByTime,
    canCancelByStatus
  });

  if (canCancelByTime && canCancelByStatus) {
    return [[{ text: "❌ Отменить заказ", callback_data: `confirm_cancel_${order.id}` }]];
  }

  return [];
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
      keyboard.push(
        [{ text: "🔁 Переназначить курьера", callback_data: `reassign_${order.id}` }],
        [{ text: "🗑 Удалить заказ", callback_data: `admin_delete_${order.id}` }]
      );
    }
  }
  return keyboard;
}



  // delivered / canceled — без кнопок
  return [];
}

function buildOrderMessage(order) {
  const lines = [
    `🧾 Заказ №${order.id}`,
    `👤 Клиент: ${withAt(order.tgNick)}`,
    `🏙 Город: ${order.city || "—"}`,
    `🚚 Доставка: ${order.delivery || "—"}`,
    `💰 Оплата: ${order.payment || "—"}`,
    `📝 Заказ: ${order.orderText || "—"}`,
    `📅 Дата: ${order.date ? new Date(order.date).toLocaleDateString("ru-RU") : "—"}`,
    `⏰ Время: ${order.time || "—"}`,
    `🚚 Курьер: ${withAt(order.courier_username || "—")}`,
    `📌 Статус: ${order.status || "—"}`
  ];

  if (order.original_price && order.final_price) {
    if (Number(order.final_price) < Number(order.original_price)) {
      lines.push(`💸 Цена: ${order.final_price}€ (вместо ${order.original_price}€)`);

      if (order.discount_type === "first_order") {
  lines.push("🎁 Скидка применена: первый заказ по реферальной ссылке");
}
if (order.discount_type === "referral_bonus") {
  lines.push("🎁 Скидка применена: скидка за приглашённого друга");
}
    } else {
      lines.push(`💸 Цена: ${order.original_price}€`);
    }
  }

  return lines.join("\n");
}

function buildTextForOrder(order) {
  let msgText = buildOrderMessage({
    ...order,
    courier_username: order.courier_username || "—"
  });

  if (order.status === "canceled") {
    msgText += "\n\n❌ Заказ был отменён покупателем";
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
        reply_markup: keyboard.length
          ? { inline_keyboard: keyboard }
          : undefined
      });
    } else {
      const sent = await bot.sendMessage(chatId, msgText, {
        reply_markup: keyboard.length
          ? { inline_keyboard: keyboard }
          : undefined
      });
      await saveOrderMessage(order.id, chatId, sent.message_id);
    }
  } catch (err) {
    const emsg = String(err?.message || "");

    // сообщение не изменилось — норм
    if (emsg.includes("message is not modified")) return;

    // сообщение нельзя отредактировать — удаляем запись
    if (
      emsg.includes("message to edit not found") ||
      emsg.includes("message identifier is not specified") ||
      emsg.includes("message can't be edited") ||
      emsg.includes("MESSAGE_ID_INVALID")
    ) {
      await clearOrderMessage(order.id, chatId);
    }

    // пробуем отправить заново
    try {
      const sent = await bot.sendMessage(chatId, msgText, {
        reply_markup: keyboard.length
          ? { inline_keyboard: keyboard }
          : undefined
      });
      await saveOrderMessage(order.id, chatId, sent.message_id);
    } catch (e2) {
      console.error(
        `[ERROR] sendOrUpdateOrderToChat ${order.id} -> ${chatId}:`,
        e2.message
      );
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
  // НЕ перетираем админа ролью courier
  if (Number(c.chat_id) === Number(ADMIN_ID)) continue;

  recipientsMap.set(c.chat_id, {
    chatId: c.chat_id,
    role: "courier",
    username: c.username
  });
}


    // ✅ Если client_chat_id пустой — пытаемся найти по tgNick и сохранить в orders
  if (!order.client_chat_id && order.tgNick) {
    try {
      const cleanNick = String(order.tgNick).replace(/^@+/, "").trim();
      const client = await getClient(cleanNick);

      if (client?.chat_id) {
        order.client_chat_id = client.chat_id;

        await db.execute(
          "UPDATE orders SET client_chat_id=? WHERE id=? AND (client_chat_id IS NULL OR client_chat_id=0)",
          [client.chat_id, order.id]
        );
      }
    } catch (e) {
      console.error("[sendOrUpdateOrderAll] resolve client_chat_id error:", e?.message || e);
    }
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
  console.log("[DEBUG sendOrUpdateOrderAll]", {
  orderId: order.id,
  status: order.status,
  tgNick: order.tgNick,
  client_chat_id: order.client_chat_id,
  recipients: recipients.map(r => `${r.role}:${r.username}:${r.chatId}`)
});

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
  if (!order.client_chat_id) return;

  const orderId = String(order.id);
  const clientId = order.client_chat_id;

  const already = await hasReviewForOrder(orderId);
  if (already) return;

  if (waitingReview.has(clientId)) {
    const cur = waitingReview.get(clientId);
    if (cur && String(cur.orderId) !== orderId) {
      waitingReview.delete(clientId);
    } else {
      return;
    }
  }

  waitingReview.set(clientId, {
    orderId,
    courier: order.courier_username
      ? `@${String(order.courier_username).replace(/^@/, "")}`
      : "—",
    client: order.tgNick
      ? `@${String(order.tgNick).replace(/^@/, "")}`
      : "—",
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
  `✅ Заказ №${orderId} доставлен.\n` +
    `🚚 Курьер: ${courier}\n\n` +
    `Оцените работу курьера ⭐ (1–5)\n\n` +
    `После оценки напишите пару слов:\n` +
    `• опоздал/вовремя?\n` +
    `• как общался?\n` +
    `• всё ли было нормально?\n\n` +
    `Если не хотите — нажмите «Пропустить».`,
  { reply_markup: kb }
);


    console.log("[DEBUG] review request sent to client:", clientId, "order:", orderId);
  } catch (e) {
    console.error("[ERROR] cannot send review request:", e?.message || e, { clientId, orderId });
  }
}






// =================== Восстановление заказов для клиентов ===================
async function restoreOrdersForClients() {
  console.log("[INFO] Восстановление заказов для клиентов...");
  const [clients] = await db.execute(
    "SELECT username, chat_id FROM clients WHERE chat_id IS NOT NULL"
  );

  const limit = pLimit(5);

  for (const client of clients) {
   const [orders] = await db.execute(
  `SELECT * FROM orders
   WHERE client_chat_id = ?
   AND status IN ('new','taken')
   ORDER BY created_at DESC`,
  [client.chat_id]
);

    const tasks = orders.map(order =>
      limit(async () => {
        try {
          // Проверяем, есть ли уже сообщение клиенту
          const messages = await getOrderMessages(order.id);
          const alreadySent = messages.some(
            m => m.chat_id === client.chat_id
          );
          if (alreadySent) return;

          const text = buildTextForOrder(order);

          const sent = await bot.sendMessage(
            client.chat_id,
            text
          );

          await saveOrderMessage(
            order.id,
            client.chat_id,
            sent.message_id
          );

          console.log(
            `[INFO] Отправлен заказ №${order.id} клиенту @${client.username}`
          );
        } catch (err) {
          console.error(
            `[ERROR] Ошибка отправки заказа №${order.id} клиенту @${client.username}:`,
            err.message
          );
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


// ================= ПАГИНАЦИЯ ЗАКАЗОВ В ПАНЕЛИ КУРЬЕРА =================
const ORDERS_PAGE_SIZE = 10;

// chatId -> { msgId, type, page, view: 'list'|'detail', orderId, role, username }
const ordersPagerState = new Map();

function typeLabel(t) {
  if (t === "new") return "🆕 Новые заказы";
  if (t === "taken") return "🚚 Взятые заказы";
  if (t === "del") return "✅ Выполненные заказы";
  return "Заказы";
}

function typeToStatus(t) {
  if (t === "new") return "new";
  if (t === "taken") return "taken";
  if (t === "del") return "delivered";
  return "new";
}

function buildPagerKeyboardList(type, page, pages, orders) {
  // кнопки заказов (по 2 в ряд)
  const rows = [];
  let row = [];
  for (const o of orders) {
    row.push({ text: `№${o.id}`, callback_data: `pgopen_${type}_${page}_${o.id}` });
    if (row.length === 2) { rows.push(row); row = []; }
  }
  if (row.length) rows.push(row);

  // навигация
  const nav = [];
  if (page > 1) nav.push({ text: "⬅️", callback_data: `pg_${type}_${page - 1}` });
  else nav.push({ text: "·", callback_data: "noop" });

  nav.push({ text: "🔄", callback_data: `pg_${type}_${page}` });

  if (page < pages) nav.push({ text: "➡️", callback_data: `pg_${type}_${page + 1}` });
  else nav.push({ text: "·", callback_data: "noop" });

  rows.push(nav);

  // переключатели типов + закрыть
  rows.push([
    { text: "🆕", callback_data: "pg_new_1" },
    { text: "🚚", callback_data: "pg_taken_1" },
    { text: "✅", callback_data: "pg_del_1" }
  ]);

  rows.push([{ text: "❌ Закрыть", callback_data: "pgclose" }]);

  return { inline_keyboard: rows };
}

async function fetchOrdersPagerPage({ type, page, role, username }) {
  const status = typeToStatus(type);
  const p = Math.max(1, Number(page) || 1);

  const offset = (p - 1) * ORDERS_PAGE_SIZE;

  // admin может смотреть taken/delivered ВСЕ (а courier — только свои)
  const isAdmin = role === "admin";
  const courierName = String(username || "").replace(/^@/, "");

  let where = "status=?";
  const paramsCount = [status];
  const paramsList = [status];

  if (status === "new") {
    where += " AND courier_username IS NULL";
  } else {
    // taken/delivered
    if (!isAdmin) {
      where += " AND courier_username=?";
      paramsCount.push(courierName);
      paramsList.push(courierName);
    }
  }

  // count
  const [[cntRow]] = await db.execute(
    `SELECT COUNT(*) AS cnt FROM orders WHERE ${where}`,
    paramsCount
  );
  const total = Number(cntRow?.cnt || 0);
  const pages = Math.max(1, Math.ceil(total / ORDERS_PAGE_SIZE));
  const pageClamped = Math.min(Math.max(1, p), pages);
  const offsetClamped = (pageClamped - 1) * ORDERS_PAGE_SIZE;

  // сортировка
  let orderBy = "created_at DESC";
  if (status === "taken") orderBy = "taken_at DESC";
  if (status === "delivered") orderBy = "delivered_at DESC";

  const [rows] = await db.execute(
    `SELECT * FROM orders WHERE ${where} ORDER BY ${orderBy} LIMIT ? OFFSET ?`,
    [...paramsList, ORDERS_PAGE_SIZE, offsetClamped]
  );

  return { total, pages, page: pageClamped, orders: rows || [] };
}

function priceLine(order) {
  const op = Number(order.original_price || 0);
  const fp = Number(order.final_price || 0);

  if (fp > 0 && op > 0 && fp < op) return `${fp.toFixed(2)}€ (вместо ${op.toFixed(2)}€)`;
  if (fp > 0) return `${fp.toFixed(2)}€`;
  if (op > 0) return `${op.toFixed(2)}€`;
  return "—";
}

async function renderOrdersPagerList(chatId, msgId, type, page, role, username) {
const data = await fetchOrdersPagerPage({ type, page, role, username });


  const head =
    `${typeLabel(type)}\n` +
    `📄 Страница ${data.page}/${data.pages} • всего: ${data.total}\n\n`;

  if (!data.orders.length) {
    const text = head + "Пусто.";
    const kb = buildPagerKeyboardList(type, data.page, data.pages, []);
    try {
      await bot.editMessageText(text, { chat_id: chatId, message_id: msgId, reply_markup: kb });
    } catch (e) {
      // если "message is not modified" — игнор
    }
    ordersPagerState.set(chatId, { msgId, type, page: data.page, view: "list", orderId: null, role, username });
    return;
  }

  const lines = data.orders.map((o, i) => {
    const client = withAt(o.tgNick);
    const city = o.city || "—";
    const time = o.time || "—";
    const pr = priceLine(o);
    return `${i + 1}) №${o.id} • ${client} • ${city} • ${pr} • ${time}`;
  });

  const text = head + lines.join("\n");
  const kb = buildPagerKeyboardList(type, data.page, data.pages, data.orders);

  try {
    await bot.editMessageText(text, { chat_id: chatId, message_id: msgId, reply_markup: kb });
  } catch (e) {}

  ordersPagerState.set(chatId, { msgId, type, page: data.page, view: "list", orderId: null, role, username });
}

async function renderOrdersPagerDetail(chatId, msgId, type, page, orderId, role, username) {
  const order = await getOrderById(orderId);
  if (!order) {
    await bot.answerCallbackQuery(chatId, { text: "Заказ не найден", show_alert: true }).catch(() => {});
    return;
  }

  const text = buildTextForOrder(order);

  // действия (как у тебя) + кнопка назад + закрыть
  const kbRows = buildKeyboardForRecipient(order, { role, username });
  kbRows.push([{ text: "⬅️ Назад к списку", callback_data: `pg_${type}_${page}` }]);
  kbRows.push([{ text: "❌ Закрыть", callback_data: "pgclose" }]);

  try {
    await bot.editMessageText(text, {
      chat_id: chatId,
      message_id: msgId,
      reply_markup: { inline_keyboard: kbRows }
    });
  } catch (e) {}

  ordersPagerState.set(chatId, { msgId, type, page, view: "detail", orderId, role, username });
}

async function openOrdersPager(chatId, username, role, type) {
  // создаём одно сообщение и дальше только редактируем
  const sent = await bot.sendMessage(chatId, "⏳ Загружаю заказы...", {
    reply_markup: { inline_keyboard: [[{ text: "·", callback_data: "noop" }]] }
  });

  ordersPagerState.set(chatId, { msgId: sent.message_id, type, page: 1, view: "list", orderId: null, role, username });
  await renderOrdersPagerList(chatId, sent.message_id, type, 1, role, username);
}


// ================= ПАГИНАЦИЯ ДЛЯ ПАНЕЛИ КУРЬЕРА (10 заказов/страница) =================
const PAGE_SIZE = 10;

// запоминаем message_id списка, чтобы не спамить новыми сообщениями
const PANEL_LIST_MSG = new Map(); // key: `${chatId}:${mode}` -> messageId

function panelKey(chatId, mode) {
  return `${chatId}:${mode}`;
}

function modeTitle(mode) {
  if (mode === "new") return "🆕 Новые заказы";
  if (mode === "taken") return "🚚 Взятые заказы";
  if (mode === "delivered") return "✅ Выполненные заказы";
  return "📦 Заказы";
}

function safeFixed2(n) {
  const x = Number(n);
  if (!Number.isFinite(x)) return "—";
  return x.toFixed(2);
}

// короткая строка, без orderText
function shortOrderLine(o) {
  const nick = withAt(o.tgNick);
  const city = o.city || "—";
  const price = (o.final_price != null) ? `${safeFixed2(o.final_price)}€` : "—";
  const time = o.time || "—";
  return `№${o.id} • ${nick} • ${city} • ${price} • ${time}`;
}

// грузим 1 страницу + общее количество
async function fetchPanelOrdersPage(mode, courierUsername, page) {
  const p = Math.max(1, Number(page || 1));

  let where = "";
  const params = [];

  if (mode === "new") {
    where = "WHERE status='new' AND courier_username IS NULL";
  } else if (mode === "taken") {
    where = "WHERE status='taken' AND courier_username=?";
    params.push(String(courierUsername || "").replace(/^@/, ""));
  } else if (mode === "delivered") {
    where = "WHERE status='delivered' AND courier_username=?";
    params.push(String(courierUsername || "").replace(/^@/, ""));
  } else {
    where = "WHERE status IN ('new','taken','delivered')";
  }

  // COUNT
  const [[cntRow]] = await db.query(
    `SELECT COUNT(*) AS cnt FROM orders ${where}`,
    params
  );
  const total = Number(cntRow?.cnt || 0);
  const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE));

  const pageFixed = Math.min(p, totalPages);
  const offsetFixed = (pageFixed - 1) * PAGE_SIZE;

  // LIST (LIMIT/OFFSET без placeholders)
  const sql =
    `SELECT * FROM orders ${where} ` +
    `ORDER BY created_at DESC ` +
    `LIMIT ${Number(PAGE_SIZE)} OFFSET ${Number(offsetFixed)}`;

  const [rows] = await db.query(sql, params);

  return { rows, total, totalPages, page: pageFixed };
}


async function showOrdersList(chatId, role, username, mode, page, editMessageId) {
  const courierUsername = (role === "courier" || role === "admin") ? username : null;
const { rows, total, totalPages, page: p } = await fetchPanelOrdersPage(mode, courierUsername, page);


  let text = `${modeTitle(mode)}\nСтраница: ${p}/${totalPages}\nВсего: ${total}\n\n`;

  if (!rows.length) {
    text += "Пусто.";
  } else {
    rows.forEach((o, i) => {
      text += `${(i + 1) + (p - 1) * PAGE_SIZE}) ${shortOrderLine(o)}\n`;
    });
  }

  // клавиатура: список кнопок "№ID" (открыть детали)
  const kb = [];
  rows.forEach(o => {
    kb.push([{ text: `№${o.id}`, callback_data: `view_${o.id}_${mode}_${p}` }]);
  });

  // навигация
  const nav = [];
  if (p > 1) nav.push({ text: "⬅️ Назад", callback_data: `page_${mode}_${p - 1}` });
  nav.push({ text: "🔄 Обновить", callback_data: `page_${mode}_${p}` });
  if (p < totalPages) nav.push({ text: "Вперёд ➡️", callback_data: `page_${mode}_${p + 1}` });

  if (nav.length) kb.push(nav);

  const opts = { reply_markup: { inline_keyboard: kb } };

  // редактируем существующее сообщение списка, чтобы не спамить
  if (editMessageId) {
    try {
      await bot.editMessageText(text, { chat_id: chatId, message_id: editMessageId, ...opts });
      return editMessageId;
    } catch (e) {
      // если не получилось отредактировать — шлём новое
    }
  }

  const sent = await bot.sendMessage(chatId, text, opts);
  PANEL_LIST_MSG.set(panelKey(chatId, mode), sent.message_id);
  return sent.message_id;
}

async function showOrderDetails(chatId, role, username, orderId, mode, page, editMessageId) {
  const order = await getOrderById(String(orderId));
  if (!order) {
    // вернём к списку
    return showOrdersList(chatId, role, username, mode, page, editMessageId);
  }

  // полный текст (как у тебя)
  const fullText = buildTextForOrder(order);

  // твои кнопки действий (взять/доставлено/отказаться/и т.д.)
  const actionKb = buildKeyboardForRecipient(order, { role, username });

  // добавляем кнопку "назад к списку"
  const kb = [];
  if (actionKb && actionKb.length) {
    actionKb.forEach(r => kb.push(r));
  }
  kb.push([{ text: "⬅️ Назад к списку", callback_data: `back_${mode}_${page}` }]);

  const opts = { reply_markup: { inline_keyboard: kb } };

  try {
    await bot.editMessageText(fullText, { chat_id: chatId, message_id: editMessageId, ...opts });
    return editMessageId;
  } catch (e) {
    // если не редактируется — отправим новым
    const sent = await bot.sendMessage(chatId, fullText, opts);
    return sent.message_id;
  }
}


// ============== Telegram: callback =================

bot.on("callback_query", async (q) => {
 try {
  const data = q.data || "";
  const fromId = q.from.id;
  const username = q.from.username;

    // ===== ПАГИНАЦИЯ ПАНЕЛИ КУРЬЕРА (список/детали) =====
  try {
    if (data.startsWith("page_") || data.startsWith("view_") || data.startsWith("back_")) {
      const chatId = fromId;
      const role = (fromId === ADMIN_ID) ? "admin" : (isCourier(username) ? "courier" : "client");

      // message_id списка (чтобы редактировать)
      const listMsgId =
        q.message?.message_id ||
        PANEL_LIST_MSG.get(panelKey(chatId, (data.split("_")[1] || ""))) ||
        null;

      if (data.startsWith("page_")) {
        const parts = data.split("_"); // page_mode_page
        const mode = parts[1];
        const page = Number(parts[2] || 1);
        await showOrdersList(chatId, role, username, mode, page, listMsgId);
        await bot.answerCallbackQuery(q.id);
        return;
      }

      if (data.startsWith("view_")) {
        const parts = data.split("_"); // view_orderId_mode_page
        const orderId = parts[1];
        const mode = parts[2];
        const page = Number(parts[3] || 1);
        await showOrderDetails(chatId, role, username, orderId, mode, page, listMsgId);
        await bot.answerCallbackQuery(q.id);
        return;
      }

      if (data.startsWith("back_")) {
        const parts = data.split("_"); // back_mode_page
        const mode = parts[1];
        const page = Number(parts[2] || 1);
        await showOrdersList(chatId, role, username, mode, page, listMsgId);
        await bot.answerCallbackQuery(q.id);
        return;
      }
    }
  } catch (e) {
    console.error("[PANEL PAGINATION ERROR]", e?.message || e);
    try { await bot.answerCallbackQuery(q.id); } catch {}
    // не return, пусть дальше твой старый код обработает другие callbacks
  }


  console.log(`[CALLBACK] Пользователь @${username} (${fromId}) нажал: ${data}`);

    // ===== SUPPORT CALLBACKS =====
  if (data === "faq") {
    await bot.sendMessage(
      fromId,
      "❓ Частые вопросы\n\n" +
        "• Где мой заказ? — «Мои заказы» → «Активные»\n" +
        "• Как отменить? — кнопка «Отменить заказ» доступна 20 минут\n" +
        "• Не пришло сообщение? — нажмите /start и повторите\n\n" +
        "Если не помогло — нажмите «Написать в поддержку»."
    );
    return bot.answerCallbackQuery(q.id);
  }

  if (!username) {
    console.log("У пользователя нет username");
    return bot.answerCallbackQuery(q.id, {
      text: "У вас нет username",
      show_alert: true
    });
  }

  // ===== 📎 СКОПИРОВАТЬ РЕФЕРАЛЬНУЮ ССЫЛКУ =====
if (data === "copy_ref_link") {
  const uname = q.from.username;

  if (!uname) {
    await bot.answerCallbackQuery(q.id, {
      text: "У вас не установлен username в Telegram",
      show_alert: true
    });
    return;
  }

  const refLink = `https://t.me/crazydecloud_bot?start=ref_${uname}`;


  await bot.answerCallbackQuery(q.id, {
    text: "Ссылка готова 👇",
    show_alert: false
  });

  await bot.sendMessage(
  fromId,
  `🔗 *Ваша реферальная ссылка:*\n\n${refLink}\n\n📎 *Зажмите ссылку и выберите «Копировать»*`,
  { parse_mode: "Markdown" }
);


  return;
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

  if (!review || String(review.orderId) !== orderId) {
    return bot.answerCallbackQuery(q.id, {
      text: "Отзыв уже обработан или устарел",
      show_alert: true
    });
  }

  // если отзыв уже есть в БД — просто выходим (чтобы не было дублей)
  const already = await hasReviewForOrder(orderId);
  if (already) {
    waitingReview.delete(fromId);
    await bot.sendMessage(fromId, "Ок ✅ Отзыв по этому заказу уже был сохранён ранее.");
    return bot.answerCallbackQuery(q.id, { text: "Готово" });
  }

  const courierNick = String(review.courier || "—").replace(/^@/, "");
  const clientNick = String(review.client || "—").replace(/^@/, "");

  // ✅ АНТИСПАМ админу: шлём лог 1 раз
  const logType = "review_skip_notify";
  const logUser = clientNick || "unknown";
  const logDetails = `order:${orderId}:courier:@${courierNick}:rating:${review.rating ?? "none"}`;

  let canNotifyAdmin = true;
  try {
    const exists = await hasReferralLog(logType, logUser, logDetails);
    if (exists) canNotifyAdmin = false;
    else await addReferralLog(logType, logUser, logDetails);
  } catch (e) {
    // если лог не записался — всё равно лучше уведомить админа
    console.error("[review_skip_notify log error]", e?.message || e);
  }

  // Если успел выбрать оценку — сохраняем только рейтинг (без текста)
  if (review.rating !== null) {
    try {
      const now = new Date().toISOString().slice(0, 19).replace("T", " ");
      await db.execute(
        `INSERT INTO reviews (order_id, client_username, courier_username, rating, review_text, created_at)
         VALUES (?, ?, ?, ?, ?, ?)`,
        [orderId, clientNick, courierNick, Number(review.rating), null, now]
      );
    } catch (e) {
      console.error("[skip_review] save rating only error:", e?.message || e);
    }
  }

  // ✅ Уведомление админу ВСЕГДА
  if (ADMIN_ID && canNotifyAdmin) {
    try {
      const adminMsg =
        review.rating === null
          ? `⚠️ Клиент @${escapeMarkdownV2(clientNick)} отказался оставлять отзыв по заказу №${escapeMarkdownV2(orderId)} (без оценки).`
          : `⚠️ Клиент @${escapeMarkdownV2(clientNick)} отказался писать отзыв по заказу №${escapeMarkdownV2(orderId)}, но поставил оценку ${escapeMarkdownV2(String(review.rating))}\/5.\nКурьер: @${escapeMarkdownV2(courierNick)}`;

      await bot.sendMessage(ADMIN_ID, adminMsg, { parse_mode: "MarkdownV2" });
    } catch (e) {
      console.error("[skip_review] notify admin error:", e?.message || e);
    }
  }

  waitingReview.delete(fromId);

  // ответ клиенту
  if (review.rating === null) {
    await bot.sendMessage(fromId, "Ок ✅ Отзыв пропущен.");
  } else {
    await bot.sendMessage(fromId, "Ок ✅ Спасибо! Оценка сохранена, отзыв пропущен.");
  }

  return bot.answerCallbackQuery(q.id, { text: "Готово" });
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
  `⭐ Оценка: ${escapeMarkdownV2(String(r.rating))}\/5\n` +
  `📝 Отзыв: ${escapeMarkdownV2(r.review_text || "—")}\n` +
  `📅 Дата: ${escapeMarkdownV2(new Date(r.created_at).toLocaleString("ru-RU"))}`
).join("\n\n——————————————\n\n");




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

// ================== ADMIN DELETE ORDER (confirmed) ==================
if (data.startsWith("admin_delete_confirm_") && fromId === ADMIN_ID) {
  const orderId = data.split("_")[3];

  const order = await getOrderById(orderId);
  if (!order) {
    await bot.answerCallbackQuery(q.id, {
      text: "Заказ уже удалён",
      show_alert: true
    });
    return;
  }

  // ✅ ВОТ ЭТО ДОБАВЬ (до удаления order из БД)
  await refundReservedBonusIfNeeded(order);

  // 1) удалить сообщения заказа у всех
  const msgs = await getOrderMessages(orderId);
  for (const m of msgs) {
    try {
      await bot.deleteMessage(m.chat_id, m.message_id);
    } catch (e) {}
  }

  // 2) очистить таблицу сообщений
  await db.execute("DELETE FROM order_messages WHERE order_id=?", [orderId]);

  // 3) удалить сам заказ
  await db.execute("DELETE FROM orders WHERE id=?", [orderId]);

  // 4) лог (если нужно)
  try {
    await db.execute(
      `INSERT INTO referral_logs (type, username, details, created_at)
       VALUES ('admin_delete', ?, ?, NOW())`,
      [ADMIN_USERNAME, `Админ удалил заказ №${orderId}`]
    );
  } catch (e) {}

  // (не обязательно) заменить текст сообщения с подтверждением
  try {
    if (q.message?.chat?.id && q.message?.message_id) {
      await bot.editMessageText(`✅ Заказ №${orderId} удалён`, {
        chat_id: q.message.chat.id,
        message_id: q.message.message_id
      });
    }
  } catch (e) {}

  await bot.answerCallbackQuery(q.id, { text: "Удалено" });

  // (опционально) уведомление админу отдельным сообщением
  // await bot.sendMessage(ADMIN_ID, `🗑 Заказ №${orderId} удалён администратором`);

  return;
}

// ================== ADMIN DELETE ORDER (cancel) ==================
if (data.startsWith("admin_delete_cancel_") && fromId === ADMIN_ID) {
  await bot.answerCallbackQuery(q.id, { text: "Удаление отменено" });

  try {
    if (q.message?.chat?.id && q.message?.message_id) {
      await bot.editMessageText("❌ Удаление отменено", {
        chat_id: q.message.chat.id,
        message_id: q.message.message_id
      });
    }
  } catch (e) {}

  return;
}

// ================== ADMIN DELETE ORDER (ask confirm) ==================
if (data.startsWith("admin_delete_") && fromId === ADMIN_ID) {
  if (data.startsWith("admin_delete_confirm_") || data.startsWith("admin_delete_cancel_")) return;

  const orderId = data.split("_")[2];

  const kb = {
    inline_keyboard: [
      [
        { text: "🗑 Да, удалить", callback_data: `admin_delete_confirm_${orderId}` },
        { text: "❌ Отмена", callback_data: `admin_delete_cancel_${orderId}` }
      ]
    ]
  };

  const text =
    `⚠️ Удаление заказа №${orderId}\n\n` +
    `Заказ будет полностью удалён:\n` +
    `• у клиента\n` +
    `• у курьеров\n` +
    `• из базы данных\n\n` +
    `Это действие необратимо.`;

  try {
    // ✅ если кнопка нажата на сообщении — редактируем его
    if (q.message?.chat?.id && q.message?.message_id) {
      await bot.editMessageText(text, {
        chat_id: q.message.chat.id,
        message_id: q.message.message_id,
        reply_markup: kb
      });
    } else {
      // ✅ запасной вариант
      await bot.sendMessage(fromId, text, { reply_markup: kb });
    }
  } catch (e) {
    // если не смогли отредактировать — шлём новым
    await bot.sendMessage(fromId, text, { reply_markup: kb });
  }

  await bot.answerCallbackQuery(q.id);
  return;
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
    String(order.courier_username || "").replace(/^@/, "") === String(username || "").replace(/^@/, "") ||
    fromId === ADMIN_ID;

  if (!isOwnerOrAdmin) {
    return bot.answerCallbackQuery(q.id, { text: "Нельзя отметить", show_alert: true });
  }

  try {
    // 1) Обновляем статус на 'delivered'
    await updateOrderStatus(orderId, "delivered", String(username || "").replace(/^@/, ""));
    const updatedOrder = await getOrderById(orderId);

// ✅ ВОТ СЮДА: закрепляем списание бонуса 3€ при delivered
try {
  if (
    String(updatedOrder.status || "") === "delivered" &&
    String(updatedOrder.discount_type || "") === "referral_bonus" &&
    Number(updatedOrder.referral_bonus_reserved_qty || 0) > 0 &&
    Number(updatedOrder.referral_bonus_spent || 0) === 0
  ) {
    await db.execute(
      "UPDATE orders SET referral_bonus_spent=1 WHERE id=?",
      [updatedOrder.id]
    );
    updatedOrder.referral_bonus_spent = 1;
  }
} catch (e) {
  console.error("[BONUS SPENT MARK ERROR]", e?.message || e);
}

    // ✅ Разблокировать рефералку покупателю после 1 delivered
try {
  const buyerUsername = updatedOrder.tgNick?.replace(/^@/, "");
  if (buyerUsername) {
    await db.execute(
      "UPDATE clients SET referrals_locked=0, eligible_referrer=1 WHERE username=?",
      [buyerUsername]
    );
  }
} catch (e) {
  console.error("[UNLOCK REFERRALS ERROR]", e?.message || e);
}


    // 2) Обновляем сообщение у всех участников
    await sendOrUpdateOrderAll(updatedOrder);

    // 3) Начисление бонуса пригласившему (ТОЛЬКО если это first_order и бонус ещё не выдавали)
    try {
      const alreadyGiven = Number(updatedOrder.referral_bonus_given || 0) === 1;
      const eligible = updatedOrder.discount_type === "first_order" && !alreadyGiven;

      if (eligible) {
        const buyerUsername = String(updatedOrder.tgNick || "").replace(/^@/, "").trim();
        if (buyerUsername) {
          const buyer = await getClient(buyerUsername);

          const referrerUsername = String(buyer?.referrer || "").replace(/^@+/, "").trim();
          if (referrerUsername) {
            // ✅ антидубль через logs (на случай гонок/повторных delivered)
            const details = `bonus_for_order:${updatedOrder.id}:buyer:@${buyerUsername}`;
            const alreadyLogged = await hasReferralLog("ref_bonus_given", referrerUsername, details);

            if (!alreadyLogged) {
              // начисляем 1 бонус пригласившему
              await db.execute(
                "UPDATE clients SET referral_bonus_available = referral_bonus_available + 1 WHERE username=?",
                [referrerUsername]
              );

              // помечаем заказ, что бонус уже выдан
              await db.execute(
                "UPDATE orders SET referral_bonus_given = 1 WHERE id=?",
                [updatedOrder.id]
              );

              // пишем лог
              await addReferralLog("ref_bonus_given", referrerUsername, details);

              console.log(`[REFERRAL BONUS] +1 для @${referrerUsername} за заказ @${buyerUsername}`);

              // уведомление пригласившему
              await notifyReferrer(
                referrerUsername,
                `✅ Друг @${buyerUsername} сделал первый заказ.\nСкидка 2€ применится автоматически к вашему следующему заказу.`
              );
            } else {
              console.log(`[REFERRAL BONUS] SKIP duplicate for @${referrerUsername} | ${details}`);
            }
          }
        }
      }
    } catch (e) {
      console.error("[REFERRAL BONUS ERROR]", e?.message || e);
    }

    // 4) Просим отзыв (1 раз)
    try {
      await askForReview(updatedOrder);
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
  const oldCourierUsername = order.courier_username ? String(order.courier_username).replace(/^@/, "") : null
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

    await refundReservedBonusIfNeeded(updatedOrder)

    await sendOrUpdateOrderAll(updatedOrder);

    // ✅ Уведомление админа всегда
try {
  await bot.sendMessage(
    ADMIN_ID,
    `❌ Клиент отменил заказ №${orderId} (в течение 20 минут).`
  );
} catch (e) {
  console.error("[ERROR] notify admin cancel:", e?.message || e);
}

// ✅ Уведомление курьера, который взял
if (oldCourierUsername) {
  try {
    // ищем chat_id курьера
    let courierChatId = COURIERS[oldCourierUsername];

    if (!courierChatId) {
      const [rows] = await db.execute(
        "SELECT chat_id FROM couriers WHERE username=? LIMIT 1",
        [oldCourierUsername]
      );
      courierChatId = rows[0]?.chat_id;
    }

    if (courierChatId) {
      await bot.sendMessage(
        courierChatId,
        `⚠️ Заказ №${orderId} отменён клиентом.`
      );
    }
  } catch (e) {
    console.error("[ERROR] notify courier cancel:", e?.message || e);
  }
}
    broadcastStock();

    return bot.answerCallbackQuery(q.id, { text: "✅ Заказ успешно отменен" });
  } catch (err) {
    console.error(`Ошибка при cancel для заказа ${orderId}:`, err.message);
    return bot.answerCallbackQuery(q.id, { text: "Ошибка при отмене", show_alert: true });
  }
}

} catch (err) {
    console.error("[CALLBACK ERROR]", err?.message || err);

    // ❗ ОБЯЗАТЕЛЬНО оборачиваем answerCallbackQuery
    try {
      await bot.answerCallbackQuery(q.id, {
        text: "⏱ Действие устарело",
        show_alert: false
      });
    } catch {}
  }
});

// ================== /start ==================
bot.onText(/\/start(?:\s+(.+))?/, async (msg, match) => {
  const id = msg.from.id;
  const username = msg.from.username; // ❗ только реальный username
  const first_name = msg.from.first_name || "";
  const ref = match?.[1]; // например "ref_username"

  // 🚫 ЕСЛИ НЕТ USERNAME — СТОП
  if (!username) {
    await bot.sendMessage(
      id,
      "❗ Для работы с ботом нужен Telegram-ник (username)\n\n" +
        "Он используется для:\n" +
        "• оформления заказов\n" +
        "• реферальной программы\n" +
        "• связи с курьером\n\n" +
        "👉 Как включить ник:\n" +
        "Telegram → Настройки → Имя пользователя\n\n" +
        "После установки ника нажмите /start"
    );
    return;
  }

  console.log(` /start от @${username} (id: ${id}), имя: ${first_name}`);

  try {
    // Проверяем, новый ли пользователь
   const [existing] = await db.execute(
  "SELECT id FROM clients WHERE chat_id=? LIMIT 1",
  [id]
);
const isNew = existing.length === 0;


    // Сохраняем/обновляем клиента
    await addOrUpdateClient(username, first_name, id);
    console.log(`Клиент @${username} добавлен/обновлён в базе`);

    // ===== Если это курьер — обновим chat_id (как у тебя было) =====
    if (isCourier(username)) {
      await db.execute(
        `INSERT INTO couriers (username, chat_id)
         VALUES (?, ?)
         ON DUPLICATE KEY UPDATE chat_id = VALUES(chat_id)`,
        [username, id]
      );
      COURIERS = await getCouriers();
      console.log(`Курьер @${username} добавлен/обновлён, chat_id: ${id}`);
    }

    // ===== РЕФЕРАЛ (привязываем только если реально принят) =====
    let referralAccepted = false;

    if (isNew && ref && ref.startsWith("ref_")) {
      const referrer = ref.replace("ref_", "").replace(/^@/, "").trim();
      const me = String(username || "").replace(/^@/, "").trim();

      // самореф
      if (referrer === me) {
        await addReferralLog("self_referral", me, "Попытка самореферала");
      } else {
        const refClient = await getClient(referrer);
        const eligible = refClient && (await isEligibleReferrer(referrer));

        if (!eligible) {
          await addReferralLog(
            "referrer_not_eligible",
            referrer || "unknown",
            `Попытка рефералки для @${me} (реферер без delivered)`
          );
        } else {
          // привязываем реферера
          await db.execute(
            "UPDATE clients SET referrer=? WHERE username=?",
            [referrer, me]
          );

          // новый пришёл по рефке -> блокируем ему приглашения, пока не будет delivered
          await db.execute(
            "UPDATE clients SET referrals_locked=1 WHERE username=?",
            [me]
          );

          referralAccepted = true;

          // уведомление рефереру 1 раз
          try {
            const details = `friend_started:@${me}`;
            const already = await hasReferralLog("ref_start_notify", referrer, details);

            if (!already) {
              await addReferralLog("ref_start_notify", referrer, details);
              await notifyReferrer(
                referrer,
                `👋 Ваш друг @${me} запустил бота по вашей ссылке.\n` +
                  `Если он сделает первый заказ, вам будет доступна скидка 2€ (автоматически).`
              );
            }
          } catch (e) {
            console.error("[REF START NOTIFY ERROR]", e?.message || e);
          }
        }
      }
    }

    // ===== ТЕКСТ ПРИВЕТСТВИЯ (1 основное сообщение) =====
 let welcomeText = [
  "👋 Добро пожаловать в *CRAZY CLOUD!*",
  "",
  "🛒 Оформляйте заказ прямо в боте",
  "🚚 Доставка по вашему городу в день заказа",
  "⭐ Отзывы клиентов: [crazy_cloud_reviews](https://t.me/crazy_cloud_reviews)",
  "",
  "Чтобы оформить заказ, нажмите",
  "КУПИТЬ ЖИЖУ 👇"
].join("\n");



// ===== ВЫБОР КЛАВИАТУРЫ (ОДИН РАЗ, БЕЗ ДУБЛЕЙ) =====
const u = String(username || "").replace(/^@+/, "").trim();
const adminU = String(ADMIN_USERNAME || "").replace(/^@+/, "").trim();

const isAdmin = Number(id) === Number(ADMIN_ID) || (u && adminU && u === adminU);
const isC = isCourier(u);

// клиент по умолчанию
let replyMarkup = mainMenuKeyboard;

if (isAdmin) {
  welcomeText += "\n\nПанель администратора и Панель курьера доступны через кнопки ниже.";
  replyMarkup = adminStartKeyboard;
} else if (isC) {
  welcomeText += "\n\nПанель курьера доступна через кнопки ниже.";
  replyMarkup = courierStartKeyboard;
}

    // ✅ 1️⃣ Отправляем ОДНО стартовое сообщение
    await bot.sendMessage(id, welcomeText, {
      parse_mode: "Markdown",
      reply_markup: replyMarkup
    });

    // ✅ 2️⃣ Реф-сообщение только если реферал реально принят
    if (referralAccepted) {
      await bot.sendMessage(id, referralText, { parse_mode: "Markdown" });
    }

    // ===== Уведомление админу о новом пользователе =====
    if (isNew && ADMIN_ID) {
      try {
        await bot.sendMessage(
          ADMIN_ID,
          `🆕 *Новый пользователь*\n\nИмя: *${escapeMarkdown(first_name) || "—"}*\nЛогин: @${escapeMarkdown(username)}\nChat ID: \`${id}\``,
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

// ===== Основной обработчик сообщений =====
bot.on("message", async (msg) => {
  try {
  const id = msg.from.id;
  const username = msg.from.username; // username должен быть для курьеров
  const first_name = msg.from.first_name || "";

  if (!msg.text) return;
  const text = msg.text.trim();
  // ⛔️ чтобы /start обрабатывался только bot.onText(/\/start/)
if (text.startsWith("/start")) return;


  // ✅ чтобы кнопки меню не перехватывались режимами "ожидания"
if (Number(id) === Number(ADMIN_ID)) {
  const adminMenuClicks = [
    "Панель курьера",
    "Панель администратора",
    "Новые заказы",
    "Взятые заказы",
    "Выполненные заказы",
    "Взятые сейчас",
    "Сводка курьеров",
    "✅ Доставлено сегодня",     // ✅ если ты переименовал кнопку
    "Активные по курьеру",
    "Выполненные по курьеру",
    "⬅️ Назад",
    "Назад",
    "⬅️ Назад в меню"
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

  // 🔥 ВАЖНО: сначала чистим ВСЕ старые сообщения заказов у админа
  for (const o of orders) {
    await clearOrderMessage(o.id, id);
  }

  // ✅ теперь шлём каждый заказ как НОВОЕ сообщение с кнопками
  for (const o of orders) {
    await sendOrUpdateOrderToChat(o, id, "admin", ADMIN_USERNAME);
  }

  return;
}



// ===== Админ: Сводка курьеров (доставленные ЗА СЕГОДНЯ + фулл инфа по каждому заказу) =====
if (text === "✅ Доставлено сегодня" && id === ADMIN_ID) {
  try {
    // ⚠️ ВАЖНО: DATE(delivered_at)=CURDATE() зависит от таймзоны MySQL.
    // Если MySQL в UTC, "сегодня" будет по UTC. Пока оставляем как у тебя раньше.

    // 1) Список курьеров
    const [couriers] = await db.execute(
      "SELECT username FROM couriers ORDER BY username ASC"
    );

    if (!couriers.length) {
      await bot.sendMessage(id, "Нет курьеров");
      return;
    }

    // 2) Все delivered за сегодня одним запросом
    const [todayOrders] = await db.execute(
      `SELECT * FROM orders
       WHERE status='delivered'
         AND delivered_at IS NOT NULL
         AND DATE(delivered_at)=CURDATE()
       ORDER BY delivered_at DESC`
    );

    // 3) Группируем по courier_username
    const byCourier = {};
    for (const o of todayOrders) {
      const c = String(o.courier_username || "").replace(/^@/, "").trim();
      if (!c) continue;
      if (!byCourier[c]) byCourier[c] = [];
      byCourier[c].push(o);
    }

    const todayStr = new Date().toLocaleDateString("ru-RU");
    await bot.sendMessage(id, `📦 Сводка курьеров за сегодня (${todayStr})`);

    // 4) Отправляем по каждому курьеру: шапка + ВСЕ заказы фулл инфой
    for (const c of couriers) {
      const courierU = String(c.username || "").replace(/^@/, "").trim();
      const list = byCourier[courierU] || [];

      await bot.sendMessage(
        id,
        `🚚 Курьер: @${courierU}\n✅ Доставлено сегодня: ${list.length}`
      );

      if (!list.length) continue;

      // фулл карточка заказа (твой buildTextForOrder)
      for (const o of list) {
        await bot.sendMessage(id, buildTextForOrder(o));
      }

      // разделитель, чтобы читаемо
      await bot.sendMessage(id, "——————————————");
    }

    return;
  } catch (err) {
    console.error("[Сводка курьеров] error:", err?.message || err);
    await bot.sendMessage(id, "Ошибка при формировании сводки курьеров");
    return;
  }
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
const now = new Date().toISOString().slice(0, 19).replace("T", " ");

const courierNick = review.courier.replace(/^@/, "");
const clientNick = review.client.replace(/^@/, "");

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

const safeReviewText = reviewText
  ? escapeMarkdownV2(reviewText)
  : "—";

await bot.sendMessage(
  ADMIN_ID,
  `Новый отзыв

Заказ: №${escapeMarkdownV2(String(review.orderId))}
Клиент: @${escapeMarkdownV2(clientNick)}
Курьер: @${escapeMarkdownV2(courierNick)}
Оценка: ${escapeMarkdownV2(String(review.rating))}\/5

Отзыв:
${safeReviewText}`,
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
  [{ text: "Взятые сейчас" }, { text: "✅ Доставлено сегодня" }],
  [{ text: "🤝 Рефералы" }, { text: "🚨 Логи рефералов" }],
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

/// ===== НАЗАД + СБРОС РЕЖИМОВ (универсально, ОДИН раз) =====
// ставь ПОСЛЕ waitingReview и ПОСЛЕ adminWaitingOrdersCourier
const backClicks = new Set(["Назад", "⬅️ Назад", "⬅️ Назад в меню"]);

if (backClicks.has(text)) {
  // сбрасываем все "режимы ожидания", чтобы меню не ломалось
  adminWaitingOrdersCourier.delete(username);
  adminWaitingBroadcast.delete(username);
  adminWaitingCourier.delete(username);

  // админ
  if (Number(id) === Number(ADMIN_ID)) {
    return bot.sendMessage(id, "Главное меню админа", {
      reply_markup: adminStartKeyboard
    });
  }

  // курьер
  if (isCourier(username)) {
    return bot.sendMessage(id, "Главное меню курьера", {
      reply_markup: courierStartKeyboard
    });
  }

  // клиент
  return bot.sendMessage(id, "Главное меню", {
    reply_markup: mainMenuKeyboard
  });
}

// ===== 💸 ПОЛУЧИТЬ СКИДКУ (подменю) =====
if (text === "💸 Получить скидку") {
  // если админ/курьер был в ожиданиях — тоже сбросим
  adminWaitingOrdersCourier.delete(username);
  adminWaitingBroadcast.delete(username);
  adminWaitingCourier.delete(username);

  return bot.sendMessage(id, "💸 Реферальная программа\n\nВыберите действие 👇", {
    reply_markup: discountMenuKeyboard
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

// ===== 💸 ПОЛУЧИТЬ СКИДКУ (ЭКРАН ОПИСАНИЯ) =====
if (text === "🔗 Моя реферальная ссылка") {
  const uname = (username || "").replace(/^@/, "");

  // ✅ ЗАЩИТА: если юзер пришёл по рефке и ещё не сделал 1 заказ — не даём пиарить рефку
  const client = await getClient(uname);
  if (client && Number(client.referrals_locked || 0) === 1) {
    await bot.sendMessage(
      id,
      "⛔️ Реферальная ссылка станет доступна после вашего первого заказа.\n" +
      "Сделайте заказ — и сможете приглашать друзей."
    );
    return;
  }

  const refLink = `https://t.me/crazydecloud_bot?start=ref_${uname}`;


const msg =
  "🎁 Скидки за друзей\n\n" +
  "1) Отправь другу свою ссылку\n" +
  "2) Друг сделает ПЕРВЫЙ заказ — он получит скидку 2€\n" +
  "3) После доставки его заказа тебе станет доступна скидка 2€ на следующий заказ\n\n" +
  "⚠️ Важно:\n" +
  "• 1 друг = 1 скидка\n" +
  "• скидки не суммируются\n\n" +
  "🔗 Твоя ссылка:\n" +
  refLink + "\n\n" +
  "📎 Зажми ссылку → «Копировать»";



  // ✅ БЕЗ inline кнопок
  await bot.sendMessage(id, msg);

  return;
}

// ===== 📊 МОИ ПРИГЛАШЁННЫЕ (БЕЗ Markdown, чтобы _ в никах не ломал сообщение) =====
if (text === "🤝 Мои приглашённые") {
  const uname = (username || "").replace(/^@/, "").trim();

  const me = await getClient(uname);
  const availableBonuses = Number(me?.referral_bonus_available || 0);

  const [rows] = await db.execute(
    `
    SELECT
      c.username AS invited,
      COUNT(o.id) AS orders_total,
      MAX(CASE WHEN o.status='delivered' THEN 1 ELSE 0 END) AS has_delivered,
      SUBSTRING_INDEX(
        GROUP_CONCAT(o.status ORDER BY o.created_at DESC SEPARATOR ','),
        ',', 1
      ) AS last_status
    FROM clients c
    LEFT JOIN orders o
      ON REPLACE(o.tgNick,'@','') = c.username
    WHERE c.referrer = ?
    GROUP BY c.username
    ORDER BY c.username ASC
    `,
    [uname]
  );

  if (!rows.length) {
    return bot.sendMessage(
      id,
      "👥 Мои приглашённые\n\n" +
      "Пока никого нет.\n" +
      "Зайди в «💸 Получить скидку» → «🔗 Моя реферальная ссылка» и отправь другу."
    );
  }

  const statusLabel = (ordersTotal, hasDelivered, lastStatus) => {
    if (!ordersTotal) return "⏳ ждём первый заказ";
    if (Number(hasDelivered) === 1) return "✅ первый заказ доставлен";

    const s = String(lastStatus || "");
    if (s === "new") return "🛒 заказ оформлен";
    if (s === "taken") return "🚚 заказ в пути";
    if (s === "canceled") return "❌ заказ отменён";
    return "🛒 заказ в обработке";
  };

  const invitedCnt = rows.length;
  const orderedCnt = rows.filter(r => Number(r.orders_total) > 0).length;
  const deliveredCnt = rows.filter(r => Number(r.has_delivered) === 1).length;

  const head =
    "👥 Мои приглашённые\n\n" +
    `💸 Доступно скидок 2€: ${availableBonuses}\n` +
    "Скидка применяется автоматически к следующему заказу.\n\n" +
    "📌 Итоги:\n" +
    `• Запустили бота: ${invitedCnt}\n` +
    `• Оформили заказ: ${orderedCnt}\n` +
    `• Доставлено: ${deliveredCnt}\n\n` +
    "📋 Список:\n";

  // Telegram лимит ~4096, шлём частями
  const MAX = 3900;
  await bot.sendMessage(id, head);

  let chunk = "";
  for (const r of rows) {
    const invited = String(r.invited || "").trim();
    const line = statusLabel(r.orders_total, r.has_delivered, r.last_status);
    const rowLine = `• @${invited} — ${line}\n`;

    if ((chunk + rowLine).length > MAX) {
      await bot.sendMessage(id, chunk);
      chunk = rowLine;
    } else {
      chunk += rowLine;
    }
  }
  if (chunk) await bot.sendMessage(id, chunk);

  return;
}







// ===== Личный кабинет (красиво, без Markdown, без слешей) =====
if (text === "👤 Личный кабинет") {
  console.log("[DEBUG] Личный кабинет нажали:", { id, username });

  try {
    const uname = (username || "").replace(/^@/, "");

    const roleLabel =
      (id === ADMIN_ID) ? "👑 Админ" :
      (isCourier(username) ? "🚚 Курьер" : "🧑 Клиент");

    // Всего заказов
    const [[{ cnt: totalOrders }]] = await db.execute(
  "SELECT COUNT(*) AS cnt FROM orders WHERE client_chat_id=?",
  [id]
);

const [[{ cnt: newCnt }]] = await db.execute(
  "SELECT COUNT(*) AS cnt FROM orders WHERE client_chat_id=? AND status='new'",
  [id]
);

const [[{ cnt: takenCnt }]] = await db.execute(
  "SELECT COUNT(*) AS cnt FROM orders WHERE client_chat_id=? AND status='taken'",
  [id]
);

const [[{ cnt: deliveredCnt }]] = await db.execute(
  "SELECT COUNT(*) AS cnt FROM orders WHERE client_chat_id=? AND status='delivered'",
  [id]
);

const [lastOrders] = await db.execute(
  "SELECT id, status, created_at FROM orders WHERE client_chat_id=? ORDER BY created_at DESC LIMIT 1",
  [id]
);

    const lastOrder = lastOrders[0];

    // Клиент из БД (у тебя уже есть getClient)
    const client = await getClient(uname);

    // Человекопонятная дата
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

    const lastActiveStr = formatRu(client?.last_active);
    const lastCreatedStr = lastOrder ? formatRu(lastOrder.created_at) : "—";

    // Собираем текст без Markdown
    const msg =
      `👤 Личный кабинет\n\n` +
      `🧑 Имя: ${client?.first_name || "—"}\n` +
      `🔗 Ник: @${uname || "—"}\n` +
      `🏷 Статус: ${roleLabel}\n\n` +
      `🧾 Всего заказов: ${totalOrders || 0}\n` +
      `🆕 Новые: ${newCnt || 0}\n` +
      `🚚 В пути: ${takenCnt || 0}\n` +
      `✅ Выполнено: ${deliveredCnt || 0}\n\n` +
      `🕒 Последняя активность: ${lastActiveStr}\n` +
      (lastOrder
        ? `📦 Последний заказ: №${lastOrder.id} (${lastOrder.status})\n` +
          `📅 Создан: ${lastCreatedStr}`
        : `📦 Последний заказ: —`);

    await bot.sendMessage(id, msg);
    return;

  } catch (err) {
    console.error("[ERROR] Личный кабинет общий:", err?.message || err);
    return bot.sendMessage(id, "Ошибка при открытии личного кабинета.");
  }
}





  // ===== Поддержка =====
if (text === "🛟 Поддержка") {
  const kb = {
    inline_keyboard: [
      [{ text: "💬 Написать в поддержку", url: "https://t.me/crazycloud_manager" }],
      [{ text: "❓ Частые вопросы", callback_data: "faq" }],
    ]
  };

  await bot.sendMessage(
    id,
    "🛟 *Поддержка*\n\n" +
      "Если есть вопрос по заказу — напишите нам, мы ответим как можно быстрее.\n\n" +
      "📌 Чтобы помочь быстрее, пришлите:\n" +
      "• номер заказа\n" +
      "• что случилось (коротко)\n" +
      "• город и время заказа",
    { parse_mode: "Markdown", reply_markup: kb }
  );
  return;
}

// ===== Менюшка =====
if (text === "🧾 Мои заказы") {
  return bot.sendMessage(id, "Что показать?", {
    reply_markup: myOrdersKeyboard
  });
}


// ===== Мои заказы: Активные (new/taken) =====
if (text === "Активные заказы") {
  const uname = (username || "").replace(/^@/, "");

 const [orders] = await db.execute(
  "SELECT * FROM orders WHERE client_chat_id=? AND status IN ('new','taken') ORDER BY created_at DESC",
  [id]
);


  if (!orders.length) return bot.sendMessage(id, "Активных заказов нет");

  for (const o of orders) {
    await clearOrderMessage(o.id, id); // чтобы прислало заново
    await sendOrUpdateOrderToChat(o, id, "client", uname);
  }
  return;
}

// ===== Мои заказы: Выполненные (delivered) =====
if (text === "Выполненные заказы") {
  const uname = (username || "").replace(/^@/, "");

  const [orders] = await db.execute(
  "SELECT * FROM orders WHERE client_chat_id=? AND status IN ('delivered','canceled') ORDER BY created_at DESC",
  [id]
);

  if (!orders.length) return bot.sendMessage(id, "Выполненных заказов нет");

  for (const o of orders) {
    await clearOrderMessage(o.id, id);
    await sendOrUpdateOrderToChat(o, id, "client", uname);
  }
  return;
}


 // ===== Панель администратора =====
if (text === "Панель администратора" && Number(id) === Number(ADMIN_ID)) {
  return bot.sendMessage(id, "Панель администратора", {
    reply_markup: adminStartKeyboard
  });
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


if (text === "🤝 Рефералы" && id === ADMIN_ID) {
  const [rows] = await db.execute(`
    SELECT
      c.username AS referrer,
      r.username AS referral
    FROM clients c
    JOIN clients r ON r.referrer = c.username
    ORDER BY c.username
  `);

  if (!rows.length) {
    return bot.sendMessage(id, "🤝 Рефералов пока нет");
  }

  const grouped = {};
  for (const r of rows) {
    if (!grouped[r.referrer]) grouped[r.referrer] = [];
    grouped[r.referrer].push(r.referral);
  }

  let msg = "🤝 Кто кого пригласил\n\n";

  for (const referrer in grouped) {
    msg += `👤 @${referrer}\n`;

    for (const ref of grouped[referrer]) {
      const [[{ cnt }]] = await db.execute(
  "SELECT COUNT(*) AS cnt FROM orders WHERE REPLACE(tgNick,'@','')=? AND status='delivered'",
  [ref]
);


      msg += cnt > 0
        ? `  ✅ @${ref} — заказ выполнен\n`
        : `  ⏳ @${ref} — без заказа\n`;
    }

    msg += "\n";
  }

  return bot.sendMessage(id, msg);
}


if (text === "🚨 Логи рефералов" && id === ADMIN_ID) {
  const [logs] = await db.execute(
    "SELECT * FROM referral_logs ORDER BY created_at DESC LIMIT 20"
  );

  if (!logs.length) {
    return bot.sendMessage(id, "🚨 Логи пока пусты");
  }

  let msg = "🚨 Последние подозрительные действия\n\n";

  for (const l of logs) {
    msg +=
      `⚠️ ${l.type}\n` +
      `👤 @${l.username}\n` +
      `📝 ${l.details}\n` +
      `🕒 ${l.created_at}\n\n`;
  }

  return bot.sendMessage(id, msg);
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
    // 1) Берем уникальные chat_id
    const [rows] = await db.execute(`
      SELECT chat_id, MAX(username) AS username
      FROM clients
      WHERE subscribed = 1 AND chat_id IS NOT NULL
      GROUP BY chat_id
    `);

    console.log(`Начало рассылки от @${username}`);
    console.log(`Уникальных получателей: ${rows.length}`);

    const limit = pLimit(5);

    const okUsers = [];
    const failUsers = [];

    // защита от дублей на всякий случай
    const sentSet = new Set();

    const tasks = rows.map(r =>
      limit(async () => {
        const chatId = r.chat_id;
        const uname = r.username ? String(r.username) : "";

        if (!chatId) return;
        if (sentSet.has(chatId)) return;
        sentSet.add(chatId);

        try {
          // ❗ РАССЫЛКА БЕЗ parse_mode
          await bot.sendMessage(chatId, msgText);

          okUsers.push(
            uname ? `@${uname.replace(/^@/, "")}` : `chat_id:${chatId}`
          );
          console.log(`✅ Отправлено: ${uname || chatId}`);
        } catch (err) {
          failUsers.push(
            uname ? `@${uname.replace(/^@/, "")}` : `chat_id:${chatId}`
          );
          console.error(`❌ Ошибка отправки ${uname || chatId}:`, err.message);
        }
      })
    );

    await Promise.all(tasks);

    adminWaitingBroadcast.delete(username);

    // ===== ОТЧЕТ АДМИНУ =====

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
      `Ошибки: ${failUsers.length}`;

    // отчёт — можно MarkdownV2
    await bot.sendMessage(
      ADMIN_ID,
      escapeMarkdownV2(header),
      { parse_mode: "MarkdownV2" }
    );

    if (okUsers.length) {
      const okChunks = makeChunks(okUsers);
      for (let i = 0; i < okChunks.length; i++) {
        await bot.sendMessage(
          ADMIN_ID,
          escapeMarkdownV2(
            `✅ Доставлено (часть ${i + 1}/${okChunks.length}):\n${okChunks[i]}`
          ),
          { parse_mode: "MarkdownV2" }
        );
      }
    }

    if (failUsers.length) {
      const failChunks = makeChunks(failUsers);
      for (let i = 0; i < failChunks.length; i++) {
        await bot.sendMessage(
          ADMIN_ID,
          escapeMarkdownV2(
            `❌ Не доставлено (часть ${i + 1}/${failChunks.length}):\n${failChunks[i]}`
          ),
          { parse_mode: "MarkdownV2" }
        );
      }
    }

  } catch (err) {
    console.error(`Ошибка при рассылке от @${username}:`, err.message);
    await bot.sendMessage(
      ADMIN_ID,
      escapeMarkdownV2(`Ошибка при рассылке:\n${err.message}`),
      { parse_mode: "MarkdownV2" }
    );
  }

  return;
}




// ===== Панель курьера =====
if (text === "Панель курьера" && (isCourier(username) || Number(id) === Number(ADMIN_ID))) {
  return bot.sendMessage(id, "Панель курьера", { reply_markup: courierPanelKeyboard });
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

// вместо 50 сообщений — показываем 1 страницу списком (10 шт)
let mode = "new";
if (text === "Взятые заказы") mode = "taken";
if (text === "Выполненные заказы") mode = "delivered";

const role = (id === ADMIN_ID) ? "admin" : "courier";

// создаём/обновляем “список” как ОДНО сообщение
const existingMsgId = PANEL_LIST_MSG.get(panelKey(id, mode)) || null;
await showOrdersList(id, role, username, mode, 1, existingMsgId);
return;
} // закрыли IF

  } catch (e) {
    console.error("[MESSAGE HANDLER ERROR]", e?.message || e, e);

    // Мягко ответим пользователю (чтобы не было тишины)
    try {
      await bot.sendMessage(msg.from.id, "⚠️ Ошибка. Попробуйте ещё раз.");
    } catch {}
  }

}); // ✅ закрыли bot.on("message", async (msg) => { ... })

// ================= Express / WebSocket =================
const app = express();

// ✅ CORS для GitHub Pages (mini app)
app.use(cors({
  origin: [
    "https://cn4tzwpqvg-ops.github.io",
    "https://cn4tzwpqvg-ops.github.io/crazycloud"
  ],
  methods: ["GET", "POST", "OPTIONS"],
  allowedHeaders: ["Content-Type"]
}));

app.use(express.json());

// ✅ health-check endpoints
app.get("/", (req, res) => res.status(200).send("OK"));
app.get("/health", (req, res) => res.status(200).json({ ok: true }));

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
  let reservedBonusQty = 0;       // сколько бонусов зарезервировали
let reservedBonusUser = "";     // кому резервировали (username)
  try {
    let {
  tgNick,
  city,
  delivery,
  payment,
  orderText,
  date,
  time,
  client_chat_id,
  tgUser
} = req.body;

// ✅ username берём либо из tgNick (если фронт прислал), либо из Telegram WebApp user.username
const rawUsername = tgNick || tgUser?.username;

// ✅ если сайт не прислал client_chat_id — берём из Telegram WebApp user.id
if (!client_chat_id && tgUser?.id) {
  client_chat_id = tgUser.id;
}

// ✅ приводим к числу (Telegram id — число)
const clientChatIdNum = client_chat_id ? Number(client_chat_id) : null;

console.log("[DEBUG api body]", req.body);
console.log(
  "[DEBUG api client_chat_id FIXED]",
  client_chat_id,
  "=>",
  clientChatIdNum,
  "type:",
  typeof clientChatIdNum
);

// ===== ПРОВЕРКА ВХОДНЫХ ДАННЫХ =====
if (!rawUsername || !orderText) {
  return res.status(400).json({
    success: false,
    error: "USERNAME_REQUIRED"
  });
}

const cleanUsername = String(rawUsername).replace(/^@+/, "").trim();

// 1) username обязателен
if (!cleanUsername) {
  return res.status(400).json({
    success: false,
    error: "USERNAME_REQUIRED"
  });
}

// 2) Telegram username: 3..32, латиница/цифры/_
if (!/^[a-zA-Z0-9_]{3,32}$/.test(cleanUsername)) {
  return res.status(400).json({
    success: false,
    error: "INVALID_USERNAME"
  });
}

console.log(`[API] Новый заказ от @${cleanUsername}`);

// ✅ 3) гарантируем что клиент есть в БД ДО расчёта скидок
await addOrUpdateClient(cleanUsername, tgUser?.first_name || "", clientChatIdNum);


// 4) получаем клиента уже гарантированно
const client = await getClient(cleanUsername);


// ===== ЦЕНА И СКИДКИ =====
let originalPrice = 15;
let finalPrice = 15;
let discountType = null;


// ✅ если есть активный заказ (new/taken) — скидки НЕ применяем, второй заказ только по 15€
const [[activeOrder]] = await db.execute(
  `SELECT id FROM orders
   WHERE REPLACE(tgNick,'@','')=?
     AND status IN ('new','taken')
   LIMIT 1`,
  [cleanUsername]
);
const hasActive = !!activeOrder?.id;

// считаем сколько заказов уже было (без canceled)
const [[{ cnt: ordersCount }]] = await db.execute(
  `SELECT COUNT(*) AS cnt
   FROM orders
   WHERE REPLACE(tgNick,'@','')=?
     AND status <> 'canceled'`,
  [cleanUsername]
);

// 🔒 Если уже есть активный заказ — никаких скидок, без резерва бонусов
if (hasActive) {
  finalPrice = 15;
  discountType = null;
  reservedBonusQty = 0;
} else {
  // 🟢 ПЕРВЫЙ ЗАКАЗ ПО РЕФЕРАЛКЕ → -2€ (15 -> 13)
  if (ordersCount === 0 && client?.referrer) {
    const okRef = await isEligibleReferrer(client.referrer);
    if (okRef) {
      finalPrice = 13;
      discountType = "first_order";
    } else {
      discountType = null;
      finalPrice = 15;
    }
  }

  // 🟢 НЕ ПЕРВЫЙ, НО ЕСТЬ РЕФ-БОНУС → -2€ (15 -> 13) + резервируем 1 бонус
 else if (Number(client?.referral_bonus_available || 0) > 0) {
  finalPrice = 13;
  discountType = "referral_bonus";
  reservedBonusQty = 1;

    const [resv] = await db.execute(
      "UPDATE clients SET referral_bonus_available = referral_bonus_available - ? WHERE username=? AND referral_bonus_available >= ?",
      [reservedBonusQty, cleanUsername, reservedBonusQty]
    );

    if (resv.affectedRows !== 1) {
      finalPrice = 15;
      discountType = null;
      reservedBonusQty = 0;
    } else {
      reservedBonusUser = cleanUsername; // ✅ чтобы catch смог вернуть

      await db.execute(
        "INSERT INTO referral_logs (type, username, details, created_at) VALUES (?, ?, ?, NOW())",
        ["reserve_bonus", cleanUsername, "Зарезервирована скидка 2€ (реферальный бонус)"]
      );
    }
  }
}

console.log("[PRICE]", {
  user: cleanUsername,
  originalPrice,
  finalPrice,
  discountType,
  reservedBonusQty,
  hasActive,
  ordersCount
});

// ===== ИТОГОВОЕ КОЛИЧЕСТВО И ИТОГОВАЯ СУММА (если в заказе 2+ шт) =====
const qtyMatches = String(orderText || "").match(/×\s*(\d+)\s*шт/gi) || [];
let totalQty = 0;

for (const m of qtyMatches) {
  const mm = m.match(/×\s*(\d+)\s*шт/i);
  if (mm && mm[1]) totalQty += Number(mm[1]) || 0;
}

// если вдруг не распарсилось — считаем 1
if (!totalQty || totalQty < 1) totalQty = 1;

// originalPrice / finalPrice у тебя — это ЦЕНА ЗА 1 ШТ
const originalTotal = Number(originalPrice || 15) * totalQty;
const finalTotal = Number(finalPrice || 15) * totalQty;


// ✅ уведомление пригласившему: друг оформил первый заказ (1 раз)
try {
  if (discountType === "first_order" && client?.referrer) {
    const referrerUsername = String(client.referrer).replace(/^@+/, "").trim();
    const details = `friend_order_created:@${cleanUsername}`;

    const already = await hasReferralLog("ref_order_notify", referrerUsername, details);
    if (!already) {
      await addReferralLog("ref_order_notify", referrerUsername, details);

      await notifyReferrer(
        referrerUsername,
        `🛒 Ваш друг @${cleanUsername} оформил первый заказ.\n` +
          `Скидка 2€ применена.`
      );
    }
  }
} catch (e) {
  console.error("[REF ORDER NOTIFY ERROR]", e?.message || e);
}

    // ===== ПРОВЕРКА БАНА =====
    let banned = false;

    if (clientChatIdNum) {
      const [rows] = await db.execute(
        "SELECT banned FROM clients WHERE chat_id = ? LIMIT 1",
        [clientChatIdNum]
      );
      if (rows.length && Number(rows[0].banned) === 1) banned = true;
    }

    if (!banned) {
      const [rows2] = await db.execute(
        "SELECT banned FROM clients WHERE username = ? LIMIT 1",
        [cleanUsername]
      );
      if (rows2.length && Number(rows2[0].banned) === 1) banned = true;
    }

    if (banned) {
      console.log(
        `⛔ Заблокированный пользователь ${cleanUsername} (${clientChatIdNum || "no chat_id"})`
      );

      // ✅ если был резерв бонуса — вернём сразу (на всякий)
      if (reservedBonusQty > 0) {
        await db.execute(
          "UPDATE clients SET referral_bonus_available = referral_bonus_available + ? WHERE username=?",
          [reservedBonusQty, cleanUsername]
        );
        await db.execute(
          "INSERT INTO referral_logs (type, username, details, created_at) VALUES (?, ?, ?, NOW())",
          ["bonus_return_banned", cleanUsername, "Возврат зарезервированной скидки 2€ (пользователь забанен)"]
        );
      }

      return res.json({
        success: false,
        error: "USER_BANNED",
        message: "Вы заблокированы и не можете создавать заказы"
      });
    }

    // ===== Всегда создаём новый заказ =====
    const id = await generateOrderId();
    console.log(`Присвоен новый ID заказа: ${id}`);

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
      client_chat_id: clientChatIdNum,
      original_price: originalTotal,
      final_price: finalTotal,
      discount_type: discountType,

      // ✅ для возврата при отмене/удалении
      referral_bonus_reserved_qty: reservedBonusQty,
      referral_bonus_spent: 0
    };

    // ===== Добавляем заказ в базу =====
    await addOrder(order);
    console.log(`Заказ ${id} добавлен в базу`);

    // ✅ СТРАХОВКА: гарантируем client_chat_id у заказа (как у тебя)
   if (clientChatIdNum) {
  await db.execute(
    "UPDATE orders SET client_chat_id=? WHERE id=? AND (client_chat_id IS NULL OR client_chat_id=0)",
    [clientChatIdNum, id]
  );
} else {
  console.log(`Заказ ${id} без client_chat_id (сайт/вебапп не прислал)`);
}


    // ===== Получаем заказ из базы =====
    const updated = await getOrderById(id);

    // ✅ тест: принудительно слать новым сообщением всем (сброс message_id)
    await clearOrderMessage(updated.id, ADMIN_ID);

    // ===== Отправляем уведомления в Telegram =====
    await sendOrUpdateOrderAll(updated);
    console.log(`Уведомления отправлены для заказа ${id}`);

    // ===== WebSocket: обновление stock =====
    broadcastStock();
    console.log(`WebSocket: отправлено обновление stock`);

    return res.json({ success: true, orderId: id });
 } catch (err) {
  console.error("Ошибка при обработке /api/send-order:", err);

  // ✅ ВОТ ЭТО ДОБАВЬ: возврат зарезервированного бонуса, если упали после резерва
  try {
    if (reservedBonusQty > 0 && reservedBonusUser) {
      await db.execute(
        "UPDATE clients SET referral_bonus_available = referral_bonus_available + ? WHERE username=?",
        [reservedBonusQty, reservedBonusUser]
      );

      await db.execute(
        "INSERT INTO referral_logs (type, username, details, created_at) VALUES (?, ?, ?, NOW())",
        ["bonus_return_error", reservedBonusUser, "Возврат зарезервированной скидки 2€ из-за ошибки API"]
      );
    }
  } catch (e) {
    console.error("[BONUS RETURN IN CATCH ERROR]", e?.message || e);
  }

  return res.status(500).json({ success: false, error: "Внутренняя ошибка сервера" });
}
});



// ================= API: узнать цену/скидку (без резерва бонусов) =================
app.post("/api/price-info", async (req, res) => {
  try {
    const body = req.body || {};
const tgNick = body.tgNick || body.tgUser?.username;  // ✅ добавили fallback


    // 1) Без tgNick — значит Mini App открыт вне Telegram / нет username
    if (!tgNick) {
      return res.json({
        ok: false,
        finalPrice: 15,
        discountType: null,
        error: "USERNAME_REQUIRED"
      });
    }

    // 2) Нормализуем и валидируем username
    const cleanUsername = String(tgNick).replace(/^@+/, "").trim();

    // Telegram username: 3..32, латиница/цифры/подчеркивание
    if (!/^[a-zA-Z0-9_]{3,32}$/.test(cleanUsername)) {
      return res.json({
        ok: false,
        finalPrice: 15,
        discountType: null,
        error: "INVALID_USERNAME"
      });
    }

    // 3) Достаём клиента (может быть null)
    const client = await getClient(cleanUsername);

    // 4) Активный заказ (new/taken)? тогда скидки не показываем
    const activeRows = await db.execute(
      "SELECT id FROM orders WHERE REPLACE(tgNick,'@','')=? AND status IN ('new','taken') LIMIT 1",
      [cleanUsername]
    );
    const activeOrder = (activeRows && activeRows[0] && activeRows[0][0]) ? activeRows[0][0] : null;
    const hasActive = !!(activeOrder && activeOrder.id);

    // 5) Сколько заказов было (без canceled)
    const cntRows = await db.execute(
      "SELECT COUNT(*) AS cnt FROM orders WHERE REPLACE(tgNick,'@','')=? AND status <> 'canceled'",
      [cleanUsername]
    );
    const ordersCount =
      (cntRows && cntRows[0] && cntRows[0][0] && typeof cntRows[0][0].cnt !== "undefined")
        ? Number(cntRows[0][0].cnt)
        : 0;

    var originalPrice = 15;
    var finalPrice = 15;
    var discountType = null;

    if (!hasActive) {
      // 6) Первый заказ по рефке → 13 (только если реферер eligible)
      if (ordersCount === 0 && client && client.referrer) {
        const okRef = await isEligibleReferrer(client.referrer);
        if (okRef) {
          finalPrice = 13;
          discountType = "first_order";
        }
      }
      // 7) Реф-бонусы → 13 (если есть доступные бонусы)
      else if (client && Number(client.referral_bonus_available || 0) > 0) {
        finalPrice = 13;
        discountType = "referral_bonus";
      }
    }

    return res.json({
      ok: true,
      originalPrice: originalPrice,
      finalPrice: finalPrice,
      discountType: discountType,
      hasActive: hasActive,
      ordersCount: ordersCount
    });
  } catch (e) {
    console.error("[/api/price-info] error:", e && e.message ? e.message : e);
    return res.status(500).json({
      ok: false,
      finalPrice: 15,
      discountType: null,
      error: "SERVER_ERROR"
    });
  }
});


process.on("unhandledRejection", (reason) => {
  console.error("[unhandledRejection]", reason);
});

process.on("uncaughtException", (err) => {
  console.error("[uncaughtException]", err);
});




// ================= Запуск сервера =================
server.listen(PORT, HOST, () => {
  console.log(`Server running on ${HOST}:${PORT}`);
});
