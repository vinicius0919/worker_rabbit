const amqp = require("amqplib");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
require("dotenv").config();

const RABBIT_URL = process.env.RABBIT_URL;
const QUEUE = process.env.QUEUE_JOBS || "jobs";
const NLP_API_URL = process.env.NLP_API_URL;

const CACHE_FILE = path.resolve("cache.json");

/* ===============================
   Cache helpers
================================ */

function loadCache() {
  if (!fs.existsSync(CACHE_FILE)) return [];
  return JSON.parse(fs.readFileSync(CACHE_FILE, "utf-8"));
}

function saveCache(data) {
  fs.writeFileSync(CACHE_FILE, JSON.stringify(data, null, 2));
}

function getBatchId() {
  return new Date().toISOString().slice(0, 10); // YYYY-MM-DD
}

async function processJob(rawText) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 30000);

  try {
    const response = await fetch(
      NLP_API_URL,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ job_description: rawText }),
        signal: controller.signal,
      }
    );

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    return await response.json();
  } finally {
    clearTimeout(timeout);
  }
}

/* ===============================
   Worker
================================ */

async function startWorker() {
  const conn = await amqp.connect(RABBIT_URL);
  const channel = await conn.createChannel();

  await channel.assertQueue(QUEUE, { durable: true });
  channel.prefetch(1);

  console.log("🟢 Worker aguardando vagas...");

  channel.consume(QUEUE, async (msg) => {
    if (!msg) return;

    try {
      const payload = JSON.parse(msg.content.toString());
      const rawText = payload.job;

      const result = await processJob(rawText);

      // salva no cache
      const cache = loadCache();
      cache.push({
        id: payload.jobId,
        batchId: getBatchId(),
        createdAt: new Date().toISOString(),
        result,
        status: "ready",
      });
      saveCache(cache);

      channel.ack(msg); // 👈 SOMENTE AQUI
      console.log("✅ Processado com sucesso");
    } catch (err) {
      console.error("❌ Erro:", err.message);
      channel.nack(msg, false, false); // descarta ou manda pra DLQ
    }
  });
}

startWorker().catch((err) => {
  console.error("❌ Falha ao iniciar worker:", err);
});
