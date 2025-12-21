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
      const jobId = payload.jobId || crypto.randomUUID();

      console.log(`⚙️ Processando vaga ${jobId}`);

      /* ===============================
         Chamada API NLP
      ================================ */

      const response = await fetch(NLP_API_URL, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          job_description: rawText,
        }),
      });

      if (!response.ok) {
        throw new Error(
          `Erro NLP API: ${response.status} ${response.statusText}`
        );
      }

      const nlpResult = await response.json();

      console.log(`🧠 NLP concluído para ${jobId}`);

      /* ===============================
         Persistência em cache
      ================================ */

      const cache = loadCache();

      cache.push({
        id: jobId,
        batchId: getBatchId(),
        createdAt: new Date().toISOString(),
        status: "ready",
        originalText: rawText,
        extracted: nlpResult.extracted_data ?? nlpResult,
      });

      saveCache(cache);

      /* ===============================
         ACK FINAL
      ================================ */

      channel.ack(msg);
      console.log(`✅ Vaga ${jobId} salva e confirmada`);
    } catch (err) {
      console.error("❌ Erro no processamento:", err.message);

      // descarta a mensagem (não reprocessa)
      channel.nack(msg, false, false);
    }
  });
}

startWorker().catch((err) => {
  console.error("❌ Falha ao iniciar worker:", err);
});
