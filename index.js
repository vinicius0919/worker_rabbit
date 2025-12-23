const amqp = require("amqplib");
const crypto = require("crypto");
require("dotenv").config();

const {
  RABBIT_URL,
  QUEUE_JOBS = "jobs",
  QUEUE_RESULTS = "job_results",
  NLP_API_URL,
} = process.env;

/* ===============================
   NLP processing
================================ */

async function processJob(rawText) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 30000);

  try {
    const res = await fetch(NLP_API_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ job_description: rawText }),
      signal: controller.signal,
    });

    if (!res.ok) {
      throw new Error(`NLP API HTTP ${res.status}`);
    }

    return await res.json();
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

  await channel.assertQueue(QUEUE_JOBS, { durable: true });
  await channel.assertQueue(QUEUE_RESULTS, { durable: true });

  channel.prefetch(1);

  console.log("🟢 Worker NLP aguardando vagas...");

  channel.consume(QUEUE_JOBS, async (msg) => {
    if (!msg) return;

    try {
      const payload = JSON.parse(msg.content.toString());

      const jobId = payload.jobId || crypto.randomUUID();
      const rawText = payload.job;

      console.log(`⚙️ Processando vaga ${jobId}`);

      const nlpResult = await processJob(rawText);

      const resultPayload = {
        id: jobId,
        processedAt: new Date().toISOString(),
        originalText: rawText,
        extracted: nlpResult.extracted_data ?? nlpResult,
      };

      channel.sendToQueue(
        QUEUE_RESULTS,
        Buffer.from(JSON.stringify(resultPayload)),
        { persistent: true }
      );

      channel.ack(msg);
      console.log(`✅ Vaga ${jobId} enviada para job_results`);
    } catch (err) {
      console.error("❌ Erro NLP:", err.message);
      channel.nack(msg, false, false); // descarta ou DLQ
    }
  });
}

startWorker().catch(console.error);
