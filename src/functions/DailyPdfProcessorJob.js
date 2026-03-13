const { app } = require("@azure/functions");
const { BlobServiceClient } = require("@azure/storage-blob");
const { DefaultAzureCredential } = require("@azure/identity");
const AdmZip = require("adm-zip");
const axios = require("axios");
const FormData = require("form-data");
const jwt = require("jsonwebtoken");
const util = require("util");
const { performance } = require("perf_hooks");

const CONFIG = {
  BACKEND_URL: process.env.BACKEND_API_URL,
  BACKEND_EMAIL: process.env.BACKEND_EMAIL,
  BACKEND_PASSWORD: process.env.BACKEND_PASSWORD,

  MEDICAL_API_URL: process.env.MEDICAL_EXTRACTION_API_URL,
  MEDICAL_API_TOKEN: process.env.MEDICAL_EXTRACTION_API_TOKEN,

  ACCOUNT_URL: process.env.ACCOUNT_URL,

  CONTAINER: "834labs-sftp",
  BATCH_SIZE: 3,
};

const ERROR_CODES = {
  DUPLICATE_FILE: "DUPLICATE_FILE",
};

app.timer("DailyPdfProcessorJob", {
  schedule: "0 30 2 * * *",
  runOnStartup: true,

  handler: async (timer, context) => {
    const log = (...args) => context.log(...args);

    log("🚀 Daily PDF Processor Started");

    try {
      const session = await login(log);
      const container = await getContainer(log);

      for await (const blob of container.listBlobsFlat()) {
        if (!blob.name.endsWith(".zip")) continue;
        await processZipBlob(container, blob.name, session, log);
      }
    } catch (err) {
      log("❌ CRITICAL ERROR");
      log(util.inspect(err, { depth: null }));
      throw err;
    }
  },
});

/* ---------------- LOGIN ---------------- */

async function login(log) {
  log("🔐 Logging in...");

  const res = await axios.post(
    `${CONFIG.BACKEND_URL}/api/trpc/auth.login?batch=1`,
    {
      0: {
        email: CONFIG.BACKEND_EMAIL,
        password: CONFIG.BACKEND_PASSWORD,
      },
    },
    { withCredentials: true }
  );

  const cookies = res.headers["set-cookie"];
  const cookieHeader = cookies.map((c) => c.split(";")[0]).join("; ");

  const tokenCookie = cookies.find((c) => c.includes("TOKEN="));
  const token = tokenCookie.split("TOKEN=")[1].split(";")[0];
  const decoded = jwt.decode(token);

  log(`✅ Authenticated userId=${decoded.userId}`);

  return { cookieHeader, userId: decoded.userId };
}

/* ---------------- STORAGE ---------------- */

async function getContainer(log) {
  const client = new BlobServiceClient(
    CONFIG.ACCOUNT_URL,
    new DefaultAzureCredential()
  );

  const container = client.getContainerClient(CONFIG.CONTAINER);
  log("📦 Connected to blob storage");

  return container;
}

/* ---------------- ZIP PROCESS ---------------- */

async function processZipBlob(container, blobName, session, log) {
  log(`📁 Processing ZIP ${blobName}`);

  const buffer = await downloadBlob(container, blobName);
  const zip = new AdmZip(buffer);

  const pdfs = zip
    .getEntries()
    .filter((e) => e.entryName.toLowerCase().endsWith(".pdf"));

  log(`📄 Found ${pdfs.length} PDFs`);

  for (let i = 0; i < pdfs.length; i += CONFIG.BATCH_SIZE) {
    const batch = pdfs.slice(i, i + CONFIG.BATCH_SIZE);

    await Promise.allSettled(
      batch.map((pdf) => processPdf(pdf, session, log))
    );
  }
}

/* ---------------- PDF PROCESS ---------------- */

async function processPdf(entry, session, log) {
  const fileName = entry.entryName;

  try {
    const match = fileName.trim().match(/^([0-9]+)_/);

    if (!match) {
      throw new Error(`Invalid filename format: ${fileName}`);
    }

    const rpaAppointmentId = match[1];
    const buffer = entry.getData();

    await checkDuplicate(fileName, session);

    const retryStatus = await checkRetryStatus(rpaAppointmentId, session);

    const apiData = await callMedicalApi(buffer, fileName);

    const result = await sendToBackend(
      apiData,
      buffer,
      fileName,
      session.userId
    );

    log(`✅ ${fileName} → ${result.claimId}`);

    return { success: true };
  } catch (err) {
    log(`❌ ${fileName} ERROR:`, err.message);

    try {
      await updateRetryCount(rpaAppointmentId, 1, session);
    } catch (retryErr) {
      log("⚠️ Retry update failed:", retryErr.message);
    }

    return { success: false };
  }
}

/* ---------------- DUPLICATE CHECK ---------------- */

async function checkDuplicate(fileName, session) {
  const url = `${CONFIG.BACKEND_URL}/api/trpc/medicalDuplicate.isDuplicateMedicalFile`;

  const input = encodeURIComponent(JSON.stringify({ fileName }));

  const res = await axios.get(`${url}?input=${input}`, {
    headers: { Cookie: session.cookieHeader },
  });

  const isDuplicate =
    res?.data?.result?.data?.isDuplicate ??
    res?.data?.result?.isDuplicate ??
    false;

  if (isDuplicate) {
    const err = new Error("Duplicate file");
    err.code = ERROR_CODES.DUPLICATE_FILE;
    throw err;
  }
}

/* ---------------- RETRY STATUS ---------------- */

async function checkRetryStatus(rpaAppointmentId, session) {
  try {
    const url = `${CONFIG.BACKEND_URL}/api/trpc/medicalDuplicate.getRetryCount`;

    const fileName = `${rpaAppointmentId}_temp.pdf`;

    const input = encodeURIComponent(JSON.stringify({ fileName }));

    const res = await axios.get(`${url}?input=${input}`, {
      headers: { Cookie: session.cookieHeader },
    });

    console.log("📥 Retry API response:", res.data);

    const retryData = res?.data?.result?.data;

    if (!retryData) return { retry_count: 0 };

    if (retryData.retry_count > 2) {
      throw new Error("Max retries exhausted");
    }

    return retryData;
  } catch (err) {
    console.log("❌ Retry status error");

    if (err.response) {
      console.log(err.response.status, err.response.data);
    }

    throw err;
  }
}

/* ---------------- UPDATE RETRY ---------------- */

async function updateRetryCount(rpaAppointmentId, retryCount, session) {
  const url = `${CONFIG.BACKEND_URL}/api/trpc/medicalDuplicate.updateRetryCount`;

  const payload = {
    input: {
      rpa_appointment_id: rpaAppointmentId,
      retry_count: retryCount,
    },
  };

  const res = await axios.post(url, payload, {
    headers: { Cookie: session.cookieHeader },
  });

  return res.data;
}

/* ---------------- MEDICAL API ---------------- */

async function callMedicalApi(buffer, fileName) {
  const form = new FormData();
  form.append("files", buffer, fileName);

  const res = await axios.post(CONFIG.MEDICAL_API_URL, form, {
    headers: {
      ...form.getHeaders(),
      "x-auth-token": CONFIG.MEDICAL_API_TOKEN,
    },
  });

  return res.data;
}

/* ---------------- BACKEND ---------------- */

async function sendToBackend(apiResponse, buffer, fileName, userId) {
  const payload = {
    apiResponse,
    fileName,
    fileType: "pdf",
    fileBuffer: buffer.toString("base64"),
    userId,
  };

  const res = await axios.post(
    `${CONFIG.BACKEND_URL}/api/medical-extraction/process`,
    payload
  );

  return res.data.data;
}

/* ---------------- HELPERS ---------------- */

async function downloadBlob(container, name) {
  const client = container.getBlobClient(name);
  const res = await client.download();

  return streamToBuffer(res.readableStreamBody);
}

function streamToBuffer(stream) {
  return new Promise((resolve, reject) => {
    const chunks = [];

    stream.on("data", (c) => chunks.push(c));
    stream.on("end", () => resolve(Buffer.concat(chunks)));
    stream.on("error", reject);
  });
}