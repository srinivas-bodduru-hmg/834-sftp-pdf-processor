const { app } = require("@azure/functions");
const { BlobServiceClient } = require("@azure/storage-blob");
const { DefaultAzureCredential } = require("@azure/identity");
const AdmZip = require("adm-zip");
const axios = require("axios");
const jwt = require("jsonwebtoken");
const util = require("util");
const { performance } = require("perf_hooks");
require("process");

/* -------------------------------------------------------------------------- */
/*                                CONFIG                                      */
/* -------------------------------------------------------------------------- */

const CONFIG = {
  BACKEND_URL: process.env.BACKEND_API_URL,

  IS_DEVELOPMENT: process.env.IS_DEVELOPMENT === "true",

  BACKEND_EMAIL: process.env.BACKEND_EMAIL,
  BACKEND_PASSWORD: process.env.BACKEND_PASSWORD,

  ACCOUNT_URL: process.env.ACCOUNT_URL,
  STORAGE_CONNECTION_STRING: process.env.AzureWebJobsStorage,

  CONTAINER: "834labs-sftp",

  RESTRICTED_FOLDERS: ["archived", "processed", "deleted"],

  MAX_RETRIES: 3,

  BATCH_SIZE: 3,
};

const ERROR_CODES = {
  DUPLICATE_FILE: "DUPLICATE_FILE",
  EXHAUSTED: "EXHAUSTED",
};

/* -------------------------------------------------------------------------- */
/*                                MAIN JOB                                    */
/* -------------------------------------------------------------------------- */

app.timer("DailyPdfProcessorJob", {
  // Runs daily at 1:30 AM and 2:30 AM UTC (7:00 AM and 8:00 AM IST)
  // Cron format: second minute hour day month dayOfWeek
  schedule: "0 30 1,2 * * *",
  runOnStartup: CONFIG.IS_DEVELOPMENT,

  handler: async (timer, context) => {
    const log = (...args) => context.log(...args);

    log("🚀 Daily PDF Processor Started");

    try {
      validateConfig();

      const session = await login(log);

      const container = await getContainer(log);

      const stats = {
        total: 0,
        processed: 0,
        skipped: 0,
        exhausted: 0,
        failed: 0,
      };

      // await restoreZipsFromProcessedFolder(container, log);

      await processBlobPrefix(container, "", session, stats, log);

      printSummary(stats, log);
    } catch (err) {
      log("❌ CRITICAL ERROR");

      if (err?.toJSON) {
        log(JSON.stringify(err.toJSON(), null, 2));
      } else {
        log(util.inspect(err, { depth: null }));
      }

      throw err;
    }
  },
});

/* -------------------------------------------------------------------------- */
/*                               AUTH                                         */
/* -------------------------------------------------------------------------- */

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
    { withCredentials: true, timeout: 600000 },
  );

  const cookies = res.headers["set-cookie"];

  if (!cookies) {
    throw new Error("Login failed: No cookies");
  }

  const cookieHeader = cookies.map((c) => c.split(";")[0]).join("; ");

  const tokenCookie = cookies.find((c) => c.includes("TOKEN="));

  let userId = null;

  if (tokenCookie) {
    const token = tokenCookie.split("TOKEN=")[1].split(";")[0];
    const decoded = jwt.decode(token);
    userId = decoded?.userId;
  }

  if (!tokenCookie) {
    throw new Error("Login failed: No tokenCookie");
  }

  if (!userId) {
    throw new Error("Login failed: No userId");
  }

  log(`✅ Authenticated (userId: ${userId})`);

  return { cookieHeader, userId };
}

/* -------------------------------------------------------------------------- */
/*                              STORAGE                                       */
/* -------------------------------------------------------------------------- */

async function getContainer(log) {
  log(`📁 Connecting to blob storage ${CONFIG.CONTAINER}`);
  const client = CONFIG.STORAGE_CONNECTION_STRING
    ? BlobServiceClient.fromConnectionString(CONFIG.STORAGE_CONNECTION_STRING)
    : new BlobServiceClient(CONFIG.ACCOUNT_URL, new DefaultAzureCredential());

  const container = client.getContainerClient(CONFIG.CONTAINER);

  log(`✅ Connected to ${container?.containerName}`);

  return container;
}

/* -------------------------------------------------------------------------- */
/*                              ZIP PROCESS                                   */
/* -------------------------------------------------------------------------- */

async function processZipBlob(container, blobName, session, stats, log) {
  const zipTimerStart = performance.now();
  const zipStats = {
    total: 0,
    processed: 0,
    skipped: 0,
    exhausted: 0,
    failed: 0,
  };
  let aggregateBatchTime = 0;
  const failedRpaApplicationIds = [];
  log(`\n📁 Processing ZIP: ${blobName}`);
  const buffer = await downloadBlob(log, container, blobName);

  const zip = new AdmZip(buffer);

  const pdfs = zip
    .getEntries()
    .filter((e) => e.entryName.toLowerCase().endsWith(".pdf"));

  log(`📄 Found ${pdfs.length} PDFs`);

  for (let i = 0; i < pdfs.length; i += CONFIG.BATCH_SIZE) {
    const batch = pdfs.slice(i, i + CONFIG.BATCH_SIZE);

    const start = i + 1;
    const end = Math.min(i + CONFIG.BATCH_SIZE, pdfs.length);
    const batchTimerStart = performance.now();
    const batchStats = {
      total: 0,
      processed: 0,
      skipped: 0,
      exhausted: 0,
      failed: 0,
    };

    log(`📦 Starting batch ${start}-${end}`);

    // for (const pdf of batch) {
    //   log(`   ➡️ ${pdf.entryName}`);
    // }

    const results = await Promise.allSettled(
      batch.map((pdf) =>
        processPdf(pdf, session, log, failedRpaApplicationIds),
      ),
    );

    for (const result of results) {
      stats.total++;
      batchStats.total++;
      zipStats.total++;

      if (result.status === "rejected") {
        stats.failed++;
        batchStats.failed++;
        zipStats.failed++;
        log("❌ Unhandled PDF error:", result.reason);
        continue;
      }

      if (result.value?.success) {
        stats.processed++;
        batchStats.processed++;
        zipStats.processed++;
      } else if (result.value?.errorCode === ERROR_CODES.DUPLICATE_FILE) {
        stats.skipped++;
        batchStats.skipped++;
        zipStats.skipped++;
      } else if (result.value?.errorCode === ERROR_CODES.EXHAUSTED) {
        stats.exhausted++;
        batchStats.exhausted++;
        zipStats.exhausted++;
      } else {
        stats.failed++;
        batchStats.failed++;
        zipStats.failed++;
      }
    }
    const batchTime = performance.now() - batchTimerStart;
    aggregateBatchTime += batchTime;
    log(
      `📊 Batch ${start}-${end} (Processed: ${batchStats.processed}, Skipped: ${batchStats.skipped}, Exhausted: ${batchStats.exhausted}, Failed: ${batchStats.failed}, Total: ${batchStats.total}, TimeTaken: ${(batchTime / 1000).toFixed(2)}s)`,
    );
  }
  const zipTime = performance.now() - zipTimerStart;
  const batchSize = CONFIG.BATCH_SIZE || 1;
  const totalBatches = Math.ceil(pdfs.length / batchSize) || 1;
  const avgBatchTime = aggregateBatchTime / totalBatches;

  log(
    `\n📁 ZIP ${blobName} (Processed: ${zipStats.processed}, Skipped: ${zipStats.skipped}, Exhausted: ${zipStats.exhausted}, Failed: ${zipStats.failed}, Total: ${zipStats.total}, TimeTaken: ${(zipTime / 1000).toFixed(2)}, AvgTimeTakenPerBatch: ${(avgBatchTime / 1000).toFixed(2)}s ,  )`,
  );

  if (
    zipStats.processed + zipStats.exhausted + zipStats.skipped ===
    pdfs.length
  ) {
    log(
      `✅ All PDFs in ${blobName} processed or exhausted - moving ZIP to processed folder`,
    );

    try {
      await moveZipToProcessed(container, blobName, log);
    } catch (err) {
      log(
        `❌ Failed to move ZIP to processed folder for ${blobName}: ${err.message}`,
      );
    }
  }
}

/* -------------------------------------------------------------------------- */

async function processBlobPrefix(container, prefix, session, stats, log) {
  log(`📂 Scanning folder: ${prefix || "/"}`);

  for await (const item of container.listBlobsByHierarchy("/", { prefix })) {
    if (item.kind === "prefix") {
      if (isRestrictedBlob(item.name, log)) {
        log(`⏭️ Skipping restricted folder: ${item.name}`);
        continue;
      }

      await processBlobPrefix(container, item.name, session, stats, log);
      continue;
    }

    if (!item.name.endsWith(".zip")) continue;
    await processZipBlob(container, item.name, session, stats, log);
  }
}

/* -------------------------------------------------------------------------- */
/*                              PDF PROCESS                                   */
/* -------------------------------------------------------------------------- */

async function processPdf(entry, session, log, failedRpaApplicationIds) {
  const fileName = entry.entryName;
  const rpa_appointment_id_match = fileName.trim().match(/^([0-9]+)_/);
  const rpa_appointment_id = rpa_appointment_id_match?.[1];
  let retryStatus;

  try {
    const buffer = entry.getData();

    if (!buffer?.length) {
      throw new Error("Empty PDF");
    }

    retryStatus = await checkDuplicate(log, fileName, session);

    if (retryStatus.retry_count > CONFIG.MAX_RETRIES) {
      const err = new Error("Max retries exhausted");
      err.code = ERROR_CODES.EXHAUSTED;
      throw err;
    }

    const apiData = await callMedicalApi(log, buffer, fileName, session);

    log(
      `✅ File processed and claim created: ${fileName} → ${apiData?.claimId} -> ${apiData.serviceFacilityName}`,
    );

    return {
      success: true,
      claimId: apiData?.claimId,
    };
  } catch (err) {
    let errorDetails = {
      message: err.message,
      code: err.code,
    };

    // If this is an HTTP/API error (Axios)
    if (err.response) {
      errorDetails.status = err.response.status;
      errorDetails.statusText = err.response.statusText;
      errorDetails.data = err.response.data;
      errorDetails.headers = err.response.headers;
    }

    // If request was sent but no response
    else if (err.request) {
      errorDetails.request = "No response received from API";
    }

    // Other errors (coding, timeout, etc.)
    else {
      errorDetails.internal = err.toString();
    }

    log(`❌ ${fileName} ERROR: ${JSON.stringify(errorDetails, null, 2)}`);

    // Attempt to update retry count on backend
    if (
      retryStatus?.retry_count !== undefined &&
      err.code !== ERROR_CODES.DUPLICATE_FILE
    ) {
      try {
        await updateRetryCount(
          log,
          rpa_appointment_id,
          retryStatus.retry_count,
          session,
        );
        log(
          `✅ Retry count for appointment ${rpa_appointment_id} updated successfully`,
        );
      } catch (retryErr) {
        log(
          `⚠️  Failed to update retry count for appointment ${rpa_appointment_id}: ${retryErr.message}`,
        );

        errorDetails.retryUpdateError = retryErr.response
          ? {
              status: retryErr.response.status,
              message: retryErr.response.data?.error || retryErr.message,
            }
          : { message: retryErr.message };
      }
    }

    failedRpaApplicationIds.push(rpa_appointment_id);

    return {
      success: false,
      errorCode: err.code,
      error: errorDetails,
    };
  }
}

/* -------------------------------------------------------------------------- */
/*                           EXTERNAL CALLS                                   */
/* -------------------------------------------------------------------------- */

async function checkDuplicate(log, fileName, session) {
  const url = `${CONFIG.BACKEND_URL}/api/trpc/medicalDuplicate.isDuplicateMedicalFile`;

  const input = encodeURIComponent(JSON.stringify({ fileName }));

  const res = await axios.get(`${url}?input=${input}`, {
    headers: {
      Cookie: session.cookieHeader,
      "Content-Type": "application/json",
    },
    timeout: 600000,
  });

  const resultData = res?.data?.result?.data || {};
  const isDuplicate = resultData.isDuplicate ?? true;
  // We only proceed when the API explicitly confirms that the file is not a duplicate

  if (isDuplicate) {
    const err = new Error("Duplicate file");
    err.code = ERROR_CODES.DUPLICATE_FILE;
    throw err;
  }

  const retry_count = resultData.retry_count ?? 0;
  return { retry_count };
}

/* -------------------------------------------------------------------------- */

async function updateRetryCount(log, rpaAppointmentId, retryCount, session) {
  const url = `${CONFIG.BACKEND_URL}/api/trpc/medicalDuplicate.updateRetryCount`;

  const payload = {
    rpa_appointment_id: rpaAppointmentId,
    retry_count: (retryCount ?? 0) + 1,
  };

  const res = await axios.post(url, payload, {
    headers: {
      Cookie: session.cookieHeader,
      "Content-Type": "application/json",
    },
    timeout: 600000,
  });

  return res.data;
}
/* -------------------------------------------------------------------------- */

async function callMedicalApi(log, buffer, fileName, session) {
  const fileContent = buffer.toString("base64");

  const payload = {
    fileName,
    fileContent,
    fileType: "application/pdf",
  };

  const res = await axios.post(
    `${CONFIG.BACKEND_URL}/api/trpc/medicalExtraction.uploadMedicalFile`,
    payload,
    {
      headers: {
        "Content-Type": "application/json",
        Cookie: session.cookieHeader,
      },
      timeout: 600000,
    },
  );

  if (res.status === 200) {
    log(`   ✅ Medical extraction API returned 200 OK for ${fileName}`);
  } else {
    log(`   ⚠️ Medical extraction API returned status ${res.status}:`);

    log(
      `   📋 Medical extraction API response received for ${fileName}:`,
      JSON.stringify(res.data, null, 2),
    );
  }

  if (!res.data) {
    throw new Error("Empty medical API response");
  }

  // Extract nested data structure from TRPC response
  const extractedData = res.data?.result?.data || res.data;

  // If claimError is present, the upstream system failed - throw an error
  if (extractedData?.claimError) {
    throw new Error(`Claim creation failed: ${extractedData.claimError}`);
  }

  return extractedData;
}

/* -------------------------------------------------------------------------- */

async function sendToBackend(log, apiResponse, buffer, fileName, userId) {
  try {
    const payload = {
      apiResponse: [apiResponse], // Wrap in array - API expects array format
      fileName,
      fileType: "pdf",
      fileBuffer: buffer.toString("base64"),
      userId,
    };

    log(`   📤 Sending to backend for ${fileName}...`);

    const res = await axios.post(
      `${CONFIG.BACKEND_URL}/api/medical-extraction/process`,
      payload,
      { timeout: 600000 },
    );

    if (res.status === 200) {
      log(`   ✅ Backend processing returned 200 OK for ${fileName}`);
    } else {
      log(`   ⚠️ Backend returned status ${res.status}:`);
      log(`   📋 Response:`, JSON.stringify(res.data, null, 2));
    }

    if (!res.data?.success) {
      throw new Error(
        `Backend rejected request: ${res.data?.error || "Unknown error"}`,
      );
    }

    return res.data.data;
  } catch (err) {
    if (err.response) {
      log(`   ❌ Backend API error - Status: ${err.response.status}`);
      log(`   📋 Error details:`, JSON.stringify(err.response.data, null, 2));
    } else if (err.request) {
      log(`   ❌ Backend API - No response received`);
      log(`   Request was made but no response received`);
    } else {
      log(`   ❌ Backend API - Error: ${err.message}`);
    }
    throw err;
  }
}

/* -------------------------------------------------------------------------- */
/*                              HELPERS                                       */
/* -------------------------------------------------------------------------- */

async function downloadBlob(log, container, name) {
  const client = container.getBlobClient(name);

  const res = await client.download();

  return streamToBuffer(res.readableStreamBody);
}

/* -------------------------------------------------------------------------- */

async function moveZipToProcessed(container, blobName, log) {
  const sourceClient = container.getBlobClient(blobName);
  const lastSlashIndex = blobName.lastIndexOf("/");
  const folderPath =
    lastSlashIndex >= 0 ? blobName.slice(0, lastSlashIndex) : "";
  const fileName =
    lastSlashIndex >= 0 ? blobName.slice(lastSlashIndex + 1) : blobName;
  const destinationBlobName = folderPath
    ? `${folderPath}/processed/${fileName}`
    : `processed/${fileName}`;
  const destinationClient = container.getBlobClient(destinationBlobName);

  log(
    `📦 Moving ZIP to processed folder: ${blobName} -> ${destinationBlobName}`,
  );

  const copyPoller = await destinationClient.beginCopyFromURL(sourceClient.url);
  await copyPoller.pollUntilDone();
  await sourceClient.delete();

  log(`✅ Moved ZIP to processed folder: ${destinationBlobName}`);
}

/* -------------------------------------------------------------------------- */

async function restoreZipsFromProcessedFolder(container, log) {
  log("♻️ Restoring ZIPs from processed folders");
  try {
    await restoreProcessedPrefixes(container, "", log);
  } catch (err) {
    log(`❌ Restore operation failed: ${err.message}`);
  }
  log("✅ Finished restoring ZIPs from processed folders");
}

/* -------------------------------------------------------------------------- */

async function restoreProcessedPrefixes(container, prefix, log) {
  for await (const item of container.listBlobsByHierarchy("/", { prefix })) {
    if (item.kind !== "prefix") {
      continue;
    }

    if (isProcessedFolder(item.name)) {
      log(`📂 Restoring ZIPs from processed folder: ${item.name}`);

      try {
        await restoreZipsUnderPrefix(container, item.name, log);
      } catch (err) {
        log(
          `❌ Failed to restore ZIPs from processed folder ${item.name}: ${err.message}`,
        );
      }

      continue;
    }

    await restoreProcessedPrefixes(container, item.name, log);
  }
}

/* -------------------------------------------------------------------------- */

async function restoreZipsUnderPrefix(container, prefix, log) {
  for await (const blob of container.listBlobsFlat({ prefix })) {
    if (!blob.name.endsWith(".zip")) continue;

    try {
      await moveZipOutOfProcessedFolder(container, blob.name, log);
    } catch (err) {
      log(`❌ Failed to restore ZIP ${blob.name}: ${err.message}`);
    }
  }
}

/* -------------------------------------------------------------------------- */

async function moveZipOutOfProcessedFolder(container, blobName, log) {
  const sourceClient = container.getBlobClient(blobName);
  const destinationBlobName = removeProcessedSegment(blobName);

  if (destinationBlobName === blobName) {
    log(`⚠️ Could not derive restore path for ZIP: ${blobName}`);
    return;
  }

  const destinationClient = container.getBlobClient(destinationBlobName);

  log(`📦 Restoring ZIP: ${blobName} -> ${destinationBlobName}`);

  const copyPoller = await destinationClient.beginCopyFromURL(sourceClient.url);
  await copyPoller.pollUntilDone();
  await sourceClient.delete();

  log(`✅ Restored ZIP: ${destinationBlobName}`);
}

/* -------------------------------------------------------------------------- */

function removeProcessedSegment(blobName) {
  const pathParts = blobName.split("/").filter(Boolean);
  const processedIndex = pathParts.findIndex(
    (part) => part.toLowerCase() === "processed",
  );

  if (processedIndex === -1) {
    return blobName;
  }

  pathParts.splice(processedIndex, 1);
  return pathParts.join("/");
}

/* -------------------------------------------------------------------------- */

function isProcessedFolder(prefix) {
  const pathParts = prefix.toLowerCase().split("/").filter(Boolean);
  return pathParts[pathParts.length - 1] === "processed";
}

/* -------------------------------------------------------------------------- */

function isRestrictedBlob(blobName, log) {
  const pathParts = blobName.toLowerCase().split("/").filter(Boolean);
  // log(`   Checking if blob is in restricted folder: ${blobName}, ${pathParts}`);
  return pathParts.some((part) => CONFIG.RESTRICTED_FOLDERS.includes(part));
}

/* -------------------------------------------------------------------------- */

function streamToBuffer(stream) {
  return new Promise((resolve, reject) => {
    const chunks = [];

    stream.on("data", (c) => chunks.push(c));
    stream.on("end", () => resolve(Buffer.concat(chunks)));
    stream.on("error", reject);
  });
}

/* -------------------------------------------------------------------------- */

function validateConfig() {
  const required = [
    "BACKEND_EMAIL",
    "BACKEND_PASSWORD",
    "ACCOUNT_URL",
    "BACKEND_URL",
  ];

  for (const key of required) {
    if (!CONFIG[key]) {
      throw new Error(`Missing env: ${key}`);
    }
  }
}

/* -------------------------------------------------------------------------- */

function printSummary(stats, log) {
  log("\n==============================");
  log("📊 SUMMARY");
  log(`Total     : ${stats.total}`);
  log(`Processed : ${stats.processed}`);
  log(`Skipped   : ${stats.skipped}`);
  log(`Exhausted : ${stats.exhausted}`);
  log(`Failed    : ${stats.failed}`);

  const rate = stats.total
    ? (((stats.processed + stats.skipped) / stats.total) * 100).toFixed(2)
    : 0;

  log(`Success % : ${rate}%`);
  log("==============================");
}
