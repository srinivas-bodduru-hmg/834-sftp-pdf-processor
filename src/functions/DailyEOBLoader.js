const { app } = require("@azure/functions");
const axios = require("axios");

const CONFIG = {
  IS_DEVELOPMENT: process.env.IS_DEVELOPMENT === "true",
  EOB_API_URL:
    process.env.EOB_API_URL ||
    "https://pch-eob-pipeline.agreeablesea-c96d1c8a.centralus.azurecontainerapps.io/trigger",
  EOB_BLOB_PREFIX: process.env.EOB_BLOB_PREFIX || "EOB/test",
  EOB_ENTITY: process.env.EOB_ENTITY || "PCH",
  EOB_SUB_ENTITY: process.env.EOB_SUB_ENTITY || null,
  EOB_FILE: process.env.EOB_FILE || null,
  EOB_STORAGE_ACCOUNT: process.env.EOB_STORAGE_ACCOUNT || "pchdatadev001",
  EOB_CONTAINER: process.env.EOB_CONTAINER || "834labs-sftp",
  REQUEST_TIMEOUT_MS: 10 * 60 * 1000,
};

/**
 * 7 AM Job - Triggers EOB pipeline
 */
app.timer("dailyEOBLoader", {
  // Runs daily at 7:00 AM UTC (12:30 PM IST - India Standard Time)
  // Cron format: second minute hour day month dayOfWeek
  schedule: "0 0 7 * * *",
  runOnStartup: CONFIG.IS_DEVELOPMENT,

  handler: async (timer, context) => {
    const log = (...args) => {
      const msg = args.join(" ");
      context.log(msg);
    };

    log("🚀 dailyEOBLoader (7AM) Started");

    try {
      validateConfig();
      const payload = buildTriggerPayload();

      log(`📤 Step 1: Calling ${CONFIG.EOB_API_URL}`);
      log(`   Payload: ${JSON.stringify(payload)}`);

      const response = await axios.post(CONFIG.EOB_API_URL, payload, {
        timeout: CONFIG.REQUEST_TIMEOUT_MS,
        headers: {
          "Content-Type": "application/json",
        },
      });

      log(`   ✅ EOB trigger completed with status ${response.status}`);
      log(`   Response Data:`, JSON.stringify(response.data, null, 2));

      log("🎉 dailyEOBLoader completed successfully");
    } catch (err) {
      log("❌ dailyEOBLoader Failed - Error handling initiated");
      log(`   Error Type: ${err.name}`);
      log(`   Error Message: ${err.message}`);

      if (err.response) {
        log("📊 HTTP Response Error Details:");
        log(`   Status Code: ${err.response.status}`);
        log(`   Status Text: ${err.response.statusText}`);
        log(`   Response Data:`, JSON.stringify(err.response.data, null, 2));
      } else if (err.request) {
        log("🌐 Request was made but no response received:");
        log(`   Request:`, err.request);
      } else {
        log("⚠️  Error occurred during request setup:");
        log(`   Details: ${err.message}`);
      }
    }
  },
});

function validateConfig() {
  const required = [
    ["EOB_BLOB_PREFIX", CONFIG.EOB_BLOB_PREFIX],
    ["EOB_ENTITY", CONFIG.EOB_ENTITY],
    ["EOB_API_URL", CONFIG.EOB_API_URL],
  ];

  const missing = required
    .filter(([, value]) => typeof value !== "string" || !value.trim())
    .map(([key]) => key);

  if (missing.length > 0) {
    throw new Error(`Missing required configuration: ${missing.join(", ")}`);
  }
}

function buildTriggerPayload() {
  const payload = {
    blob_prefix: CONFIG.EOB_BLOB_PREFIX,
    entity: CONFIG.EOB_ENTITY,
    storage_account: CONFIG.EOB_STORAGE_ACCOUNT,
    container: CONFIG.EOB_CONTAINER,
  };

  if (CONFIG.EOB_SUB_ENTITY) {
    payload.sub_entity = CONFIG.EOB_SUB_ENTITY;
  }

  if (CONFIG.EOB_FILE) {
    payload.file = CONFIG.EOB_FILE;
  }

  return payload;
}

function logPracticeError(log, err) {
  log(`   Error Type: ${err.name}`);
  log(`   Error Message: ${err.message}`);

  if (err.response) {
    log(`   Status Code: ${err.response.status}`);
    log(`   Status Text: ${err.response.statusText}`);
    log(`   Response Data:`, JSON.stringify(err.response.data, null, 2));
    return;
  }

  if (err.request) {
    log("   Request was made but no response received");
    return;
  }

  log(`   Details: ${err.message}`);
}
