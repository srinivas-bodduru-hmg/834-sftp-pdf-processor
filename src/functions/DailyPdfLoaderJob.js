const { app } = require("@azure/functions");
const axios = require("axios");
const jwt = require("jsonwebtoken");

const CONFIG = {
  BACKEND_URL: process.env.BACKEND_API_URL,
  BACKEND_EMAIL: process.env.BACKEND_EMAIL,
  BACKEND_PASSWORD: process.env.BACKEND_PASSWORD,
  IS_DEVELOPMENT: process.env.IS_DEVELOPMENT === "true",
  RETRIEVAL_DATE_RANGE: Number(process.env.RETRIEVAL_DATE_RANGE || 1),
  TEBRA_API_URL:
    process.env.TEBRA_API_URL || "http://10.0.0.6:8010/run-tebra",
  REQUEST_TIMEOUT_MS: 10 * 60 * 1000,
};

/**
 * 7 AM Job - Triggers Scraping API
 */
app.timer("DailyPdfLoaderJob", {
  // Runs daily at 7:00 AM UTC (12:30 PM IST - India Standard Time)
  // Cron format: second minute hour day month dayOfWeek
  schedule: "0 0 7 * * *", // 7 AM UTC
  runOnStartup: CONFIG.IS_DEVELOPMENT,

  handler: async (timer, context) => {
    const log = (...args) => {
      const msg = args.join(" ");
      context.log(msg);
    };

    log("🚀 DailyPdfLoaderJob (7AM) Started");

    try {
      const { formattedStartDate, formattedEndDate } = getInclusiveDateRange(
        CONFIG.RETRIEVAL_DATE_RANGE,
      );
      log(`   Formatted range: ${formattedStartDate} to ${formattedEndDate}`);

      log("🔐 Step 2: Logging into backend");
      const session = await login(log);

      log("🏥 Step 3: Fetching practice names from V3 API");
      const practiceNames = await fetchPracticeNames(log, session);
      log(`   Retrieved ${practiceNames.length} practice names`);
      for (const practiceName of practiceNames) {
        log(`   ${practiceName}`);
      }

      log("📤 Step 4: Calling Tebra API endpoint for each practice");

      const failures = [];

      for (const practiceName of practiceNames) {
        const payload = {
          start_date: formattedStartDate,
          end_date: formattedEndDate,
          practice_name: practiceName,
          ehr_name: "Tebra",
        };

        log(`   ▶ Processing practice: ${practiceName}`);

        try {
          const response = await axios.post(CONFIG.TEBRA_API_URL, payload, {
            timeout: CONFIG.REQUEST_TIMEOUT_MS,
          });

          log(`   ✅ ${practiceName} completed with status ${response.status}`);
          log(`   Response Data:`, JSON.stringify(response.data, null, 2));
        } catch (practiceError) {
          failures.push({
            practiceName: practiceName,
            error: practiceError,
          });
          log(`   ❌ ${practiceName} failed`);
          logPracticeError(log, practiceError);
        }
      }

      log(
        `🎯 DailyPdfLoaderJob completed with ${failures.length} failures out of ${practiceNames.length} practices`,
        failures,
      );

      log("🎉 DailyPdfLoaderJob completed successfully");
    } catch (err) {
      log("❌ DailyPdfLoaderJob Failed - Error handling initiated");
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

async function fetchPracticeNames(log, session) {
  const url = `${CONFIG.BACKEND_URL}/api/trpc/getPracticesV3`;
  const response = await axios.get(url, {
    headers: {
      Cookie: session.cookieHeader,
      "Content-Type": "application/json",
    },
    timeout: CONFIG.REQUEST_TIMEOUT_MS,
  });

  const practices = extractTrpcData(response.data);

  if (!Array.isArray(practices)) {
    throw new Error("Practices API returned an unexpected response shape");
  }

  const practiceNames = [
    ...new Set(
      practices
        .map((practice) => practice?.practiceName)
        .filter(
          (practiceName) =>
            typeof practiceName === "string" && practiceName.trim(),
        ),
    ),
  ];

  if (practiceNames.length === 0) {
    throw new Error("Practices API returned no practice names");
  }

  return practiceNames;
}

function extractTrpcData(responseData) {
  const payload = Array.isArray(responseData) ? responseData[0] : responseData;

  if (payload?.result?.data?.json !== undefined) {
    return payload.result.data.json;
  }

  if (payload?.result?.data !== undefined) {
    return payload.result.data;
  }

  if (payload?.data?.json !== undefined) {
    return payload.data.json;
  }

  if (payload?.data !== undefined) {
    return payload.data;
  }

  return payload;
}

function getInclusiveDateRange(days, endDate = new Date()) {
  if (!Number.isInteger(days) || days <= 0) {
    throw new Error(
      "Inclusive date range requires a positive integer number of days",
    );
  }

  const normalizedEndDate = new Date(endDate);
  const startDate = new Date(normalizedEndDate);
  startDate.setDate(normalizedEndDate.getDate() - (days - 1));

  return {
    formattedStartDate: formatDate(startDate),
    formattedEndDate: formatDate(normalizedEndDate),
  };
}

function formatDate(date) {
  return date.toISOString().split("T")[0];
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
