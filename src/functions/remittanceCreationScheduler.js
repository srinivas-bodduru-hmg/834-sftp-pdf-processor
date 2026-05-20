const { app } = require("@azure/functions");
const axios = require("axios");
const jwt = require("jsonwebtoken");

const CONFIG = {
  BACKEND_URL:
    process.env.BACKEND_API_URL || "https://your-backend.azurewebsites.net",

  IS_DEVELOPMENT: process.env.IS_DEVELOPMENT === "true",

  RCM_SYSTEM_EMAIL: process.env.RCM_SYSTEM_EMAIL,
  RCM_SYSTEM_PASSWORD: process.env.RCM_SYSTEM_PASSWORD,

  ACCOUNT_URL: process.env.ACCOUNT_URL,

  CONTAINER: "834labs-sftp",

  BATCH_SIZE: 3,
};

// /**
//  * Azure Function Timer Trigger: RCM Agent Scheduler
//  *
//  * Triggers the RCM agent start process:
//  * 1. Authenticates with the backend
//  * 2. Calls the RCM agent scheduler API
//  * 3. Handles any errors and logs the process
//  */
app.timer("remittanceCreationScheduler", {
  // Runs every 3 hours at UTC: 00:00, 03:00, 06:00, 09:00, 12:00, 15:00, 18:00, 21:00
  // Cron format: second minute hour day month dayOfWeek
  schedule: "0 0 */3 * * *", // Every 3 hours on the hour
  runOnStartup: CONFIG.IS_DEVELOPMENT,
  handler: async (myTimer, context) => {
    const startTime = Date.now();
    context.log("[Remittance Creation Scheduler] ⏰ Timer trigger fired");
    context.log("[Remittance Creation Scheduler] ⏰ Timer trigger fired");
    let session;
    try {
      session = await login(context);
    } catch (error) {
      context.log("[Remittance Creation Scheduler] ❌ Login failed:", error.message);
      context.log("[Remittance Creation Scheduler] ❌ Login failed:");
      context.log(`   Message: ${error.message}`);
      throw error;
    }
    try {
      try {
        const url = `${CONFIG.BACKEND_URL}/api/trpc/eobDepositProcessing.processPendingDeposits`;
        const res = await axios.post(`${url}`, {}, {
          headers: {
            Cookie: session.cookieHeader,
            "Content-Type": "application/json",
          },
          timeout: 600000,
        });
      } catch (error) {
        context.log(
          "[Remittance Creation Scheduler] ❌ Axios request failed:",
          error.message,
        );
        context.log("[Remittance Creation Scheduler] ❌ Axios request failed:");
        context.log(`   Message: ${error.message}`);

        // If this is an HTTP/API error (Axios)

        let errorDetails = {
          message: error.message,
          code: error.code,
        };

        if (error.response) {
          errorDetails.status = error.response.status;
          errorDetails.statusText = error.response.statusText;
          errorDetails.data = error.response.data;
          errorDetails.headers = error.response.headers;
        }

        // If request was sent but no response
        else if (error.request) {
          errorDetails.request = "No response received from API";
        }

        // Other errors (coding, timeout, etc.)
        else {
          errorDetails.internal = error.toString();
        }

        context.log(`❌ ERROR: ${JSON.stringify(errorDetails, null, 2)}`);

        if (error.response) {
          context.log(
            "[Remittance Creation Scheduler] HTTP Error Details:",
            error.response.status,
          );
          context.log(`   HTTP Status: ${error.response.status}`);
          context.log(`   Data: ${JSON.stringify(error.response.data)}`);
        }

        throw error;
      }

      context.log("[Remittance Creation Scheduler] 📥 Axios response received");
      context.log("[Remittance Creation Scheduler] 📥 Axios response received");
      context.log("[Remittance Creation Scheduler] 📥 Axios response received");

      context.log("[Remittance Creation Scheduler] ✅ Backend login successful");
      context.log("[Remittance Creation Scheduler] ✅ Backend login successful");

      const duration = Date.now() - startTime;
      context.log(`[Remittance Creation Scheduler] ✅ Job completed in ${duration}ms`);
      context.log(`[Remittance Creation Scheduler] ✅ Job completed in ${duration}ms`);

      return {
        success: true,
        duration,
      };
    } finally {
      await logout(context, session);
    }
  },
});

async function login(context) {
  context.log("🔐 Logging in with RCM System credentials...");

  const res = await axios.post(
    `${CONFIG.BACKEND_URL}/api/trpc/auth.login?batch=1`,
    {
      0: {
        email: CONFIG.RCM_SYSTEM_EMAIL,
        password: CONFIG.RCM_SYSTEM_PASSWORD,
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

  context.log(`✅ Authenticated (userId: ${userId})`);

  return { cookieHeader, userId };
}

async function logout(context, session) {
  if (!session?.cookieHeader) {
    return;
  }

  try {
    context.log("[Remittance Creation Scheduler] 🔓 Logging out backend session...");

    await axios.post(`${CONFIG.BACKEND_URL}/api/trpc/auth.logout?batch=1`, {}, {
      headers: {
        Cookie: session.cookieHeader,
        "Content-Type": "application/json",
      },
      timeout: 600000,
    });

    context.log("[Remittance Creation Scheduler] ✅ Backend logout successful");
  } catch (error) {
    context.log(
      "[Remittance Creation Scheduler] ⚠️ Backend logout failed:",
      error.message,
    );
  }
}
