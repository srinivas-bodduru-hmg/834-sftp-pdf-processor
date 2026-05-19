const { app } = require("@azure/functions");
const axios = require("axios");
const jwt = require("jsonwebtoken");

const CONFIG = {
  BACKEND_URL:
    process.env.BACKEND_API_URL || "https://your-backend.azurewebsites.net",

  IS_DEVELOPMENT: process.env.IS_DEVELOPMENT === "true",

  BACKEND_EMAIL: process.env.BACKEND_EMAIL,
  BACKEND_PASSWORD: process.env.BACKEND_PASSWORD,

  COLLECTION_AGENT_EMAIL: process.env.COLLECTION_AGENT_EMAIL,
  COLLECTION_AGENT_PASSWORD: process.env.COLLECTION_AGENT_PASSWORD,

  MEDICAL_API_URL: process.env.MEDICAL_EXTRACTION_API_URL,
  MEDICAL_API_TOKEN: process.env.MEDICAL_EXTRACTION_API_TOKEN,

  ACCOUNT_URL: process.env.ACCOUNT_URL,

  CONTAINER: "834labs-sftp",

  BATCH_SIZE: 3,
};

// /**
//  * Azure Function Timer Trigger: Posting Agent Scheduler
//  * Runs every 15 minutes (0 */15 * * * *)
//  *
//  * Triggers the collection agent start process:
//  * 1. Authenticates with the backend
//  * 2. Calls the collection agent scheduler API
//  * 3. Handles any errors and logs the process
//  */
app.timer("collectionAgentScheduler", {
  // Runs every 6 hours at UTC: 00:30, 06:30, 12:30, 18:30 (6:00 AM, 12:00 PM, 6:00 PM, 12:00 AM IST)
  // Cron format: second minute hour day month dayOfWeek
  schedule: "0 30 */6 * * *", // Every 6 hours and 30 minutes UTC offset
  runOnStartup: CONFIG.IS_DEVELOPMENT,
  handler: async (myTimer, context) => {
    const startTime = Date.now();
    context.log("[Collection Agent Scheduler] ⏰ Timer trigger fired");
    context.log("[Collection Agent Scheduler] ⏰ Timer trigger fired");
    let session;
    try {
      session = await login(context);
    } catch (error) {
      context.log("[Collection Agent Scheduler] ❌ Login failed:", error.message);
      context.log("[Collection Agent Scheduler] ❌ Login failed:");
      context.log(`   Message: ${error.message}`);
      throw error;
    }
    try {
      try {
        const url = `${CONFIG.BACKEND_URL}/api/trpc/agent.AutoSecondaryClaimsforPatientRessponsibility`;
        const res = await axios.post(`${url}`, {}, {
          headers: {
            Cookie: session.cookieHeader,
            "Content-Type": "application/json",
          },
          timeout: 600000,
        });
      } catch (error) {
        context.log(
          "[Collection Agent Scheduler] ❌ Axios request failed:",
          error.message,
        );
        context.log("[Collection Agent Scheduler] ❌ Axios request failed:");
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
            "[Collection Agent Scheduler] HTTP Error Details:",
            error.response.status,
          );
          context.log(`   HTTP Status: ${error.response.status}`);
          context.log(`   Data: ${JSON.stringify(error.response.data)}`);
        }

        throw error;
      }

      context.log("[Collection Agent Scheduler] 📥 Axios response received");
      context.log("[Collection Agent Scheduler] 📥 Axios response received");

      context.log("[Collection Agent Scheduler] ✅ Backend login successful");
      context.log("[Collection Agent Scheduler] ✅ Backend login successful");

      const duration = Date.now() - startTime;
      context.log(`[Collection Agent Scheduler] ✅ Job completed in ${duration}ms`);
      context.log(`[Collection Agent Scheduler] ✅ Job completed in ${duration}ms`);

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
  context.log("🔐 Logging in with Collection Agent credentials...");

  const res = await axios.post(
    `${CONFIG.BACKEND_URL}/api/trpc/auth.login?batch=1`,
    {
      0: {
        email: CONFIG.COLLECTION_AGENT_EMAIL,
        password: CONFIG.COLLECTION_AGENT_PASSWORD,
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
    context.log("[Collection Agent Scheduler] 🔓 Logging out backend session...");

    await axios.post(`${CONFIG.BACKEND_URL}/api/trpc/auth.logout?batch=1`, {}, {
      headers: {
        Cookie: session.cookieHeader,
        "Content-Type": "application/json",
      },
      timeout: 600000,
    });

    context.log("[Collection Agent Scheduler] ✅ Backend logout successful");
  } catch (error) {
    context.log(
      "[Collection Agent Scheduler] ⚠️ Backend logout failed:",
      error.message,
    );
  }
}
