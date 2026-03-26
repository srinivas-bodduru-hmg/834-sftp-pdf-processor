const { app } = require("@azure/functions");
const axios = require("axios");
const jwt = require("jsonwebtoken");

const CONFIG = {
  BACKEND_URL:
    process.env.BACKEND_API_URL || "https://your-backend.azurewebsites.net",

  BACKEND_EMAIL: process.env.BACKEND_EMAIL,
  BACKEND_PASSWORD: process.env.BACKEND_PASSWORD,

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
//  * Triggers the posting agent start process:
//  * 1. Authenticates with the backend
//  * 2. Calls the posting agent scheduler API
//  * 3. Handles any errors and logs the process
//  */
app.timer("postingAgentScheduler", {
  schedule: "0 0 */6 * * *", // Every 6 hours (00:00, 06:00, 12:00, 18:00)
  runOnStartup: true,
  handler: async (myTimer, context) => {
    const startTime = Date.now();
    context.log("[Posting Agent Scheduler] ⏰ Timer trigger fired");
    context.log("[Posting Agent Scheduler] ⏰ Timer trigger fired");
    let session;
    try {
      session = await login(context);
    } catch (error) {
      context.log("[Posting Agent Scheduler] ❌ Login failed:", error.message);
      context.log("[Posting Agent Scheduler] ❌ Login failed:");
      context.log(`   Message: ${error.message}`);
      throw error;
    }
    try {
      const url = `${CONFIG.BACKEND_URL}/api/trpc/scheduler.postingAgentStart`;
      const res = await axios.post(`${url}`,{}, {
        headers: {
          Cookie: session.cookieHeader,
          "Content-Type": "application/json",
        },
        timeout: 600000,
      });
    } catch (error) {
      context.log(
        "[Posting Agent Scheduler] ❌ Axios request failed:",
        error.message,
      );
      context.log("[Posting Agent Scheduler] ❌ Axios request failed:");
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
          "[Posting Agent Scheduler] HTTP Error Details:",
          error.response.status,
        );
        context.log(`   HTTP Status: ${error.response.status}`);
        context.log(`   Data: ${JSON.stringify(error.response.data)}`);
      }

      throw error;
    }

    context.log("[Posting Agent Scheduler] 📥 Axios response received");
    context.log("[Posting Agent Scheduler] 📥 Axios response received");

    context.log("[Posting Agent Scheduler] ✅ Backend login successful");
    context.log("[Posting Agent Scheduler] ✅ Backend login successful");

    const duration = Date.now() - startTime;
    context.log(`[Posting Agent Scheduler] ✅ Job completed in ${duration}ms`);
    context.log(`[Posting Agent Scheduler] ✅ Job completed in ${duration}ms`);

    return {
      success: true,
      duration,
    };
  },
});

async function login(context) {
  context.log("🔐 Logging in...");

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

  context.log(`✅ Authenticated (userId: ${userId})`);

  return { cookieHeader, userId };
}
