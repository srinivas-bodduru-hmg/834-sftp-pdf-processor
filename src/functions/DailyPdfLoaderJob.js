const { app } = require("@azure/functions");
const axios = require("axios");

/**
 * 7 AM Job - Triggers Scraping API
 */
app.timer("DailyPdfLoaderJob", {
  // Runs daily at 7:00 AM UTC (12:30 PM IST - India Standard Time)
  // Cron format: second minute hour day month dayOfWeek
  schedule: "0 0 7 * * *", // 7 AM UTC
  runOnStartup: true,

  handler: async (timer, context) => {
    const log = (...args) => {
      const msg = args.join(" ");
      context.log(msg);
      console.log(msg);
    };

    log("🚀 DailyPdfLoaderJob (7AM) Started");

    try {
      // Step 1: Calculate 7 day window ending today
      log("📅 Step 1: Calculating date range");
      const today = new Date();
      log(`   Current date: ${today.toISOString()}`);

      const startDate = new Date(today);
      startDate.setDate(today.getDate() - 7);
      log(`   Start date (7 days before end date): ${startDate.toISOString()}`);

      const format = (d) => d.toISOString().split("T")[0];
      const formattedStartDate = format(startDate);
      const formattedToday = format(today);
      log(`   Formatted range: ${formattedStartDate} to ${formattedToday}`);

      // Step 2: Create payload
      log("📋 Step 2: Creating payload");
      const payload = {
        start_date: formattedStartDate,
        end_date: formattedToday,
        practice_name: "PreOp Memphis",
        ehr_name: "Tebra",
      };
      log("   Payload created:", JSON.stringify(payload, null, 2));

      // Step 3: Make API call
      log("📤 Step 3: Calling Tebra API endpoint");
      const apiUrl = "http://57.154.234.15:8010/run-tebra";
      log(`   URL: ${apiUrl}`);
      log("   Sending request with 10 minute timeout...");

      const response = await axios.post(
        apiUrl,
        payload,
        { timeout: 10 * 60 * 1000 }, // 10 min timeout
      );

      // Step 4: Handle success
      log("✅ Step 4: API call successful");
      log(`   Status Code: ${response.status}`);
      log(`   Response Data:`, JSON.stringify(response.data, null, 2));
      log("🎉 DailyPdfLoaderJob completed successfully");
    } catch (err) {
      console.error("❌ DailyPdfLoaderJob Failed - Error handling initiated");
      log("❌ DailyPdfLoaderJob Failed - Error handling initiated");
      console.error(`   Error Type: ${err.name}`);
      log(`   Error Type: ${err.name}`);
      console.error(`   Error Message: ${err.message}`);
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

      log(
        "💾 Logging complete, re-throwing error for Azure Functions retry mechanism",
      );
      throw err; // Important for retry
    }
  },
});
