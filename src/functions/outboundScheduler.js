const { app } = require('@azure/functions');
const axios = require('axios');


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
//  * Azure Function Timer Trigger: Outbound File Processor
//  * Runs every 30 minutes (0 */30 * * * *)
//  *
//  * Pulls outbound files from SFTP:
//  * 1. Downloads files from /outbound directory
//  * 2. Uploads to Azure Blob Storage (OfficeAlly/OUTBOUND folder)
//  * 3. Inserts metadata record to database (atomic transaction)
//  * 4. Deletes from SFTP (non-blocking cleanup)
//  */
app.timer('outboundScheduler', {
  schedule: '0 */30 * * * *', // Every 30 minutes at :00 and :30
  handler: async (myTimer, context) => {
    const startTime = Date.now();
    console.log('[Outbound Scheduler] ⏰ Timer trigger fired');
    context.log('[Outbound Scheduler] ⏰ Timer trigger fired');

    try {
      if (myTimer.isPastDue) {
        console.log('[Outbound Scheduler] ⚠️  Timer is past due!');
        context.log('[Outbound Scheduler] ⚠️  Timer is past due!');
      }

      console.log('[Outbound Scheduler] 🔐 Attempting backend login...');
      context.log('[Outbound Scheduler] 🔐 Attempting backend login...');

      const res = await axios.post(
        `${CONFIG.BACKEND_URL}/server/src/routers/scheduler.router/schedulerRouter.outboundSchedulerStart`,
        {
          0: {
            email: CONFIG.BACKEND_EMAIL,
            password: CONFIG.BACKEND_PASSWORD,
          },
        },
        { withCredentials: true, timeout: 600000 },
      );

      console.log('[Outbound Scheduler] ✅ Backend login successful');
      context.log('[Outbound Scheduler] ✅ Backend login successful');
      context.log(`   Status: ${res.status}`);

      const duration = Date.now() - startTime;
      console.log(`[Outbound Scheduler] ✅ Job completed in ${duration}ms`);
      context.log(`[Outbound Scheduler] ✅ Job completed in ${duration}ms`);

      return {
        success: true,
        duration,
      };
    } catch (error) {
      const duration = Date.now() - startTime;
      console.error('[Outbound Scheduler] ❌ Error occurred:', error.message);
      context.log.error('[Outbound Scheduler] ❌ Error occurred:');
      context.log.error(`   Message: ${error.message}`);
      context.log.error(`   Duration: ${duration}ms`);

      if (error.response) {
        console.error('[Outbound Scheduler] HTTP Error Details:', error.response.status);
        context.log.error(`   HTTP Status: ${error.response.status}`);
        context.log.error(`   Data: ${JSON.stringify(error.response.data)}`);
      }

      throw error;
    }
  },
});
