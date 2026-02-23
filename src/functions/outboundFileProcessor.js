const { app } = require('@azure/functions');
const { getProcessorConfig, validateProcessorConfig, processOutboundFilesJob } = require('../shared/processorLogic');
const { drizzle } = require('drizzle-orm/postgres-js');
const postgres = require('postgres');

// Initialize database connection (lazy-loaded)
let db = null;
let dbSchema = null;

/**
 * Initialize database connection
 */
async function initializeDatabase(config) {
  if (db) {
    return { db, schema: dbSchema };
  }

  try {
    const client = postgres(config.database.url);
    db = drizzle(client);

    // Import schema (adjust path if needed)
    // For now, we'll use a simplified schema definition
    dbSchema = {
      outboundFiles: {},
      inboundFiles: {},
      claimAttachments: {},
      claimProcessingHistory: {},
      claimHeader: {},
    };

    return { db, schema: dbSchema };
  } catch (error) {
    console.error('[Outbound File Processor] Failed to initialize database:', error.message);
    throw error;
  }
}

/**
 * Azure Function Timer Trigger: Outbound File Processor
 * Runs at 15 and 45 minutes past each hour (0 15,45 * * * *)
 *
 * Processes unprocessed outbound files from Azure Blob Storage:
 * 1. Downloads files from Azure
 * 2. Handles "PleaseRead" error files by parsing and linking to claims
 * 3. Handles "ErrorReport" files by parsing and linking to claims
 * 4. Uploads files to Azure attachment storage
 * 5. Inserts metadata records to database (atomic transaction)
 * 6. Updates claim status to 'G' (processed)
 * 7. Marks file as processed
 */
app.timer('outboundFileProcessor', {
  schedule: '0 15,45 * * * *', // At 15 and 45 minutes past each hour
  runOnStartup: true,
  handler: async (myTimer, context) => {
    const startTime = Date.now();

    context.log('[Outbound File Processor] ⏰ Timer trigger fired');

    try {
      // Get and validate configuration
      const config = getProcessorConfig();
      validateProcessorConfig(config);

      // Initialize database connection
      let database = null;
      try {
        context.log('[Outbound File Processor] Initializing database connection...');
        const dbConnection = await initializeDatabase(config);
        database = dbConnection.db;
      } catch (dbError) {
        context.log(`[Outbound File Processor] ❌ Database initialization failed: ${dbError.message}`);
        // Throw error - can't process files without database
        throw dbError;
      }

      // Process outbound files
      const logger = (msg) => {
        context.log(msg);
        console.log(msg);
      };

      context.log('[Outbound File Processor] Starting file processing...');
      const result = await processOutboundFilesJob(database, config, logger);

      // Log final results
      const duration = Date.now() - startTime;
      context.log(`[Outbound File Processor] ✅ Execution completed`);
      context.log(`  - Files processed: ${result.filesProcessed}`);
      context.log(`  - Errors: ${result.errors.length}`);
      context.log(`  - Duration: ${duration}ms`);

      if (result.errors.length > 0) {
        context.log(`  - Errors: ${result.errors.join('; ')}`);
      }

      // Return execution summary (visible in Application Insights)
      return {
        success: result.errors.length === 0,
        filesProcessed: result.filesProcessed,
        errors: result.errors,
        duration,
      };
    } catch (error) {
      context.log(`[Outbound File Processor] ❌ Error: ${error.message}`);
      context.log(`Stack: ${error.stack}`);

      // Throw error to trigger Application Insights alerts if configured
      throw new Error(`[Outbound File Processor] Job failed: ${error.message}`);
    }
  },
});
