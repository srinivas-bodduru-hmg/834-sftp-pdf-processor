const { BlobServiceClient } = require('@azure/storage-blob');

/**
 * Get processor configuration from environment variables
 */
function getProcessorConfig() {
  return {
    azure: {
      connectionString: process.env.AZURE_STORAGE_CONNECTION_STRING || '',
    },
    database: {
      url: process.env.DATABASE_URL || '',
    },
  };
}

/**
 * Validate that all required configuration is available
 */
function validateProcessorConfig(config) {
  const missingConfigs = [];

  if (!config.azure.connectionString) missingConfigs.push('AZURE_STORAGE_CONNECTION_STRING');
  if (!config.database.url) missingConfigs.push('DATABASE_URL');

  if (missingConfigs.length > 0) {
    throw new Error(`Missing configuration: ${missingConfigs.join(', ')}`);
  }
}

/**
 * Get current timestamp in CST timezone
 */
function getCSTTimestamp() {
  const now = new Date();
  // Convert to CST (UTC-6)
  const cstOffset = -6 * 60; // CST is UTC-6
  const utcTime = now.getTime() + (now.getTimezoneOffset() * 60000);
  const cstTime = new Date(utcTime + (cstOffset * 60000));
  return cstTime;
}

/**
 * Process unprocessed outbound files
 * - Downloads files from Azure
 * - Parses "PleaseRead" files
 * - Parses "ErrorReport" files
 * - Links to claims via inbound_files
 * - Updates database records
 */
async function processOutboundFilesJob(db, config, logger) {
  const startTime = new Date();

  logger('[Outbound File Processor] 🔄 Starting outbound file processing...');

  if (!db) {
    logger('[Outbound File Processor] ⚠️  Database not initialized, cannot process files');
    return { filesProcessed: 0, errors: [] };
  }

  try {
    // Get all unprocessed outbound files
    const unprocessedFiles = await db
      .select()
      .from(db.schema.outboundFiles)
      .where(db.eq(db.schema.outboundFiles.processed, false));

    if (unprocessedFiles.length === 0) {
      logger('[Outbound File Processor] No unprocessed files found');
      return { filesProcessed: 0, errors: [] };
    }

    logger(`[Outbound File Processor] Found ${unprocessedFiles.length} unprocessed file(s) to process`);

    const errors = [];
    let filesProcessed = 0;

    // Process each file
    for (const file of unprocessedFiles) {
      try {
        await processFile(db, config, file, logger);
        filesProcessed++;
      } catch (error) {
        const errorMsg = `Error processing file ${file.fileName}: ${error.message}`;
        logger(`[Outbound File Processor] ❌ ${errorMsg}`);
        errors.push(errorMsg);
        // Continue processing remaining files even if one fails
      }
    }

    const duration = new Date().getTime() - startTime.getTime();
    logger(`[Outbound File Processor] ✅ Job completed in ${duration}ms (${filesProcessed}/${unprocessedFiles.length} succeeded)`);

    return { filesProcessed, errors };
  } catch (error) {
    const errorMsg = error.message;
    logger(`[Outbound File Processor] ❌ Error in job execution: ${errorMsg}`);
    return { filesProcessed: 0, errors: [errorMsg] };
  }
}

/**
 * Process a single outbound file
 * 1. Download from Azure
 * 2. Check if it's a "PleaseRead" or "ErrorReport" file
 * 3. Parse and link to claim if applicable
 * 4. Mark as processed
 */
async function processFile(db, config, file, logger) {
  if (!config?.azure.connectionString) {
    throw new Error('Azure Storage connection string not configured');
  }

  try {
    logger(`[Outbound File Processor] Processing file: ${file.fileName}`);

    // Download file from Azure
    const blobServiceClient = BlobServiceClient.fromConnectionString(
      config.azure.connectionString
    );

    const containerClient = blobServiceClient.getContainerClient('rcm-attachments');
    const blockBlobClient = containerClient.getBlockBlobClient(file.filePath);

    const downloadResponse = await blockBlobClient.download();
    const fileContent = await streamToString(downloadResponse.readableStreamBody);

    // Download again as buffer for uploading to attachment location
    const downloadResponseBuffer = await blockBlobClient.download();
    const fileBuffer = await streamToBuffer(downloadResponseBuffer.readableStreamBody);

    logger(`[Outbound File Processor] ✅ Downloaded file: ${file.fileName}`);

    // Check if it's a PleaseRead file
    if (file.fileName.toLowerCase().includes('pleaseread')) {
      logger(`[Outbound File Processor] 📋 Processing PleaseRead file...`);
      await processPleaseReadFile(db, config, file, fileContent, fileBuffer, logger);
    } else if (
      file.fileName.toLowerCase().includes('errorreport') ||
      file.fileName.toLowerCase().includes('error')
    ) {
      logger(`[Outbound File Processor] 📋 Processing ErrorReport file...`);
      await processErrorReportFile(db, config, file, fileContent, fileBuffer, logger);
    } else {
      // Other outbound files are just marked as processed
      logger(`[Outbound File Processor] ℹ️  Non-error file, marking as processed`);
      await db
        .update(db.schema.outboundFiles)
        .set({
          processed: true,
          updatedAt: getCSTTimestamp(),
        })
        .where(db.eq(db.schema.outboundFiles.id, file.id));
    }

    logger(`[Outbound File Processor] ✅ Successfully processed: ${file.fileName}`);
  } catch (error) {
    logger(`[Outbound File Processor] ❌ Failed to process ${file.fileName}: ${error.message}`);
    throw error;
  }
}

/**
 * Process a "PleaseRead" error file from Office Ally
 */
async function processPleaseReadFile(db, config, file, fileContent, fileBuffer, logger) {
  try {
    // ========== STEP 1: Extract FileName field ==========
    logger('[Outbound File Processor] STEP 1/6: Extracting FileName from content...');
    const fileNameMatch = fileContent.match(/^\s*FileName:\s*(.+?)\s*$/m);
    if (!fileNameMatch || !fileNameMatch[1]) {
      logger('[Outbound File Processor] ⚠️  Could not extract FileName from PleaseRead file');
      return;
    }

    const extractedFileName = fileNameMatch[1].trim();
    logger(`[Outbound File Processor] ✅ Extracted filename: ${extractedFileName}`);

    // ========== STEP 2: Find matching inbound_files ==========
    logger('[Outbound File Processor] STEP 2/6: Finding matching inbound_files record...');
    const matchingInboundFile = await db
      .select({
        id: db.schema.inboundFiles.id,
        claimId: db.schema.inboundFiles.claimId,
        fileName: db.schema.inboundFiles.fileName,
      })
      .from(db.schema.inboundFiles)
      .where(
        db.sql`LOWER(TRIM(${db.schema.inboundFiles.fileName})) LIKE LOWER(TRIM(${`%${extractedFileName}%`}))`
      )
      .limit(1);

    if (!matchingInboundFile || matchingInboundFile.length === 0) {
      logger(`[Outbound File Processor] ⚠️  No matching inbound file found for: ${extractedFileName}`);
      return;
    }

    const inboundFile = matchingInboundFile[0];
    const inboundFileId = inboundFile.id;
    const claimId = inboundFile.claimId;
    logger(`[Outbound File Processor] ✅ Found matching claim: ${claimId}`);

    // ========== STEP 3: UPLOAD to Azure attachment storage ==========
    logger('[Outbound File Processor] STEP 3/6: Uploading file to Azure attachment storage...');
    process.env.AZURE_STORAGE_CONNECTION_STRING = config.azure.connectionString;

    // Dynamic import of AzureBlobAttachmentManager
    const { AzureBlobAttachmentManager } = await import('../utils/azureBlobAttachmentManager.js');
    const attachmentManager = new AzureBlobAttachmentManager();
    const uploadResult = await attachmentManager.uploadFile(
      claimId,
      fileBuffer,
      file.fileName,
      'text/plain',
      'claim'
    );

    if (!uploadResult.success || !uploadResult.fileKey) {
      logger(`[Outbound File Processor] ❌ Failed to upload file to Azure: ${uploadResult.error}`);
      throw new Error(`File upload failed: ${uploadResult.error}`);
    }

    const fileKey = uploadResult.fileKey;
    logger(`[Outbound File Processor] ✅ Uploaded file to Azure: ${fileKey}`);

    // ========== STEP 4: INSERT into claimAttachments ==========
    logger('[Outbound File Processor] STEP 4/6: Inserting into claimAttachments...');
    const cstTimestamp = getCSTTimestamp();
    await db.insert(db.schema.claimAttachments).values({
      typeId: claimId,
      clmAttPath: fileKey,
      clmAttFilename: file.fileName,
      clmAttDatetime: cstTimestamp,
      clmLogin: 'system',
      attachmentType: 'claim',
      user_id: null,
      createdAt: cstTimestamp,
      updatedAt: cstTimestamp,
    });
    logger(`[Outbound File Processor] ✅ Inserted into claimAttachments`);

    // ========== STEP 5: INSERT into claimProcessingHistory ==========
    logger('[Outbound File Processor] STEP 5/6: Inserting into claimProcessingHistory...');
    await db.insert(db.schema.claimProcessingHistory).values({
      claimId: claimId,
      inboundFileId: inboundFileId,
      clearingHouseId: file.clearingHouseId,
      outboundFileId: file.id,
      processType: 'receive_response',
      processDescription: 'Received Instructions while Sending Claim to Clearing House',
      response: {
        fileContent: fileContent,
        extractedFileName: extractedFileName,
        originalFileName: file.fileName,
        receivedAt: getCSTTimestamp().toISOString(),
        errorType: 'FILE_NOT_PROCESSED',
        message: 'Office Ally Received File but Could not Process - Filename Keywords not Recognized',
      },
      processedBy: 'system',
      processedAt: getCSTTimestamp(),
    });
    logger(`[Outbound File Processor] ✅ Inserted into claimProcessingHistory`);

    // ========== STEP 6: UPDATE claim_header status to 'G' ==========
    logger('[Outbound File Processor] STEP 6/6: Updating claim_header status to G...');
    await db
      .update(db.schema.claimHeader)
      .set({
        status: 'G',
        updatedAt: getCSTTimestamp(),
      })
      .where(db.eq(db.schema.claimHeader.claimId, claimId));
    logger(`[Outbound File Processor] ✅ Updated claim_header status to G`);

    // ========== ALL STEPS SUCCEED: UPDATE outbound_files processed=true ==========
    logger('[Outbound File Processor] ✅ All steps completed successfully, marking file as processed...');
    await db
      .update(db.schema.outboundFiles)
      .set({
        processed: true,
        updatedAt: getCSTTimestamp(),
      })
      .where(db.eq(db.schema.outboundFiles.id, file.id));

    logger(`[Outbound File Processor] ✅ Successfully completed processing for claim: ${claimId}`);
  } catch (error) {
    logger(`[Outbound File Processor] ❌ Error processing PleaseRead file: ${error.message}`);
    logger('[Outbound File Processor] File will be retried in next schedule (processed stays false)');
    // Don't update processed flag - it stays false for retry
  }
}

/**
 * Process Error Report file from clearing house
 */
async function processErrorReportFile(db, config, file, fileContent, fileBuffer, logger) {
  try {
    // ========== STEP 1: Extract File Name and Error Description ==========
    logger('[Outbound File Processor] STEP 1/6: Extracting File Name and Error Description from ErrorReport...');

    // Extract File Name field (handles leading whitespace)
    const fileNameMatch = fileContent.match(/^\s*File Name:\s*(.+?)\s*$/m);
    if (!fileNameMatch || !fileNameMatch[1]) {
      logger('[Outbound File Processor] ⚠️  Could not extract File Name from ErrorReport');
      return;
    }

    const extractedFileName = fileNameMatch[1].trim();
    logger(`[Outbound File Processor] ✅ Extracted File Name: ${extractedFileName}`);

    // Extract Error Description (handles multiline errors)
    const errorDescMatch = fileContent.match(/Error Description:\s*([^\n]+(?:\n(?!Technician|File ID|Date Uploaded)[^\n]*)*)/i);
    const errorDescription = errorDescMatch ? errorDescMatch[1].trim() : 'Unknown error';
    logger(`[Outbound File Processor] ✅ Extracted Error: ${errorDescription.substring(0, 100)}...`);

    // ========== STEP 2: Find matching inbound_files record by filename ==========
    logger('[Outbound File Processor] STEP 2/6: Finding matching inbound_files record...');

    // Try exact match first
    let matchingInboundFile = await db
      .select({
        id: db.schema.inboundFiles.id,
        claimId: db.schema.inboundFiles.claimId,
        fileName: db.schema.inboundFiles.fileName,
      })
      .from(db.schema.inboundFiles)
      .where(db.sql`LOWER(TRIM(${db.schema.inboundFiles.fileName})) = LOWER(TRIM(${extractedFileName}))`)
      .limit(1);

    if (!matchingInboundFile || matchingInboundFile.length === 0) {
      logger(`[Outbound File Processor] ⚠️  No matching inbound file found for: ${extractedFileName}`);
      return;
    }

    const inboundFile = matchingInboundFile[0];
    const inboundFileId = inboundFile.id;
    const claimId = inboundFile.claimId;
    logger(`[Outbound File Processor] ✅ Found matching claim: ${claimId}`);

    // ========== STEP 3: UPLOAD to Azure attachment storage ==========
    logger('[Outbound File Processor] STEP 3/6: Uploading file to Azure attachment storage...');
    process.env.AZURE_STORAGE_CONNECTION_STRING = config.azure.connectionString;

    const { AzureBlobAttachmentManager } = await import('../utils/azureBlobAttachmentManager.js');
    const attachmentManager = new AzureBlobAttachmentManager();
    const uploadResult = await attachmentManager.uploadFile(
      claimId,
      fileBuffer,
      file.fileName,
      'text/plain',
      'claim'
    );

    if (!uploadResult.success || !uploadResult.fileKey) {
      logger(`[Outbound File Processor] ❌ Failed to upload file to Azure: ${uploadResult.error}`);
      throw new Error(`File upload failed: ${uploadResult.error}`);
    }

    const fileKey = uploadResult.fileKey;
    logger(`[Outbound File Processor] ✅ Uploaded file to Azure: ${fileKey}`);

    // ========== STEP 4: INSERT into claimAttachments ==========
    logger('[Outbound File Processor] STEP 4/6: Inserting into claimAttachments...');
    const cstTimestamp = getCSTTimestamp();
    await db.insert(db.schema.claimAttachments).values({
      typeId: claimId,
      clmAttPath: fileKey,
      clmAttFilename: file.fileName,
      clmAttDatetime: cstTimestamp,
      clmLogin: 'system',
      attachmentType: 'claim',
      user_id: null,
      createdAt: cstTimestamp,
      updatedAt: cstTimestamp,
    });
    logger(`[Outbound File Processor] ✅ Inserted into claimAttachments`);

    // ========== STEP 5: INSERT into claimProcessingHistory ==========
    logger('[Outbound File Processor] STEP 5/6: Inserting into claimProcessingHistory...');
    await db.insert(db.schema.claimProcessingHistory).values({
      claimId: claimId,
      inboundFileId: inboundFileId,
      clearingHouseId: file.clearingHouseId,
      outboundFileId: file.id,
      processType: 'receive_response',
      processDescription: 'Received Error Report from Clearing House',
      response: {
        errorReportFileName: file.fileName,
        originalFileName: extractedFileName,
        errorDescription: errorDescription,
        fileContent: fileContent,
        receivedAt: getCSTTimestamp().toISOString(),
        errorType: 'File Processing Failed',
        message: 'Error Report Received From Clearing House - Claim Review and Corrections Needed',
      },
      processedBy: 'system',
      processedAt: getCSTTimestamp(),
    });
    logger(`[Outbound File Processor] ✅ Inserted into claimProcessingHistory`);

    // ========== STEP 6: UPDATE claim_header status to 'G' (Processed) ==========
    logger('[Outbound File Processor] STEP 6/6: Updating claim_header status to G...');
    await db
      .update(db.schema.claimHeader)
      .set({
        status: 'G',
        updatedAt: getCSTTimestamp(),
      })
      .where(db.eq(db.schema.claimHeader.claimId, claimId));
    logger(`[Outbound File Processor] ✅ Updated claim_header status to G (Processed)`);

    // ========== ALL STEPS SUCCEED: UPDATE outbound_files processed=true ==========
    logger('[Outbound File Processor] ✅ All steps completed successfully, marking file as processed...');
    await db
      .update(db.schema.outboundFiles)
      .set({
        processed: true,
        updatedAt: getCSTTimestamp(),
      })
      .where(db.eq(db.schema.outboundFiles.id, file.id));

    logger(`[Outbound File Processor] ✅ Successfully completed processing for claim: ${claimId}`);
  } catch (error) {
    logger(`[Outbound File Processor] ❌ Error processing ErrorReport file: ${error.message}`);
    logger('[Outbound File Processor] File will be retried in next schedule (processed stays false)');
    // Don't update processed flag - it stays false for retry
  }
}

/**
 * Process stream to string
 */
async function streamToString(readableStream) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    readableStream.on('data', (data) => {
      chunks.push(data.toString('utf8'));
    });
    readableStream.on('end', () => {
      resolve(chunks.join(''));
    });
    readableStream.on('error', reject);
  });
}

/**
 * Process stream to buffer
 */
async function streamToBuffer(readableStream) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    readableStream.on('data', (data) => {
      chunks.push(data);
    });
    readableStream.on('end', () => {
      resolve(Buffer.concat(chunks));
    });
    readableStream.on('error', reject);
  });
}

module.exports = {
  getProcessorConfig,
  validateProcessorConfig,
  getCSTTimestamp,
  processOutboundFilesJob,
  processFile,
  processPleaseReadFile,
  processErrorReportFile,
  streamToString,
  streamToBuffer,
};
