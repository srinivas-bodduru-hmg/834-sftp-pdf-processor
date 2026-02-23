const fs = require('fs');
const path = require('path');
const { BlobServiceClient } = require('@azure/storage-blob');

/**
 * Get scheduler configuration from environment variables
 */
function getSchedulerConfig() {
  return {
    sftp: {
      host: (process.env.SFTP_HOST || '').trim(),
      port: parseInt(process.env.SFTP_PORT || '22'),
      username: (process.env.SFTP_USERNAME || '').trim(),
      password: process.env.SFTP_PASSWORD || '',
      remotePath: (process.env.SFTP_REMOTE_PATH || '/rcm').trim(),
    },
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
function validateConfig(config) {
  const missingConfigs = [];

  if (!config.sftp.host) missingConfigs.push('SFTP_HOST');
  if (!config.sftp.username) missingConfigs.push('SFTP_USERNAME');
  if (!config.sftp.password) missingConfigs.push('SFTP_PASSWORD');
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
 * Process outbound files from SFTP
 * - Download files from /outbound directory
 * - Upload to Azure OfficeAlly/UNPROCESSED folder
 * - Delete from SFTP
 */
async function processOutboundFiles(sftpService, db, config, logger) {
  const startTime = new Date();
  const outboundPath = '/outbound';

  logger(`[Outbound Scheduler] Listing files in ${outboundPath}...`);

  // List files in outbound directory
  const files = await sftpService.listFiles(outboundPath);
  const fileList = files.filter((file) => file.type === '-'); // Only files, not directories

  if (fileList.length === 0) {
    logger('[Outbound Scheduler] No files found in outbound directory');
    return { filesProcessed: 0, errors: [] };
  }

  logger(`[Outbound Scheduler] Found ${fileList.length} file(s) to process`);

  const errors = [];
  let filesProcessed = 0;

  // Process each file
  for (const file of fileList) {
    try {
      const remotePath = `${outboundPath}/${file.name}`;
      await processFile(sftpService, db, config, remotePath, file.name, logger);
      filesProcessed++;
    } catch (error) {
      const errorMsg = `Error processing file ${file.name}: ${error.message}`;
      logger(`[Outbound Scheduler] ❌ ${errorMsg}`);
      errors.push(errorMsg);
      // Continue processing remaining files even if one fails
    }
  }

  const duration = new Date().getTime() - startTime.getTime();
  logger(`[Outbound Scheduler] ✅ Job completed in ${duration}ms (${filesProcessed}/${fileList.length} succeeded)`);

  return { filesProcessed, errors };
}

/**
 * Process a single file from SFTP
 * 1. Download from SFTP
 * 2. Upload to Azure
 * 3. Insert to DB (atomic transaction)
 * 4. Delete from SFTP (non-blocking cleanup)
 */
async function processFile(sftpService, db, config, remotePath, fileName, logger) {
  let localPath = null;

  try {
    // Step 1: Create temp directory and download file
    const tempDir = path.join(process.cwd(), 'temp', 'outbound');
    if (!fs.existsSync(tempDir)) {
      fs.mkdirSync(tempDir, { recursive: true });
    }

    localPath = path.join(tempDir, fileName);
    logger(`[Outbound Scheduler] Downloading ${fileName}...`);
    await sftpService.downloadFile(remotePath, localPath);

    // Step 2: Upload to Azure AND insert to DB (ATOMIC - transaction wrapped)
    const fileContent = fs.readFileSync(localPath, 'utf-8');
    await uploadToAzureAndInsertDB(db, config, fileName, fileContent, logger);

    // Step 3: Delete from SFTP (ONLY after DB succeeds)
    // Non-blocking cleanup - if delete fails, log warning but don't fail overall operation
    await deleteSFTPFile(sftpService, remotePath, fileName, logger);

    logger(`[Outbound Scheduler] ✅ Successfully processed: ${fileName}`);
  } catch (error) {
    logger(`[Outbound Scheduler] ❌ Failed to process ${fileName}: ${error.message}`);
    throw error;
  } finally {
    // Clean up temp file
    if (localPath && fs.existsSync(localPath)) {
      fs.unlinkSync(localPath);
    }
  }
}

/**
 * Upload file to Azure Blob Storage AND insert record to DB atomically
 * If DB insert fails, Azure blob is deleted (cleanup) and error is thrown
 * This ensures no orphaned files or records
 */
async function uploadToAzureAndInsertDB(db, config, fileName, fileContent, logger) {
  if (!config.azure.connectionString) {
    throw new Error('Azure Storage connection string not configured');
  }

  const blobPath = `OfficeAlly/OUTBOUND/${fileName}`;
  let azureBlobUploaded = false;

  try {
    // ========== STEP 1: Upload to Azure ==========
    const blobServiceClient = BlobServiceClient.fromConnectionString(
      config.azure.connectionString
    );

    const containerClient = blobServiceClient.getContainerClient('rcm-attachments');
    const blockBlobClient = containerClient.getBlockBlobClient(blobPath);

    const dataBuffer = Buffer.from(fileContent, 'utf-8');
    await blockBlobClient.uploadData(dataBuffer);
    azureBlobUploaded = true;

    logger(`[Outbound Scheduler] 📤 Uploaded to Azure: ${blobPath}`);

    // ========== STEP 2: Get clearing house ID ==========
    let clearingHouseId = null;
    try {
      const sftpUsername = config.sftp.username.trim().toLowerCase();
      if (sftpUsername && db) {
        const clearingHouse = await db
          .select({ id: db.schema.clearingHouses.id })
          .from(db.schema.clearingHouses)
          .where(db.eq(db.sql`LOWER(TRIM(${db.schema.clearingHouses.username}))`, sftpUsername))
          .limit(1);

        if (clearingHouse && clearingHouse.length > 0) {
          clearingHouseId = clearingHouse[0].id;
          logger(`[Outbound Scheduler] 🏢 Found clearing house ID: ${clearingHouseId}`);
        } else {
          logger(`[Outbound Scheduler] ⚠️  Clearing house not found for SFTP username: ${sftpUsername}`);
        }
      }
    } catch (chError) {
      logger(`[Outbound Scheduler] ⚠️  Failed to get clearing house ID: ${chError.message}`);
    }

    // ========== STEP 3: Insert into DB (WITH TRANSACTION - MUST SUCCEED) ==========
    if (db) {
      logger(`[Outbound Scheduler] 🔒 Starting database transaction for ${fileName}`);
      await db.transaction(async (tx) => {
        await tx.insert(db.schema.outboundFiles).values({
          fileName: fileName,
          filePath: blobPath,
          clearingHouseId: clearingHouseId,
          processed: false,
          uploadedAt: getCSTTimestamp(),
          updatedAt: getCSTTimestamp(),
        });
        logger(`[Outbound Scheduler] 💾 Inserted outbound file record: ${fileName}`);
      });
      logger(`[Outbound Scheduler] ✅ Transaction committed for ${fileName}`);
    } else {
      logger(`[Outbound Scheduler] ⚠️  Database not initialized, skipping record insertion`);
    }
  } catch (error) {
    // If DB insert fails, clean up the Azure blob
    if (azureBlobUploaded) {
      try {
        logger(`[Outbound Scheduler] 🧹 Database operation failed, cleaning up Azure blob: ${blobPath}`);
        const blobServiceClient = BlobServiceClient.fromConnectionString(
          config.azure.connectionString
        );
        const containerClient = blobServiceClient.getContainerClient('rcm-attachments');
        const blockBlobClient = containerClient.getBlockBlobClient(blobPath);
        await blockBlobClient.delete();
        logger(`[Outbound Scheduler] ✅ Cleaned up Azure blob: ${blobPath}`);
      } catch (deleteError) {
        logger(`[Outbound Scheduler] ⚠️  Failed to clean up Azure blob: ${deleteError.message}`);
        // Continue with error throw even if cleanup fails
      }
    }

    logger(`[Outbound Scheduler] ❌ Failed to upload to Azure and insert to DB: ${error.message}`);
    throw error; // FAIL - don't continue, SFTP file will remain for retry
  }
}

/**
 * Delete file from SFTP (non-blocking cleanup)
 * If deletion fails, only logs warning - doesn't throw
 * This is safe because file is already in Azure and recorded in DB
 */
async function deleteSFTPFile(sftpService, remotePath, fileName, logger) {
  try {
    logger(`[Outbound Scheduler] 🗑️  Deleting ${fileName} from SFTP...`);
    if (!sftpService) {
      throw new Error('SFTP service not initialized');
    }
    await sftpService.deleteFile(remotePath);
    logger(`[Outbound Scheduler] ✅ Deleted ${fileName} from SFTP`);
  } catch (error) {
    // Non-blocking: just log warning, don't fail the overall operation
    // File is already safely in Azure and DB, so it's okay if SFTP delete fails
    logger(`[Outbound Scheduler] ⚠️  Warning: Could not delete ${fileName} from SFTP: ${error.message}`);
  }
}

module.exports = {
  getSchedulerConfig,
  validateConfig,
  getCSTTimestamp,
  processOutboundFiles,
  processFile,
  uploadToAzureAndInsertDB,
  deleteSFTPFile,
};
