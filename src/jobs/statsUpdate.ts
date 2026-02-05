/**
 * Standalone Stats Update Job
 * Entry point for Kubernetes CronJob. Fetches clips needing refresh
 * from clip-service, runs batch stats update, then exits.
 */

import { logger } from '../lib/logger';
import { config } from '../config';
import { batchRefreshStats } from '../services/statsCollector';
import { redis } from '../lib/redis';
import axios from 'axios';

async function main(): Promise<void> {
  logger.info('Stats update job started');

  try {
    // Fetch clips that need refresh from clip-service
    const response = await axios.get(
      `${config.clipServiceUrl}/clips/needs-refresh`,
      {
        headers: {
          'X-Internal-Service': 'statistics-service',
        },
        timeout: 30000,
      }
    );

    const clips: Array<{ id: string; platform: string; videoId: string }> =
      response.data.clips || [];

    logger.info({ clipCount: clips.length }, 'Fetched clips needing refresh');

    if (clips.length === 0) {
      logger.info('No clips need refresh, exiting');
      await redis.quit();
      process.exit(0);
    }

    // Run batch refresh
    const result = await batchRefreshStats(clips);

    logger.info(
      {
        total: clips.length,
        success: result.successCount,
        failed: result.failCount,
      },
      'Stats update job completed'
    );

    // Clean up
    await redis.quit();
    process.exit(0);
  } catch (error) {
    logger.error(error, 'Stats update job failed');
    await redis.quit();
    process.exit(1);
  }
}

main();
