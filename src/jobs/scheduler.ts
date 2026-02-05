/**
 * Scheduler
 * Node-cron based job scheduler for periodic tasks.
 */

import cron from 'node-cron';
import axios from 'axios';
import { logger } from '../lib/logger';
import { config } from '../config';
import { batchRefreshStats } from '../services/statsCollector';
import {
  calculateWeeklyClipRankings,
  calculateWeeklyCampaignRankings,
} from '../services/rankingsService';

/**
 * Start all scheduled jobs
 */
export function startScheduler(): void {
  // Hourly stats refresh - runs at the top of every hour
  cron.schedule('0 * * * *', async () => {
    logger.info('Running hourly stats refresh...');

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

      if (clips.length > 0) {
        const result = await batchRefreshStats(clips);
        logger.info(
          {
            total: clips.length,
            success: result.successCount,
            failed: result.failCount,
          },
          'Hourly stats refresh completed'
        );
      } else {
        logger.info('No clips need refresh');
      }
    } catch (error) {
      logger.error(error, 'Error in hourly stats refresh');
    }
  });

  // Daily rankings calculation - runs at midnight
  cron.schedule('0 0 * * *', async () => {
    logger.info('Running daily rankings calculation...');

    try {
      await Promise.all([
        calculateWeeklyClipRankings(),
        calculateWeeklyCampaignRankings(),
      ]);
      logger.info('Daily rankings calculation completed');
    } catch (error) {
      logger.error(error, 'Error in daily rankings calculation');
    }
  });

  logger.info('Scheduler started');
}
