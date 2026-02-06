/**
 * Event Handlers
 * Consumes events from RabbitMQ and triggers appropriate actions.
 */

import {
  createConsumer,
  withRetry,
  withLogging,
} from '@clipdeck/events';
import type { EventConsumer } from '@clipdeck/events';
import { config } from '../config';
import { logger } from '../lib/logger';
import { refreshClipStats } from '../services/statsCollector';
import { clipClient } from '../lib/serviceClients';
import { syncCampaignCache } from '../services/cacheService';

let consumer: EventConsumer | null = null;

/**
 * Start the event consumer and register handlers
 */
export async function startEventHandlers(): Promise<void> {
  consumer = createConsumer({
    serviceName: 'statistics-service',
    connectionUrl: config.rabbitmqUrl,
    queueName: 'statistics.events',
    exchange: config.eventExchange,
    routingKeys: ['clip.submitted', 'clip.approved', 'stats.requested', 'campaign.created', 'campaign.status_changed'],
    prefetchCount: 10,
    enableDeadLetterQueue: true,
    maxRetries: 3,
    enableLogging: true,
    logger: {
      info: (msg, data) => logger.info(data, msg),
      error: (msg, err) => logger.error(err, msg),
      debug: (msg, data) => logger.debug(data, msg),
    },
  });

  /**
   * Handle clip.approved events
   * Schedule a stats refresh for the approved clip.
   */
  consumer.on(
    'clip.approved',
    withRetry(
      withLogging(async (event, context) => {
        const { clipId, campaignId } = event.payload;

        logger.info(
          { clipId, campaignId },
          'Clip approved - scheduling stats refresh'
        );

        try {
          if (!clipClient) {
            logger.warn('Clip service URL not configured, skipping stats refresh');
            await context.ack();
            return;
          }

          const response = await clipClient.get(`/clips/${clipId}`);
          const clip = response.data;

          if (clip && clip.platformVideoId) {
            await refreshClipStats(clipId, clip.platform, clip.platformVideoId);
          } else {
            logger.warn({ clipId }, 'Clip has no platformVideoId, skipping stats refresh');
          }
        } catch (error) {
          logger.error({ clipId, error }, 'Error handling clip.approved event');
          throw error; // Re-throw for retry
        }

        await context.ack();
      }, { info: (msg: string, data?: unknown) => logger.info(data, msg) })
    )
  );

  /**
   * Handle clip.submitted events
   */
  consumer.on(
    'clip.submitted',
    withRetry(
      withLogging(async (event, context) => {
        const { clipId, platform, linkUrl } = event.payload;

        logger.info(
          { clipId, platform, linkUrl },
          'Clip submitted - registered for future stats tracking'
        );

        await context.ack();
      }, { info: (msg: string, data?: unknown) => logger.info(data, msg) })
    )
  );

  /**
   * Handle campaign.created - cache campaign data
   */
  consumer.on(
    'campaign.created',
    withRetry(
      withLogging(async (event, context) => {
        const { campaignId, title } = event.payload;

        await syncCampaignCache(campaignId, { title, status: 'ACTIVE' });

        logger.info({ campaignId, title }, 'Campaign created - cached');
        await context.ack();
      }, { info: (msg: string, data?: unknown) => logger.info(data, msg) })
    )
  );

  /**
   * Handle campaign.status_changed - update cache
   */
  consumer.on(
    'campaign.status_changed',
    withRetry(
      withLogging(async (event, context) => {
        const { campaignId, newStatus } = event.payload;

        await syncCampaignCache(campaignId, { status: newStatus });

        logger.debug({ campaignId, newStatus }, 'Campaign status changed - cache synced');
        await context.ack();
      }, { info: (msg: string, data?: unknown) => logger.info(data, msg) })
    )
  );

  await consumer.start();
  logger.info('Event handlers started');
}

/**
 * Stop the event consumer
 */
export async function stopEventHandlers(): Promise<void> {
  if (consumer) {
    await consumer.stop();
    consumer = null;
    logger.info('Event handlers stopped');
  }
}
