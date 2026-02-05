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
import axios from 'axios';

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
    routingKeys: ['clip.submitted', 'clip.approved', 'stats.requested'],
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
        const { clipId, campaignId, userId } = event.payload;

        logger.info(
          { clipId, campaignId },
          'Clip approved - scheduling stats refresh'
        );

        try {
          // Fetch clip details from clip-service
          const response = await axios.get(
            `${config.clipServiceUrl}/clips/${clipId}`,
            {
              headers: {
                'X-Internal-Service': 'statistics-service',
              },
              timeout: 10000,
            }
          );

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
   * Log the submission for tracking purposes.
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
