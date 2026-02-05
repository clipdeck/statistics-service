import type { FastifyInstance } from 'fastify';
import { requireAuth, requireStaff } from '../middleware/auth';
import { sendError, badRequest, notFound } from '../lib/errors';
import { logger } from '../lib/logger';
import {
  refreshClipStats,
  batchRefreshStats,
  getOrFetchStats,
} from '../services/statsCollector';
import { runBotDetection } from '../services/botDetection';
import axios from 'axios';
import { config } from '../config';

export async function statsRoutes(app: FastifyInstance) {
  /**
   * POST /stats/refresh/:clipId
   * Refresh stats for a single clip. Requires authentication.
   */
  app.post<{ Params: { clipId: string } }>(
    '/refresh/:clipId',
    async (request, reply) => {
      try {
        requireAuth(request);

        const { clipId } = request.params;
        if (!clipId) {
          throw badRequest('clipId is required');
        }

        // Fetch clip metadata from clip-service
        const clipResponse = await axios.get(
          `${config.clipServiceUrl}/clips/${clipId}`,
          {
            headers: {
              'X-Internal-Service': 'statistics-service',
            },
            timeout: 10000,
          }
        );

        const clip = clipResponse.data;
        if (!clip || !clip.platformVideoId) {
          throw notFound('Clip not found or missing video ID');
        }

        const stats = await refreshClipStats(
          clipId,
          clip.platform,
          clip.platformVideoId
        );

        // Also run bot detection
        const botResult = await runBotDetection(clipId);

        return reply.send({
          stats,
          botDetection: botResult,
        });
      } catch (error) {
        sendError(reply, error);
      }
    }
  );

  /**
   * GET /stats/:clipId
   * Get cached stats for a clip (from Redis or fetch fresh).
   */
  app.get<{ Params: { clipId: string } }>(
    '/:clipId',
    async (request, reply) => {
      try {
        const { clipId } = request.params;
        if (!clipId) {
          throw badRequest('clipId is required');
        }

        // Fetch clip metadata from clip-service
        const clipResponse = await axios.get(
          `${config.clipServiceUrl}/clips/${clipId}`,
          {
            headers: {
              'X-Internal-Service': 'statistics-service',
            },
            timeout: 10000,
          }
        );

        const clip = clipResponse.data;
        if (!clip || !clip.platformVideoId) {
          throw notFound('Clip not found or missing video ID');
        }

        const stats = await getOrFetchStats(
          clipId,
          clip.platform,
          clip.platformVideoId
        );

        return reply.send({ stats });
      } catch (error) {
        sendError(reply, error);
      }
    }
  );

  /**
   * POST /stats/batch-refresh
   * Batch refresh stats for multiple clips. Requires staff access.
   */
  app.post('/batch-refresh', async (request, reply) => {
    try {
      requireStaff(request);

      const body = request.body as {
        clips?: Array<{ id: string; platform: string; videoId: string }>;
      };

      if (!body.clips || !Array.isArray(body.clips) || body.clips.length === 0) {
        throw badRequest('clips array is required');
      }

      if (body.clips.length > 500) {
        throw badRequest('Maximum 500 clips per batch');
      }

      const result = await batchRefreshStats(body.clips);

      return reply.send(result);
    } catch (error) {
      sendError(reply, error);
    }
  });
}
