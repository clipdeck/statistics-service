import type { FastifyInstance } from 'fastify';
import { requireStaff } from '../middleware/auth';
import { sendError, badRequest } from '../lib/errors';
import { logger } from '../lib/logger';
import { startOfWeek } from 'date-fns';
import {
  calculateWeeklyClipRankings,
  calculateWeeklyCampaignRankings,
  getWeeklyClipRankings,
  getWeeklyCampaignRankings,
} from '../services/rankingsService';

export async function rankingsRoutes(app: FastifyInstance) {
  /**
   * GET /rankings/weekly-clips
   * Get weekly clip rankings. Query params: weekStart, limit, platform
   */
  app.get<{
    Querystring: {
      weekStart?: string;
      limit?: string;
      platform?: string;
    };
  }>('/weekly-clips', async (request, reply) => {
    try {
      const { weekStart: weekStartStr, limit: limitStr, platform } = request.query;

      const weekStart = weekStartStr
        ? new Date(weekStartStr)
        : startOfWeek(new Date(), { weekStartsOn: 1 });

      if (isNaN(weekStart.getTime())) {
        throw badRequest('Invalid weekStart date');
      }

      const limit = limitStr ? parseInt(limitStr, 10) : 50;
      if (isNaN(limit) || limit < 1 || limit > 200) {
        throw badRequest('limit must be between 1 and 200');
      }

      const rankings = await getWeeklyClipRankings(weekStart, limit, platform);

      return reply.send({
        weekStart: weekStart.toISOString(),
        rankings,
        total: rankings.length,
      });
    } catch (error) {
      sendError(reply, error);
    }
  });

  /**
   * GET /rankings/weekly-campaigns
   * Get weekly campaign rankings. Query params: weekStart, limit
   */
  app.get<{
    Querystring: {
      weekStart?: string;
      limit?: string;
    };
  }>('/weekly-campaigns', async (request, reply) => {
    try {
      const { weekStart: weekStartStr, limit: limitStr } = request.query;

      const weekStart = weekStartStr
        ? new Date(weekStartStr)
        : startOfWeek(new Date(), { weekStartsOn: 1 });

      if (isNaN(weekStart.getTime())) {
        throw badRequest('Invalid weekStart date');
      }

      const limit = limitStr ? parseInt(limitStr, 10) : 50;
      if (isNaN(limit) || limit < 1 || limit > 200) {
        throw badRequest('limit must be between 1 and 200');
      }

      const rankings = await getWeeklyCampaignRankings(weekStart, limit);

      return reply.send({
        weekStart: weekStart.toISOString(),
        rankings,
        total: rankings.length,
      });
    } catch (error) {
      sendError(reply, error);
    }
  });

  /**
   * POST /rankings/calculate
   * Trigger rankings calculation. Requires staff access.
   */
  app.post('/calculate', async (request, reply) => {
    try {
      requireStaff(request);

      const body = request.body as { weekStart?: string } | undefined;
      const weekStart = body?.weekStart ? new Date(body.weekStart) : undefined;

      if (weekStart && isNaN(weekStart.getTime())) {
        throw badRequest('Invalid weekStart date');
      }

      logger.info({ weekStart: weekStart?.toISOString() }, 'Triggering rankings calculation');

      // Run both calculations
      await Promise.all([
        calculateWeeklyClipRankings(weekStart),
        calculateWeeklyCampaignRankings(weekStart),
      ]);

      return reply.send({
        message: 'Rankings calculation completed',
        weekStart: (weekStart || startOfWeek(new Date(), { weekStartsOn: 1 })).toISOString(),
      });
    } catch (error) {
      sendError(reply, error);
    }
  });
}
