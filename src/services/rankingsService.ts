/**
 * Rankings Service
 * Calculates and stores weekly rankings for clips and campaigns.
 */

import axios from 'axios';
import { startOfWeek, endOfWeek } from 'date-fns';
import { prisma } from '../lib/prisma';
import { config } from '../config';
import { logger } from '../lib/logger';
import type { Platform } from '@prisma/client';

// ============================================================================
// Types
// ============================================================================

interface ClipData {
  id: string;
  platform: string;
  campaignId: string;
  userId: string;
  views: number;
  likes: number;
  engagement: number;
}

interface CampaignData {
  id: string;
  title: string;
  totalViews: number;
  totalLikes: number;
  avgEngagement: number;
  clipsCount: number;
}

// ============================================================================
// Weekly Clip Rankings
// ============================================================================

/**
 * Calculate and store weekly clip rankings.
 * Fetches approved submissions from clip-service and ranks them by views.
 */
export async function calculateWeeklyClipRankings(
  weekStart?: Date
): Promise<void> {
  const now = new Date();
  const start = weekStart || startOfWeek(now, { weekStartsOn: 1 }); // Monday
  const end = endOfWeek(start, { weekStartsOn: 1 });

  logger.info(
    { weekStart: start.toISOString(), weekEnd: end.toISOString() },
    'Calculating weekly clip rankings'
  );

  try {
    // Fetch approved clips from clip-service
    const response = await axios.get(
      `${config.clipServiceUrl}/clips/approved-for-rankings`,
      {
        params: {
          weekStart: start.toISOString(),
          weekEnd: end.toISOString(),
        },
        headers: {
          'X-Internal-Service': 'statistics-service',
        },
        timeout: 30000,
      }
    );

    const clips: ClipData[] = response.data.clips || [];

    if (clips.length === 0) {
      logger.info('No clips found for weekly rankings');
      return;
    }

    // Sort by views descending, then by engagement
    const ranked = clips.sort((a, b) => {
      if (b.views !== a.views) return b.views - a.views;
      return b.engagement - a.engagement;
    });

    // Upsert rankings into local database
    for (let i = 0; i < ranked.length; i++) {
      const clip = ranked[i];
      const rank = i + 1;

      await prisma.weeklyClipRanking.upsert({
        where: {
          weekStart_submissionId: {
            weekStart: start,
            submissionId: clip.id,
          },
        },
        update: {
          views: clip.views,
          likes: clip.likes,
          engagement: clip.engagement,
          rank,
        },
        create: {
          weekStart: start,
          weekEnd: end,
          submissionId: clip.id,
          platform: clip.platform as Platform,
          views: clip.views,
          likes: clip.likes,
          engagement: clip.engagement,
          rank,
        },
      });
    }

    logger.info(
      { totalClips: ranked.length },
      'Weekly clip rankings calculated successfully'
    );
  } catch (error) {
    logger.error(error, 'Error calculating weekly clip rankings');
    throw error;
  }
}

// ============================================================================
// Weekly Campaign Rankings
// ============================================================================

/**
 * Calculate and store weekly campaign rankings.
 * Fetches active campaigns from campaign-service and ranks them by total views.
 */
export async function calculateWeeklyCampaignRankings(
  weekStart?: Date
): Promise<void> {
  const now = new Date();
  const start = weekStart || startOfWeek(now, { weekStartsOn: 1 });
  const end = endOfWeek(start, { weekStartsOn: 1 });

  logger.info(
    { weekStart: start.toISOString(), weekEnd: end.toISOString() },
    'Calculating weekly campaign rankings'
  );

  try {
    // Fetch campaign stats from clip-service (aggregated by campaign)
    const response = await axios.get(
      `${config.clipServiceUrl}/clips/campaign-stats-for-rankings`,
      {
        params: {
          weekStart: start.toISOString(),
          weekEnd: end.toISOString(),
        },
        headers: {
          'X-Internal-Service': 'statistics-service',
        },
        timeout: 30000,
      }
    );

    const campaigns: CampaignData[] = response.data.campaigns || [];

    if (campaigns.length === 0) {
      logger.info('No campaigns found for weekly rankings');
      return;
    }

    // Sort by total views descending
    const ranked = campaigns.sort((a, b) => {
      if (b.totalViews !== a.totalViews) return b.totalViews - a.totalViews;
      return b.avgEngagement - a.avgEngagement;
    });

    // Upsert rankings into local database
    for (let i = 0; i < ranked.length; i++) {
      const campaign = ranked[i];
      const rank = i + 1;

      await prisma.weeklyCampaignRanking.upsert({
        where: {
          weekStart_campaignId: {
            weekStart: start,
            campaignId: campaign.id,
          },
        },
        update: {
          totalViews: campaign.totalViews,
          totalLikes: campaign.totalLikes,
          avgEngagement: campaign.avgEngagement,
          clipsCount: campaign.clipsCount,
          rank,
        },
        create: {
          weekStart: start,
          weekEnd: end,
          campaignId: campaign.id,
          totalViews: campaign.totalViews,
          totalLikes: campaign.totalLikes,
          avgEngagement: campaign.avgEngagement,
          clipsCount: campaign.clipsCount,
          rank,
        },
      });
    }

    logger.info(
      { totalCampaigns: ranked.length },
      'Weekly campaign rankings calculated successfully'
    );
  } catch (error) {
    logger.error(error, 'Error calculating weekly campaign rankings');
    throw error;
  }
}

// ============================================================================
// Read Rankings
// ============================================================================

/**
 * Get weekly clip rankings from local database
 */
export async function getWeeklyClipRankings(
  weekStart: Date,
  limit: number = 50,
  platform?: string
) {
  const where: any = {
    weekStart,
  };

  if (platform) {
    where.platform = platform as Platform;
  }

  return prisma.weeklyClipRanking.findMany({
    where,
    orderBy: { rank: 'asc' },
    take: limit,
  });
}

/**
 * Get weekly campaign rankings from local database
 */
export async function getWeeklyCampaignRankings(
  weekStart: Date,
  limit: number = 50
) {
  return prisma.weeklyCampaignRanking.findMany({
    where: { weekStart },
    orderBy: { rank: 'asc' },
    take: limit,
  });
}
