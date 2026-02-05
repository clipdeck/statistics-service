/**
 * Stats Collection Service
 * Handles fetching, caching, and publishing platform statistics
 */

import { fetchPlatformStats, type PlatformStats } from '../platforms';
import { redis } from '../lib/redis';
import { publisher, StatsEvents, SERVICE_NAME } from '../lib/events';
import { logger } from '../lib/logger';

const CACHE_TTL_SECONDS = 3600; // 1 hour

/**
 * Get the Redis cache key for a clip's stats
 */
function getCacheKey(platform: string, videoId: string): string {
  return `stats:${platform}:${videoId}`;
}

/**
 * Get cached stats for a clip from Redis
 */
export async function getCachedStats(
  platform: string,
  videoId: string
): Promise<PlatformStats | null> {
  try {
    const cached = await redis.get(getCacheKey(platform, videoId));
    if (cached) {
      return JSON.parse(cached) as PlatformStats;
    }
    return null;
  } catch (error) {
    logger.error(error, 'Error reading stats from cache');
    return null;
  }
}

/**
 * Refresh stats for a single clip.
 * Fetches from the platform API, caches in Redis, and publishes a stats.updated event.
 */
export async function refreshClipStats(
  submissionId: string,
  platform: string,
  videoId: string
): Promise<PlatformStats> {
  logger.info({ submissionId, platform, videoId }, 'Refreshing clip stats');

  // Fetch fresh stats from the platform
  const stats = await fetchPlatformStats(platform, videoId);

  // Cache in Redis with TTL
  const cacheKey = getCacheKey(platform, videoId);
  try {
    await redis.setex(cacheKey, CACHE_TTL_SECONDS, JSON.stringify(stats));
  } catch (error) {
    logger.error(error, 'Error caching stats in Redis');
  }

  // Calculate engagement rate
  const engagement =
    stats.views > 0 ? (stats.likes + stats.comments) / stats.views : 0;

  // Publish stats.updated event
  try {
    const event = StatsEvents.updated(
      {
        clipId: submissionId,
        views: stats.views,
        likes: stats.likes,
        comments: stats.comments,
        shares: stats.shares,
        engagement,
      },
      SERVICE_NAME
    );
    await publisher.publish(event);
  } catch (error) {
    logger.error(error, 'Error publishing stats.updated event');
  }

  logger.info(
    {
      submissionId,
      views: stats.views,
      likes: stats.likes,
      comments: stats.comments,
      shares: stats.shares,
    },
    'Clip stats refreshed successfully'
  );

  return stats;
}

/**
 * Batch refresh stats for multiple clips with rate limiting.
 * Processes clips sequentially with a 100ms delay between each.
 */
export async function batchRefreshStats(
  clips: Array<{ id: string; platform: string; videoId: string }>
): Promise<{ successCount: number; failCount: number }> {
  let successCount = 0;
  let failCount = 0;

  for (const clip of clips) {
    try {
      await refreshClipStats(clip.id, clip.platform, clip.videoId);
      successCount++;
    } catch (error) {
      logger.error(
        { clipId: clip.id, platform: clip.platform },
        'Error refreshing clip stats in batch'
      );
      failCount++;
    }

    // Rate limiting: wait 100ms between requests
    await new Promise((resolve) => setTimeout(resolve, 100));
  }

  logger.info(
    { total: clips.length, successCount, failCount },
    'Batch stats refresh completed'
  );

  return { successCount, failCount };
}

/**
 * Get stats for a clip - from cache if available, otherwise fetch fresh.
 */
export async function getOrFetchStats(
  submissionId: string,
  platform: string,
  videoId: string
): Promise<PlatformStats> {
  // Try cache first
  const cached = await getCachedStats(platform, videoId);
  if (cached) {
    logger.debug({ submissionId }, 'Returning cached stats');
    return cached;
  }

  // Fetch fresh
  return refreshClipStats(submissionId, platform, videoId);
}
