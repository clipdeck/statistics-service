import { getTikTokStats } from './tiktok';
import { getInstagramStats } from './instagram';
import { getYouTubeStats } from './youtube';
import { getTwitterStats } from './twitter';
import { logger } from '../lib/logger';

export interface PlatformStats {
  views: number;
  likes: number;
  comments: number;
  shares: number;
  thumbnailUrl?: string | null;
}

/**
 * Unified platform stats fetcher.
 * Routes to the correct platform API based on the platform string.
 * Returns normalized stats across all platforms.
 */
export async function fetchPlatformStats(
  platform: string,
  videoId: string
): Promise<PlatformStats> {
  try {
    switch (platform) {
      case 'YOUTUBE': {
        const youtubeStats = await getYouTubeStats(videoId);
        return {
          views: youtubeStats.views,
          likes: youtubeStats.likes,
          comments: youtubeStats.comments,
          shares: youtubeStats.shares,
          thumbnailUrl: youtubeStats.thumbnailUrl,
        };
      }

      case 'TIKTOK': {
        const tiktokStats = await getTikTokStats(videoId);
        return {
          views: tiktokStats.views,
          likes: tiktokStats.likes,
          comments: tiktokStats.comments,
          shares: tiktokStats.shares,
          thumbnailUrl: tiktokStats.thumbnailUrl,
        };
      }

      case 'INSTAGRAM': {
        try {
          const instagramStats = await getInstagramStats(videoId);
          logger.info({ videoId }, 'Instagram stats fetched successfully');
          return {
            views: instagramStats.views,
            likes: instagramStats.likes,
            comments: instagramStats.comments,
            shares: instagramStats.shares,
            thumbnailUrl: instagramStats.thumbnailUrl,
          };
        } catch (error: any) {
          logger.error(
            { videoId, message: error.message },
            'Instagram stats fetch failed'
          );
          return {
            views: 0,
            likes: 0,
            comments: 0,
            shares: 0,
            thumbnailUrl: null,
          };
        }
      }

      case 'TWITTER': {
        const twitterStats = await getTwitterStats(videoId);
        if (twitterStats) {
          return {
            views: twitterStats.views,
            likes: twitterStats.likes,
            comments: twitterStats.comments,
            shares: twitterStats.shares,
            thumbnailUrl: twitterStats.thumbnailUrl,
          };
        }
        return {
          views: 0,
          likes: 0,
          comments: 0,
          shares: 0,
          thumbnailUrl: null,
        };
      }

      default:
        throw new Error(`Unsupported platform: ${platform}`);
    }
  } catch (error) {
    logger.error(error, `Error fetching stats for platform ${platform}`);
    throw error;
  }
}

export { getTikTokStats } from './tiktok';
export { getInstagramStats } from './instagram';
export { getYouTubeStats } from './youtube';
export { getTwitterStats } from './twitter';
