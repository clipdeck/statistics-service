/**
 * YouTube stats fetcher
 * Uses YouTube Data API v3 with native fetch (10k requests/day free tier)
 */

import { config } from '../config';
import { logger } from '../lib/logger';

export interface YouTubeStats {
  views: number;
  likes: number;
  comments: number;
  shares: number;
  title?: string;
  publishedAt?: string;
  thumbnailUrl?: string;
}

/**
 * Fetches statistics for a YouTube video
 * @param videoId - YouTube video ID
 * @returns Video statistics
 */
export async function getYouTubeStats(videoId: string): Promise<YouTubeStats> {
  if (!config.youtubeApiKey) {
    throw new Error('YOUTUBE_API_KEY is not configured');
  }

  if (!videoId) {
    throw new Error('videoId is required');
  }

  try {
    const url = `https://www.googleapis.com/youtube/v3/videos?part=statistics,snippet&id=${videoId}&key=${config.youtubeApiKey}`;

    logger.info({ videoId }, 'Fetching YouTube stats');
    const response = await fetch(url, {
      headers: {
        'User-Agent':
          'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      },
    });

    if (!response.ok) {
      const errorBody = await response.text();
      logger.error({ errorBody, status: response.status }, 'YouTube API error response');
      throw new Error(`YouTube API error: ${response.status} ${response.statusText}`);
    }

    const data = (await response.json()) as {
      items?: Array<{
        statistics: {
          viewCount?: string;
          likeCount?: string;
          commentCount?: string;
        };
        snippet?: {
          title?: string;
          publishedAt?: string;
          thumbnails?: {
            high?: { url: string };
            medium?: { url: string };
            default?: { url: string };
          };
        };
      }>;
    };

    if (!data.items || data.items.length === 0) {
      logger.error({ videoId, data }, 'YouTube video not found in response');
      throw new Error(`Video not found: ${videoId}`);
    }

    const video = data.items[0];
    const stats = video.statistics;
    const snippet = video.snippet;

    return {
      views: parseInt(stats.viewCount || '0', 10),
      likes: parseInt(stats.likeCount || '0', 10),
      comments: parseInt(stats.commentCount || '0', 10),
      shares: 0, // YouTube does not expose shares publicly
      title: snippet?.title,
      publishedAt: snippet?.publishedAt,
      thumbnailUrl:
        snippet?.thumbnails?.high?.url ||
        snippet?.thumbnails?.medium?.url ||
        snippet?.thumbnails?.default?.url,
    };
  } catch (error) {
    logger.error(error, 'Error fetching YouTube stats');
    throw error;
  }
}

/**
 * Check if the YouTube API key is configured
 */
export function isYouTubeApiConfigured(): boolean {
  return !!config.youtubeApiKey;
}
