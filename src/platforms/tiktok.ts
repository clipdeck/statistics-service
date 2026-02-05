/**
 * TikTok stats fetcher
 * Uses public tikwm.com API (no OAuth required)
 */

import axios from 'axios';
import { logger } from '../lib/logger';

export interface TikTokStats {
  views: number;
  likes: number;
  comments: number;
  shares: number;
  title?: string;
  publishedAt?: string;
  author?: string;
  thumbnailUrl?: string | null;
}

/**
 * Fetches statistics for a TikTok video
 * @param videoId - TikTok video ID or full URL
 * @returns Video statistics
 */
export async function getTikTokStats(videoId: string): Promise<TikTokStats> {
  // TikWM needs a valid URL. Construct one if only an ID is provided.
  let url = videoId;
  if (!videoId.includes('tiktok.com')) {
    url = `https://www.tiktok.com/@tiktok/video/${videoId}`;
  }

  const apiUrl = `https://www.tikwm.com/api/?url=${encodeURIComponent(url)}`;

  try {
    const { data } = await axios.get(apiUrl);

    if (data?.data) {
      const v = data.data;
      return {
        views: v.play_count ?? 0,
        likes: v.digg_count ?? 0,
        comments: v.comment_count ?? 0,
        shares: v.share_count ?? 0,
        title: v.title,
        author: v.author?.unique_id,
        thumbnailUrl: v.cover,
        publishedAt: v.create_time
          ? new Date(v.create_time * 1000).toISOString()
          : undefined,
      };
    }

    logger.warn({ videoId }, 'TikTok stats not found for video');
    return {
      views: 0,
      likes: 0,
      comments: 0,
      shares: 0,
    };
  } catch (error: any) {
    logger.error(
      {
        videoId,
        message: error.message,
        response: error.response?.data,
        status: error.response?.status,
      },
      'Error fetching TikTok stats'
    );

    throw new Error(`TikTok stats failed: ${error.message}`);
  }
}
