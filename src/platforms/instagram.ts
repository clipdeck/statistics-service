/**
 * Instagram Reels stats fetcher
 * Uses the public Boostfluence API with challenge handling
 */

import axios from 'axios';
import { logger } from '../lib/logger';

export interface InstagramStats {
  views: number;
  likes: number;
  comments: number;
  shares: number;
  title?: string;
  publishedAt?: string;
  thumbnailUrl?: string | null;
  author?: string;
}

interface BoostfluenceResponse {
  mediaUrls?: { url: string; type: string; thumbnail_url: string }[];
  type?: string;
  username?: string;
  caption?: string;
  like_count?: number | null;
  comment_count?: number | null;
  view_count?: number | null;
  taken_at_date?: string;
  error?: string;
  challenge?: {
    timestamp: number;
    expectedCompute: number;
  };
}

const API_ENDPOINT = 'https://api.boostfluence.com/api/instagram-viewer-v2-2';
const ORIGIN = 'https://www.boostfluence.com';
const REFERER = 'https://www.boostfluence.com/free-tools/instagram-reels-viewer';

/**
 * Fetches statistics for an Instagram Reel/Post via Boostfluence proxy
 * @param mediaId - Full URL or reel ID
 * @returns Media statistics
 */
export async function getInstagramStats(mediaId: string): Promise<InstagramStats> {
  try {
    // Normalize input: ensure it is a full URL
    let targetUrl = mediaId;
    if (!mediaId.startsWith('http')) {
      targetUrl = `https://www.instagram.com/reel/${mediaId}/`;
    }

    logger.info({ targetUrl }, 'Fetching Instagram stats via Boostfluence');

    // Base headers that mimic a real browser visiting the tool
    const baseHeaders = {
      'Content-Type': 'application/json',
      Origin: ORIGIN,
      Referer: REFERER,
      'User-Agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    };

    const payload = {
      url: targetUrl,
      type: 'reels',
    };

    // 1. First attempt (expect a challenge)
    let response = await axios.post<BoostfluenceResponse>(API_ENDPOINT, payload, {
      headers: baseHeaders,
      validateStatus: (status) => status < 500,
    });

    // 2. If challenge received, solve it (simple echo of values) and retry
    if (response.data.error === 'COMPUTE_REQUIRED' && response.data.challenge) {
      logger.debug('Instagram challenge received, computing response...');

      const { timestamp, expectedCompute } = response.data.challenge;

      response = await axios.post<BoostfluenceResponse>(API_ENDPOINT, payload, {
        headers: {
          ...baseHeaders,
          'X-Compute': expectedCompute.toString(),
          'X-Timestamp': timestamp.toString(),
        },
      });
    }

    const data = response.data;

    if (data.error) {
      throw new Error(`Boostfluence API Error: ${data.error}`);
    }

    const stats: InstagramStats = {
      views: data.view_count || 0,
      likes: data.like_count || 0,
      comments: data.comment_count || 0,
      shares: 0, // This API does not return shares
      title: data.caption || '',
      publishedAt: data.taken_at_date || undefined,
      thumbnailUrl: data.mediaUrls?.[0]?.thumbnail_url || null,
      author: data.username,
    };

    logger.info(
      { targetUrl, views: stats.views, likes: stats.likes, author: stats.author },
      'Instagram stats extracted successfully'
    );

    return stats;
  } catch (error: any) {
    logger.error({ message: error.message }, 'Error fetching Instagram stats');
    if (axios.isAxiosError(error) && error.response) {
      logger.error({ responseData: error.response.data }, 'Instagram API Response');
    }

    // Return zeros as safe fallback
    return {
      views: 0,
      likes: 0,
      comments: 0,
      shares: 0,
      thumbnailUrl: null,
    };
  }
}
