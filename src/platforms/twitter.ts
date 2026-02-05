/**
 * Twitter/X stats fetcher
 * Uses Twitter's public Syndication API (used for embeds)
 */

import axios from 'axios';
import { logger } from '../lib/logger';

export interface TwitterStats {
  views: number;
  likes: number;
  comments: number;
  shares: number;
  author?: string;
  thumbnailUrl?: string;
  description?: string;
}

/**
 * Fetches Tweet stats using Twitter's public Syndication API
 * @param url - Full tweet URL (twitter.com or x.com)
 * @returns Tweet statistics or null if extraction fails
 */
export async function getTwitterStats(url: string): Promise<TwitterStats | null> {
  try {
    // 1. Extract Tweet ID
    const match = url.match(
      /(?:twitter\.com|x\.com|nitter\.[^/]+)\/([^/]+)\/status\/(\d+)/
    );
    if (!match) {
      logger.error({ url }, 'Invalid Twitter URL format');
      return null;
    }

    const tweetId = match[2];
    const apiUrl = `https://cdn.syndication.twimg.com/tweet-result?id=${tweetId}&token=x`;

    logger.info({ apiUrl }, 'Fetching from Twitter Syndication API');

    const response = await axios.get(apiUrl, {
      headers: {
        'User-Agent':
          'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      },
      timeout: 5000,
    });

    if (response.status === 200) {
      const data = response.data;

      return {
        views: data.impression_count || 0,
        likes: data.favorite_count || 0,
        comments: data.conversation_count || 0,
        shares: (data.retweet_count || 0) + (data.quote_count || 0),
        author: data.user?.screen_name,
        thumbnailUrl:
          data.photos?.[0]?.url ||
          data.video?.poster ||
          data.user?.profile_image_url_https,
        description: data.text,
      };
    }

    return null;
  } catch (error: any) {
    logger.error({ message: error.message }, 'Twitter API error');
    return null;
  }
}
