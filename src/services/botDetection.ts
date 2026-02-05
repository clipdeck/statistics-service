/**
 * Bot Detection System
 * Detects suspicious engagement patterns using advanced statistical analysis.
 * Supports TikTok, Instagram Reels, YouTube Shorts, and Twitter.
 */

import axios from 'axios';
import { config } from '../config';
import { logger } from '../lib/logger';
import { publisher, StatsEvents, SERVICE_NAME } from '../lib/events';

// ============================================================================
// Types
// ============================================================================

export interface BotDetectionResult {
  hasAnomalies: boolean;
  flags: BotFlag[];
  confidenceScore: number; // 0-100
}

export interface BotFlag {
  type:
    | 'VIEWS_SPIKE'
    | 'LIKES_SPIKE'
    | 'COMMENTS_SPIKE'
    | 'ENGAGEMENT_RATIO'
    | 'SUSPICIOUS_PATTERN'
    | 'VELOCITY_ANOMALY'
    | 'TIME_PATTERN'
    | 'RATIO_ANOMALY'
    | 'ZERO_VARIANCE'
    | 'SUDDEN_STOP';
  severity: 'LOW' | 'MEDIUM' | 'HIGH';
  description: string;
  confidence: number; // 0-100
}

export interface StatsHistoryEntry {
  views: number;
  likes: number;
  comments: number;
  shares: number;
  recordedAt: string; // ISO date string
}

// ============================================================================
// Platform-specific thresholds
// ============================================================================

const PLATFORM_THRESHOLDS = {
  TIKTOK: {
    viewsSpike: { high: 800, medium: 300 },
    likesSpike: { high: 400, medium: 200 },
    commentsSpike: { high: 500, medium: 250 },
    engagementRatio: { high: 0.4, medium: 0.25 },
    minViews: 500,
  },
  INSTAGRAM: {
    viewsSpike: { high: 600, medium: 250 },
    likesSpike: { high: 350, medium: 180 },
    commentsSpike: { high: 450, medium: 220 },
    engagementRatio: { high: 0.35, medium: 0.2 },
    minViews: 300,
  },
  YOUTUBE: {
    viewsSpike: { high: 700, medium: 280 },
    likesSpike: { high: 380, medium: 190 },
    commentsSpike: { high: 480, medium: 240 },
    engagementRatio: { high: 0.38, medium: 0.22 },
    minViews: 400,
  },
  TWITTER: {
    viewsSpike: { high: 700, medium: 280 },
    likesSpike: { high: 380, medium: 190 },
    commentsSpike: { high: 480, medium: 240 },
    engagementRatio: { high: 0.38, medium: 0.22 },
    minViews: 400,
  },
};

// ============================================================================
// Helper functions
// ============================================================================

/**
 * Calculate growth rate between two values
 */
function calculateGrowthRate(previous: number, current: number): number {
  if (previous === 0) {
    return current > 0 ? Infinity : 0;
  }
  return ((current - previous) / previous) * 100;
}

/**
 * Calculate Z-score for outlier detection
 */
function calculateZScore(value: number, mean: number, stdDev: number): number {
  if (stdDev === 0) return 0;
  return (value - mean) / stdDev;
}

/**
 * Calculate Interquartile Range (IQR) for robust outlier detection
 */
function calculateIQR(values: number[]): { q1: number; q3: number; iqr: number } {
  const sorted = [...values].sort((a, b) => a - b);
  const q1Index = Math.floor(sorted.length * 0.25);
  const q3Index = Math.floor(sorted.length * 0.75);
  const q1 = sorted[q1Index];
  const q3 = sorted[q3Index];
  const iqr = q3 - q1;
  return { q1, q3, iqr };
}

// ============================================================================
// Detection algorithms
// ============================================================================

/**
 * Detect velocity anomalies (acceleration/deceleration patterns)
 */
function detectVelocityAnomaly(history: StatsHistoryEntry[]): BotFlag | null {
  if (history.length < 4) return null;

  const velocities: number[] = [];
  for (let i = 0; i < history.length - 1; i++) {
    const velocity = history[i].views - history[i + 1].views;
    velocities.push(velocity);
  }

  // Calculate acceleration (change in velocity)
  const accelerations: number[] = [];
  for (let i = 0; i < velocities.length - 1; i++) {
    accelerations.push(velocities[i] - velocities[i + 1]);
  }

  const avgAcceleration =
    accelerations.reduce((a, b) => a + b, 0) / accelerations.length;
  const maxAcceleration = Math.max(...accelerations.map(Math.abs));

  // Sudden acceleration is suspicious
  if (maxAcceleration > avgAcceleration * 5 && maxAcceleration > 1000) {
    return {
      type: 'VELOCITY_ANOMALY',
      severity: 'HIGH',
      description: `Abnormal acceleration detected: ${maxAcceleration.toFixed(0)} views/hr^2 (avg: ${avgAcceleration.toFixed(0)})`,
      confidence: 85,
    };
  }

  return null;
}

/**
 * Detect suspicious time patterns (bot activity concentrated in specific hours)
 */
function detectTimePattern(history: StatsHistoryEntry[]): BotFlag | null {
  if (history.length < 24) return null;

  const hourlyGrowth: { [hour: number]: number } = {};

  for (let i = 0; i < history.length - 1; i++) {
    const hour = new Date(history[i].recordedAt).getHours();
    const growth = history[i].views - history[i + 1].views;
    hourlyGrowth[hour] = (hourlyGrowth[hour] || 0) + growth;
  }

  const growthValues = Object.values(hourlyGrowth);
  const maxGrowth = Math.max(...growthValues);
  const avgGrowth =
    growthValues.reduce((a, b) => a + b, 0) / growthValues.length;

  // If 80%+ of growth happens in specific hours, suspicious
  if (maxGrowth > avgGrowth * 8 && maxGrowth > 5000) {
    const suspiciousHour = Object.keys(hourlyGrowth).find(
      (h) => hourlyGrowth[parseInt(h)] === maxGrowth
    );
    return {
      type: 'TIME_PATTERN',
      severity: 'MEDIUM',
      description: `Suspicious time pattern: ${((maxGrowth / avgGrowth) * 100).toFixed(0)}% of growth concentrated in hour ${suspiciousHour}`,
      confidence: 70,
    };
  }

  return null;
}

/**
 * Detect ratio anomalies (unusual likes/views or comments/views ratios)
 */
function detectRatioAnomaly(
  latest: StatsHistoryEntry,
  platform: string
): BotFlag | null {
  if (latest.views < 100) return null;

  const likesRatio = latest.likes / latest.views;
  const commentsRatio = latest.comments / latest.views;

  // Organic content typically has 2-10% like rate and 0.5-2% comment rate
  if (likesRatio > 0.15 && latest.views > 1000) {
    return {
      type: 'RATIO_ANOMALY',
      severity: 'HIGH',
      description: `Abnormally high likes ratio: ${(likesRatio * 100).toFixed(1)}% (typical organic: 2-10%)`,
      confidence: 90,
    };
  }

  if (commentsRatio > 0.05 && latest.views > 1000) {
    return {
      type: 'RATIO_ANOMALY',
      severity: 'MEDIUM',
      description: `Abnormally high comments ratio: ${(commentsRatio * 100).toFixed(1)}% (typical organic: 0.5-2%)`,
      confidence: 75,
    };
  }

  return null;
}

/**
 * Detect zero variance (perfectly linear growth - bot signature)
 */
function detectZeroVariance(history: StatsHistoryEntry[]): BotFlag | null {
  if (history.length < 6) return null;

  const growthRates: number[] = [];
  for (let i = 0; i < history.length - 1; i++) {
    const rate = calculateGrowthRate(history[i + 1].views, history[i].views);
    if (isFinite(rate)) {
      growthRates.push(rate);
    }
  }

  if (growthRates.length < 5) return null;

  const mean = growthRates.reduce((a, b) => a + b, 0) / growthRates.length;
  const variance =
    growthRates.reduce((sum, rate) => sum + Math.pow(rate - mean, 2), 0) /
    growthRates.length;
  const stdDev = Math.sqrt(variance);

  // Coefficient of variation (CV) < 0.1 indicates suspiciously consistent growth
  const cv = stdDev / Math.abs(mean);

  if (cv < 0.1 && mean > 20) {
    return {
      type: 'ZERO_VARIANCE',
      severity: 'HIGH',
      description: `Suspiciously constant growth: ${mean.toFixed(1)}% +/- ${stdDev.toFixed(1)}% (CV: ${(cv * 100).toFixed(1)}%)`,
      confidence: 95,
    };
  }

  return null;
}

/**
 * Detect sudden stop (engagement spike followed by abrupt cessation)
 */
function detectSuddenStop(history: StatsHistoryEntry[]): BotFlag | null {
  if (history.length < 12) return null;

  const recent6 = history.slice(0, 6);
  const previous6 = history.slice(6, 12);

  const recentAvgGrowth =
    recent6.reduce((sum, h, i) => {
      if (i === recent6.length - 1) return sum;
      return sum + (h.views - recent6[i + 1].views);
    }, 0) / 5;

  const previousAvgGrowth =
    previous6.reduce((sum, h, i) => {
      if (i === previous6.length - 1) return sum;
      return sum + (h.views - previous6[i + 1].views);
    }, 0) / 5;

  // If growth dropped by 90%+ after a spike, suspicious
  if (previousAvgGrowth > 500 && recentAvgGrowth < previousAvgGrowth * 0.1) {
    return {
      type: 'SUDDEN_STOP',
      severity: 'MEDIUM',
      description: `Abrupt stop: growth dropped ${((1 - recentAvgGrowth / previousAvgGrowth) * 100).toFixed(0)}% (${previousAvgGrowth.toFixed(0)} -> ${recentAvgGrowth.toFixed(0)} views/hr)`,
      confidence: 70,
    };
  }

  return null;
}

// ============================================================================
// Main detection functions
// ============================================================================

/**
 * Detect anomalies in a clip's stats history.
 * Accepts pre-fetched history so the caller controls the data source.
 */
export function detectAnomalies(
  history: StatsHistoryEntry[],
  platform: string
): BotDetectionResult {
  const thresholds =
    PLATFORM_THRESHOLDS[platform as keyof typeof PLATFORM_THRESHOLDS] ||
    PLATFORM_THRESHOLDS.YOUTUBE;

  const flags: BotFlag[] = [];

  if (history.length < 2) {
    return { hasAnomalies: false, flags: [], confidenceScore: 0 };
  }

  const latest = history[0];
  const previous = history[1];

  // 1. Views Spike Detection
  const viewsGrowth = calculateGrowthRate(previous.views, latest.views);
  const viewsDelta = latest.views - previous.views;

  if (
    viewsGrowth > thresholds.viewsSpike.high &&
    viewsDelta > thresholds.minViews * 2
  ) {
    flags.push({
      type: 'VIEWS_SPIKE',
      severity: 'HIGH',
      description: `Views increased ${viewsGrowth.toFixed(0)}% in 1 hour (${previous.views.toLocaleString()} -> ${latest.views.toLocaleString()})`,
      confidence: 90,
    });
  } else if (
    viewsGrowth > thresholds.viewsSpike.medium &&
    viewsDelta > thresholds.minViews
  ) {
    flags.push({
      type: 'VIEWS_SPIKE',
      severity: 'MEDIUM',
      description: `Views increased ${viewsGrowth.toFixed(0)}% in 1 hour (${previous.views.toLocaleString()} -> ${latest.views.toLocaleString()})`,
      confidence: 70,
    });
  }

  // 2. Likes Spike
  const likesGrowth = calculateGrowthRate(previous.likes, latest.likes);
  const likesDelta = latest.likes - previous.likes;

  if (likesGrowth > thresholds.likesSpike.high && likesDelta > 100) {
    flags.push({
      type: 'LIKES_SPIKE',
      severity: 'HIGH',
      description: `Likes increased ${likesGrowth.toFixed(0)}% in 1 hour (${previous.likes.toLocaleString()} -> ${latest.likes.toLocaleString()})`,
      confidence: 85,
    });
  } else if (likesGrowth > thresholds.likesSpike.medium && likesDelta > 50) {
    flags.push({
      type: 'LIKES_SPIKE',
      severity: 'MEDIUM',
      description: `Likes increased ${likesGrowth.toFixed(0)}% in 1 hour (${previous.likes.toLocaleString()} -> ${latest.likes.toLocaleString()})`,
      confidence: 65,
    });
  }

  // 3. Comments Spike
  const commentsGrowth = calculateGrowthRate(previous.comments, latest.comments);
  const commentsDelta = latest.comments - previous.comments;

  if (commentsGrowth > thresholds.commentsSpike.high && commentsDelta > 50) {
    flags.push({
      type: 'COMMENTS_SPIKE',
      severity: 'HIGH',
      description: `Comments increased ${commentsGrowth.toFixed(0)}% in 1 hour (${previous.comments.toLocaleString()} -> ${latest.comments.toLocaleString()})`,
      confidence: 88,
    });
  }

  // 4. Engagement Ratio
  const engagementRatio =
    latest.views > 0 ? (latest.likes + latest.comments) / latest.views : 0;

  if (
    engagementRatio > thresholds.engagementRatio.high &&
    latest.views > thresholds.minViews
  ) {
    flags.push({
      type: 'ENGAGEMENT_RATIO',
      severity: 'HIGH',
      description: `Abnormally high engagement ratio: ${(engagementRatio * 100).toFixed(1)}% (${(latest.likes + latest.comments).toLocaleString()} interactions / ${latest.views.toLocaleString()} views)`,
      confidence: 92,
    });
  } else if (
    engagementRatio > thresholds.engagementRatio.medium &&
    latest.views > thresholds.minViews
  ) {
    flags.push({
      type: 'ENGAGEMENT_RATIO',
      severity: 'MEDIUM',
      description: `Suspicious engagement ratio: ${(engagementRatio * 100).toFixed(1)}%`,
      confidence: 75,
    });
  }

  // 5. Advanced Pattern Detection
  if (history.length >= 5) {
    const zeroVarianceFlag = detectZeroVariance(history);
    if (zeroVarianceFlag) flags.push(zeroVarianceFlag);

    const velocityFlag = detectVelocityAnomaly(history);
    if (velocityFlag) flags.push(velocityFlag);

    const ratioFlag = detectRatioAnomaly(latest, platform);
    if (ratioFlag) flags.push(ratioFlag);
  }

  if (history.length >= 12) {
    const suddenStopFlag = detectSuddenStop(history);
    if (suddenStopFlag) flags.push(suddenStopFlag);
  }

  if (history.length >= 24) {
    const timePatternFlag = detectTimePattern(history);
    if (timePatternFlag) flags.push(timePatternFlag);
  }

  // Calculate overall confidence score
  const confidenceScore =
    flags.length > 0
      ? Math.min(
          100,
          flags.reduce((sum, flag) => sum + flag.confidence, 0) / flags.length
        )
      : 0;

  return {
    hasAnomalies: flags.length > 0,
    flags,
    confidenceScore,
  };
}

/**
 * Run bot detection for a specific clip by fetching its history from clip-service.
 * Publishes a stats.bot_detected event if anomalies are found.
 */
export async function runBotDetection(
  clipId: string
): Promise<BotDetectionResult> {
  try {
    // Fetch stats history from clip-service
    const response = await axios.get(
      `${config.clipServiceUrl}/clips/${clipId}/stats-history`,
      {
        headers: {
          'X-Internal-Service': 'statistics-service',
        },
        timeout: 10000,
      }
    );

    const clipData = response.data;
    const history: StatsHistoryEntry[] = clipData.history || [];
    const platform: string = clipData.platform || 'YOUTUBE';
    const campaignId: string = clipData.campaignId || '';
    const userId: string = clipData.userId || '';

    const result = detectAnomalies(history, platform);

    // Publish bot detected event if anomalies found
    if (result.hasAnomalies) {
      const significantFlags = result.flags.filter(
        (f) => f.severity === 'HIGH' || f.severity === 'MEDIUM'
      );

      if (significantFlags.length > 0) {
        try {
          const event = StatsEvents.botDetected(
            {
              clipId,
              campaignId,
              userId,
              flagType: significantFlags[0].type,
              confidence: result.confidenceScore / 100,
              evidence: significantFlags
                .map((f) => `${f.type}: ${f.description}`)
                .join('; '),
            },
            SERVICE_NAME
          );
          await publisher.publish(event);
        } catch (error) {
          logger.error(error, 'Error publishing bot detected event');
        }
      }
    }

    return result;
  } catch (error) {
    logger.error({ clipId, error }, 'Error running bot detection');
    return { hasAnomalies: false, flags: [], confidenceScore: 0 };
  }
}
