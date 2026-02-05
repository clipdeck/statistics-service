import { z } from 'zod';

const envSchema = z.object({
  PORT: z.coerce.number().default(3003),
  HOST: z.string().default('0.0.0.0'),
  NODE_ENV: z.enum(['development', 'production', 'test']).default('development'),
  LOG_LEVEL: z.enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace']).default('info'),

  DATABASE_URL: z.string().url(),
  DIRECT_URL: z.string().url().optional(),

  RABBITMQ_URL: z.string().optional(),
  REDIS_URL: z.string().default('redis://localhost:6379'),
  EVENT_EXCHANGE: z.string().default('clipdeck.events'),

  JWT_SECRET: z.string().min(16),

  YOUTUBE_API_KEY: z.string().optional(),
  CLIP_SERVICE_URL: z.string().url().default('http://localhost:3002'),

  ALLOWED_ORIGINS: z.string().default('http://localhost:3000'),
});

const parsed = envSchema.safeParse(process.env);

if (!parsed.success) {
  console.error('Invalid environment variables:', parsed.error.flatten().fieldErrors);
  process.exit(1);
}

export const config = {
  port: parsed.data.PORT,
  host: parsed.data.HOST,
  nodeEnv: parsed.data.NODE_ENV,
  logLevel: parsed.data.LOG_LEVEL,
  databaseUrl: parsed.data.DATABASE_URL,
  directUrl: parsed.data.DIRECT_URL,
  rabbitmqUrl: parsed.data.RABBITMQ_URL,
  redisUrl: parsed.data.REDIS_URL,
  eventExchange: parsed.data.EVENT_EXCHANGE,
  jwtSecret: parsed.data.JWT_SECRET,
  youtubeApiKey: parsed.data.YOUTUBE_API_KEY,
  clipServiceUrl: parsed.data.CLIP_SERVICE_URL,
  allowedOrigins: parsed.data.ALLOWED_ORIGINS.split(',').map((s) => s.trim()),
  isDev: parsed.data.NODE_ENV === 'development',
  isProd: parsed.data.NODE_ENV === 'production',
};
