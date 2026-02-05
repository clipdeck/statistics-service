import Fastify from 'fastify';
import cors from '@fastify/cors';
import helmet from '@fastify/helmet';
import { config } from './config';
import { logger } from './lib/logger';
import { statsRoutes } from './routes/stats';
import { rankingsRoutes } from './routes/rankings';
import { publisher } from './lib/events';
import { startEventHandlers, stopEventHandlers } from './events/handlers';
import { startScheduler } from './jobs/scheduler';
import { redis } from './lib/redis';

async function main() {
  const app = Fastify({
    logger: logger as any,
  });

  // Plugins
  await app.register(cors, {
    origin: config.allowedOrigins,
    credentials: true,
  });
  await app.register(helmet);

  // Health check
  app.get('/health', async () => ({ status: 'ok', service: 'statistics-service' }));
  app.get('/ready', async () => {
    // Could add DB/Redis connectivity check here
    return { status: 'ready', service: 'statistics-service' };
  });

  // Routes
  await app.register(statsRoutes, { prefix: '/stats' });
  await app.register(rankingsRoutes, { prefix: '/rankings' });

  // Connect event publisher
  await publisher.connect();

  // Start event handlers (consumer)
  await startEventHandlers();

  // Start scheduler
  startScheduler();

  // Graceful shutdown
  const shutdown = async () => {
    logger.info('Shutting down...');
    await stopEventHandlers();
    await publisher.disconnect();
    await redis.quit();
    await app.close();
    process.exit(0);
  };

  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);

  // Start server
  await app.listen({ port: config.port, host: config.host });
  logger.info(`Statistics service listening on ${config.host}:${config.port}`);
}

main().catch((err) => {
  logger.error(err, 'Failed to start statistics service');
  process.exit(1);
});
