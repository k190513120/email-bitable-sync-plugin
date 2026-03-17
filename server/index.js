import express from 'express';
import cors from 'cors';
import { registerEmailSyncRoutes } from './email-sync.js';
import { registerScheduleRoutes, startAllSchedules } from './schedule-manager.js';

const app = express();
const port = Number(process.env.PORT || 8787);
const host = process.env.HOST || '0.0.0.0';
const baseUrl = process.env.PUBLIC_BASE_URL || `http://localhost:${port}`;

app.use(cors());
app.use(express.json({ limit: '1mb' }));
registerEmailSyncRoutes(app);
registerScheduleRoutes(app);

app.get('/healthz', (_, res) => {
  res.json({ ok: true, now: Date.now() });
});

app.listen(port, host, () => {
  console.log(`email sync server is running at ${baseUrl}`);
  startAllSchedules().catch(err => {
    console.error('Failed to restore scheduled sync jobs:', err);
  });
});
