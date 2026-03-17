import cron from 'node-cron';
import { BaseClient } from '@lark-base-open/node-sdk';
import fs from 'fs/promises';
import path from 'path';
import { fetchEmailsFromImap, resolveImapConfig } from './email-sync.js';

const SCHEDULES_FILE = path.resolve(process.cwd(), 'server/schedules.json');

// In-memory map: scheduleId -> cron.ScheduledTask
const cronJobs = new Map();

// --- Persistence ---

async function loadSchedules() {
  try {
    const content = await fs.readFile(SCHEDULES_FILE, 'utf8');
    return JSON.parse(content);
  } catch (_) {
    return {};
  }
}

async function saveSchedules(data) {
  await fs.writeFile(SCHEDULES_FILE, JSON.stringify(data, null, 2), 'utf8');
}

async function getSchedule(id) {
  const data = await loadSchedules();
  return data[id] || null;
}

async function putSchedule(schedule) {
  const data = await loadSchedules();
  data[schedule.id] = schedule;
  await saveSchedules(data);
}

async function removeSchedule(id) {
  const data = await loadSchedules();
  delete data[id];
  await saveSchedules(data);
}

// --- Cron helpers ---

function intervalToCron(hours) {
  const h = Number(hours);
  if (h <= 1) return '0 * * * *';        // every hour
  if (h <= 3) return '0 */3 * * *';      // every 3 hours
  if (h <= 6) return '0 */6 * * *';      // every 6 hours
  if (h <= 12) return '0 */12 * * *';    // every 12 hours
  return '0 9 * * *';                     // once a day at 9:00
}

// --- Core sync execution ---

async function executeScheduledSync(scheduleId) {
  const schedule = await getSchedule(scheduleId);
  if (!schedule || !schedule.enabled) return;

  console.log(`[schedule] executing sync for ${scheduleId}`);

  try {
    // 1. Fetch emails via IMAP (reuse existing logic)
    const imapConfig = resolveImapConfig(schedule.imapConfig);
    const emails = await fetchEmailsFromImap(imapConfig, {
      maxMessages: schedule.imapConfig.maxMessages || 100,
      maxAttachmentsPerMail: 0, // no attachments in scheduled sync
      maxAttachmentBytes: 0
    });

    if (!emails.length) {
      schedule.lastSyncAt = Date.now();
      schedule.lastSyncStatus = 'success';
      schedule.lastSyncMessage = '未发现新邮件';
      await putSchedule(schedule);
      console.log(`[schedule] ${scheduleId}: no new emails`);
      return;
    }

    // 2. Use node-sdk to read existing records for dedup
    const { personalBaseToken, appToken, tableId } = schedule.feishuConfig;
    const client = new BaseClient({ appToken, personalBaseToken });

    const existingIds = new Set();
    try {
      const iterator = await client.base.appTableRecord.listWithIterator({
        params: { page_size: 500 },
        path: { table_id: tableId }
      });
      for await (const page of iterator) {
        const items = page?.items || [];
        for (const item of items) {
          const emailIdField = item?.fields?.['邮件ID'];
          if (emailIdField) {
            // The field value may be a string or an array of text segments
            const val = Array.isArray(emailIdField)
              ? String((emailIdField[0])?.text || '').trim()
              : String(emailIdField).trim();
            if (val) existingIds.add(val);
          }
        }
      }
    } catch (err) {
      // If table is empty or field doesn't exist yet, proceed with empty set
      console.log(`[schedule] ${scheduleId}: could not read existing records, proceeding: ${err.message}`);
    }

    // 3. Filter new emails
    const newEmails = emails.filter(e => e.messageId && !existingIds.has(e.messageId));
    if (!newEmails.length) {
      schedule.lastSyncAt = Date.now();
      schedule.lastSyncStatus = 'success';
      schedule.lastSyncMessage = `拉取 ${emails.length} 封，全部已存在`;
      await putSchedule(schedule);
      console.log(`[schedule] ${scheduleId}: all emails already exist`);
      return;
    }

    // 4. Batch create records (max 500 per batch)
    const provider = schedule.imapConfig.provider || 'custom';
    const syncTime = Date.now();
    const BATCH_SIZE = 500;

    for (let i = 0; i < newEmails.length; i += BATCH_SIZE) {
      const batch = newEmails.slice(i, i + BATCH_SIZE);
      await client.base.appTableRecord.batchCreate({
        path: { table_id: tableId },
        data: {
          records: batch.map(email => ({
            fields: {
              '邮件ID': email.messageId,
              '主题': email.subject || '(无主题)',
              '发件人': email.from || '',
              '收件人': email.to || '',
              '抄送': email.cc || '',
              '接收时间': email.date,
              '摘要': email.snippet || '',
              '正文': email.textBody || '',
              '邮箱服务商': provider,
              '同步时间': syncTime
            }
          }))
        }
      });
    }

    schedule.lastSyncAt = syncTime;
    schedule.lastSyncStatus = 'success';
    schedule.lastSyncMessage = `拉取 ${emails.length} 封，新增 ${newEmails.length} 封，跳过 ${emails.length - newEmails.length} 封（定时同步暂不支持附件）`;
    await putSchedule(schedule);
    console.log(`[schedule] ${scheduleId}: synced ${newEmails.length} new emails`);

  } catch (error) {
    console.error(`[schedule] ${scheduleId} sync failed:`, error);
    schedule.lastSyncAt = Date.now();
    schedule.lastSyncStatus = 'failed';
    schedule.lastSyncMessage = error?.message || '同步失败';
    await putSchedule(schedule);
  }
}

// --- Cron job management ---

function stopCronJob(scheduleId) {
  const job = cronJobs.get(scheduleId);
  if (job) {
    job.stop();
    cronJobs.delete(scheduleId);
  }
}

function startCronJob(schedule) {
  stopCronJob(schedule.id);
  const cronExpr = intervalToCron(schedule.intervalHours);
  const task = cron.schedule(cronExpr, () => {
    executeScheduledSync(schedule.id).catch(err => {
      console.error(`[schedule] cron callback error for ${schedule.id}:`, err);
    });
  });
  cronJobs.set(schedule.id, task);
  console.log(`[schedule] started cron job for ${schedule.id} with expression "${cronExpr}"`);
}

export async function startAllSchedules() {
  const data = await loadSchedules();
  let count = 0;
  for (const [id, schedule] of Object.entries(data)) {
    if (schedule.enabled) {
      startCronJob(schedule);
      count++;
    }
  }
  console.log(`[schedule] restored ${count} scheduled sync job(s)`);
}

// --- Routes ---

export function registerScheduleRoutes(app) {

  // Create or update a schedule
  app.post('/api/schedule/create', async (req, res) => {
    try {
      const body = req.body || {};
      const id = String(body.id || '').trim() || `sch_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
      const intervalHours = Math.max(1, Math.min(24, Number(body.intervalHours) || 3));

      // Validate required fields
      const email = String(body.imapConfig?.email || '').trim();
      const password = String(body.imapConfig?.password || '').trim();
      if (!email || !password) {
        return res.status(400).json({ status: 'failed', message: '请填写邮箱账号和授权码' });
      }

      const personalBaseToken = String(body.feishuConfig?.personalBaseToken || '').trim();
      const appToken = String(body.feishuConfig?.appToken || '').trim();
      const tableId = String(body.feishuConfig?.tableId || '').trim();
      if (!personalBaseToken || !appToken || !tableId) {
        return res.status(400).json({ status: 'failed', message: '请填写多维表格授权码、appToken 和 tableId' });
      }

      const schedule = {
        id,
        enabled: true,
        intervalHours,
        imapConfig: {
          provider: body.imapConfig?.provider || 'gmail',
          email,
          password,
          imapHost: body.imapConfig?.imapHost || '',
          imapPort: body.imapConfig?.imapPort || 993,
          secure: body.imapConfig?.secure !== false,
          folder: body.imapConfig?.folder || 'INBOX',
          maxMessages: Math.max(1, Math.min(500, Number(body.imapConfig?.maxMessages) || 100))
        },
        feishuConfig: { personalBaseToken, appToken, tableId },
        userId: String(body.userId || '').trim(),
        createdAt: Date.now(),
        lastSyncAt: null,
        lastSyncStatus: null,
        lastSyncMessage: null
      };

      await putSchedule(schedule);
      startCronJob(schedule);

      res.json({ status: 'ok', id: schedule.id, intervalHours: schedule.intervalHours });
    } catch (error) {
      res.status(500).json({ status: 'failed', message: error?.message || '创建定时任务失败' });
    }
  });

  // Delete a schedule
  app.post('/api/schedule/delete', async (req, res) => {
    try {
      const id = String(req.body?.id || '').trim();
      if (!id) {
        return res.status(400).json({ status: 'failed', message: '缺少 id 参数' });
      }
      stopCronJob(id);
      await removeSchedule(id);
      res.json({ status: 'ok' });
    } catch (error) {
      res.status(500).json({ status: 'failed', message: error?.message || '删除定时任务失败' });
    }
  });

  // Query schedule status
  app.get('/api/schedule/status', async (req, res) => {
    try {
      const id = String(req.query.id || '').trim();
      if (!id) {
        return res.status(400).json({ status: 'failed', message: '缺少 id 参数' });
      }
      const schedule = await getSchedule(id);
      if (!schedule) {
        return res.json({ exists: false });
      }
      res.json({
        exists: true,
        id: schedule.id,
        enabled: schedule.enabled,
        intervalHours: schedule.intervalHours,
        lastSyncAt: schedule.lastSyncAt,
        lastSyncStatus: schedule.lastSyncStatus,
        lastSyncMessage: schedule.lastSyncMessage,
        createdAt: schedule.createdAt
      });
    } catch (error) {
      res.status(500).json({ status: 'failed', message: error?.message || '查询状态失败' });
    }
  });

  // Manually trigger a sync (for debugging)
  app.post('/api/schedule/trigger', async (req, res) => {
    try {
      const id = String(req.body?.id || '').trim();
      if (!id) {
        return res.status(400).json({ status: 'failed', message: '缺少 id 参数' });
      }
      const schedule = await getSchedule(id);
      if (!schedule) {
        return res.status(404).json({ status: 'failed', message: '定时任务不存在' });
      }
      // Execute in background, return immediately
      executeScheduledSync(id).catch(err => {
        console.error(`[schedule] manual trigger error for ${id}:`, err);
      });
      res.json({ status: 'ok', message: '已触发同步，请稍后查询状态' });
    } catch (error) {
      res.status(500).json({ status: 'failed', message: error?.message || '触发同步失败' });
    }
  });
}
