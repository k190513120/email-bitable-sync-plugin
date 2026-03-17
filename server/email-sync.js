import { ImapFlow } from 'imapflow';
import { simpleParser } from 'mailparser';
import fs from 'fs/promises';
import path from 'path';

const FREE_EMAIL_QUOTA = 3;
const USAGE_FILE = path.resolve(process.cwd(), 'server/email-usage.json');

async function loadUsage() {
  try {
    const content = await fs.readFile(USAGE_FILE, 'utf8');
    return JSON.parse(content);
  } catch (_) {
    return {};
  }
}

async function saveUsage(data) {
  await fs.writeFile(USAGE_FILE, JSON.stringify(data, null, 2), 'utf8');
}

async function getUserUsage(userId) {
  const data = await loadUsage();
  return data[userId] || { used: 0 };
}

async function addUserUsage(userId, count) {
  const data = await loadUsage();
  if (!data[userId]) data[userId] = { used: 0 };
  data[userId].used += count;
  data[userId].updatedAt = Date.now();
  await saveUsage(data);
  return data[userId];
}

const EMAIL_PROVIDER_PRESETS = {
  gmail: { host: 'imap.gmail.com', port: 993, secure: true },
  qq: { host: 'imap.qq.com', port: 993, secure: true },
  '163': { host: 'imap.163.com', port: 993, secure: true },
  feishu: { host: 'imap.larkoffice.com', port: 993, secure: true },
  outlook: { host: 'outlook.office365.com', port: 993, secure: true },
  yahoo: { host: 'imap.mail.yahoo.com', port: 993, secure: true }
};

function normalizeNumber(value, fallback, min, max) {
  const raw = Number(value);
  if (!Number.isFinite(raw)) return fallback;
  return Math.max(min, Math.min(max, Math.floor(raw)));
}

function normalizeBoolean(value, fallback) {
  if (typeof value === 'boolean') return value;
  if (value === 'true') return true;
  if (value === 'false') return false;
  return fallback;
}

function formatAddressList(addresses) {
  if (!Array.isArray(addresses)) return '';
  return addresses
    .map((item) => {
      const name = String(item?.name || '').trim();
      const address = String(item?.address || '').trim();
      if (!name) return address;
      if (!address) return name;
      return `${name} <${address}>`;
    })
    .filter(Boolean)
    .join('; ');
}

function stripHtml(input) {
  const html = String(input || '');
  if (!html) return '';
  return html
    .replace(/<style[\s\S]*?>[\s\S]*?<\/style>/gi, ' ')
    .replace(/<script[\s\S]*?>[\s\S]*?<\/script>/gi, ' ')
    .replace(/<\/?[^>]+>/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function normalizeProvider(input) {
  const normalized = String(input || '')
    .trim()
    .toLowerCase();
  if (normalized === 'mail163' || normalized === '163mail') return '163';
  return normalized;
}

function resolveImapConfig(body) {
  const provider = normalizeProvider(body?.provider);
  const preset = EMAIL_PROVIDER_PRESETS[provider] || null;
  const host = String(body?.imapHost || preset?.host || '')
    .trim()
    .toLowerCase();
  const email = String(body?.email || '')
    .trim()
    .toLowerCase();
  const password = String(body?.password || body?.appPassword || '').trim();
  const port = normalizeNumber(body?.imapPort ?? preset?.port ?? 993, 993, 1, 65535);
  const secure = normalizeBoolean(body?.secure, Boolean(preset?.secure ?? true));
  const folder = String(body?.folder || 'INBOX').trim() || 'INBOX';

  if (!host) {
    throw new Error('缺少 IMAP Host，请选择邮箱类型或手动填写');
  }
  if (!email || !password) {
    throw new Error('请填写邮箱账号和邮箱授权码/密码');
  }

  return {
    provider: provider || 'custom',
    folder,
    auth: { user: email, pass: password },
    connection: {
      host,
      port,
      secure,
      logger: false
    }
  };
}

async function fetchEmailsFromImap(config, options) {
  const maxMessages = normalizeNumber(options?.maxMessages, 100, 1, 500);
  const maxAttachmentsPerMail = normalizeNumber(options?.maxAttachmentsPerMail, 8, 0, 20);
  const maxAttachmentBytes = normalizeNumber(options?.maxAttachmentBytes, 5 * 1024 * 1024, 1024, 20 * 1024 * 1024);
  const client = new ImapFlow({
    ...config.connection,
    auth: config.auth
  });

  await client.connect();
  const lock = await client.getMailboxLock(config.folder);

  try {
    const totalMessages = Number(client.mailbox?.exists || 0);
    if (totalMessages <= 0) return [];
    const from = Math.max(1, totalMessages - maxMessages + 1);
    const range = `${from}:*`;
    const list = [];

    for await (const message of client.fetch(range, { uid: true, envelope: true, internalDate: true, source: true })) {
      const parsed = await simpleParser(message.source);
      const messageId = String(parsed.messageId || message.envelope?.messageId || `uid-${message.uid}`).trim();
      const html = typeof parsed.html === 'string' ? parsed.html : '';
      const text = String(parsed.text || '').trim() || stripHtml(html);
      const snippet = text.slice(0, 500);
      const attachments = [];
      for (const attachment of parsed.attachments.slice(0, maxAttachmentsPerMail)) {
        const content = attachment.content;
        const size = Number(attachment.size || content?.length || 0);
        if (!content || size <= 0 || size > maxAttachmentBytes) {
          continue;
        }
        attachments.push({
          fileName: String(attachment.filename || 'attachment.bin'),
          contentType: String(attachment.contentType || 'application/octet-stream'),
          size,
          contentBase64: content.toString('base64')
        });
      }
      list.push({
        messageId,
        subject: String(parsed.subject || message.envelope?.subject || ''),
        from: formatAddressList(parsed.from?.value || message.envelope?.from || []),
        to: formatAddressList(parsed.to?.value || message.envelope?.to || []),
        cc: formatAddressList(parsed.cc?.value || message.envelope?.cc || []),
        date: parsed.date ? parsed.date.getTime() : message.internalDate ? message.internalDate.getTime() : Date.now(),
        snippet,
        textBody: text,
        htmlBody: html,
        attachments
      });
    }

    return list.sort((a, b) => b.date - a.date);
  } finally {
    lock.release();
    await client.logout();
  }
}

export { fetchEmailsFromImap, resolveImapConfig };

export function registerEmailSyncRoutes(app) {
  app.get('/api/email/quota', async (req, res) => {
    try {
      const userId = String(req.query.userId || '').trim();
      if (!userId) {
        return res.status(400).json({ status: 'failed', message: '缺少 userId 参数' });
      }
      const usage = await getUserUsage(userId);
      const used = usage.used || 0;
      const remaining = Math.max(0, FREE_EMAIL_QUOTA - used);
      res.json({
        paid: false,
        used,
        remaining,
        total: FREE_EMAIL_QUOTA
      });
    } catch (error) {
      res.status(500).json({ status: 'failed', message: error?.message || '查询配额失败' });
    }
  });

  app.post('/api/email/usage', async (req, res) => {
    try {
      const userId = String(req.body?.userId || '').trim();
      const count = normalizeNumber(req.body?.count, 0, 0, 500);
      if (!userId) {
        return res.status(400).json({ status: 'failed', message: '缺少 userId 参数' });
      }
      const updated = await addUserUsage(userId, count);
      res.json({ status: 'ok', used: updated.used });
    } catch (error) {
      res.status(500).json({ status: 'failed', message: error?.message || '更新用量失败' });
    }
  });

  app.post('/api/email/sync', async (req, res) => {
    try {
      const userId = String(req.body?.userId || '').trim();
      let maxMessages = normalizeNumber(req.body?.maxMessages, 100, 1, 500);

      if (userId) {
        const usage = await getUserUsage(userId);
        const used = usage.used || 0;
        const remaining = Math.max(0, FREE_EMAIL_QUOTA - used);
        if (remaining <= 0) {
          return res.status(403).json({
            status: 'quota_exceeded',
            message: `免费 ${FREE_EMAIL_QUOTA} 封额度已用完，请升级付费版`,
            used,
            total: FREE_EMAIL_QUOTA
          });
        }
        maxMessages = Math.min(maxMessages, remaining);
      }

      const config = resolveImapConfig(req.body || {});
      const emails = await fetchEmailsFromImap(config, {
        maxMessages,
        maxAttachmentsPerMail: 8,
        maxAttachmentBytes: 5 * 1024 * 1024
      });
      res.json({
        status: 'completed',
        provider: config.provider,
        mailbox: config.folder,
        emails
      });
    } catch (error) {
      res.status(500).json({
        status: 'failed',
        message: error?.message || '邮箱同步失败'
      });
    }
  });
}
