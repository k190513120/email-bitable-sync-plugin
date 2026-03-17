/**
 * Cloudflare Worker — 邮箱同步插件（全功能合并版）
 *
 * 集成：IMAP 邮件拉取、定时同步、配额管理、Stripe 支付
 * 使用 cloudflare:sockets 原生 TCP 连接实现 IMAP
 */

import { connect } from 'cloudflare:sockets';

const FREE_EMAIL_QUOTA = 3;
const YEAR_SECONDS = 365 * 24 * 60 * 60;
const SCHEDULE_KV_KEY = 'email:schedules';

// ─── helpers ──────────────────────────────────────────────

function corsHeaders(req, env) {
  const requestOrigin = req.headers.get('Origin') || '*';
  const allowOrigin = env.ALLOWED_ORIGIN || requestOrigin || '*';
  return {
    'Access-Control-Allow-Origin': allowOrigin,
    'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type,Authorization',
    Vary: 'Origin'
  };
}

function jsonResponse(req, env, status, data) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'Content-Type': 'application/json; charset=utf-8', ...corsHeaders(req, env) }
  });
}

function normalizeInt(value, fallback, min = 0, max = 1000000) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, Math.floor(n)));
}

function normalizeBoolean(value, fallback) {
  if (typeof value === 'boolean') return value;
  if (value === 'true') return true;
  if (value === 'false') return false;
  return fallback;
}

function normalizeUserId(input) {
  return String(input || '').trim();
}

function normalizeEmail(input) {
  return String(input || '').trim().toLowerCase();
}

// ─── KV state ─────────────────────────────────────────────

async function putState(env, key, value, ttl) {
  if (!env.EMAIL_STATE) return;
  const opts = ttl ? { expirationTtl: ttl } : { expirationTtl: 60 * 60 * 24 * 90 };
  await env.EMAIL_STATE.put(key, JSON.stringify(value), opts);
}

async function putStatePermanent(env, key, value) {
  if (!env.EMAIL_STATE) return;
  await env.EMAIL_STATE.put(key, JSON.stringify(value));
}

async function getState(env, key) {
  if (!env.EMAIL_STATE) return null;
  const value = await env.EMAIL_STATE.get(key);
  if (!value) return null;
  return JSON.parse(value);
}

// ─── entitlement ──────────────────────────────────────────

function resolveEntitlementActive(entitlement) {
  if (!entitlement || !entitlement.active) return false;
  const expiresAt = Number(entitlement.expiresAt || 0);
  if (!expiresAt) return true;
  return Date.now() < expiresAt;
}

async function getEntitlementByUserId(env, userId) {
  const id = normalizeUserId(userId);
  if (!id) return null;
  return getState(env, `email:entitlement:user:${id}`);
}

async function getEntitlementByEmail(env, email) {
  const e = normalizeEmail(email);
  if (!e) return null;
  return getState(env, `email:entitlement:email:${e}`);
}

async function markEntitlementActive(env, identity, source, expiresAt) {
  const email = normalizeEmail(identity?.email);
  const userId = normalizeUserId(identity?.userId);
  if (!email && !userId) return;
  const resolvedExpiresAt = Number(expiresAt || 0) || Date.now() + YEAR_SECONDS * 1000;
  const payload = { active: true, email: email || '', userId: userId || '', source, expiresAt: resolvedExpiresAt, updatedAt: Date.now() };
  if (email) await putState(env, `email:entitlement:email:${email}`, payload);
  if (userId) await putState(env, `email:entitlement:user:${userId}`, payload);
}

async function markEntitlementInactive(env, identity, source) {
  const email = normalizeEmail(identity?.email);
  const userId = normalizeUserId(identity?.userId);
  if (!email && !userId) return;
  const payload = { active: false, email: email || '', userId: userId || '', source, updatedAt: Date.now() };
  if (email) await putState(env, `email:entitlement:email:${email}`, payload);
  if (userId) await putState(env, `email:entitlement:user:${userId}`, payload);
}

// ─── usage ────────────────────────────────────────────────

async function getUsageByUserId(env, userId) {
  const id = normalizeUserId(userId);
  if (!id) return { usedRecords: 0, userId: id };
  const usage = await getState(env, `email:usage:user:${id}`);
  if (!usage) return { usedRecords: 0, userId: id };
  return { usedRecords: normalizeInt(usage.usedRecords, 0), userId: id };
}

async function setUsageByUserId(env, userId, usedRecords) {
  const id = normalizeUserId(userId);
  if (!id) return;
  await putState(env, `email:usage:user:${id}`, {
    userId: id,
    usedRecords: Math.max(0, Number(usedRecords) || 0),
    updatedAt: Date.now()
  });
}

// ─── IMAP helpers ─────────────────────────────────────────

const EMAIL_PROVIDER_PRESETS = {
  gmail: { host: 'imap.gmail.com', port: 993, secure: true },
  qq: { host: 'imap.qq.com', port: 993, secure: true },
  '163': { host: 'imap.163.com', port: 993, secure: true },
  feishu: { host: 'imap.larkoffice.com', port: 993, secure: true },
  outlook: { host: 'outlook.office365.com', port: 993, secure: true },
  yahoo: { host: 'imap.mail.yahoo.com', port: 993, secure: true }
};

// 需要在 LOGIN 前发送 IMAP ID 命令的服务商（RFC 2971）
const PROVIDERS_REQUIRING_ID = ['163'];

function normalizeProvider(input) {
  const normalized = String(input || '').trim().toLowerCase();
  if (normalized === 'mail163' || normalized === '163mail') return '163';
  return normalized;
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
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<\/?(p|div|tr|li|h[1-6])[^>]*>/gi, '\n')
    .replace(/<\/?[^>]+>/g, ' ')
    .replace(/&nbsp;/gi, ' ')
    .replace(/&amp;/gi, '&')
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/&quot;/gi, '"')
    .replace(/&#(\d+);/g, (_, n) => String.fromCharCode(Number(n)))
    .replace(/[ \t]+/g, ' ')
    .replace(/\n\s*\n/g, '\n')
    .trim();
}

function resolveImapConfig(body) {
  const provider = normalizeProvider(body?.provider);
  const preset = EMAIL_PROVIDER_PRESETS[provider] || null;
  const host = String(body?.imapHost || preset?.host || '').trim().toLowerCase();
  const email = String(body?.email || '').trim().toLowerCase();
  const password = String(body?.password || body?.appPassword || '').trim();
  const port = normalizeInt(body?.imapPort ?? preset?.port ?? 993, 993, 1, 65535);
  const secure = normalizeBoolean(body?.secure, Boolean(preset?.secure ?? true));
  const folder = String(body?.folder || 'INBOX').trim() || 'INBOX';

  if (!host) throw new Error('缺少 IMAP Host，请选择邮箱类型或手动填写');
  if (!email || !password) throw new Error('请填写邮箱账号和邮箱授权码/密码');

  return {
    provider: provider || 'custom',
    folder,
    auth: { user: email, pass: password },
    connection: { host, port, secure, logger: false }
  };
}

// ─── Workers 原生 IMAP 客户端 (cloudflare:sockets) ──────

class WorkersImapClient {
  constructor(host, port, secure, user, pass, provider) {
    this.host = host;
    this.port = port;
    this.secure = secure;
    this.user = user;
    this.pass = pass;
    this.provider = provider || '';
    this.socket = null;
    this.writer = null;
    this.reader = null;
    this.encoder = new TextEncoder();
    this.decoder = new TextDecoder();
    this.tagCounter = 0;
    this.buffer = '';
  }

  nextTag() {
    return `A${++this.tagCounter}`;
  }

  async connect() {
    const opts = { allowHalfOpen: true };
    if (this.secure) {
      // 端口 993 使用隐式 TLS，直接建立加密连接
      opts.secureTransport = 'on';
    }
    this.socket = connect({ hostname: this.host, port: this.port }, opts);
    this.writer = this.socket.writable.getWriter();
    this.reader = this.socket.readable.getReader();
    // 读取服务器问候语
    await this._readUntilTag(null);
  }

  async login() {
    // 163 邮箱等需要先发送 IMAP ID 命令（RFC 2971），否则后续 SELECT 会报 "Unsafe Login"
    if (PROVIDERS_REQUIRING_ID.includes(this.provider)) {
      await this._sendId();
    }

    const tag = this.nextTag();
    // IMAP LOGIN 需要对含特殊字符的用户名/密码加引号
    const cmd = `${tag} LOGIN "${this._escape(this.user)}" "${this._escape(this.pass)}"\r\n`;
    await this._send(cmd);
    const resp = await this._readUntilTag(tag);
    if (!resp.tagged.toUpperCase().includes(`${tag} OK`)) {
      throw new Error(`IMAP 登录失败: ${resp.tagged}`);
    }
  }

  async _sendId() {
    const tag = this.nextTag();
    const cmd = `${tag} ID ("name" "email-sync" "version" "1.0.0" "vendor" "bitable-plugin")\r\n`;
    await this._send(cmd);
    await this._readUntilTag(tag);
  }

  async listFolders() {
    const tag = this.nextTag();
    await this._send(`${tag} LIST "" "*"\r\n`);
    const resp = await this._readUntilTag(tag);
    if (!resp.tagged.toUpperCase().includes(`${tag} OK`)) {
      throw new Error(`LIST 失败: ${resp.tagged}`);
    }
    const folders = [];
    for (const line of resp.untagged) {
      // * LIST (\HasNoChildren) "/" "INBOX"
      // * LIST (\HasNoChildren) "/" "[Gmail]/Sent Mail"
      const m = line.match(/^\*\s+LIST\s+\(([^)]*)\)\s+"([^"]*)"\s+(?:"([^"]+)"|(\S+))/i);
      if (m) {
        const flags = m[1] || '';
        const name = m[3] || m[4];
        // 跳过不可选择的文件夹
        if (/\\Noselect/i.test(flags)) continue;
        if (name) folders.push(name);
      }
    }
    return folders;
  }

  async select(folder) {
    const tag = this.nextTag();
    await this._send(`${tag} SELECT "${this._escape(folder)}"\r\n`);
    const resp = await this._readUntilTag(tag);
    if (!resp.tagged.toUpperCase().includes(`${tag} OK`)) {
      throw new Error(`选择邮箱 ${folder} 失败: ${resp.tagged}`);
    }
    let exists = 0;
    for (const line of resp.untagged) {
      const m = line.match(/^\*\s+(\d+)\s+EXISTS/i);
      if (m) exists = parseInt(m[1], 10);
    }
    return { exists };
  }

  async fetchHeaders(range) {
    const tag = this.nextTag();
    const cmd = `${tag} FETCH ${range} (BODY.PEEK[HEADER.FIELDS (SUBJECT FROM TO CC MESSAGE-ID DATE)] BODY.PEEK[TEXT])\r\n`;
    await this._send(cmd);
    const resp = await this._readUntilTag(tag);
    if (!resp.tagged.toUpperCase().includes(`${tag} OK`)) {
      throw new Error(`FETCH 失败: ${resp.tagged}`);
    }
    return this._parseFetchResponse(resp.raw);
  }

  async logout() {
    try {
      const tag = this.nextTag();
      await this._send(`${tag} LOGOUT\r\n`);
      await this.socket.close();
    } catch (_) { /* ignore */ }
  }

  _escape(str) {
    return String(str).replace(/\\/g, '\\\\').replace(/"/g, '\\"');
  }

  async _send(cmd) {
    await this.writer.write(this.encoder.encode(cmd));
  }

  async _readUntilTag(tag) {
    const untagged = [];
    let tagged = '';
    let rawLines = [];
    // 空闲超时：只要持续收到数据就不会超时
    const idleTimeout = 45000;
    let lastDataTime = Date.now();

    while (true) {
      if (Date.now() - lastDataTime > idleTimeout) {
        throw new Error('IMAP 响应超时');
      }

      // 尝试从 buffer 中处理完整行
      while (this.buffer.includes('\r\n')) {
        const idx = this.buffer.indexOf('\r\n');
        const line = this.buffer.slice(0, idx);
        this.buffer = this.buffer.slice(idx + 2);
        rawLines.push(line);

        if (tag && line.startsWith(`${tag} `)) {
          tagged = line;
          return { untagged, tagged, raw: rawLines.join('\r\n') };
        }
        if (line.startsWith('*')) {
          untagged.push(line);
        }
        // 没有 tag 的情况下（读取问候语），只要读到一行就返回
        if (!tag) {
          return { untagged: [line], tagged: '', raw: line };
        }
      }

      // 继续读取更多数据
      const { done, value } = await this.reader.read();
      if (done) break;
      lastDataTime = Date.now();
      this.buffer += this.decoder.decode(value, { stream: true });
    }

    return { untagged, tagged, raw: rawLines.join('\r\n') };
  }

  _parseFetchResponse(raw) {
    const emails = [];
    // 按 "* N FETCH" 分割
    const parts = raw.split(/(?=^\* \d+ FETCH)/m);

    for (const part of parts) {
      if (!part.includes('FETCH')) continue;

      const headers = {};
      // 提取 header fields — 匹配 HEADER.FIELDS (...)] {N}\r\n 之后到空行之间的内容
      const headerMatch = part.match(/HEADER\.FIELDS\s*\([^)]*\)\]\s*\{\d+\}\r?\n([\s\S]*?)(?:\r?\n\r?\n)/i);
      if (headerMatch) {
        const headerBlock = headerMatch[1];
        // 解析多行折叠的 header（RFC 2822: 续行以空格/tab开头）
        const unfolded = headerBlock.replace(/\r?\n(?=[ \t])/g, ' ');
        for (const line of unfolded.split(/\r?\n/)) {
          const colonIdx = line.indexOf(':');
          if (colonIdx === -1) continue;
          const key = line.slice(0, colonIdx).trim().toLowerCase();
          const value = line.slice(colonIdx + 1).trim();
          headers[key] = decodeMimeWords(value);
        }
      }

      // 提取 body text — 匹配 BODY[TEXT] {N}\r\n 之后的内容
      // 注意: IMAP literal {N} 的 N 是字节数，而 JS string slice 是字符数
      // UTF-8 多字节字符会导致 byte count > char count
      let body = '';
      const bodyMatch = part.match(/BODY\[TEXT\]\s*\{(\d+)\}\r?\n([\s\S]*)/i);
      if (bodyMatch) {
        const declaredBytes = parseInt(bodyMatch[1], 10);
        const rawContent = bodyMatch[2];
        // 快速路径：先按字符数截取（对纯 ASCII 正确）
        body = rawContent.slice(0, declaredBytes);
        // 检查是否有多字节字符导致超出
        const encoder = new TextEncoder();
        const actualBytes = encoder.encode(body).length;
        if (actualBytes > declaredBytes) {
          // 有多字节字符，用编码后按字节精确截取再解码
          const decoder = new TextDecoder('utf-8', { fatal: false });
          body = decoder.decode(encoder.encode(body).slice(0, declaredBytes));
        }
      } else {
        const simpleMatch = part.match(/BODY\[TEXT\]\s+"([\s\S]*?)"/i);
        if (simpleMatch) body = simpleMatch[1];
      }

      if (!headers['subject'] && !headers['from'] && !headers['message-id']) continue;

      emails.push({
        messageId: String(headers['message-id'] || `ts-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`).trim().replace(/^<|>$/g, ''),
        subject: headers['subject'] || '',
        from: headers['from'] || '',
        to: headers['to'] || '',
        cc: headers['cc'] || '',
        date: headers['date'] ? new Date(headers['date']).getTime() : Date.now(),
        rawBody: body
      });
    }

    return emails;
  }
}

// MIME encoded-word 解码 (=?charset?encoding?text?=)
function decodeMimeWords(str) {
  if (!str) return '';
  return str.replace(/=\?([^?]+)\?([BbQq])\?([^?]*)\?=/g, (_, charset, encoding, text) => {
    try {
      if (encoding.toUpperCase() === 'B') {
        const bytes = Uint8Array.from(atob(text), c => c.charCodeAt(0));
        return new TextDecoder(charset).decode(bytes);
      }
      if (encoding.toUpperCase() === 'Q') {
        const decoded = text
          .replace(/_/g, ' ')
          .replace(/=([0-9A-Fa-f]{2})/g, (_, hex) => String.fromCharCode(parseInt(hex, 16)));
        const bytes = Uint8Array.from(decoded, c => c.charCodeAt(0));
        return new TextDecoder(charset).decode(bytes);
      }
    } catch (_) { /* fall through */ }
    return text;
  });
}

async function fetchEmailsFromImap(config, options, env) {
  const maxMessages = normalizeInt(options?.maxMessages, 100, 1, 500);
  const maxAttachmentsPerMail = normalizeInt(options?.maxAttachmentsPerMail, 8, 0, 20);
  const maxAttachmentBytes = normalizeInt(options?.maxAttachmentBytes, 5 * 1024 * 1024, 1024, 20 * 1024 * 1024);
  const storeAttachments = Boolean(options?.storeAttachments && env);

  const client = new WorkersImapClient(
    config.connection.host,
    config.connection.port,
    config.connection.secure,
    config.auth.user,
    config.auth.pass,
    config.provider
  );

  await client.connect();
  await client.login();

  try {
    const { exists } = await client.select(config.folder);
    if (exists <= 0) return [];

    const from = Math.max(1, exists - maxMessages + 1);
    const allEmails = [];

    // 分批 FETCH，每批最多 20 封，避免超时和内存溢出
    const BATCH_SIZE = 20;
    for (let batchStart = from; batchStart <= exists; batchStart += BATCH_SIZE) {
      const batchEnd = Math.min(batchStart + BATCH_SIZE - 1, exists);
      const range = `${batchStart}:${batchEnd}`;

      const rawEmails = await client.fetchHeaders(range);

      for (const email of rawEmails) {
        const textBody = extractPlainText(email.rawBody);
        const snippet = textBody.slice(0, 500);
        const attachments = extractMimeAttachments(email.rawBody, maxAttachmentsPerMail, maxAttachmentBytes);

        // 附件内容存 KV，只返回元信息 + 下载 key
        const attachmentMeta = [];
        for (const att of attachments) {
          if (storeAttachments && att.contentBase64) {
            const attKey = `att:${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
            try {
              await env.EMAIL_STATE.put(attKey, att.contentBase64, { expirationTtl: 3600 });
              attachmentMeta.push({
                fileName: att.fileName,
                contentType: att.contentType,
                size: att.size,
                attKey
              });
            } catch (_) {
              // KV 写失败，返回完整内容
              attachmentMeta.push(att);
            }
          } else {
            attachmentMeta.push(att);
          }
        }

        allEmails.push({
          messageId: email.messageId,
          subject: email.subject,
          from: email.from,
          to: email.to,
          cc: email.cc,
          date: isNaN(email.date) ? Date.now() : email.date,
          snippet,
          textBody,
          htmlBody: '',
          attachments: attachmentMeta
        });
      }
    }

    return allEmails.sort((a, b) => b.date - a.date);
  } finally {
    await client.logout();
  }
}

/**
 * 从 MIME 正文中提取附件
 * 识别 Content-Disposition: attachment 或非 text/* 的 MIME 部分
 */
function extractMimeAttachments(raw, maxCount = 8, maxBytes = 5 * 1024 * 1024) {
  if (!raw || !raw.includes('--')) return [];

  const attachments = [];
  // 按 MIME boundary 分割
  const parts = raw.split(/^--[^\r\n]+\r?$/m);

  for (const part of parts) {
    if (attachments.length >= maxCount) break;

    // 分离 header 和 body
    let headerEnd = part.indexOf('\r\n\r\n');
    let bodyStart = headerEnd + 4;
    if (headerEnd === -1) {
      headerEnd = part.indexOf('\n\n');
      bodyStart = headerEnd + 2;
    }
    if (headerEnd === -1) continue;

    const headerSection = part.slice(0, headerEnd);
    const headerLower = headerSection.toLowerCase();
    const bodyRaw = part.slice(bodyStart).trim();

    if (!bodyRaw) continue;

    // 判断是否为附件
    const isAttachment = headerLower.includes('content-disposition: attachment') ||
      headerLower.includes('content-disposition:attachment');

    // 检查是否有文件名（filename= 或 name=，含 RFC 2231 的 *=）
    const hasFilename = /(?:filename|name)[*]?\s*=/i.test(headerSection);
    const isInlineWithFile = headerLower.includes('content-disposition: inline') && hasFilename;

    // 非 text/plain、非 text/html、非 multipart 的 content-type 且有 name/filename
    const contentTypeMatch = headerSection.match(/Content-Type:\s*([^\s;]+)/i);
    const contentType = contentTypeMatch ? contentTypeMatch[1].toLowerCase() : '';
    const isNonTextWithName = contentType &&
      !contentType.startsWith('text/') &&
      !contentType.startsWith('multipart/') &&
      hasFilename;

    if (!isAttachment && !isInlineWithFile && !isNonTextWithName) continue;

    // 提取文件名
    let fileName = 'attachment.bin';
    // RFC 2231 encoded: filename*=utf-8''encoded_name 或 name*=utf-8''encoded_name
    const encodedNameMatch = headerSection.match(/(?:filename|name)\*\s*=\s*(?:utf-8|UTF-8)?''([^\s;\r\n]+)/i);
    if (encodedNameMatch) {
      try {
        fileName = decodeURIComponent(encodedNameMatch[1]);
      } catch (_) {
        fileName = encodedNameMatch[1];
      }
    } else {
      // Standard: filename="name" or name="name"
      const nameMatch = headerSection.match(/filename="([^"]+)"/i) ||
        headerSection.match(/filename=([^\s;\r\n]+)/i) ||
        headerSection.match(/name="([^"]+)"/i) ||
        headerSection.match(/name=([^\s;\r\n]+)/i);
      if (nameMatch) {
        fileName = decodeMimeWords(nameMatch[1].trim());
      }
    }

    // 提取 content-type
    const attachContentType = contentType || 'application/octet-stream';

    // 提取编码方式
    const isBase64 = headerLower.includes('content-transfer-encoding: base64') ||
      headerLower.includes('content-transfer-encoding:base64');

    let contentBase64;
    if (isBase64) {
      // 已经是 base64，清理空白
      contentBase64 = bodyRaw.replace(/[\r\n\s]+/g, '');
    } else {
      // 非 base64 编码（7bit/8bit/quoted-printable）→ 转 base64
      try {
        // 先用 TextEncoder 转为 UTF-8 字节，再转 base64
        const bytes = new TextEncoder().encode(bodyRaw);
        let binary = '';
        for (let i = 0; i < bytes.length; i++) {
          binary += String.fromCharCode(bytes[i]);
        }
        contentBase64 = btoa(binary);
      } catch (_) {
        continue;
      }
    }

    // 检查大小限制（base64 约为原始大小的 4/3）
    const estimatedSize = Math.floor(contentBase64.length * 3 / 4);
    if (estimatedSize > maxBytes) continue;

    attachments.push({
      fileName,
      contentType: attachContentType,
      size: estimatedSize,
      contentBase64
    });
  }

  return attachments;
}

/**
 * 从 MIME 正文中提取纯文本内容
 */
function extractPlainText(raw) {
  if (!raw) return '';

  // 非 multipart → 直接解码
  if (!raw.includes('--') || !raw.includes('Content-Type')) {
    return decodeQuotedPrintable(raw);
  }

  // 按 MIME boundary 分割（boundary 行以 -- 开头）
  const parts = raw.split(/^--[^\r\n]+\r?$/m);

  // 从每个 part 中提取 header 和 body（头部与正文之间用空行分隔）
  function extractPartBody(part) {
    // 支持 \r\n\r\n 和 \n\n 两种空行格式
    let headerEnd = part.indexOf('\r\n\r\n');
    let bodyStart = headerEnd + 4;
    if (headerEnd === -1) {
      headerEnd = part.indexOf('\n\n');
      bodyStart = headerEnd + 2;
    }
    if (headerEnd === -1) return null;
    const headerSection = part.slice(0, headerEnd);
    const headerLower = headerSection.toLowerCase();
    let body = part.slice(bodyStart).trim();

    // 提取 charset
    const charsetMatch = headerSection.match(/charset="?([^";\s\r\n]+)/i);
    const charset = charsetMatch ? charsetMatch[1].toLowerCase() : 'utf-8';

    if (headerLower.includes('quoted-printable')) {
      body = decodeQuotedPrintable(body, charset);
    } else if (headerLower.includes('base64')) {
      try { body = decodeBase64Text(body, part); } catch (_) {}
    }
    return { headerSection: headerLower, body };
  }

  // 优先找 text/plain
  for (const part of parts) {
    const lower = part.toLowerCase();
    if (!lower.includes('content-type:')) continue;
    if (lower.includes('text/plain')) {
      const result = extractPartBody(part);
      if (result && result.body) return result.body;
    }
  }

  // fallback: text/html → 去标签
  for (const part of parts) {
    const lower = part.toLowerCase();
    if (!lower.includes('content-type:')) continue;
    if (lower.includes('text/html')) {
      const result = extractPartBody(part);
      if (result && result.body) return stripHtml(result.body);
    }
  }

  return raw.slice(0, 2000);
}

function decodeQuotedPrintable(str, charset = 'utf-8') {
  // 移除软换行
  const unfolded = str.replace(/=\r?\n/g, '');
  // 将 =XX 转为字节，再按 charset 解码（正确处理 UTF-8 多字节中文等）
  const bytes = [];
  for (let i = 0; i < unfolded.length; i++) {
    if (unfolded[i] === '=' && i + 2 < unfolded.length) {
      const hex = unfolded.substring(i + 1, i + 3);
      if (/^[0-9A-Fa-f]{2}$/.test(hex)) {
        bytes.push(parseInt(hex, 16));
        i += 2;
        continue;
      }
    }
    bytes.push(unfolded.charCodeAt(i));
  }
  try {
    return new TextDecoder(charset).decode(new Uint8Array(bytes));
  } catch (_) {
    return new TextDecoder('utf-8').decode(new Uint8Array(bytes));
  }
}

function decodeBase64Text(body, part) {
  const raw = atob(body.replace(/\s/g, ''));
  // 检测 charset
  const charsetMatch = part.match(/charset="?([^";\s]+)/i);
  const charset = charsetMatch ? charsetMatch[1] : 'utf-8';
  const bytes = Uint8Array.from(raw, c => c.charCodeAt(0));
  return new TextDecoder(charset).decode(bytes);
}

// ─── Schedule storage (KV) ───────────────────────────────

async function loadSchedules(env) {
  return (await getState(env, SCHEDULE_KV_KEY)) || {};
}

async function saveSchedules(env, data) {
  await putStatePermanent(env, SCHEDULE_KV_KEY, data);
}

async function getSchedule(env, id) {
  const data = await loadSchedules(env);
  return data[id] || null;
}

async function putSchedule(env, schedule) {
  const data = await loadSchedules(env);
  data[schedule.id] = schedule;
  await saveSchedules(env, data);
}

async function removeSchedule(env, id) {
  const data = await loadSchedules(env);
  delete data[id];
  await saveSchedules(env, data);
}

// ─── Lark/Feishu Bitable API (直接 fetch，替代 node-sdk) ──

// PersonalBaseToken 需要使用 base-api 域名（非 open.feishu.cn）
const LARK_API_BASE = 'https://base-api.feishu.cn/open-apis/bitable/v1';

async function larkApiGet(personalBaseToken, path, params = {}) {
  const query = new URLSearchParams(params).toString();
  const url = `${LARK_API_BASE}${path}${query ? `?${query}` : ''}`;
  const resp = await fetch(url, {
    headers: { 'Authorization': `Bearer ${personalBaseToken}` }
  });
  const data = await resp.json();
  if (data.code !== 0) {
    throw new Error(`Lark API 错误: ${data.msg || data.code}`);
  }
  return data.data;
}

async function larkApiPost(personalBaseToken, path, body) {
  const url = `${LARK_API_BASE}${path}`;
  const resp = await fetch(url, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${personalBaseToken}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(body)
  });
  const data = await resp.json();
  if (data.code !== 0) {
    throw new Error(`Lark API 错误: ${data.msg || data.code}`);
  }
  return data.data;
}

async function larkListExistingRecords(personalBaseToken, appToken, tableId) {
  // 返回 Map<messageId, { recordId, hasAttachment }>
  const existing = new Map();
  let pageToken = '';
  let hasMore = true;

  while (hasMore) {
    const params = { page_size: '500' };
    if (pageToken) params.page_token = pageToken;

    const result = await larkApiGet(
      personalBaseToken,
      `/apps/${appToken}/tables/${tableId}/records`,
      params
    );

    for (const item of result?.items || []) {
      const emailIdField = item?.fields?.['邮件ID'];
      const val = Array.isArray(emailIdField)
        ? String((emailIdField[0])?.text || '').trim()
        : String(emailIdField || '').trim();
      if (val) {
        const att = item?.fields?.['附件'];
        const hasAttachment = Array.isArray(att) && att.length > 0;
        existing.set(val, { recordId: item.record_id, hasAttachment });
      }
    }

    hasMore = result?.has_more || false;
    pageToken = result?.page_token || '';
  }

  return existing;
}

async function larkBatchCreateRecords(personalBaseToken, appToken, tableId, records) {
  const BATCH_SIZE = 500;
  const allRecordIds = [];
  for (let i = 0; i < records.length; i += BATCH_SIZE) {
    const batch = records.slice(i, i + BATCH_SIZE);
    const result = await larkApiPost(
      personalBaseToken,
      `/apps/${appToken}/tables/${tableId}/records/batch_create`,
      { records: batch }
    );
    // 收集返回的 record_id
    const items = result?.records || [];
    for (const item of items) {
      allRecordIds.push(item?.record_id || '');
    }
  }
  return allRecordIds;
}

// PersonalBaseToken 也支持 drive API（同样走 base-api 域名）
const LARK_DRIVE_BASE = 'https://base-api.feishu.cn/open-apis/drive/v1';

function sanitizeBase64(b64) {
  // 移除非 base64 字符（保留 A-Z a-z 0-9 + / =）
  let cleaned = b64.replace(/[^A-Za-z0-9+/=]/g, '');
  // 确保长度是 4 的倍数（补 =）
  const remainder = cleaned.length % 4;
  if (remainder === 2) cleaned += '==';
  else if (remainder === 3) cleaned += '=';
  else if (remainder === 1) cleaned = cleaned.slice(0, -1); // 单字符无效，去掉
  return cleaned;
}

async function larkUploadAttachment(personalBaseToken, appToken, fileName, contentBase64) {
  const cleanBase64 = sanitizeBase64(contentBase64);
  const binaryStr = atob(cleanBase64);
  const bytes = new Uint8Array(binaryStr.length);
  for (let i = 0; i < binaryStr.length; i++) {
    bytes[i] = binaryStr.charCodeAt(i);
  }

  // 清理文件名中的换行符和非法字符，避免 Lark API 报 "params error"
  const cleanFileName = String(fileName || 'attachment.bin')
    .replace(/[\r\n\t]+/g, ' ')
    .replace(/\s{2,}/g, ' ')
    .trim() || 'attachment.bin';

  const formData = new FormData();
  formData.append('file_name', cleanFileName);
  formData.append('parent_type', 'bitable_file');
  formData.append('parent_node', appToken);
  // 使用实际解码后的字节数，而非 base64 估算值，避免 Lark API 报 size 不一致
  formData.append('size', String(bytes.length));
  formData.append('file', new Blob([bytes]), cleanFileName);

  const resp = await fetch(`${LARK_DRIVE_BASE}/medias/upload_all`, {
    method: 'POST',
    headers: { 'Authorization': `Bearer ${personalBaseToken}` },
    body: formData
  });

  const data = await resp.json();
  if (data.code !== 0) {
    throw new Error(`附件上传失败: ${data.msg || data.code}`);
  }
  return data.data.file_token;
}

async function larkUpdateRecord(personalBaseToken, appToken, tableId, recordId, fields) {
  const url = `${LARK_API_BASE}/apps/${appToken}/tables/${tableId}/records/${recordId}`;
  const resp = await fetch(url, {
    method: 'PUT',
    headers: {
      'Authorization': `Bearer ${personalBaseToken}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({ fields })
  });
  const data = await resp.json();
  if (data.code !== 0) {
    throw new Error(`Lark API 更新失败: ${data.msg || data.code}`);
  }
  return data.data;
}

// ─── Scheduled sync execution ────────────────────────────

/**
 * 从 KV 或 email 对象中获取附件的 base64 内容
 * 支持两种模式：
 *   - contentBase64: 内容直接在 email 对象中（旧模式）
 *   - attKey: 内容存储在 KV 中（新模式，减少内存占用）
 */
async function resolveAttachmentBase64(att, env) {
  if (att.contentBase64) return att.contentBase64;
  if (att.attKey && env?.EMAIL_STATE) {
    try {
      const content = await env.EMAIL_STATE.get(att.attKey);
      return content || null;
    } catch (_) {
      return null;
    }
  }
  return null;
}

async function executeScheduledSync(env, scheduleId) {
  const schedule = await getSchedule(env, scheduleId);
  if (!schedule || !schedule.enabled) return;

  console.log(`[schedule] executing sync for ${scheduleId}`);

  try {
    const imapConfig = resolveImapConfig(schedule.imapConfig);
    // 使用 storeAttachments + env 将附件存入 KV，减少内存压力
    // 与手动同步使用相同的策略
    const emails = await fetchEmailsFromImap(imapConfig, {
      maxMessages: schedule.imapConfig.maxMessages || 100,
      maxAttachmentsPerMail: 8,
      maxAttachmentBytes: 5 * 1024 * 1024,
      storeAttachments: true
    }, env);

    if (!emails.length) {
      schedule.lastSyncAt = Date.now();
      schedule.lastSyncStatus = 'success';
      schedule.lastSyncMessage = '未发现新邮件';
      await putSchedule(env, schedule);
      return;
    }

    const { personalBaseToken, appToken, tableId } = schedule.feishuConfig;

    // 读取表中已有记录（含附件状态）用于去重和附件补丁
    let existingRecords = new Map();
    try {
      existingRecords = await larkListExistingRecords(personalBaseToken, appToken, tableId);
    } catch (err) {
      console.log(`[schedule] ${scheduleId}: read existing records failed, proceeding: ${err.message}`);
    }

    const newEmails = emails.filter(e => e.messageId && !existingRecords.has(e.messageId));

    // 找出已存在但缺附件的记录，本次拉取的邮件有附件 → 需要补丁
    const toPatchAttachments = [];
    for (const email of emails) {
      if (!email.messageId) continue;
      const existing = existingRecords.get(email.messageId);
      if (!existing) continue;
      if (existing.hasAttachment) continue;
      const attachments = Array.isArray(email.attachments) ? email.attachments : [];
      if (attachments.some(a => (a.contentBase64 || a.attKey) && a.size > 0)) {
        toPatchAttachments.push({ recordId: existing.recordId, email });
      }
    }

    const provider = schedule.imapConfig.provider || 'custom';
    const syncTime = Date.now();

    // ── 借鉴手动同步的处理方式：先创建记录，再逐条补充附件 ──
    // 这样即使附件上传失败，记录已存在，下次同步可以补丁
    let newRecordIds = [];
    if (newEmails.length) {
      const records = newEmails.map(email => ({
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
      }));

      newRecordIds = await larkBatchCreateRecords(personalBaseToken, appToken, tableId, records);
      console.log(`[schedule] ${scheduleId}: created ${newRecordIds.length} new records`);
    }

    // 逐条处理新记录的附件（从 KV 按需读取 base64，减少内存占用）
    let totalAttachments = 0;
    let attachFailures = 0;
    for (let i = 0; i < newEmails.length; i++) {
      const email = newEmails[i];
      const recordId = newRecordIds[i];
      if (!recordId) continue;
      const attachments = Array.isArray(email.attachments) ? email.attachments : [];
      if (!attachments.some(a => (a.contentBase64 || a.attKey) && a.size > 0)) continue;

      const tokens = [];
      for (const att of attachments) {
        if (att.size <= 0) continue;
        try {
          const base64 = await resolveAttachmentBase64(att, env);
          if (!base64) {
            console.log(`[schedule] ${scheduleId}: no base64 for ${att.fileName} (attKey: ${att.attKey || 'none'})`);
            attachFailures++;
            continue;
          }
          const fileToken = await larkUploadAttachment(
            personalBaseToken, appToken, att.fileName, base64
          );
          tokens.push({ file_token: fileToken });
          totalAttachments++;
        } catch (err) {
          console.log(`[schedule] ${scheduleId}: attachment upload failed (${att.fileName}): ${err.message}`);
          attachFailures++;
        }
      }
      if (tokens.length) {
        try {
          await larkUpdateRecord(personalBaseToken, appToken, tableId, recordId, { '附件': tokens });
        } catch (err) {
          console.log(`[schedule] ${scheduleId}: update record attachment failed (${recordId}): ${err.message}`);
        }
      }
    }

    // 补充已有记录缺失的附件（同样从 KV 按需读取）
    let patchedAttachments = 0;
    for (const { recordId, email } of toPatchAttachments) {
      const attachments = Array.isArray(email.attachments) ? email.attachments : [];
      const tokens = [];
      for (const att of attachments) {
        if (att.size <= 0) continue;
        try {
          const base64 = await resolveAttachmentBase64(att, env);
          if (!base64) {
            console.log(`[schedule] ${scheduleId}: patch - no base64 for ${att.fileName}`);
            continue;
          }
          const fileToken = await larkUploadAttachment(
            personalBaseToken, appToken, att.fileName, base64
          );
          tokens.push({ file_token: fileToken });
          totalAttachments++;
        } catch (err) {
          console.log(`[schedule] ${scheduleId}: patch attachment upload failed (${att.fileName}): ${err.message}`);
        }
      }
      if (tokens.length) {
        try {
          await larkUpdateRecord(personalBaseToken, appToken, tableId, recordId, { '附件': tokens });
          patchedAttachments++;
        } catch (err) {
          console.log(`[schedule] ${scheduleId}: patch record failed (${recordId}): ${err.message}`);
        }
      }
    }

    const attachMsg = totalAttachments > 0 ? `，附件 ${totalAttachments} 个` : '';
    const failMsg = attachFailures > 0 ? `，附件失败 ${attachFailures} 个` : '';
    const patchMsg = patchedAttachments > 0 ? `，补充 ${patchedAttachments} 条旧附件` : '';
    const patchTryMsg = toPatchAttachments.length > 0 && patchedAttachments === 0
      ? `，尝试补丁 ${toPatchAttachments.length} 条（全部失败）` : '';
    schedule.lastSyncAt = syncTime;
    schedule.lastSyncStatus = 'success';
    schedule.lastSyncMessage = `拉取 ${emails.length} 封，新增 ${newEmails.length} 封，跳过 ${emails.length - newEmails.length} 封${attachMsg}${failMsg}${patchMsg}${patchTryMsg}`;
    await putSchedule(env, schedule);
    console.log(`[schedule] ${scheduleId}: synced ${newEmails.length} new, ${totalAttachments} attachments, ${attachFailures} failures, patchTried=${toPatchAttachments.length}, patched=${patchedAttachments}`);
  } catch (error) {
    console.error(`[schedule] ${scheduleId} sync failed:`, error);
    schedule.lastSyncAt = Date.now();
    schedule.lastSyncStatus = 'failed';
    schedule.lastSyncMessage = error?.message || '同步失败';
    await putSchedule(env, schedule);
  }
}

// ─── Stripe helpers ───────────────────────────────────────

function getStripeSecret(env) {
  return String(env.STRIPE_SECRET_KEY || '').trim();
}

function getStripePublishableKey(env) {
  return String(env.STRIPE_PUBLISHABLE_KEY || '').trim();
}

function getStripeProductName(env) {
  return String(env.STRIPE_PRODUCT_NAME || '邮箱同步飞书多维表格').trim();
}

function asArray(value) {
  return Array.isArray(value) ? value : [];
}

async function stripeApiRequest(env, path, bodyParams) {
  const secret = getStripeSecret(env);
  if (!secret) throw new Error('未配置 STRIPE_SECRET_KEY');
  const response = await fetch(`https://api.stripe.com${path}`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${secret}`, 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams(bodyParams).toString()
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(payload?.error?.message || `Stripe 请求失败: ${response.status}`);
  return payload;
}

async function stripeApiGet(env, path, params = {}) {
  const secret = getStripeSecret(env);
  if (!secret) throw new Error('未配置 STRIPE_SECRET_KEY');
  const query = new URLSearchParams(params).toString();
  const response = await fetch(`https://api.stripe.com${path}${query ? `?${query}` : ''}`, {
    method: 'GET',
    headers: { Authorization: `Bearer ${secret}` }
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(payload?.error?.message || `Stripe 请求失败: ${response.status}`);
  return payload;
}

async function resolveStripePrice(env, productName) {
  const configuredPriceId = String(env.STRIPE_PRICE_ID || '').trim();
  if (configuredPriceId) {
    const price = await stripeApiGet(env, `/v1/prices/${configuredPriceId}`);
    return { priceId: configuredPriceId, mode: price?.recurring ? 'subscription' : 'payment' };
  }
  const targetName = String(productName || '').trim();
  const products = await stripeApiGet(env, '/v1/products', { active: 'true', limit: '100' });
  const matchedProduct = asArray(products?.data).find((p) => String(p?.name || '').trim() === targetName);
  if (!matchedProduct?.id) throw new Error(`未在 Stripe 中找到产品：${targetName}`);
  const prices = await stripeApiGet(env, '/v1/prices', { active: 'true', limit: '100', product: String(matchedProduct.id) });
  const matchedPrice = asArray(prices?.data).find((p) => String(p?.id || '').startsWith('price_'));
  if (!matchedPrice?.id) throw new Error(`未在 Stripe 中找到价格：${targetName}`);
  return { priceId: String(matchedPrice.id), mode: matchedPrice?.recurring ? 'subscription' : 'payment' };
}

// ─── Stripe webhook verification ─────────────────────────

function timingSafeEqual(a, b) {
  if (a.length !== b.length) return false;
  let out = 0;
  for (let i = 0; i < a.length; i += 1) out |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return out === 0;
}

function parseStripeSignature(header) {
  const output = { t: '', v1: '' };
  for (const part of String(header || '').split(',')) {
    const [k, v] = part.split('=');
    if (k === 't') output.t = v;
    if (k === 'v1') output.v1 = v;
  }
  return output;
}

async function hmacSha256Hex(secret, content) {
  const key = await crypto.subtle.importKey('raw', new TextEncoder().encode(secret), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
  const sig = await crypto.subtle.sign('HMAC', key, new TextEncoder().encode(content));
  return [...new Uint8Array(sig)].map((b) => b.toString(16).padStart(2, '0')).join('');
}

async function verifyStripeWebhook(req, env, rawBody) {
  const secret = String(env.STRIPE_WEBHOOK_SECRET || '').trim();
  if (!secret) throw new Error('未配置 STRIPE_WEBHOOK_SECRET');
  const parsed = parseStripeSignature(req.headers.get('Stripe-Signature'));
  if (!parsed.t || !parsed.v1) throw new Error('缺少 Stripe-Signature');
  const expected = await hmacSha256Hex(secret, `${parsed.t}.${rawBody}`);
  if (!timingSafeEqual(expected, parsed.v1)) throw new Error('Stripe webhook 签名校验失败');
}

// ─── route handlers ───────────────────────────────────────

async function handleEmailQuota(req, env) {
  const url = new URL(req.url);
  const userId = normalizeUserId(url.searchParams.get('userId'));
  if (!userId) {
    return jsonResponse(req, env, 400, { status: 'failed', message: '缺少 userId 参数' });
  }
  const entitlement = await getEntitlementByUserId(env, userId);
  const paid = resolveEntitlementActive(entitlement);
  if (paid) {
    return jsonResponse(req, env, 200, { paid: true, used: 0, remaining: Infinity, total: Infinity });
  }
  const usage = await getUsageByUserId(env, userId);
  const used = usage.usedRecords;
  const remaining = Math.max(0, FREE_EMAIL_QUOTA - used);
  return jsonResponse(req, env, 200, { paid: false, used, remaining, total: FREE_EMAIL_QUOTA });
}

async function handleEmailUsage(req, env) {
  const body = (await req.json().catch(() => ({}))) || {};
  const userId = normalizeUserId(body?.userId);
  const count = normalizeInt(body?.count, 0, 0, 500);
  if (!userId) {
    return jsonResponse(req, env, 400, { status: 'failed', message: '缺少 userId 参数' });
  }
  const usage = await getUsageByUserId(env, userId);
  const newUsed = (usage.usedRecords || 0) + count;
  await setUsageByUserId(env, userId, newUsed);
  return jsonResponse(req, env, 200, { status: 'ok', used: newUsed });
}

async function handleEmailSync(req, env) {
  try {
    const body = (await req.json().catch(() => ({}))) || {};
    const userId = normalizeUserId(body?.userId);
    let maxMessages = normalizeInt(body?.maxMessages, 100, 1, 500);

    if (userId) {
      const usage = await getUsageByUserId(env, userId);
      const entitlement = await getEntitlementByUserId(env, userId);
      const paid = resolveEntitlementActive(entitlement);
      if (!paid) {
        const used = usage.usedRecords || 0;
        const remaining = Math.max(0, FREE_EMAIL_QUOTA - used);
        if (remaining <= 0) {
          return jsonResponse(req, env, 403, {
            status: 'quota_exceeded',
            message: `免费 ${FREE_EMAIL_QUOTA} 封额度已用完，请升级付费版`,
            used,
            total: FREE_EMAIL_QUOTA
          });
        }
        maxMessages = Math.min(maxMessages, remaining);
      }
    }

    const config = resolveImapConfig(body);
    const emails = await fetchEmailsFromImap(config, {
      maxMessages,
      maxAttachmentsPerMail: 8,
      maxAttachmentBytes: 5 * 1024 * 1024,
      storeAttachments: true
    }, env);
    return jsonResponse(req, env, 200, {
      status: 'completed',
      provider: config.provider,
      mailbox: config.folder,
      emails
    });
  } catch (error) {
    return jsonResponse(req, env, 500, {
      status: 'failed',
      message: error?.message || '邮箱同步失败'
    });
  }
}

async function handleGetEntitlement(req, env) {
  const url = new URL(req.url);
  const userId = normalizeUserId(url.searchParams.get('userId'));
  const email = normalizeEmail(url.searchParams.get('email'));
  if (!userId && !email) {
    return jsonResponse(req, env, 400, { status: 'failed', message: '缺少 userId 参数' });
  }
  const entitlement = userId ? await getEntitlementByUserId(env, userId) : await getEntitlementByEmail(env, email);
  const usage = await getUsageByUserId(env, userId || '');
  const active = resolveEntitlementActive(entitlement);
  const remainingFree = Math.max(0, FREE_EMAIL_QUOTA - usage.usedRecords);
  return jsonResponse(req, env, 200, {
    status: 'ok',
    entitlement: entitlement ? { ...entitlement, active } : { active: false, userId: userId || '', email: email || '', expiresAt: 0 },
    freeQuota: { total: FREE_EMAIL_QUOTA, used: usage.usedRecords, remaining: remainingFree }
  });
}

async function handleCreateCheckoutSession(req, env) {
  const body = (await req.json().catch(() => ({}))) || {};
  const successUrl = String(body?.successUrl || '').trim();
  const cancelUrl = String(body?.cancelUrl || '').trim();
  const customerEmail = String(body?.customerEmail || '').trim();
  const userId = String(body?.userId || '').trim();
  const productName = String(body?.productName || getStripeProductName(env)).trim();
  if (!successUrl || !cancelUrl) {
    return jsonResponse(req, env, 400, { status: 'failed', message: 'successUrl 和 cancelUrl 不能为空' });
  }
  try {
    const resolved = await resolveStripePrice(env, productName);
    const mode = String(env.STRIPE_CHECKOUT_MODE || resolved.mode).trim();
    const checkoutParams = {
      mode,
      success_url: successUrl,
      cancel_url: cancelUrl,
      'line_items[0][price]': resolved.priceId,
      'line_items[0][quantity]': '1',
      allow_promotion_codes: 'true',
      'metadata[user_id]': userId,
      'metadata[product_name]': productName
    };
    if (customerEmail) {
      checkoutParams.customer_email = customerEmail;
      checkoutParams['metadata[customer_email]'] = customerEmail;
    }
    const session = await stripeApiRequest(env, '/v1/checkout/sessions', checkoutParams);
    return jsonResponse(req, env, 200, { status: 'ok', url: session?.url, sessionId: session?.id, publishableKey: getStripePublishableKey(env) });
  } catch (error) {
    return jsonResponse(req, env, 500, { status: 'failed', message: error?.message || '创建结算会话失败' });
  }
}

async function handleStripeWebhook(req, env) {
  const rawBody = await req.text();
  let event;
  try {
    await verifyStripeWebhook(req, env, rawBody);
    event = JSON.parse(rawBody);
  } catch (error) {
    return jsonResponse(req, env, 400, { status: 'failed', message: error?.message || 'webhook 校验失败' });
  }

  const eventId = String(event?.id || '');
  if (eventId) {
    const seen = await getState(env, `email:webhook:event:${eventId}`);
    if (seen) return jsonResponse(req, env, 200, { received: true, dedup: true });
    await putState(env, `email:webhook:event:${eventId}`, { receivedAt: Date.now() });
  }

  try {
    const type = String(event?.type || '');
    const object = event?.data?.object || {};
    const expectedProduct = getStripeProductName(env);

    if (type === 'checkout.session.completed') {
      const eventProduct = String(object?.metadata?.product_name || '').trim();
      if (eventProduct && eventProduct !== expectedProduct) {
        return jsonResponse(req, env, 200, { received: true, skipped: true, reason: 'product_mismatch' });
      }
      const email = String(object?.customer_details?.email || object?.customer_email || object?.metadata?.customer_email || '').trim();
      const userId = String(object?.metadata?.user_id || '').trim();
      const customerId = String(object?.customer || '').trim();
      const mode = String(object?.mode || '');
      if (customerId && (email || userId)) {
        await putState(env, `email:customer:${customerId}`, { email, userId, updatedAt: Date.now() });
      }
      const expiresAt = mode === 'subscription' ? 0 : Date.now() + YEAR_SECONDS * 1000;
      await markEntitlementActive(env, { email, userId }, 'checkout.session.completed', expiresAt);
    }
    if (type === 'customer.subscription.updated' || type === 'customer.subscription.created') {
      const customerId = String(object?.customer || '').trim();
      const customerState = customerId ? await getState(env, `email:customer:${customerId}`) : null;
      if (!customerState) {
        return jsonResponse(req, env, 200, { received: true, skipped: true, reason: 'unknown_customer' });
      }
      const status = String(object?.status || '');
      const email = String(customerState.email || '').trim();
      const userId = String(customerState.userId || '').trim();
      const periodEnd = Number(object?.current_period_end || 0);
      const expiresAt = periodEnd > 0 ? periodEnd * 1000 : Date.now() + YEAR_SECONDS * 1000;
      if (status === 'active' || status === 'trialing') {
        await markEntitlementActive(env, { email, userId }, type, expiresAt);
      } else if (status) {
        await markEntitlementInactive(env, { email, userId }, `${type}:${status}`);
      }
    }
    if (type === 'customer.subscription.deleted') {
      const customerId = String(object?.customer || '').trim();
      const customerState = customerId ? await getState(env, `email:customer:${customerId}`) : null;
      if (!customerState) {
        return jsonResponse(req, env, 200, { received: true, skipped: true, reason: 'unknown_customer' });
      }
      const email = String(customerState.email || '').trim();
      const userId = String(customerState.userId || '').trim();
      await markEntitlementInactive(env, { email, userId }, type);
    }
  } catch (error) {
    return jsonResponse(req, env, 500, { status: 'failed', message: error?.message || 'webhook 处理失败' });
  }

  return jsonResponse(req, env, 200, { received: true });
}

// ─── schedule route handlers ─────────────────────────────

async function handleScheduleCreate(req, env) {
  try {
    const body = (await req.json().catch(() => ({}))) || {};
    const id = String(body.id || '').trim() || `sch_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    const intervalHours = Math.max(1, Math.min(24, Number(body.intervalHours) || 3));

    const email = String(body.imapConfig?.email || '').trim();
    const password = String(body.imapConfig?.password || '').trim();
    if (!email || !password) {
      return jsonResponse(req, env, 400, { status: 'failed', message: '请填写邮箱账号和授权码' });
    }

    const personalBaseToken = String(body.feishuConfig?.personalBaseToken || '').trim();
    const appToken = String(body.feishuConfig?.appToken || '').trim();
    const tableId = String(body.feishuConfig?.tableId || '').trim();
    if (!personalBaseToken || !appToken || !tableId) {
      return jsonResponse(req, env, 400, { status: 'failed', message: '请填写多维表格授权码、appToken 和 tableId' });
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

    await putSchedule(env, schedule);
    return jsonResponse(req, env, 200, { status: 'ok', id: schedule.id, intervalHours: schedule.intervalHours });
  } catch (error) {
    return jsonResponse(req, env, 500, { status: 'failed', message: error?.message || '创建定时任务失败' });
  }
}

async function handleScheduleDelete(req, env) {
  try {
    const body = (await req.json().catch(() => ({}))) || {};
    const id = String(body.id || '').trim();
    if (!id) {
      return jsonResponse(req, env, 400, { status: 'failed', message: '缺少 id 参数' });
    }
    await removeSchedule(env, id);
    return jsonResponse(req, env, 200, { status: 'ok' });
  } catch (error) {
    return jsonResponse(req, env, 500, { status: 'failed', message: error?.message || '删除定时任务失败' });
  }
}

async function handleScheduleList(req, env) {
  try {
    const url = new URL(req.url);
    const userId = normalizeUserId(url.searchParams.get('userId'));
    const all = await loadSchedules(env);
    const list = Object.values(all)
      .filter(s => !userId || !s.userId || s.userId === userId)
      .map(s => ({
        id: s.id,
        enabled: s.enabled,
        intervalHours: s.intervalHours,
        provider: s.imapConfig?.provider || 'custom',
        email: s.imapConfig?.email || '',
        folder: s.imapConfig?.folder || 'INBOX',
        lastSyncAt: s.lastSyncAt,
        lastSyncStatus: s.lastSyncStatus,
        lastSyncMessage: s.lastSyncMessage,
        createdAt: s.createdAt
      }));
    return jsonResponse(req, env, 200, { status: 'ok', schedules: list });
  } catch (error) {
    return jsonResponse(req, env, 500, { status: 'failed', message: error?.message || '查询列表失败' });
  }
}

async function handleScheduleStatus(req, env) {
  try {
    const url = new URL(req.url);
    const id = String(url.searchParams.get('id') || '').trim();
    if (!id) {
      return jsonResponse(req, env, 400, { status: 'failed', message: '缺少 id 参数' });
    }
    const schedule = await getSchedule(env, id);
    if (!schedule) {
      return jsonResponse(req, env, 200, { exists: false });
    }
    return jsonResponse(req, env, 200, {
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
    return jsonResponse(req, env, 500, { status: 'failed', message: error?.message || '查询状态失败' });
  }
}

async function handleScheduleTrigger(req, env) {
  try {
    const body = (await req.json().catch(() => ({}))) || {};
    const id = String(body.id || '').trim();
    if (!id) {
      return jsonResponse(req, env, 400, { status: 'failed', message: '缺少 id 参数' });
    }
    const schedule = await getSchedule(env, id);
    if (!schedule) {
      return jsonResponse(req, env, 404, { status: 'failed', message: '定时任务不存在' });
    }
    // Execute synchronously for manual trigger so user gets immediate result
    await executeScheduledSync(env, id);
    const updated = await getSchedule(env, id);
    return jsonResponse(req, env, 200, {
      status: 'ok',
      lastSyncAt: updated?.lastSyncAt,
      lastSyncStatus: updated?.lastSyncStatus,
      lastSyncMessage: updated?.lastSyncMessage
    });
  } catch (error) {
    return jsonResponse(req, env, 500, { status: 'failed', message: error?.message || '触发同步失败' });
  }
}

// ─── router ───────────────────────────────────────────────

export default {
  async fetch(req, env) {
    const { pathname } = new URL(req.url);
    if (req.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: corsHeaders(req, env) });
    }
    // health
    if (req.method === 'GET' && pathname === '/healthz') {
      return jsonResponse(req, env, 200, { ok: true, now: Date.now() });
    }
    // list IMAP folders
    if (req.method === 'POST' && pathname === '/api/email/folders') {
      try {
        const body = (await req.json().catch(() => ({}))) || {};
        const config = resolveImapConfig(body);
        const client = new WorkersImapClient(
          config.connection.host,
          config.connection.port,
          config.connection.secure,
          config.auth.user,
          config.auth.pass,
          config.provider
        );
        await client.connect();
        await client.login();
        const folders = await client.listFolders();
        await client.logout();
        return jsonResponse(req, env, 200, { status: 'ok', folders });
      } catch (error) {
        return jsonResponse(req, env, 500, { status: 'failed', message: error?.message || '获取文件夹列表失败' });
      }
    }
    // email sync
    if (req.method === 'POST' && pathname === '/api/email/sync') {
      return handleEmailSync(req, env);
    }
    // attachment download (从 KV 获取缓存的附件内容)
    if (req.method === 'GET' && pathname === '/api/email/attachment') {
      const key = new URL(req.url).searchParams.get('key');
      if (!key) return jsonResponse(req, env, 400, { status: 'failed', message: '缺少 key' });
      const data = await env.EMAIL_STATE.get(key);
      if (!data) return jsonResponse(req, env, 404, { status: 'failed', message: '附件已过期或不存在' });
      return jsonResponse(req, env, 200, { status: 'ok', contentBase64: data });
    }
    // quota & usage
    if (req.method === 'GET' && pathname === '/api/email/quota') {
      return handleEmailQuota(req, env);
    }
    if (req.method === 'POST' && pathname === '/api/email/usage') {
      return handleEmailUsage(req, env);
    }
    // stripe
    if (req.method === 'GET' && pathname === '/api/stripe/entitlement') {
      return handleGetEntitlement(req, env);
    }
    if (req.method === 'POST' && pathname === '/api/stripe/create-checkout-session') {
      return handleCreateCheckoutSession(req, env);
    }
    if (req.method === 'POST' && pathname === '/api/stripe/webhook') {
      return handleStripeWebhook(req, env);
    }
    // schedule
    if (req.method === 'GET' && pathname === '/api/schedule/list') {
      return handleScheduleList(req, env);
    }
    if (req.method === 'POST' && pathname === '/api/schedule/create') {
      return handleScheduleCreate(req, env);
    }
    if (req.method === 'POST' && pathname === '/api/schedule/delete') {
      return handleScheduleDelete(req, env);
    }
    if (req.method === 'GET' && pathname === '/api/schedule/status') {
      return handleScheduleStatus(req, env);
    }
    if (req.method === 'POST' && pathname === '/api/schedule/trigger') {
      return handleScheduleTrigger(req, env);
    }
    return jsonResponse(req, env, 404, { status: 'failed', message: 'Not Found' });
  },

  async scheduled(event, env, ctx) {
    console.log('[cron] scheduled sync triggered');
    const schedules = await loadSchedules(env);
    const now = Date.now();

    // 筛选出本轮需要执行的 schedule
    const toRun = [];
    for (const [id, schedule] of Object.entries(schedules)) {
      if (!schedule.enabled) continue;
      const intervalMs = (schedule.intervalHours || 3) * 60 * 60 * 1000;
      const bufferMs = 5 * 60 * 1000;
      const lastSync = schedule.lastSyncAt || 0;
      if (now - lastSync >= intervalMs - bufferMs) {
        toRun.push(id);
      }
    }

    if (!toRun.length) return;

    // 并行执行所有到期的 schedule，互不阻塞
    console.log(`[cron] running ${toRun.length} syncs in parallel: ${toRun.join(', ')}`);
    const results = await Promise.allSettled(
      toRun.map(id => executeScheduledSync(env, id))
    );
    for (let i = 0; i < results.length; i++) {
      if (results[i].status === 'rejected') {
        console.error(`[cron] sync ${toRun[i]} rejected:`, results[i].reason);
      }
    }
  }
};
