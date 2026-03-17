export interface EmailAttachment {
  fileName: string;
  contentType: string;
  size: number;
  contentBase64?: string;
  attKey?: string;
}

export interface SyncedEmail {
  messageId: string;
  subject: string;
  from: string;
  to: string;
  cc: string;
  date: number;
  snippet: string;
  textBody: string;
  htmlBody: string;
  attachments: EmailAttachment[];
}

export interface EmailSyncRequestPayload {
  provider: string;
  email: string;
  password: string;
  imapHost?: string;
  imapPort?: number;
  secure?: boolean;
  folder?: string;
  maxMessages?: number;
  userId?: string;
}

export interface EmailQuotaResponse {
  paid: boolean;
  used: number;
  remaining: number;
  total: number;
}

export interface EmailSyncResponse {
  status: 'completed' | 'failed';
  message?: string;
  provider?: string;
  mailbox?: string;
  emails?: SyncedEmail[];
}

function buildUrl(baseUrl: string, path: string): string {
  const normalizedBase = baseUrl.trim().replace(/\/+$/, '');
  const normalizedPath = path.startsWith('/') ? path : `/${path}`;
  return `${normalizedBase}${normalizedPath}`;
}

async function requestJson<T>(url: string, payload: Record<string, unknown>): Promise<T> {
  let response: Response;
  try {
    response = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(payload)
    });
  } catch (error) {
    throw new Error(`网络请求失败：${(error as Error).message || 'unknown error'}`);
  }

  if (!response.ok) {
    const text = await response.text();
    try {
      const data = JSON.parse(text) as { message?: string };
      throw new Error(data.message || text);
    } catch (_) {
      throw new Error(text || `请求失败：${response.status}`);
    }
  }
  return response.json() as Promise<T>;
}

async function requestGet<T>(url: string): Promise<T> {
  let response: Response;
  try {
    response = await fetch(url);
  } catch (error) {
    throw new Error(`网络请求失败：${(error as Error).message || 'unknown error'}`);
  }
  if (!response.ok) {
    const text = await response.text();
    try {
      const data = JSON.parse(text) as { message?: string };
      throw new Error(data.message || text);
    } catch (_) {
      throw new Error(text || `请求失败：${response.status}`);
    }
  }
  return response.json() as Promise<T>;
}

export async function checkEmailQuota(baseUrl: string, userId: string): Promise<EmailQuotaResponse> {
  const url = buildUrl(baseUrl, `/api/email/quota?userId=${encodeURIComponent(userId)}`);
  return requestGet<EmailQuotaResponse>(url);
}

export async function updateEmailUsage(baseUrl: string, userId: string, count: number): Promise<void> {
  await requestJson<{ status: string }>(buildUrl(baseUrl, '/api/email/usage'), { userId, count });
}

export async function startEmailSync(baseUrl: string, payload: EmailSyncRequestPayload): Promise<EmailSyncResponse> {
  return requestJson<EmailSyncResponse>(buildUrl(baseUrl, '/api/email/sync'), payload as unknown as Record<string, unknown>);
}

export async function fetchEmailFolders(baseUrl: string, payload: Record<string, unknown>): Promise<string[]> {
  const data = await requestJson<{ status: string; folders: string[] }>(buildUrl(baseUrl, '/api/email/folders'), payload);
  return data.folders || [];
}

export async function fetchAttachmentContent(baseUrl: string, attKey: string): Promise<string> {
  const url = buildUrl(baseUrl, `/api/email/attachment?key=${encodeURIComponent(attKey)}`);
  const data = await requestGet<{ status: string; contentBase64: string }>(url);
  return data.contentBase64;
}
