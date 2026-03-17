import { bitable, FieldType, IAttachmentField, ITable } from '@lark-base-open/js-sdk';
import { SyncedEmail, EmailAttachment, fetchAttachmentContent } from './email-api';

type ProgressHandler = (progress: number, message: string) => void;

const DEFAULT_EMAIL_TABLE_NAME = '邮箱同步';

const EMAIL_FIELDS: Array<{ name: string; type: FieldType }> = [
  { name: '邮件ID', type: FieldType.Text },
  { name: '主题', type: FieldType.Text },
  { name: '发件人', type: FieldType.Text },
  { name: '收件人', type: FieldType.Text },
  { name: '抄送', type: FieldType.Text },
  { name: '接收时间', type: FieldType.DateTime },
  { name: '摘要', type: FieldType.Text },
  { name: '正文', type: FieldType.Text },
  { name: '邮箱服务商', type: FieldType.Text },
  { name: '附件', type: FieldType.Attachment },
  { name: '同步时间', type: FieldType.DateTime }
];

function requireFieldId(fieldMap: Record<string, string>, name: string): string {
  const id = fieldMap[name];
  if (!id) {
    throw new Error(`字段缺失：${name}`);
  }
  return id;
}

function normalizeAttachmentFileName(name: string, fallback: number): string {
  const normalized = String(name || '').trim();
  if (!normalized) return `attachment-${fallback}.bin`;
  return normalized.replace(/[\\/:*?"<>|]+/g, '_').slice(0, 120);
}

function base64ToFile(base64: string, fileName: string, contentType: string): File {
  const bytes = atob(base64);
  const array = new Uint8Array(bytes.length);
  for (let i = 0; i < bytes.length; i += 1) {
    array[i] = bytes.charCodeAt(i);
  }
  return new File([array], fileName, { type: contentType || 'application/octet-stream' });
}

async function resolveAttachmentFiles(
  attachments: EmailAttachment[],
  baseUrl: string
): Promise<File[]> {
  const files: File[] = [];
  for (let i = 0; i < attachments.length; i++) {
    const att = attachments[i];
    if (att.size <= 0) continue;
    let base64 = att.contentBase64;
    if (!base64 && att.attKey) {
      try {
        base64 = await fetchAttachmentContent(baseUrl, att.attKey);
      } catch (_) {
        continue; // skip if download fails
      }
    }
    if (!base64) continue;
    const fileName = normalizeAttachmentFileName(att.fileName, i + 1);
    files.push(base64ToFile(base64, fileName, att.contentType || 'application/octet-stream'));
  }
  return files;
}

async function ensureFields(table: ITable): Promise<void> {
  const existing = await table.getFieldMetaList();
  const existingNames = new Set(existing.map((item) => item.name));
  for (const field of EMAIL_FIELDS) {
    if (!existingNames.has(field.name)) {
      await table.addField({ name: field.name, type: field.type as any });
    }
  }
}

async function getFieldMap(table: ITable): Promise<Record<string, string>> {
  const fields = await table.getFieldMetaList();
  const map: Record<string, string> = {};
  for (const field of EMAIL_FIELDS) {
    const matched = fields.find((item) => item.name === field.name);
    if (matched) map[field.name] = matched.id;
  }
  return map;
}

interface ExistingRecord {
  recordId: string;
  hasAttachment: boolean;
}

async function collectExistingRecords(
  table: ITable,
  messageIdFieldId: string,
  attachmentFieldId: string
): Promise<Map<string, ExistingRecord>> {
  const existing = new Map<string, ExistingRecord>();
  let hasMore = true;
  let pageToken: string | undefined;
  while (hasMore) {
    const result = await table.getRecords({ pageSize: 5000, pageToken });
    if (result.records) {
      for (const record of result.records) {
        const cell = record.fields[messageIdFieldId];
        const text = Array.isArray(cell) ? String((cell[0] as any)?.text || '').trim() : String(cell || '').trim();
        if (text) {
          const attCell = record.fields[attachmentFieldId];
          const hasAttachment = Array.isArray(attCell) && attCell.length > 0;
          existing.set(text, { recordId: record.recordId, hasAttachment });
        }
      }
    }
    hasMore = result.hasMore ?? false;
    pageToken = result.pageToken;
  }
  return existing;
}

export async function prepareEmailTable(tableName?: string, onProgress?: ProgressHandler): Promise<ITable> {
  const finalName = String(tableName || '').trim() || DEFAULT_EMAIL_TABLE_NAME;
  onProgress?.(15, '检查数据表');
  const tables = await bitable.base.getTableMetaList();
  const existingTable = tables.find((item) => item.name === finalName);
  let table: ITable;
  if (existingTable) {
    table = await bitable.base.getTableById(existingTable.id);
  } else {
    onProgress?.(30, '创建数据表');
    const created = await bitable.base.addTable({
      name: finalName,
      fields: [{ name: '邮件ID', type: FieldType.Text }]
    });
    table = await bitable.base.getTableById(created.tableId);
  }
  onProgress?.(45, '补齐字段');
  await ensureFields(table);
  return table;
}

export async function writeEmailRecords(
  table: ITable,
  provider: string,
  emails: SyncedEmail[],
  baseUrl: string,
  onProgress?: ProgressHandler
): Promise<{ total: number; inserted: number; skipped: number; patchedAttachments: number }> {
  const fieldMap = await getFieldMap(table);
  const messageIdFieldId = requireFieldId(fieldMap, '邮件ID');
  const subjectFieldId = requireFieldId(fieldMap, '主题');
  const fromFieldId = requireFieldId(fieldMap, '发件人');
  const toFieldId = requireFieldId(fieldMap, '收件人');
  const ccFieldId = requireFieldId(fieldMap, '抄送');
  const dateFieldId = requireFieldId(fieldMap, '接收时间');
  const snippetFieldId = requireFieldId(fieldMap, '摘要');
  const bodyFieldId = requireFieldId(fieldMap, '正文');
  const providerFieldId = requireFieldId(fieldMap, '邮箱服务商');
  const syncTimeFieldId = requireFieldId(fieldMap, '同步时间');
  const attachmentFieldId = requireFieldId(fieldMap, '附件');

  onProgress?.(50, '读取历史邮件');
  const existingRecords = await collectExistingRecords(table, messageIdFieldId, attachmentFieldId);
  const toInsert = emails.filter((item) => item.messageId && !existingRecords.has(item.messageId));

  // 找出已存在但缺附件的记录，且本次拉取的邮件有附件
  const toPatchAttachments: Array<{ recordId: string; email: SyncedEmail }> = [];
  for (const email of emails) {
    if (!email.messageId) continue;
    const existing = existingRecords.get(email.messageId);
    if (!existing) continue; // 新邮件，走 insert 流程
    if (existing.hasAttachment) continue; // 已有附件，跳过
    const attachments = Array.isArray(email.attachments) ? email.attachments : [];
    if (attachments.some((a) => (a.contentBase64 || a.attKey) && a.size > 0)) {
      toPatchAttachments.push({ recordId: existing.recordId, email });
    }
  }

  const attachmentField = await table.getField<IAttachmentField>(attachmentFieldId);
  const ATTACHMENT_BATCH_SIZE = 3;

  // 插入新记录
  let newRecordIds: string[] = [];
  if (toInsert.length) {
    const syncTime = Date.now();
    const records = toInsert.map((item) => ({
      fields: {
        [messageIdFieldId]: item.messageId,
        [subjectFieldId]: item.subject || '(无主题)',
        [fromFieldId]: item.from || '',
        [toFieldId]: item.to || '',
        [ccFieldId]: item.cc || '',
        [dateFieldId]: item.date || syncTime,
        [snippetFieldId]: item.snippet || '',
        [bodyFieldId]: item.textBody || '',
        [providerFieldId]: provider,
        [syncTimeFieldId]: syncTime
      }
    }));

    onProgress?.(65, `写入 ${records.length} 条新邮件`);
    newRecordIds = await table.addRecords(records);

    // 上传新记录的附件（逐条处理，按需下载）
    for (let i = 0; i < newRecordIds.length; i++) {
      const email = toInsert[i];
      const attachments = Array.isArray(email.attachments) ? email.attachments : [];
      if (attachments.some((a) => (a.contentBase64 || a.attKey) && a.size > 0)) {
        const files = await resolveAttachmentFiles(attachments, baseUrl);
        if (files.length) {
          await attachmentField.setValue(newRecordIds[i], files);
        }
      }
      onProgress?.(65 + ((i + 1) / newRecordIds.length) * 20, `上传新附件 ${i + 1}/${newRecordIds.length}`);
    }
  }

  // 补充已有记录缺失的附件（逐条处理，按需下载）
  let patchedAttachments = 0;
  if (toPatchAttachments.length) {
    onProgress?.(85, `补充 ${toPatchAttachments.length} 条旧记录的附件`);
    for (let i = 0; i < toPatchAttachments.length; i++) {
      const { recordId, email } = toPatchAttachments[i];
      const attachments = Array.isArray(email.attachments) ? email.attachments : [];
      const files = await resolveAttachmentFiles(attachments, baseUrl);
      if (files.length) {
        patchedAttachments++;
        await attachmentField.setValue(recordId, files);
      }
      onProgress?.(85 + ((i + 1) / toPatchAttachments.length) * 15, `补充附件 ${i + 1}/${toPatchAttachments.length}`);
    }
  }

  return {
    total: emails.length,
    inserted: toInsert.length,
    skipped: emails.length - toInsert.length,
    patchedAttachments
  };
}
