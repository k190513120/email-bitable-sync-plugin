import $ from 'jquery';
import './index.scss';
import { bitable } from '@lark-base-open/js-sdk';
import { startEmailSync, checkEmailQuota, updateEmailUsage, fetchEmailFolders } from './email-api';
import { prepareEmailTable, writeEmailRecords } from './email-table-operations';

// 所有 API 统一使用 Cloudflare Worker（cf-imap + Stripe）
const BASE_URL = (import.meta.env.VITE_WORKER_BASE_URL as string) || 'https://wereadsync.xiaomiao.win';

let currentUserId = '';
let currentUserPaid = false;
let scheduleStatusTimer: ReturnType<typeof setInterval> | null = null;

$(function () {
  initializeApp();
});

async function initializeApp() {
  setDefaultConfig();
  bindEvents();
  try {
    currentUserId = await bitable.bridge.getBaseUserId();
  } catch (_) {
    currentUserId = '';
  }
  await checkUserEntitlement();
  await autoFillBitableContext();
  await loadScheduleConfig();
}

function setDefaultConfig() {
  $('#provider').val('gmail');
  $('#maxMessages').val('100');
  $('#imapHost').val('imap.gmail.com');
  $('#imapPort').val('993');
  $('#tableName').val('邮箱同步');
  $('#folder').val('INBOX');
  $('#secure').prop('checked', true);
}

function bindEvents() {
  $('#startSync').on('click', handleStartSync);
  $('#enableSchedule').on('click', handleEnableSchedule);
  $('#upgradeBtn').on('click', handleUpgrade);
  $('#loadFolders').on('click', handleLoadFolders);
}

function getTableName(): string {
  return String($('#tableName').val() || '').trim();
}

function getProvider(): string {
  return String($('#provider').val() || '').trim();
}

function getEmail(): string {
  return String($('#email').val() || '')
    .trim()
    .toLowerCase();
}

function getPassword(): string {
  return String($('#password').val() || '').trim();
}

function getImapHost(): string {
  return String($('#imapHost').val() || '')
    .trim()
    .toLowerCase();
}

function getImapPort(): number {
  const raw = Number($('#imapPort').val());
  if (!Number.isFinite(raw)) return 993;
  return Math.max(1, Math.min(65535, Math.floor(raw)));
}

function getSecure(): boolean {
  return Boolean($('#secure').prop('checked'));
}

function getFolder(): string {
  return String($('#folder').val() || '').trim() || 'INBOX';
}

function getMaxMessages(): number {
  const raw = Number($('#maxMessages').val());
  if (!Number.isFinite(raw)) return 100;
  return Math.max(1, Math.min(500, Math.floor(raw)));
}

function applyProviderPreset(provider: string) {
  const presets: Record<string, { host: string; port: number; secure: boolean }> = {
    gmail: { host: 'imap.gmail.com', port: 993, secure: true },
    qq: { host: 'imap.qq.com', port: 993, secure: true },
    '163': { host: 'imap.163.com', port: 993, secure: true },
    feishu: { host: 'imap.larkoffice.com', port: 993, secure: true },
    yahoo: { host: 'imap.mail.yahoo.com', port: 993, secure: true },
    outlook: { host: 'outlook.office365.com', port: 993, secure: true }
  };
  const preset = presets[provider];
  if (preset) {
    $('#imapHost').val(preset.host);
    $('#imapPort').val(String(preset.port));
    $('#secure').prop('checked', preset.secure);
  }

  // 显示各平台的授权码/密码提示
  const hints: Record<string, string> = {
    gmail: '需使用「应用专用密码」：Google 账号 → 安全性 → 两步验证 → 应用专用密码',
    qq: '需使用「授权码」：QQ 邮箱设置 → 账户 → 开启 IMAP → 生成授权码',
    '163': '需使用「授权码」：163 邮箱设置 → POP3/IMAP → 开启 IMAP → 新增授权密码',
    feishu: '需使用「专用密码」：飞书邮箱设置 → IMAP/SMTP → 创建专用密码',
    yahoo: '需使用「应用密码」：Yahoo 账户安全 → 生成应用密码',
    outlook: 'Outlook 目前仅支持 OAuth2 认证，暂不支持密码/授权码方式连接'
  };
  const $hint = $('#providerHint');
  if (hints[provider]) {
    $hint.text(hints[provider]).show();
  } else {
    $hint.hide();
  }
}

let loadedFolders: string[] = [];

async function handleLoadFolders() {
  const email = getEmail();
  const password = getPassword();
  if (!email || !password) {
    showResult('请先填写邮箱账号与授权码/密码', 'error');
    return;
  }

  const $btn = $('#loadFolders');
  $btn.prop('disabled', true).html('<i class="fas fa-spinner fa-spin"></i> 加载中');

  try {
    const folders = await fetchEmailFolders(BASE_URL, {
      provider: getProvider(),
      email,
      password,
      imapHost: getImapHost(),
      imapPort: getImapPort(),
      secure: getSecure()
    });

    loadedFolders = folders;
    renderFolderCheckboxes(folders);
  } catch (error) {
    showResult(`获取文件夹失败：${(error as Error).message}`, 'error');
  } finally {
    $btn.prop('disabled', false).html('<i class="fas fa-folder-open"></i> 获取文件夹');
  }
}

function renderFolderCheckboxes(folders: string[]) {
  const $container = $('#folderCheckboxes');
  $container.empty();

  if (!folders.length) {
    $container.hide();
    return;
  }

  // 手动输入框中的值作为默认选中
  const currentFolder = getFolder();

  for (const folder of folders) {
    const isChecked = folder === currentFolder || folder === 'INBOX';
    const $item = $(`
      <label class="folder-checkbox-item${isChecked ? ' checked' : ''}">
        <input type="checkbox" value="${escapeHtml(folder)}" ${isChecked ? 'checked' : ''} />
        <span>${escapeHtml(folder)}</span>
      </label>
    `);
    $item.find('input').on('change', function () {
      $item.toggleClass('checked', $(this).is(':checked'));
    });
    $container.append($item);
  }

  $container.show();
  // 隐藏手动输入框（checkbox 已替代）
  $('#folder').closest('.folder-input-shell').hide();
}

function getSelectedFolders(): string[] {
  const $checkboxes = $('#folderCheckboxes input[type="checkbox"]:checked');
  if ($checkboxes.length > 0) {
    return $checkboxes.map(function () { return $(this).val() as string; }).get();
  }
  // 没有加载过文件夹列表，使用手动输入
  return [getFolder()];
}

async function handleStartSync() {
  const tableName = getTableName();
  const provider = getProvider();
  const email = getEmail();
  const password = getPassword();
  const imapHost = getImapHost();
  const imapPort = getImapPort();
  const secure = getSecure();
  const maxMessages = getMaxMessages();
  const folders = getSelectedFolders();

  if (!email || !password) {
    showResult('请先填写邮箱账号与授权码/密码', 'error');
    return;
  }

  try {
    setSyncLoading(true);

    // Check quota if userId is available
    if (currentUserId) {
      updateProgress(5, '检查同步配额');
      try {
        const quota = await checkEmailQuota(BASE_URL, currentUserId);
        if (!quota.paid && quota.remaining <= 0) {
          showResult(`免费 ${quota.total} 封额度已用完，升级付费版可同步全部邮件。`, 'info', true);
          hideProgress();
          return;
        }
      } catch (_) {
        // Quota check failed, proceed without restriction
      }
    }

    // 逐个文件夹拉取邮件，合并到一起
    const allEmails: import('./email-api').SyncedEmail[] = [];
    const seenIds = new Set<string>();
    const folderErrors: string[] = [];

    for (let fi = 0; fi < folders.length; fi++) {
      const folder = folders[fi];
      const folderProgress = 10 + (fi / folders.length) * 40;
      updateProgress(folderProgress, `拉取 ${folder} (${fi + 1}/${folders.length})`);

      try {
        const syncResponse = await startEmailSync(BASE_URL, {
          provider,
          email,
          password,
          imapHost,
          imapPort,
          folder,
          secure,
          maxMessages,
          userId: currentUserId || undefined
        });
        if ((syncResponse as any).status === 'quota_exceeded') {
          showResult((syncResponse as any).message || '免费额度已用完', 'info');
          hideProgress();
          return;
        }
        if (syncResponse.status !== 'completed') {
          folderErrors.push(`${folder}: ${syncResponse.message || '失败'}`);
          continue;
        }
        const emails = Array.isArray(syncResponse.emails) ? syncResponse.emails : [];
        for (const e of emails) {
          if (e.messageId && !seenIds.has(e.messageId)) {
            seenIds.add(e.messageId);
            allEmails.push(e);
          }
        }
      } catch (err) {
        folderErrors.push(`${folder}: ${(err as Error).message}`);
      }
    }

    if (!allEmails.length && folderErrors.length) {
      throw new Error(`所有文件夹同步失败：\n${folderErrors.join('\n')}`);
    }
    if (!allEmails.length) {
      showResult('同步完成，但未拉取到新邮件', 'info');
      updateProgress(100, '未发现新邮件');
      return;
    }

    updateProgress(55, '准备目标数据表');
    const table = await prepareEmailTable(tableName, (progress, message) => {
      updateProgress(55 + progress * 0.1, message);
    });

    updateProgress(65, '写入多维表格');
    const stats = await writeEmailRecords(table, provider, allEmails, BASE_URL, (progress, message) => {
      updateProgress(65 + progress * 0.35, message);
    });

    // Update usage after successful sync
    if (currentUserId && stats.inserted > 0) {
      try {
        await updateEmailUsage(BASE_URL, currentUserId, stats.inserted);
      } catch (_) {
        // Usage update failed, non-critical
      }
    }

    updateProgress(100, '同步完成');
    const folderLabel = folders.length > 1 ? `（${folders.length} 个文件夹）` : '';
    let resultMsg = `同步完成${folderLabel}：拉取 ${stats.total} 封，新增 ${stats.inserted} 封，跳过重复 ${stats.skipped} 封。`;
    if (stats.patchedAttachments > 0) {
      resultMsg += `\n补充了 ${stats.patchedAttachments} 条旧记录的附件。`;
    }
    if (folderErrors.length) {
      resultMsg += `\n部分文件夹失败：${folderErrors.join('；')}`;
    }
    if (!currentUserPaid) {
      resultMsg += '\n免费版仅可同步 3 封邮件，升级付费版可同步全部邮件。';
    }
    showResult(resultMsg, folderErrors.length ? 'info' : 'success', !currentUserPaid);
  } catch (error) {
    showResult(`同步失败：${(error as Error).message}`, 'error');
    hideProgress();
  } finally {
    setSyncLoading(false);
  }
}

function setSyncLoading(loading: boolean) {
  const button = $('#startSync');
  const text = $('#syncBtnText');
  const spinner = $('#syncLoadingSpinner');
  button.prop('disabled', loading);
  text.text(loading ? '同步中...' : '确认并同步');
  if (loading) spinner.show();
  else spinner.hide();
}

function updateProgress(progress: number, message: string) {
  const safeProgress = Math.max(0, Math.min(100, progress));
  $('#syncProgressContainer').show();
  $('#syncProgressBar').css('width', `${safeProgress}%`);
  $('#syncProgressText').text(message);
  $('#syncProgressValue').text(`${Math.round(safeProgress)}%`);
}

function escapeHtml(str: string): string {
  return str
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function showResult(message: string, type: 'success' | 'error' | 'info', showUpgrade = false) {
  const messageEl = $('#resultMessage');
  let html = escapeHtml(message).replace(/\n/g, '<br>');
  if (showUpgrade) {
    html += '<br><a class="result-upgrade-link" href="javascript:void(0)" id="resultUpgradeLink">升级付费版 &rarr;</a>';
  }
  messageEl.removeClass('success error info').addClass(type).html(html);
  $('#resultContainer').show();
  if (showUpgrade) {
    $('#resultUpgradeLink').off('click').on('click', handleUpgrade);
  }
}

function hideProgress() {
  $('#syncProgressContainer').hide();
}

$('#provider').on('change', function handleProviderChange() {
  applyProviderPreset(String($(this).val() || ''));
});

// --- Entitlement / Paywall ---

async function checkUserEntitlement() {
  try {
    const params = new URLSearchParams();
    if (currentUserId) params.set('userId', currentUserId);
    const resp = await fetch(`${BASE_URL}/api/stripe/entitlement?${params.toString()}`);
    const data = await resp.json();
    currentUserPaid = Boolean(data?.entitlement?.active);
  } catch (_) {
    currentUserPaid = false;
  }
  updateScheduleLock();
}

function updateScheduleLock() {
  const overlay = $('#scheduleLockOverlay');
  if (currentUserPaid) {
    overlay.addClass('hidden');
  } else {
    overlay.removeClass('hidden');
  }
}

async function handleUpgrade() {
  try {
    const fallbackUrl = 'https://wereadsync.xiaomiao.win/healthz';
    let currentUrl = fallbackUrl;
    try {
      const href = window.location.href;
      if (href && href.startsWith('http')) currentUrl = href;
    } catch (_) { /* use fallback */ }

    const resp = await fetch(`${BASE_URL}/api/stripe/create-checkout-session`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        userId: currentUserId || undefined,
        successUrl: currentUrl,
        cancelUrl: currentUrl
      })
    });
    if (!resp.ok) {
      const errData = await resp.json().catch(() => null);
      throw new Error(errData?.message || `请求失败：${resp.status}`);
    }
    const data = await resp.json();
    if (data?.url) {
      window.open(data.url, '_blank');
    } else {
      showResult('无法创建支付链接，请稍后重试', 'error');
    }
  } catch (error) {
    showResult(`升级失败：${(error as Error).message}`, 'error');
  }
}

// --- Scheduled Sync (multi-schedule) ---

interface ScheduleItem {
  id: string;
  enabled: boolean;
  intervalHours: number;
  provider: string;
  email: string;
  folder: string;
  lastSyncAt: number | null;
  lastSyncStatus: string | null;
  lastSyncMessage: string | null;
  createdAt: number;
}

async function autoFillBitableContext() {
  try {
    const selection = await bitable.base.getSelection();
    if (selection?.baseId) {
      $('#scheduleAppToken').val(selection.baseId);
    }
    if (selection?.tableId) {
      $('#scheduleTableId').val(selection.tableId);
    }
  } catch (_) {
    // not in bitable context
  }
}

async function loadScheduleConfig() {
  await refreshScheduleList();
  startScheduleStatusPolling();
}

function showScheduleResult(message: string, type: 'success' | 'error' | 'info') {
  const el = $('#scheduleResultMessage');
  el.removeClass('success error info').addClass(type).html(escapeHtml(message).replace(/\n/g, '<br>'));
  $('#scheduleResultContainer').show();
}

function renderScheduleList(schedules: ScheduleItem[]) {
  const $container = $('#scheduleListContainer');
  const $list = $('#scheduleList');
  const $count = $('#scheduleCount');

  if (!schedules.length) {
    $container.hide();
    return;
  }

  $container.show();
  $count.text(`${schedules.length} 个任务`);
  $list.empty();

  const providerLabels: Record<string, string> = {
    gmail: 'Gmail',
    qq: 'QQ',
    '163': '163',
    feishu: '飞书',
    yahoo: 'Yahoo',
    outlook: 'Outlook',
    custom: '自定义'
  };

  const intervalLabels: Record<number, string> = {
    1: '每小时',
    3: '每3小时',
    6: '每6小时',
    12: '每12小时',
    24: '每天'
  };

  for (const s of schedules) {
    const providerLabel = providerLabels[s.provider] || s.provider;
    const intervalLabel = intervalLabels[s.intervalHours] || `每${s.intervalHours}小时`;
    const lastSyncText = s.lastSyncAt ? new Date(s.lastSyncAt).toLocaleString() : '尚未同步';
    const statusClass = s.lastSyncStatus === 'success' ? 'schedule-item-status-ok'
      : s.lastSyncStatus === 'failed' ? 'schedule-item-status-fail' : '';
    const statusIcon = s.lastSyncStatus === 'success' ? 'fa-circle-check'
      : s.lastSyncStatus === 'failed' ? 'fa-circle-xmark' : 'fa-clock';

    const $item = $(`
      <div class="schedule-item" data-schedule-id="${escapeHtml(s.id)}">
        <div class="schedule-item-header">
          <div class="schedule-item-info">
            <span class="schedule-item-provider">${escapeHtml(providerLabel)}</span>
            <span class="schedule-item-email">${escapeHtml(s.email)}</span>
          </div>
          <div class="schedule-item-actions">
            <button class="schedule-item-btn schedule-item-btn-delete" title="删除">
              <i class="fas fa-trash-can"></i>
            </button>
          </div>
        </div>
        <div class="schedule-item-details">
          <span><i class="fas fa-hourglass-half"></i> ${escapeHtml(intervalLabel)}</span>
          <span><i class="fas fa-inbox"></i> ${escapeHtml(s.folder)}</span>
          <span class="${statusClass}"><i class="fas ${statusIcon}"></i> ${escapeHtml(lastSyncText)}</span>
          ${s.lastSyncMessage ? `<span class="${statusClass}">${escapeHtml(s.lastSyncMessage)}</span>` : ''}
        </div>
      </div>
    `);

    $item.find('.schedule-item-btn-delete').on('click', () => handleDeleteSchedule(s.id, s.email));
    $list.append($item);
  }
}

async function refreshScheduleList() {
  try {
    const params = new URLSearchParams();
    if (currentUserId) params.set('userId', currentUserId);
    const resp = await fetch(`${BASE_URL}/api/schedule/list?${params.toString()}`);
    const data = await resp.json();
    if (data.status === 'ok' && Array.isArray(data.schedules)) {
      renderScheduleList(data.schedules);
    }
  } catch (_) {
    // list failed, ignore
  }
}

async function handleEnableSchedule() {
  const personalBaseToken = String($('#personalBaseToken').val() || '').trim();
  const appToken = String($('#scheduleAppToken').val() || '').trim();
  const tableId = String($('#scheduleTableId').val() || '').trim();
  const intervalHours = Number($('#scheduleInterval').val()) || 3;
  const email = getEmail();
  const password = getPassword();

  if (!email || !password) {
    showScheduleResult('请先在上方填写邮箱账号与授权码/密码', 'error');
    return;
  }
  if (!personalBaseToken) {
    showScheduleResult('请填写多维表格授权码', 'error');
    return;
  }
  if (!appToken || !tableId) {
    showScheduleResult('请填写 AppToken 和 TableId', 'error');
    return;
  }

  $('#enableSchedule').prop('disabled', true);

  try {
    const payload = {
      intervalHours,
      imapConfig: {
        provider: getProvider(),
        email,
        password,
        imapHost: getImapHost(),
        imapPort: getImapPort(),
        secure: getSecure(),
        folder: getFolder(),
        maxMessages: getMaxMessages()
      },
      feishuConfig: { personalBaseToken, appToken, tableId },
      userId: currentUserId || undefined
    };

    const resp = await fetch(`${BASE_URL}/api/schedule/create`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    const data = await resp.json();

    if (data.status !== 'ok') {
      throw new Error(data.message || '创建定时任务失败');
    }

    showScheduleResult(`已添加 ${email} 的定时同步，每 ${intervalHours} 小时自动同步`, 'success');
    await refreshScheduleList();
  } catch (error) {
    showScheduleResult(`添加失败：${(error as Error).message}`, 'error');
  } finally {
    $('#enableSchedule').prop('disabled', false);
  }
}

async function handleDeleteSchedule(scheduleId: string, email: string) {
  if (!confirm(`确定要删除 ${email} 的定时同步吗？`)) return;

  try {
    const resp = await fetch(`${BASE_URL}/api/schedule/delete`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ id: scheduleId })
    });
    const data = await resp.json();
    if (data.status !== 'ok') {
      throw new Error(data.message || '删除定时任务失败');
    }

    showScheduleResult(`已删除 ${email} 的定时同步`, 'info');
    await refreshScheduleList();
  } catch (error) {
    showScheduleResult(`删除失败：${(error as Error).message}`, 'error');
  }
}

function startScheduleStatusPolling() {
  stopScheduleStatusPolling();
  scheduleStatusTimer = setInterval(refreshScheduleList, 60_000);
}

function stopScheduleStatusPolling() {
  if (scheduleStatusTimer) {
    clearInterval(scheduleStatusTimer);
    scheduleStatusTimer = null;
  }
}
