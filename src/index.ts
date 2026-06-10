import $ from 'jquery';
import QRCode from 'qrcode';
import './index.scss';
import { bitable } from '@lark-base-open/js-sdk';
import { startEmailSync, checkEmailQuota, updateEmailUsage, fetchEmailFolders } from './email-api';
import { prepareEmailTable, writeEmailRecords } from './email-table-operations';
import { setLocale, applyI18n, t, getLocale } from './i18n';

// 所有 API 统一使用 Cloudflare Worker（cf-imap + Stripe）
const BASE_URL = (import.meta.env.VITE_WORKER_BASE_URL as string) || 'https://emailsync.xiaomiao.win';

let currentUserId = '';
let currentTenantKey = '';
let currentUserPaid = false;
let scheduleStatusTimer: ReturnType<typeof setInterval> | null = null;
let activeTab: 'setup' | 'sync' | 'schedule' | 'upgrade' = 'setup';
let upgradeTarget: { scheduleId?: string; provider?: string; email?: string; intent?: 'credit' } | null = null;
let pendingCreditTradeNo = '';
let syncStartedAt: number | null = null;
let syncElapsedTimer: ReturnType<typeof setInterval> | null = null;
let syncSeenSteps: string[] = [];
let syncAborted = false;
const SYNC_ABORTED_MARKER = '__sync_aborted__';

function checkSyncAborted(): void {
  if (syncAborted) throw new Error(SYNC_ABORTED_MARKER);
}
let lastQuota: { used: number; total: number; paid: boolean } | null = null;

const PROVIDER_MONO: Record<string, { mono: string; color: string }> = {
  gmail: { mono: 'G', color: '#E8543A' },
  qq: { mono: 'Q', color: '#2980EA' },
  feishu: { mono: 'F', color: '#3370FF' },
  '163': { mono: '网', color: '#E54545' },
  yahoo: { mono: 'Y', color: '#6001D2' },
  outlook: { mono: 'O', color: '#0078D4' },
  custom: { mono: '⋯', color: '#5C5C5C' }
};

$(function () {
  initializeApp();
});

async function initializeApp() {
  // Detect language from bitable SDK
  try {
    const env = await bitable.bridge.getEnv();
    if ((env as any)?.lang) {
      setLocale((env as any).lang);
    }
  } catch (_) {
    // fallback to zh
  }
  applyI18n();

  setDefaultConfig();
  bindEvents();
  try {
    currentUserId = await bitable.bridge.getBaseUserId();
  } catch (_) {
    currentUserId = '';
  }
  renderUidChip();
  try {
    currentTenantKey = await (bitable.bridge as any).getTenantKey();
  } catch (_) {
    currentTenantKey = '';
  }
  await checkUserEntitlement();
  await autoFillBitableContext();
  await loadScheduleConfig();
}

function setDefaultConfig() {
  setProviderValue('gmail');
  $('#maxMessages').val('100');
  $('#imapHost').val('imap.gmail.com');
  $('#imapPort').val('993');
  $('#tableName').val(t('placeholder.tableName'));
  $('#folder').val('INBOX');
  $('#secure').prop('checked', true);
}

function bindEvents() {
  $('#startSync').on('click', handleStartSync);
  $('#enableSchedule').on('click', handleEnableSchedule);
  $('#upgradeBtn').on('click', () => handleUpgrade());
  $('#loadFolders').on('click', handleLoadFolders);
  $('#alipayQRClose').on('click', () => {
    alipayPollingActive = false;
    $('#alipayQRModal').hide();
  });

  // Tabs
  $('.tab-btn').on('click', function () {
    const tab = String($(this).attr('data-tab') || '') as typeof activeTab;
    if (!tab) return;
    setTab(tab);
  });

  // Provider tiles (replaces old dropdown)
  $('#providerTiles').on('click', '.provider-tile', function () {
    const provider = String($(this).attr('data-provider') || '');
    if (!provider) return;
    setProviderValue(provider);
  });

  // Add account: pay first, then reveal the form
  $('#scheduleAddBtn').on('click', () => startAddAccountFlow());
  $('#scheduleFormClose').on('click', () => {
    $('#scheduleFormSection').prop('hidden', true);
  });

  // Sync screen controls — request cancellation; handleStartSync polls syncAborted
  $('#syncCancelBtn').on('click', () => {
    if (syncAborted) return;
    syncAborted = true;
    $('#syncCancelBtn').prop('disabled', true);
    setSyncStatus('idle', t('sync.cancelling'));
    appendSyncTimelineStep(t('sync.cancelling'), 'active');
  });

  // After-sync "next step" CTA → Schedule tab
  $('#syncNextBtn').on('click', () => {
    setTab('schedule');
  });

  // Paywall CTA (visible on Sync tab when free-cap was hit)
  $('#syncPaywallCta').on('click', () => startAddAccountFlow());
}

function setTab(tab: typeof activeTab) {
  activeTab = tab;
  $('.tab-btn').each(function () {
    const isActive = $(this).attr('data-tab') === tab;
    $(this).toggleClass('is-active', isActive).attr('aria-selected', String(isActive));
  });
  $('.tab-pane').each(function () {
    $(this).toggleClass('is-active', $(this).attr('data-pane') === tab);
  });
  // Upgrade tab is only shown when there's a target
  if (tab === 'upgrade' && !upgradeTarget) {
    $('.tab-btn[data-tab="upgrade"]').prop('hidden', true);
  } else if (tab === 'upgrade') {
    $('.tab-btn[data-tab="upgrade"]').prop('hidden', false);
  }
}

let alipayPollingActive = false;

function shouldUseAlipay(): boolean {
  return getLocale() === 'zh';
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
  const hintKeys: Record<string, string> = {
    gmail: 'hint.gmail',
    qq: 'hint.qq',
    '163': 'hint.163',
    feishu: 'hint.feishu',
    yahoo: 'hint.yahoo',
    outlook: 'hint.outlook'
  };
  const hints: Record<string, string> = {};
  for (const [k, v] of Object.entries(hintKeys)) {
    hints[k] = t(v);
  }
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
    showResult(t('msg.fillEmailFirst'), 'error');
    return;
  }

  const $btn = $('#loadFolders');
  $btn.prop('disabled', true).html(`<i class="fas fa-spinner fa-spin"></i> ${t('msg.loadingFolders')}`);

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
    showResult(t('msg.folderLoadFailed', { error: (error as Error).message }), 'error');
  } finally {
    $btn.prop('disabled', false).html(`<i class="fas fa-folder-open"></i> ${t('btn.loadFolders')}`);
  }
}

function renderFolderCheckboxes(folders: string[]) {
  const $container = $('#folderCheckboxes');
  $container.empty();

  if (!folders.length) {
    $container.hide();
    return;
  }

  const currentFolder = getFolder();
  let checkedCount = 0;

  for (const folder of folders) {
    const isChecked = folder === currentFolder || folder === 'INBOX';
    if (isChecked) checkedCount++;
    const $item = $(`
      <label class="folder-checkbox-item${isChecked ? ' checked' : ''}">
        <input type="checkbox" value="${escapeHtml(folder)}" ${isChecked ? 'checked' : ''} />
        <span class="folder-checkbox-box"><i class="fas fa-check"></i></span>
        <span>${escapeHtml(folder)}</span>
      </label>
    `);
    $item.find('input').on('change', function () {
      $item.toggleClass('checked', $(this).is(':checked'));
      updateFolderHint();
    });
    $container.append($item);
  }

  $container.show();
  $('#folder').closest('.folder-input-shell').hide();
  $('#folderHint').text(t('setup.foldersSelected', { n: String(checkedCount), total: String(folders.length) }));
}

function updateFolderHint() {
  const total = $('#folderCheckboxes input[type="checkbox"]').length;
  if (!total) return;
  const checked = $('#folderCheckboxes input[type="checkbox"]:checked').length;
  $('#folderHint').text(t('setup.foldersSelected', { n: String(checked), total: String(total) }));
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
    showResult(t('msg.fillEmailFirst'), 'error');
    return;
  }

  try {
    setSyncLoading(true);

    // Check quota if userId is available
    if (currentUserId) {
      updateProgress(5, t('msg.checkingQuota'));
      try {
        const quota = await checkEmailQuota(BASE_URL, currentUserId);
        if (!quota.paid && quota.remaining <= 0) {
          showResult(t('msg.quotaExhausted', { total: String(quota.total) }), 'info', true);
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
    let wasCapped = false;
    let capLimit = 5;

    for (let fi = 0; fi < folders.length; fi++) {
      checkSyncAborted();
      const folder = folders[fi];
      const folderProgress = 10 + (fi / folders.length) * 40;
      updateProgress(folderProgress, t('msg.fetchingFolder', { folder, current: String(fi + 1), total: String(folders.length) }));

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
          userId: currentUserId || undefined,
          tenantKey: currentTenantKey || undefined
        });
        if ((syncResponse as any).status === 'quota_exceeded') {
          showResult((syncResponse as any).message || t('msg.quotaExhausted', { total: '' }), 'info');
          hideProgress();
          return;
        }
        if (syncResponse.status !== 'completed') {
          folderErrors.push(`${folder}: ${syncResponse.message || 'failed'}`);
          continue;
        }
        if (syncResponse.capped) {
          wasCapped = true;
          capLimit = Number(syncResponse.perSyncLimit) || capLimit;
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
      throw new Error(`${t('msg.allFoldersFailed')}\n${folderErrors.join('\n')}`);
    }
    if (!allEmails.length) {
      showResult(t('msg.noNewEmails'), 'info');
      updateProgress(100, t('msg.noNewEmails'));
      finalizeSync('success', t('msg.noNewEmails'));
      return;
    }

    checkSyncAborted();
    updateProgress(55, t('msg.preparingTable'));
    const table = await prepareEmailTable(tableName, (progress, message) => {
      updateProgress(55 + progress * 0.1, message);
    });

    checkSyncAborted();
    updateProgress(65, t('msg.writingRecords'));
    const stats = await writeEmailRecords(table, provider, allEmails, BASE_URL, (progress, message) => {
      if (syncAborted) throw new Error(SYNC_ABORTED_MARKER);
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

    updateProgress(100, t('msg.syncComplete'));
    updateSyncCount(stats.inserted, stats.total);
    updateSyncSub(`${stats.inserted} ${t('sync.inserted')} · ${stats.skipped} ${t('sync.skipped')}${stats.patchedAttachments ? ` · ${stats.patchedAttachments} ${t('sync.attachments')}` : ''}`);
    finalizeSync('success', t('msg.syncComplete'));

    const folderLabel = folders.length > 1 ? ` (${folders.length} folders)` : '';
    let resultMsg = `${t('msg.syncComplete')}${folderLabel}：${stats.total} / ${stats.inserted} / ${stats.skipped}`;
    if (stats.patchedAttachments > 0) {
      resultMsg += `\n${t('msg.patchedAttachments', { count: String(stats.patchedAttachments) })}`;
    }
    if (folderErrors.length) {
      resultMsg += `\n${t('msg.partialFoldersFailed')}${folderErrors.join('；')}`;
    }
    const showUpgrade = wasCapped && !currentUserPaid;
    if (showUpgrade) {
      const requested = getMaxMessages();
      const capText = t('msg.freeCapHit', { limit: String(capLimit), requested: String(requested) });
      resultMsg += `\n${capText}`;
      // Surface paywall card on the Sync tab (the tab the user is currently looking at)
      $('#syncPaywallText').text(capText);
      $('#syncPaywall').prop('hidden', false);
      // Hide the "下一步" next-step CTA in this case — the paywall is the next step
      $('#syncNextBtn').prop('hidden', true);
    }
    showResult(resultMsg, folderErrors.length ? 'info' : 'success', showUpgrade);
  } catch (error) {
    const msg = (error as Error).message;
    if (msg === SYNC_ABORTED_MARKER) {
      showResult(t('sync.cancelled'), 'info');
      finalizeSync('idle', t('sync.cancelled'));
      $('#syncTimeline li.is-active').removeClass('is-active').addClass('is-pending');
    } else {
      showResult(t('msg.syncFailed', { error: msg }), 'error');
      finalizeSync('error', msg);
    }
  } finally {
    setSyncLoading(false);
    syncAborted = false;
  }
}

function setSyncLoading(loading: boolean) {
  const button = $('#startSync');
  const text = $('#syncBtnText');
  const spinner = $('#syncLoadingSpinner');
  button.prop('disabled', loading);
  text.text(loading ? t('btn.syncing') : t('btn.sync'));
  if (loading) spinner.show();
  else spinner.hide();
  if (loading) {
    syncAborted = false;
    setSyncStatus('running');
    setTab('sync');
    syncStartedAt = Date.now();
    syncSeenSteps = [];
    $('#syncTimeline').empty();
    $('#syncCancelBtn').prop({ hidden: false, disabled: false });
    $('#syncNextBtn').prop('hidden', true);
    $('#syncPaywall').prop('hidden', true);
    if (syncElapsedTimer) clearInterval(syncElapsedTimer);
    syncElapsedTimer = setInterval(updateSyncElapsed, 1000);
    updateSyncElapsed();
  } else {
    $('#syncCancelBtn').prop({ hidden: true, disabled: false });
    if (syncElapsedTimer) {
      clearInterval(syncElapsedTimer);
      syncElapsedTimer = null;
    }
  }
}

function setSyncStatus(state: 'idle' | 'running' | 'success' | 'error', label?: string) {
  const $pill = $('#syncStatusPill');
  $pill.removeClass('is-idle is-running is-success is-error').addClass(`is-${state}`);
  const labelKey =
    state === 'running' ? 'sync.statusRunning'
    : state === 'success' ? 'sync.statusSuccess'
    : state === 'error' ? 'sync.statusError'
    : 'sync.statusIdle';
  $('#syncStatusLabel').text(label || t(labelKey));
}

function updateProgress(progress: number, message: string) {
  const safeProgress = Math.max(0, Math.min(100, progress));
  $('#syncProgressBar').css('width', `${safeProgress}%`);
  $('#syncProgressText').text(message);
  $('#syncProgressValue').text(`${Math.round(safeProgress)}`);
  if (message && syncSeenSteps[syncSeenSteps.length - 1] !== message) {
    appendSyncTimelineStep(message, 'active');
  }
}

function appendSyncTimelineStep(msg: string, state: 'done' | 'active' | 'pending') {
  const $list = $('#syncTimeline');
  $list.find('.sync-timeline-empty').remove();
  $list.find('li.is-active').removeClass('is-active').addClass('is-done');
  const now = new Date();
  const ts = `${String(now.getMinutes()).padStart(2, '0')}:${String(now.getSeconds()).padStart(2, '0')}`;
  syncSeenSteps.push(msg);
  $list.append(`
    <li class="is-${state}">
      <span class="sync-timeline-msg">${escapeHtml(msg)}</span>
      <span class="sync-timeline-time">${ts}</span>
    </li>
  `);
}

function updateSyncCount(current: number, total: number) {
  $('#syncCountText').text(`${current.toLocaleString()} / ${total.toLocaleString()}`);
}

function updateSyncEta(eta: string) {
  $('#syncEtaText').text(eta);
}

function updateSyncSub(sub: string) {
  $('#syncSubText').text(sub);
}

function updateSyncElapsed() {
  if (!syncStartedAt) {
    $('#syncElapsedText').text('');
    return;
  }
  const sec = Math.max(0, Math.floor((Date.now() - syncStartedAt) / 1000));
  $('#syncElapsedText').text(t('sync.elapsed', { sec: String(sec) }));
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
    html += `<br><a class="result-upgrade-link" href="javascript:void(0)" id="resultUpgradeLink">${escapeHtml(t('msg.upgradeLink'))} &rarr;</a>`;
  }
  messageEl.removeClass('success error info').addClass(type).html(html);
  $('#resultContainer').show();
  if (showUpgrade) {
    // Per-account billing: buying a credit unlocks sync AND grants 1 schedule seat
    $('#resultUpgradeLink').off('click').on('click', () => startAddAccountFlow());
  }
}

function hideProgress() {
  // The Sync tab keeps the last state visible; nothing to hide.
}

function finalizeSync(state: 'success' | 'error' | 'idle', summary?: string) {
  setSyncStatus(state === 'idle' ? 'idle' : state);
  if (summary) updateSyncEta(summary);
  if (state === 'success') {
    $('#syncTimeline li.is-active').removeClass('is-active').addClass('is-done');
    $('#syncNextBtn').prop('hidden', false);
  } else {
    $('#syncNextBtn').prop('hidden', true);
    $('#syncPaywall').prop('hidden', true);
  }
}

// Provider tile picker (replaces the old dropdown; tiles are wired in bindEvents)
function setProviderValue(value: string) {
  if (!value) return;
  $('#provider').val(value);
  $('#providerTiles .provider-tile').each(function () {
    $(this).toggleClass('is-active', $(this).attr('data-provider') === value);
  });
  applyProviderPreset(value);
}

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
  await refreshQuota();
}

async function refreshQuota() {
  if (!currentUserId) {
    $('#quotaText').text('—');
    return;
  }
  try {
    const quota = await checkEmailQuota(BASE_URL, currentUserId, currentTenantKey);
    const perSync = Number((quota as any).perSync) || 5;
    lastQuota = { used: quota.used || 0, total: quota.total || 0, paid: !!quota.paid };
    if (lastQuota.paid) {
      $('#quotaChip').html(`<i class="fas fa-check"></i> <span>${t('setup.quotaPro')}</span>`);
    } else {
      $('#quotaChip').html(
        `<span class="quota-text">${perSync}</span><span> ${t('setup.quotaSuffixFree', { n: String(perSync) })}</span>`
      );
    }
  } catch (_) {
    $('#quotaText').text('—');
  }
}

async function handleUpgrade() {
  try {
    if (shouldUseAlipay()) {
      if (!currentUserId) {
        showResult(t('msg.getUserFailed'), 'error');
        return;
      }
      const payload: Record<string, unknown> = { userId: currentUserId };
      if (upgradeTarget?.scheduleId) {
        payload.scheduleId = upgradeTarget.scheduleId;
        payload.accountEmail = upgradeTarget.email;
      } else if (upgradeTarget?.intent === 'credit') {
        payload.intent = 'credit';
      }
      const resp = await fetch(`${BASE_URL}/api/alipay/create-order`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      if (!resp.ok) {
        const errData = await resp.json().catch(() => null);
        throw new Error(errData?.message || `Request failed: ${resp.status}`);
      }
      const data = await resp.json();
      if (data?.qrCode && data?.outTradeNo) {
        await showAlipayQRModal(data.qrCode, data.outTradeNo, data.product);
      } else {
        showResult(t('msg.createCheckoutFailed'), 'error');
      }
      return;
    }

    const fallbackUrl = 'https://emailsync.xiaomiao.win/healthz';
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
      throw new Error(errData?.message || `Request failed: ${resp.status}`);
    }
    const data = await resp.json();
    if (data?.url) {
      window.open(data.url, '_blank');
    } else {
      showResult(t('msg.createCheckoutFailed'), 'error');
    }
  } catch (error) {
    showResult(t('msg.upgradeFailed', { error: (error as Error).message }), 'error');
  }
}

interface AlipayProductInfo {
  name?: string;
  subject?: string;
  cnyAmount?: string;
  durationDays?: number;
}

function formatYmd(date: Date): string {
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, '0');
  const d = String(date.getDate()).padStart(2, '0');
  return `${y}.${m}.${d}`;
}

async function showAlipayQRModal(
  qrCodeUrl: string,
  outTradeNo: string,
  product?: AlipayProductInfo
): Promise<void> {
  const dataUrl = await QRCode.toDataURL(qrCodeUrl, { width: 256, margin: 2 });
  const modal = $('#alipayQRModal');
  $('#alipayQRImage').attr('src', dataUrl);

  // Render product info (plan name / amount / period)
  const planName = product?.name || product?.subject || t('alipay.planName');
  $('#alipayPlanName').text(planName);

  const durationDays = Number(product?.durationDays) || 365;
  const durationLabel = durationDays >= 365
    ? t('alipay.durationYears', { years: Math.round(durationDays / 365) })
    : t('alipay.durationDays', { days: durationDays });
  $('#alipayPlanDuration').html(`<i class="fas fa-calendar"></i><span>${durationLabel}</span>`);

  const start = new Date();
  const end = new Date(start.getTime() + durationDays * 24 * 60 * 60 * 1000);
  $('#alipayPlanPeriod').html(
    `<i class="fas fa-clock"></i><span>${formatYmd(start)} — ${formatYmd(end)}</span>`
  );

  const amount = product?.cnyAmount || '';
  if (amount) {
    $('#alipayAmount').html(
      `<span class="alipay-amount-value">¥${amount}</span><span class="alipay-amount-unit"> / ${t('alipay.duration')}</span>`
    );
  } else {
    $('#alipayAmount').empty();
  }

  // Reset status text (in case it was previously set to success)
  $('#alipayStatus').removeClass('alipay-status-success').text(t('alipay.hint'));
  $('#alipayWaiting').show();

  modal.show();
  alipayPollingActive = true;

  let paid = false;
  const maxPolls = 120;
  for (let i = 0; i < maxPolls && alipayPollingActive; i++) {
    await new Promise((resolve) => window.setTimeout(resolve, 3000));
    if (!alipayPollingActive) break;
    try {
      const resp = await fetch(
        `${BASE_URL}/api/alipay/trade/query?outTradeNo=${encodeURIComponent(outTradeNo)}`
      );
      const data = (await resp.json()) as { paid?: boolean };
      if (data.paid) {
        paid = true;
        break;
      }
    } catch (_) {}
  }

  alipayPollingActive = false;

  if (paid) {
    $('#alipayStatus').addClass('alipay-status-success').text(t('alipay.success'));
    $('#alipayWaiting').hide();

    // Capture intent before resetting upgradeTarget
    const wasCredit = upgradeTarget?.intent === 'credit';
    if (wasCredit) {
      pendingCreditTradeNo = outTradeNo;
    }

    await checkUserEntitlement();
    await refreshScheduleList();
    upgradeTarget = null;

    setTimeout(() => {
      modal.hide();
      $('.tab-btn[data-tab="upgrade"]').prop('hidden', true);
      if (wasCredit) {
        // Credit purchase: reveal the schedule form so user can fill in details
        showScheduleAddForm();
      } else {
        setTab('schedule');
      }
    }, 1200);
  } else {
    modal.hide();
    showResult(t('alipay.cancelled'), 'info');
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
  paid?: boolean;
  paidUntil?: number | null;
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
  const $list = $('#scheduleList');
  $list.empty();

  if (!schedules.length) {
    $list.append(`
      <div id="scheduleEmpty" class="schedule-empty">
        <i class="fas fa-clock"></i>
        <p>${escapeHtml(t('schedule.empty'))}</p>
      </div>
    `);
    $('#schedulePaidSummary').text('—');
    $('#schedulePaidBar').css('width', '0%');
    return;
  }

  const paidCount = schedules.filter(s => s.paid || currentUserPaid).length;
  $('#schedulePaidSummary').text(`${paidCount} / ${schedules.length}`);
  $('#schedulePaidBar').css('width', `${schedules.length ? (paidCount / schedules.length) * 100 : 0}%`);

  const intervalLabels: Record<number, string> = {
    1: t('schedule.perHour'),
    3: t('schedule.perNHours', { n: '3' }),
    6: t('schedule.perNHours', { n: '6' }),
    12: t('schedule.perNHours', { n: '12' }),
    24: t('schedule.perDay')
  };

  for (const s of schedules) {
    const isPaid = !!s.paid || currentUserPaid;
    const intervalLabel = intervalLabels[s.intervalHours] || t('schedule.perNHours', { n: String(s.intervalHours) });
    const mono = PROVIDER_MONO[s.provider] || PROVIDER_MONO.custom;
    const lastSyncText = s.lastSyncAt ? formatRelative(s.lastSyncAt) : t('msg.notSynced');
    const statusState = s.lastSyncStatus === 'failed' ? 'fail' : s.lastSyncStatus === 'success' ? 'ok' : '';
    const paidUntilLabel = s.paidUntil ? new Date(s.paidUntil).toISOString().slice(0, 10) : '';

    const paidBody = `
      <div class="schedule-item-row2">
        <span class="schedule-pill">${escapeHtml(intervalLabel)}</span>
        <span class="schedule-folder-text">${escapeHtml(s.folder)}</span>
      </div>
      <div class="schedule-item-row3${statusState ? ' is-' + statusState : ''}">
        <span class="schedule-item-status-dot"></span>
        <span>${escapeHtml(s.lastSyncStatus === 'failed' ? t('schedule.lastFail') : lastSyncText)}</span>
        ${paidUntilLabel ? `<span class="schedule-item-paid-until">${t('schedule.until')} ${escapeHtml(paidUntilLabel)}</span>` : ''}
      </div>
    `;
    const freeBody = `<div class="schedule-item-free-note">${escapeHtml(t('schedule.freeNote'))}</div>`;

    const $item = $(`
      <div class="schedule-item${isPaid ? '' : ' is-free'}" data-schedule-id="${escapeHtml(s.id)}">
        <span class="schedule-item-mono" style="background:${mono.color}">${escapeHtml(mono.mono)}</span>
        <div class="schedule-item-body">
          <div class="schedule-item-row1">
            <span class="schedule-item-email">${escapeHtml(s.email)}</span>
            ${isPaid
              ? `<span class="badge badge-pro"><i class="fas fa-check"></i> PRO</span>`
              : `<span class="badge badge-free"><i class="fas fa-lock"></i> FREE</span>`}
          </div>
          ${isPaid ? paidBody : freeBody}
        </div>
        ${isPaid
          ? `<button class="schedule-item-icon-btn schedule-item-delete" title="${escapeHtml(t('schedule.delete'))}"><i class="fas fa-trash-can"></i></button>`
          : `<button class="schedule-item-cta schedule-item-upgrade"><span class="price">¥499</span><span>${escapeHtml(t('schedule.activate'))}</span></button>`}
      </div>
    `);

    $item.find('.schedule-item-delete').on('click', () => handleDeleteSchedule(s.id, s.email));
    $item.find('.schedule-item-upgrade').on('click', () => openUpgradeFor(s));
    $list.append($item);
  }
}

function formatRelative(ts: number): string {
  const diff = Date.now() - ts;
  if (diff < 60_000) return t('time.justNow');
  if (diff < 3_600_000) return t('time.minutesAgo', { n: String(Math.floor(diff / 60_000)) });
  if (diff < 86_400_000) return t('time.hoursAgo', { n: String(Math.floor(diff / 3_600_000)) });
  return new Date(ts).toLocaleString();
}

function openUpgradeFor(s: ScheduleItem) {
  upgradeTarget = { scheduleId: s.id, provider: s.provider, email: s.email };
  const mono = PROVIDER_MONO[s.provider] || PROVIDER_MONO.custom;
  $('#upgradeTargetMono').text(mono.mono).css('background', mono.color);
  $('#upgradeTargetEmail').text(s.email);
  $('.tab-btn[data-tab="upgrade"]').prop('hidden', false);
  setTab('upgrade');
}

function openUpgradeForCredit() {
  upgradeTarget = { intent: 'credit' };
  $('#upgradeTargetMono').text('+').css('background', '#5C5C5C');
  $('#upgradeTargetEmail').text(t('upgrade.creditTarget'));
  $('.tab-btn[data-tab="upgrade"]').prop('hidden', false);
  setTab('upgrade');
}

function showScheduleAddForm() {
  $('#scheduleFormSection').prop('hidden', false);
  setTab('schedule');
  setTimeout(() => {
    $('#scheduleFormSection')[0]?.scrollIntoView({ behavior: 'smooth', block: 'center' });
  }, 30);
}

async function startAddAccountFlow() {
  // If user already holds user-level entitlement, skip payment.
  if (currentUserPaid) {
    pendingCreditTradeNo = '';
    showScheduleAddForm();
    return;
  }
  // Pay first → pop Alipay; the modal poll will reveal the form on success.
  openUpgradeForCredit();
  setTimeout(() => { handleUpgrade(); }, 250);
}

async function refreshScheduleList() {
  try {
    const params = new URLSearchParams();
    if (currentUserId) params.set('userId', currentUserId);
    if (currentTenantKey) params.set('tenantKey', currentTenantKey);
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
    showScheduleResult(t('msg.fillTokenFirst'), 'error');
    return;
  }
  if (!personalBaseToken) {
    showScheduleResult(t('msg.fillBaseToken'), 'error');
    return;
  }
  if (!appToken || !tableId) {
    showScheduleResult(t('msg.fillAppToken'), 'error');
    return;
  }

  $('#enableSchedule').prop('disabled', true);

  try {
    const payload: Record<string, unknown> = {
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
      userId: currentUserId || undefined,
      tenantKey: currentTenantKey || undefined
    };
    if (pendingCreditTradeNo) {
      payload.creditOutTradeNo = pendingCreditTradeNo;
    }

    const resp = await fetch(`${BASE_URL}/api/schedule/create`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    const data = await resp.json();

    if (data.status === 'seat_required') {
      // Backend says no credit available; kick off pay-first flow
      showScheduleResult(t('msg.seatRequired'), 'info');
      setTimeout(() => startAddAccountFlow(), 800);
      return;
    }
    if (data.status !== 'ok') {
      throw new Error(data.message || t('msg.scheduleAddFailed', { error: 'unknown' }));
    }

    // Credit consumed on the worker; clear local pointer
    pendingCreditTradeNo = '';
    showScheduleResult(t('msg.scheduleAdded', { email, interval: String(intervalHours) }), 'success');
    await refreshScheduleList();
    setTimeout(() => {
      $('#scheduleFormSection').prop('hidden', true);
      $('#scheduleResultContainer').hide();
    }, 1500);
  } catch (error) {
    showScheduleResult(t('msg.scheduleAddFailed', { error: (error as Error).message }), 'error');
  } finally {
    $('#enableSchedule').prop('disabled', false);
  }
}

async function handleDeleteSchedule(scheduleId: string, email: string) {
  if (!confirm(t('msg.confirmDelete', { email }))) return;

  try {
    const resp = await fetch(`${BASE_URL}/api/schedule/delete`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ id: scheduleId })
    });
    const data = await resp.json();
    if (data.status !== 'ok') {
      throw new Error(data.message || t('msg.scheduleDeleteFailed'));
    }

    showScheduleResult(t('msg.scheduleDeleted', { email }), 'info');
    await refreshScheduleList();
  } catch (error) {
    showScheduleResult(t('msg.deleteFailed', { error: (error as Error).message }), 'error');
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

// --- User-id chip (one-click copy, handy for support) ---

function maskUid(id: string): string {
  if (!id) return '';
  return id.length <= 12 ? id : `${id.slice(0, 6)}…${id.slice(-4)}`;
}

async function copyToClipboard(text: string): Promise<boolean> {
  try {
    if (navigator.clipboard && window.isSecureContext) {
      await navigator.clipboard.writeText(text);
      return true;
    }
  } catch (_) {
    /* fall through */
  }
  try {
    const ta = document.createElement('textarea');
    ta.value = text;
    ta.style.position = 'fixed';
    ta.style.top = '-9999px';
    document.body.appendChild(ta);
    ta.focus();
    ta.select();
    const ok = document.execCommand('copy');
    document.body.removeChild(ta);
    return ok;
  } catch (_) {
    return false;
  }
}

function renderUidChip(): void {
  const $chip = $('#uidChip');
  if (!currentUserId) {
    $chip.hide();
    return;
  }
  $chip
    .html(`<span>ID ${maskUid(currentUserId)}</span><i class="fas fa-copy"></i>`)
    .attr('title', `点击复制用户 ID：${currentUserId}`)
    .show();
  $chip.off('click').on('click', async () => {
    const ok = await copyToClipboard(currentUserId);
    if (ok) {
      const $i = $chip.find('i');
      $i.removeClass('fa-copy').addClass('fa-check');
      setTimeout(() => $i.removeClass('fa-check').addClass('fa-copy'), 1200);
    }
  });
}
