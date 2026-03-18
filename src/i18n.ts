import zh from '../locales/zh.json';
import en from '../locales/en.json';
import jp from '../locales/jp.json';

type Locale = 'zh' | 'en' | 'jp';
type Messages = Record<string, string>;

const locales: Record<Locale, Messages> = { zh, en, jp };

let currentLocale: Locale = 'zh';

export function setLocale(lang: string) {
  if (lang === 'ja') lang = 'jp';
  currentLocale = (lang in locales ? lang : 'zh') as Locale;
}

export function getLocale(): Locale {
  return currentLocale;
}

export function t(key: string, params?: Record<string, string | number>): string {
  let text = locales[currentLocale]?.[key] || locales['zh']?.[key] || key;
  if (params) {
    for (const [k, v] of Object.entries(params)) {
      text = text.replace(new RegExp(`\\{${k}\\}`, 'g'), String(v));
    }
  }
  return text;
}

/**
 * Apply translations to all elements with data-i18n attribute.
 * data-i18n="key" → sets textContent
 * data-i18n-placeholder="key" → sets placeholder
 * data-i18n-title="key" → sets title
 */
export function applyI18n() {
  document.querySelectorAll('[data-i18n]').forEach((el) => {
    const key = el.getAttribute('data-i18n')!;
    el.textContent = t(key);
  });
  document.querySelectorAll('[data-i18n-placeholder]').forEach((el) => {
    const key = el.getAttribute('data-i18n-placeholder')!;
    (el as HTMLInputElement).placeholder = t(key);
  });
  document.querySelectorAll('[data-i18n-title]').forEach((el) => {
    const key = el.getAttribute('data-i18n-title')!;
    (el as HTMLElement).title = t(key);
  });
  document.querySelectorAll('[data-i18n-html]').forEach((el) => {
    const key = el.getAttribute('data-i18n-html')!;
    el.innerHTML = t(key);
  });
  // Update page title
  document.title = t('title');
}
