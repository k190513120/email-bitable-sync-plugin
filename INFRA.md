# Infrastructure（基础设施清单）

> 这份文档记录"不在代码里"的部署事实——DNS、Pages 自定义域、Stripe 端点、Alipay 商户、KV 命名空间。每次在 Cloudflare / Stripe / 支付宝 后台改东西，**同时改这个文件**。

最后更新：2026-05-13

---

## Cloudflare Worker

| 项 | 值 |
|---|---|
| Worker 名 | `email-sync-service` |
| 配置文件 | `wrangler-email.toml` |
| 自定义域（API + SPA） | `emailsync.xiaomiao.win`（custom_domain in wrangler-email.toml） |
| 入口文件 | `cloudflare/email-worker.js` |
| SPA 托管 | Worker `[assets] directory = "./dist"` 同域出 SPA，**用户访问就是这个地址** |
| Cron | `0 * * * *`（每小时整点跑定时邮箱同步） |
| KV namespace | `EMAIL_STATE`（id `0f0d3c6453bf45daa1df06e90bca39b1`） |

### Secrets

- `STRIPE_SECRET_KEY`
- `STRIPE_WEBHOOK_SECRET`
- `ALIPAY_APP_ID`
- `ALIPAY_APP_PRIVATE_KEY`
- `ALIPAY_PUBLIC_KEY`

### Vars (in wrangler-email.toml `[vars]`)

- `STRIPE_PRODUCT_NAME = "邮箱同步飞书多维表格"`
- `STRIPE_PUBLISHABLE_KEY = "pk_live_..."`
- `ALIPAY_AMOUNT = "980.00"`
- `ALIPAY_SUBJECT = "邮箱同步飞书多维表格"`

---

## 飞书插件中心配置

- 插件 URL 填: `https://emailsync.xiaomiao.win/`
- 飞书插件中心打包 ZIP 上传方式与"URL 接入"二选一，目前用 URL 模式（指向 worker `[assets]`）

---

## Stripe

| 项 | 值 |
|---|---|
| 模式 | live |
| 产品 | "邮箱同步飞书多维表格"（CNY 980/年） |
| Webhook endpoint id | `we_1TAqxc...` |
| Webhook URL | `https://emailsync.xiaomiao.win/api/stripe/webhook` |
| 订阅事件 | `checkout.session.completed`, `customer.subscription.*` |

> ⚠️ 历史上还存在一条 `we_1TCCv9...` → `https://wereadsync.xiaomiao.win/api/stripe/webhook`，是 email worker 用 wereadsync 域时建的。2026-05-11 域名换回 weread worker 后这条变成无主孤魂，应当 disable/delete。详见 README 的"改动记录"。

---

## 支付宝当面付

| 项 | 值 |
|---|---|
| 商户 | 共用"波波 API"主体（1Password 条目 `Alipay merchant keys (波波 API)`） |
| 年费 | 980 元 |
| notify URL | `https://emailsync.xiaomiao.win/api/alipay/notify`（由 worker 动态构造，无需在支付宝控制台白名单中独立配置——只要主域名属于商户即可） |
| outTradeNo 前缀 | `EM` |
| KV 订单 key | `email:pay:trade:<outTradeNo>`（TTL 24h） |

---

## CI/CD

| 项 | 值 |
|---|---|
| GitHub Actions | `.github/workflows/deploy.yml` |
| 触发 | push to `main`（除非只改 `dist/**` / `public/**` / `**.md`） |
| 步骤 | `wrangler deploy --config wrangler-email.toml` |
| Secret | repo secret `CLOUDFLARE_API_TOKEN` |
| `.npmrc` | `legacy-peer-deps=true`（处理 cf-imap ts@^5 vs 仓库 ts@^4.7 的 peer 冲突） |

---

## 已知幽灵 / 历史包袱

（暂无）2026-05-13 已清理：删除了 `email-bitable-sync-plugin` Pages 项目及其 custom domain `email.xiaomiao.win`、对应 DNS CNAME；禁用了 `we_1TCCv9...` 这条没人验签的 Stripe webhook。

---

## 改动记录

- 2026-05-13: 清理 `email-bitable-sync-plugin` Pages 项目（含 custom domain `email.xiaomiao.win`、DNS CNAME `email.xiaomiao.win` → 删除）；Stripe webhook `we_1TCCv9...` 禁用。`email.xiaomiao.win` 现在返回 403（CF "no project here"），原来这里跑的是 4 月旧 dist 且 BASE_URL=wereadsync 已失效。
- 2026-05-13: main 收纳 `chore/add-deploy-ci` 分支两个 commit（含支付宝当面付 + emailsync 域名 + Worker `[assets]` SPA 托管 + `.npmrc`）；新建本文档。
- 2026-05-11: email worker 加 Alipay 当面付链路；切自定义域 `wereadsync.xiaomiao.win` → `emailsync.xiaomiao.win`；Stripe webhook endpoint `we_1TAqxc` URL 从 `email-sync-service.kelan656691.workers.dev` 改到 `emailsync.xiaomiao.win`。
- 2026-05-11: 救火给 `190513120@qq.com` / `ou_a1a5df117f0854fddb7785b2ace0030b` 手动写入 entitlement（绕过 webhook 漂移期间丢失的 980 入账）。
