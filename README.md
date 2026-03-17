# 邮箱 → 飞书多维表格边栏插件

该项目是一个飞书多维表格前端边栏插件。用户在插件内录入邮箱连接信息后，可将邮件与附件同步到多维表格，并基于邮件ID自动去重。

## 功能说明（当前）

- 邮箱连接：支持 Gmail、QQ 邮箱、飞书邮箱、163 邮箱与自定义 IMAP
- 本地同步：通过本地 Node 服务连接 IMAP 拉取邮件与附件
- 自动建表：若不存在目标表则自动创建并补齐字段
- 自动去重：根据“邮件ID”跳过已存在邮件，仅写入新邮件
- 附件同步：新增邮件后自动上传附件至“附件”字段

## 规划中的能力

- Cloudflare 中转：已预留 `/api/email/sync` 路由，当前返回未开启提示
- Stripe 支付：工程已包含 Stripe worker 示例接口，可在邮箱同步场景复用

## 本地开发

```bash
npm install
npm run server
npm run dev
```

构建发布包：

```bash
npm run build
```

后端默认地址是 `http://localhost:8787`。如果你要切换到线上服务，可通过前端环境变量配置：

```bash
VITE_SYNC_BASE_URL=https://your-worker.your-subdomain.workers.dev
```

开发时建议开两个终端分别运行前后端。

## 本地联调 API

当前邮箱同步接口：

- `POST /api/email/sync`
  - 请求示例：
  ```json
  {
    "provider": "gmail",
    "email": "name@example.com",
    "password": "your-app-password",
    "imapHost": "imap.gmail.com",
    "imapPort": 993,
    "secure": true,
    "folder": "INBOX",
    "maxMessages": 100
  }
  ```
  - 返回：`{ status: 'completed', emails: [...] }`

## Cloudflare Workers 部署（后续接入）

### 1) 安装依赖并登录 Cloudflare

```bash
npm install
npx wrangler login
```

### 2) 修改 Worker 名称（可选）

编辑 [wrangler.toml](file:///Users/bytedance/Documents/html-template-main_微信读书/wrangler.toml) 的 `name` 字段，避免和你账号下已有 Worker 重名。

### 3) 配置后端环境变量（推荐）

首次可只配必须项：

```bash
npx wrangler secret put WEREAD_COOKIE
```

如需 CookieCloud，可再配置：

```bash
npx wrangler secret put CC_URL
npx wrangler secret put CC_ID
npx wrangler secret put CC_PASSWORD
```

可选 CORS 限制：

```bash
npx wrangler secret put ALLOWED_ORIGIN
```

### 4) 部署

```bash
npm run cf:deploy
```

部署成功后会得到 `https://<name>.<subdomain>.workers.dev`。

### 5) 前端改为调用线上服务

在本地创建 `.env.local`：

```bash
VITE_SYNC_BASE_URL=https://<name>.<subdomain>.workers.dev
```

然后重新执行：

```bash
npm run dev
```

## Stripe 支付接入（Cloudflare Worker）

已内置以下支付接口：

- `POST /api/stripe/create-checkout-session`
- `POST /api/stripe/webhook`
- `GET /api/stripe/entitlement?email=...`

### Stripe 需要配置的密钥

```bash
npx wrangler secret put STRIPE_SECRET_KEY
npx wrangler secret put STRIPE_PUBLISHABLE_KEY
npx wrangler secret put STRIPE_WEBHOOK_SECRET
```

可选（推荐）：

```bash
npx wrangler secret put STRIPE_PRICE_ID
```

若不配 `STRIPE_PRICE_ID`，服务会按 `STRIPE_PRODUCT_NAME` 自动查找价格。

### Checkout 创建示例

```bash
curl -X POST "https://<worker>/api/stripe/create-checkout-session" \
  -H "Content-Type: application/json" \
  -d '{
    "customerEmail":"you@example.com",
    "successUrl":"https://your-app/success",
    "cancelUrl":"https://your-app/cancel",
    "productName":"微信读书同步多维表格"
  }'
```

返回 `url` 后可直接跳转到 Stripe 结算页。

### Webhook 事件建议

在 Stripe Dashboard 的 webhook endpoint 配置：

- `checkout.session.completed`
- `customer.subscription.created`
- `customer.subscription.updated`
- `customer.subscription.deleted`

### 同步接口按订阅控制（可选）

在 `wrangler.toml` 中将：

```toml
REQUIRE_ACTIVE_SUBSCRIPTION = "true"
```

开启后，调用 `/api/weread/sync` 需传 `customerEmail`，且该邮箱在 webhook 更新后处于有效订阅状态。

## 飞书插件接入

1. 在多维表格中打开「插件」→「自定义插件」
2. 填入本地开发地址或部署地址（需 HTTPS）
3. 在插件里粘贴微信读书 Cookie
4. 选择“同步记录数量”
5. 点击「确认并同步」

## 同步服务端接口约定

插件默认调用以下接口，你可以按需实现并映射到自己的后端：

- `POST /api/weread/login/session`
  - 返回：`{ sessionId, qrCodeUrl?, qrCodeBase64?, pollIntervalMs? }`
- `GET /api/weread/login/session/:sessionId/status`
  - 返回：`{ status: 'pending' | 'authorized' | 'expired' | 'failed', message? }`
- `POST /api/weread/sync`
  - 请求：`{ sessionId?, wereadCookie?, maxRecords? }`
  - 返回：
    - 直接返回数据：`{ highlights: [...] }`
    - 或异步任务：`{ status: 'processing', jobId }`
- `GET /api/weread/sync/:jobId`
  - 返回：`{ status: 'processing' | 'completed', highlights?: [...] }`

说明：Cloudflare 首版实现默认走“实时同步返回 `highlights`”，不依赖 `jobId` 轮询接口。

本仓库已内置一个可运行的 Node 服务实现，路径为 [index.js](file:///Users/bytedance/Documents/html-template-main_微信读书/server/index.js)。支持两种登录态来源：

- Provider 模式：配置 `WEREAD_LOGIN_PROVIDER_URL`，后端会把“创建会话/查询状态/拉取 cookie”转发到你的真实登录态抓取服务
- 本地模式：不配置 Provider 时，使用本地 mock 二维码流程（便于联调）

同步数据策略如下：

- 若会话中存在 `wereadCookie`，优先直接调用微信读书接口抓取真实划线与笔记
- 若抓取失败且配置了 `WEREAD_UPSTREAM_API`，回退到上游聚合接口
- 若都不可用，回退到本地示例数据文件 [sample-highlights.json](file:///Users/bytedance/Documents/html-template-main_微信读书/server/sample-highlights.json)

可选的手动绑定接口（调试用）：

- `POST /api/weread/login/session/:sessionId/cookie`
  - 请求：`{ wereadCookie: "..." }`
  - 用途：将真实 cookie 绑定到会话并直接置为已授权

说明：当“创建会话”失败时，前端会自动改为“直传 Cookie 到 `/api/weread/sync`”模式，不阻塞同步流程。

## 真实登录态抓取服务接入

当你已经有独立登录态抓取服务时，只需设置：

```bash
WEREAD_LOGIN_PROVIDER_URL=https://your-auth-provider.example.com
```

后端会自动尝试以下接口（按顺序）：

- 创建会话（POST）：
  - `/api/weread/auth/session`
  - `/api/weread/login/session`
  - `/session`
- 查询状态（GET）：
  - `/api/weread/auth/session/:id/status`
  - `/api/weread/login/session/:id/status`
  - `/session/:id/status`
- 获取 cookie（GET）：
  - `/api/weread/auth/session/:id/cookie`
  - `/api/weread/login/session/:id/cookie`
  - `/session/:id/cookie`
  - `/session/:id`

Provider 返回字段兼容：

- 会话ID：`sessionId` 或 `id`
- 二维码：`qrCodeUrl` / `qrUrl` / `qrConnectUrl` / `loginUrl` / `qrCodeBase64`
- 状态：`pending|authorized|expired|failed`（也兼容 `success|confirmed|timeout|error`）
- Cookie：`wereadCookie` 或 `cookie`

说明：如果 Provider 返回的是微信登录页面 URL（例如 `open.weixin.qq.com/connect/qrconnect...`），前端会以内嵌页面展示，并提供“外部打开”兜底链接。
说明：如果配置了 `WEREAD_LOGIN_PROVIDER_URL` 但服务临时不可用，后端会自动降级为手动 Cookie 模式，不会阻塞会话创建。

## 后端环境变量

- `PORT`：后端端口，默认 `8787`
- `PUBLIC_BASE_URL`：二维码回调使用的公开地址，默认 `http://localhost:8787`
- `SESSION_EXPIRE_MS`：扫码会话有效期（毫秒），默认 `300000`
- `WEREAD_LOGIN_PROVIDER_URL`：可选，真实登录态抓取服务地址
- `WEREAD_PREFER_REAL_DATA`：是否优先用 cookie 抓真实数据，默认 `true`
- `WEREAD_STRICT_REAL_DATA`：有 Cookie 时真实抓取失败是否直接报错，默认 `true`
- `WEREAD_MAX_SYNC_BOOKS`：每次最多扫描书籍数，默认 `2000`
- `WEREAD_MAX_SYNC_RECORDS`：每次最多返回记录数，默认 `200`
- `CC_URL`：可选，CookieCloud 服务地址
- `CC_ID`：可选，CookieCloud 用户 ID
- `CC_PASSWORD`：可选，CookieCloud 密码
- `WEREAD_COOKIE`：可选，直接提供微信读书 Cookie
- `WEREAD_UPSTREAM_API`：可选，上游微信读书聚合接口，返回 `highlights` 数组
- `WEREAD_SAMPLE_FILE`：可选，本地示例数据文件路径

Cookie 取值优先级：会话内 Cookie（扫码/Provider） > CookieCloud > `WEREAD_COOKIE`。
若设置了 Cookie 且 `WEREAD_STRICT_REAL_DATA=true`，则不会再悄悄回退示例数据，抓取失败会直接报错提示。

当前插件已优先支持“手动 Cookie / CookieCloud”模式；二维码仅作为可选能力保留。

## highlights 数据建议字段

插件已内置多套字段别名解析，推荐服务端返回下列结构之一：

```json
{
  "bookTitle": "书名",
  "author": "作者",
  "chapter": "章节",
  "highlightText": "划线内容",
  "noteText": "笔记内容",
  "tags": ["标签1", "标签2"],
  "highlightId": "划线ID",
  "bookId": "书籍ID",
  "highlightedAt": 1710000000000,
  "updatedAt": 1710000000000
}
```

## 数据安全

- 插件不直接处理微信读书账号密码
- 建议通过你自己的服务端托管鉴权和 Cookie
- 建议对服务端接口增加签名、限流、审计日志
