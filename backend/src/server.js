
/* global userAddress, userLastSeen, userClients, setWebsocketConnections, broadcastToUser, createServiceLogger, sendEmail, logError, startEscrowTimeoutChecker, refreshWsMetrics, startNotificationProcessor, startAdminReportScheduler, startWeeklyDigestScheduler, startPurgeDeletedRecords, startRecurringEscrowTicker */
/* eslint-disable */
/**
 * src/server.js
 * Stellar MarketPay — Express API server bootstrap
 */
"use strict";

require("dotenv").config();

const http = require("http");
const { WebSocketServer } = require("ws");
const nodemailer = require("nodemailer");

const { createApp } = require("./app");
const {
  realtimeClients,
  userClients,
  userLastSeen,
  realtimeRooms,
  broadcastRealtime,
  broadcastToUser,
  handleRealtimeConnection,
} = require("./ws/realtime");
const {
  scopeSessionClients,
  cleanupExpiredScopeSessions,
  handleScopeConnection,
} = require("./ws/scope");

const migrate = require("./db/migrate");
const IndexerService = require("./services/indexerService");
const { PriceAlertService } = require("./services/priceAlertService");
const pool              = require("./db/pool");
const anchorRoutes        = require("./routes/anchors");
const scopeRoutes        = require("./routes/scope");
const analyticsRoutes    = require("./routes/analytics");
const searchRoutes       = require("./routes/search");
const adminRoutes        = require("./routes/admin");
const admin2faRoutes     = require("./routes/admin2fa");
const assessmentRoutes   = require("./routes/assessments");
const auditRoutes        = require("./routes/audit");
const categoryRoutes     = require("./routes/categories");
const certificateRoutes  = require("./routes/certificates");
const developerRoutes    = require("./routes/developer");
const disputeRoutes      = require("./routes/disputes");
const faucetRoutes       = require("./routes/faucet");
const insightRoutes      = require("./routes/insights");
const jobTemplateRoutes  = require("./routes/jobTemplates");
const messageRoutes      = require("./routes/messageRoutes");
const notificationRoutes = require("./routes/notifications");
const onboardingRoutes   = require("./routes/onboarding");
const pingRoutes         = require("./routes/ping");
const publicRoutes       = require("./routes/public");
const publicJobBoardRoutes = require("./routes/publicJobBoard");
const rateLimitRoutes    = require("./routes/rateLimit");
const savedSearchRoutes  = require("./routes/savedSearches");
const skillRoutes        = require("./routes/skills");
const sponsorshipRoutes  = require("./routes/sponsorship");
const timeEntryRoutes    = require("./routes/timeEntries");
const tokenRoutes        = require("./routes/tokens");
const webauthnRoutes     = require("./routes/webauthn");
const webhookRoutes      = require("./routes/webhooks");
 
const { setWebsocketConnections } = require("./metrics");
const { startEscrowTimeoutChecker } = require("./services/escrowService");
const { scheduleStatsRefresh } = require("./services/statsService");
const { startPushSubscriptionPurge } = require("./services/pushSubscriptionService");

// Start audit worker — processes fire-and-forget audit log writes
require("./workers/auditWorker");

const PORT = process.env.PORT || 4000;

setInterval(() => {
  cleanupExpiredScopeSessions().catch((err) => {
    console.error("[scope] cleanup failed:", err.message);
  });
}, 60 * 60 * 1000).unref();

const indexerService = new IndexerService({
  platformWallet: process.env.PLATFORM_WALLET_ADDRESS,
  horizonUrl: process.env.HORIZON_URL,
  broadcast: broadcastRealtime,
});

const smtpEnabled = Boolean(process.env.SMTP_HOST && process.env.SMTP_USER && process.env.SMTP_PASS);
const smtpTransport = smtpEnabled
  ? nodemailer.createTransport({
      host: process.env.SMTP_HOST,
      port: Number(process.env.SMTP_PORT || 587),
      secure: false,
      auth: {
        user: process.env.SMTP_USER,
        pass: process.env.SMTP_PASS,
      },
    })
  : null;

const priceAlertService = new PriceAlertService({
  broadcast: broadcastRealtime,
  sendEmail: async ({ to, subject, text }) => {
    if (!smtpTransport || !to) return;
    await smtpTransport.sendMail({
      from: process.env.SMTP_FROM || process.env.SMTP_USER,
      to,
      subject,
      text,
    });
  },
});
 
app.locals.indexerService = indexerService;
app.locals.broadcastRealtime = broadcastRealtime;
 
// Middleware
app.use(helmet());
app.use(morgan("dev"));
app.use(express.json({ limit: "20kb" }));
 
const allowedOrigins = (process.env.ALLOWED_ORIGINS || "http://localhost:3000").split(",").map(o => o.trim());
app.use(cors({
  origin: (origin, cb) => (!origin || allowedOrigins.includes(origin)) ? cb(null, true) : cb(new Error("CORS blocked")),
  methods: ["GET", "POST", "PATCH", "DELETE"],
  allowedHeaders: ["Content-Type", "Authorization"],
  credentials: true,
}));
 
app.use(rateLimit({ windowMs: 15 * 60 * 1000, max: 150, standardHeaders: true, legacyHeaders: true }));
 
// ─── Routes ───────────────────────────────────────────────────────────────────
app.use("/health",            healthRoutes);
app.use("/api/auth",          authRoutes);
app.use("/api/jobs",          jobRoutes);
app.use("/api/applications",  applicationRoutes);
app.use("/api/profiles",      profileRoutes);
app.use("/api/escrow",        escrowRoutes);
app.use("/api/ratings",       ratingRoutes);
app.use("/api/progress",      progressRoutes);
app.use("/api/events",        eventRoutes);
app.use("/api/stats",         statsRoutes);
app.use("/api/contributors",  contributorRoutes);
app.use("/api/verification",  verificationRoutes);
app.use("/api/nft",           nftRoutes);
app.use("/api/ai-scorer",     aiScorerRoutes);
app.use("/api/anchors",       anchorRoutes);
 
app.get("/api/indexer/health", (req, res) => {
  res.json({
    status: "ok",
    indexer: indexerService.getHealth(),
  });
});
app.use("/api/contributors",    contributorRoutes);
app.use("/api/gas-estimate",    gasEstimatorRoutes);
app.use("/api/transactions",   transactionRoutes);
app.use("/api/dao",            daoRoutes);
app.use("/api/proposal-templates", proposalTemplateRoutes);
app.use("/api/price-alerts",      priceAlertRoutes);
app.use("/api/ai",                aiScorerRoutes);
app.use("/api/scope",             scopeRoutes);
app.use("/api/turrets",           turretRoutes);
app.use("/api/referrals",         referralRoutes);
app.use("/api/reputation",        reputationRoutes);
app.use("/api/auto-convert",      autoConvertRoutes);
app.use("/api/talent-pools",      talentPoolRoutes);
app.use("/api/invitations",       rateLimit({ windowMs: 60_000, max: 20 }), invitationRoutes);
app.use("/api/analytics",         analyticsRoutes);
app.use("/api/search",            searchRoutes);
app.use("/ping",                  pingRoutes);
app.use("/api/ping",              pingRoutes);
app.use("/api/admin/2fa",         admin2faRoutes);
app.use("/api/admin",             adminRoutes);
app.use("/api/assessments",       assessmentRoutes);
app.use("/api/audit",             auditRoutes);
app.use("/api/categories",        categoryRoutes);
app.use("/api/certificates",      certificateRoutes);
app.use("/api/developer",         developerRoutes);
app.use("/api/disputes",          disputeRoutes);
app.use("/api/faucet",            faucetRoutes);
app.use("/api/insights",          insightRoutes);
app.use("/api/job-templates",     jobTemplateRoutes);
app.use("/api/messages",          messageRoutes);
app.use("/api/notifications",     notificationRoutes);
app.use("/api/onboarding",        onboardingRoutes);
app.use("/api/public",            publicRoutes);
app.use("/api/v1/public",         publicJobBoardRoutes);
app.use("/api/rate-limit",        rateLimitRoutes);
app.use("/api/saved-searches",    savedSearchRoutes);
app.use("/api/skills",            skillRoutes);
app.use("/api/sponsorship",       sponsorshipRoutes);
app.use("/api/time-entries",      timeEntryRoutes);
app.use("/api/tokens",            tokenRoutes);
app.use("/api/webauthn",          webauthnRoutes);
app.use("/api/webhooks",          webhookRoutes);
 
// 404 handler — must come after all routes
app.use((req, res) => {
  res.status(404).json({ error: "Not found", code: "NOT_FOUND" });
});
 
app.use((err, req, res, _next) => {
  console.error("[Error]", err.message);
 
  res.status(err.status || 500).json({
    error: err.message || "Internal server error",
  });
});

const server = http.createServer(app);
const wsServer = new WebSocketServer({ noServer: true });

server.on("upgrade", (request, socket, head) => {
  const url = new URL(request.url, `http://${request.headers.host}`);
  if (url.pathname === "/ws/realtime" || url.pathname.startsWith("/ws/scope/")) {
    wsServer.handleUpgrade(request, socket, head, (ws) => {
      wsServer.emit("connection", ws, request);
    });
    return;
  }
  socket.destroy();
});

wsServer.on("connection", async (ws, request) => {
  const url = new URL(request.url, `http://${request.headers.host}`);
  if (url.pathname === "/ws/realtime") {
    await handleRealtimeConnection(ws, request);
    return;
  }
  if (url.pathname.startsWith("/ws/scope/")) {
    await handleScopeConnection(ws, request);
  }
});

async function bootstrap() {
  try {
    await migrate();
    await cleanupExpiredScopeSessions();
    await indexerService.start();
    priceAlertService.start();

    // Start job expiry checker - run every hour
    startJobExpiryChecker();

    // Start invitation cleanup job (purge expired / accepted / declined invitations)
    const { startInvitationCleanup } = require("./services/invitationCleanupService");
    startInvitationCleanup();
    // Issue #232 perf: start the 5-minute stats MV refresh cycle after migrations
    scheduleStatsRefresh();

    // Start daily purge of push subscriptions marked invalid (Issue #1438)
    startPushSubscriptionPurge();

    server.listen(PORT, () => {
      console.log(`
  🏪 Stellar MarketPay API
  🚀 Running at http://localhost:${PORT}
  🌐 Network: ${process.env.STELLAR_NETWORK || "testnet"}
  `);
    });
  } catch (err) {
    console.error("Failed to bootstrap server:", err.message);
    process.exit(1);
  }
}

/**
 * Periodically check for and expire old jobs (runs every hour).
 * Also sends warning notifications for jobs expiring within 3 days.
 */
async function startJobExpiryChecker() {
  const { expireOldJobs, getExpiringJobs } = require("./services/jobService");

  // Run immediately on startup
  try {
    const expiredCount = await expireOldJobs();
    if (expiredCount > 0) {
      console.log(`[job-expiry] Auto-expired ${expiredCount} old job(s)`);
    }
  } catch (err) {
    console.error("[job-expiry] Error on initial expiry check:", err.message);
  }

  // Schedule hourly checks
  setInterval(async () => {
    try {
      const expiredCount = await expireOldJobs();
      if (expiredCount > 0) {
        console.log(`[job-expiry] Auto-expired ${expiredCount} old job(s)`);
      }

      // Check for expiring jobs within 3 days and broadcast warnings
      const expiringJobs = await getExpiringJobs(3);
      if (expiringJobs.length > 0) {
        console.log(`[job-expiry] ${expiringJobs.length} job(s) expiring within 3 days`);
        broadcastRealtime("job:expiry-warning", {
          count: expiringJobs.length,
          jobs: expiringJobs.map((j) => ({
            id: j.id,
            title: j.title,
            expiresAt: j.expiresAt,
          })),
        });
      }
    } catch (err) {
      console.error("[job-expiry] Error on scheduled check:", err.message);
    }
  }, 60 * 60 * 1000).unref();
}

/**
 * Periodically process pending notifications (runs every 2 minutes).
 */
async function startNotificationProcessor() {
  const { processPendingNotifications } = require("./services/notificationService");
  const notificationLogger = createServiceLogger("notifications");

  const sendEmailFn = async ({ to, subject, text, html }) => {
    await sendEmail({ to, subject, text, html });
  };

  // Run immediately on startup
  try {
    const stats = await processPendingNotifications(sendEmailFn);
    if (stats.total > 0) {
      notificationLogger.info(
        {
          total: stats.total,
          sent: stats.sent,
          failed: stats.failed,
        },
        "Processed pending notifications on startup",
      );
    }
  } catch (err) {
    logError(notificationLogger, err, { operation: "initial_notification_processing" });
  }

  // Schedule checks every 2 minutes
  setInterval(async () => {
    try {
      const stats = await processPendingNotifications(sendEmailFn);
      if (stats.total > 0) {
        notificationLogger.info(
          {
            total: stats.total,
            sent: stats.sent,
            failed: stats.failed,
          },
          "Processed pending notifications",
        );
      }
    } catch (err) {
      logError(notificationLogger, err, { operation: "scheduled_notification_processing" });
    }
  }, 2 * 60 * 1000).unref();
}

/**
 * Periodically finalize expired API key rotations (runs every hour).
 */
function startApiKeyRotationFinalizer() {
  const { finalizeExpiredRotations } = require("./services/developerService");
  const rotationLogger = createServiceLogger("api-key-rotation");

  async function checkAndFinalize() {
    try {
      const finalized = await finalizeExpiredRotations();
      if (finalized.length > 0) {
        rotationLogger.info({ count: finalized.length }, "Finalized expired API key rotations");
      }
    } catch (err) {
      logError(rotationLogger, err, { operation: "api_key_rotation_finalizer" });
    }
  }

  setInterval(checkAndFinalize, 60 * 60 * 1000).unref();
}

/**
 * Schedule the weekly job-digest email for every Monday at 09:00 UTC.
 */
function startWeeklyDigestScheduler() {
  const weeklyDigestService = require("./services/weeklyDigestService");
  const digestLogger = createServiceLogger("weekly-digest-scheduler");

  const sendEmailFn = async ({ to, subject, text, html }) => {
    await sendEmail({ to, subject, text, html });
  };

  function msUntilNextMonday9amUTC() {
    const now = new Date();
    const target = new Date(now);

    const currentDay = now.getUTCDay();
    const daysUntilMonday = currentDay === 1 ? 0 : (8 - currentDay) % 7 || 7;
    target.setUTCDate(now.getUTCDate() + daysUntilMonday);
    target.setUTCHours(9, 0, 0, 0);

    if (target <= now) {
      target.setUTCDate(target.getUTCDate() + 7);
    }

    return target - now;
  }

  async function runDigest() {
    try {
      const stats = await weeklyDigestService.sendWeeklyDigest(sendEmailFn);
      digestLogger.info(stats, "Weekly digest run complete");
    } catch (err) {
      logError(digestLogger, err, { operation: "weekly_digest_run" });
    }
  }

  const delay = msUntilNextMonday9amUTC();
  const nextRun = new Date(Date.now() + delay);

  digestLogger.info(
    { nextRunUTC: nextRun.toISOString(), delayMs: delay },
    "Weekly digest scheduler armed",
  );

  setTimeout(async () => {
    await runDigest();
    setInterval(runDigest, 7 * 24 * 60 * 60 * 1000).unref();
  }, delay).unref();
}

/**
 * Schedule the weekly admin PDF report for every Monday at 08:00 UTC.
 */
function startAdminReportScheduler() {
  const { generateAndSendAdminReport } = require("./services/adminReportService");
  const reportLogger = createServiceLogger("admin-report-scheduler");

  const sendEmailFn = async (payload) => {
    await sendEmail(payload);
  };

  function msUntilNextMonday8amUTC() {
    const now = new Date();
    const target = new Date(now);
    const currentDay = now.getUTCDay();
    const daysUntilMonday = currentDay === 1 ? 0 : (8 - currentDay) % 7 || 7;
    target.setUTCDate(now.getUTCDate() + daysUntilMonday);
    target.setUTCHours(8, 0, 0, 0);
    if (target <= now) {
      target.setUTCDate(target.getUTCDate() + 7);
    }
    return target - now;
  }

  async function runReport() {
    try {
      const result = await generateAndSendAdminReport(sendEmailFn);
      reportLogger.info(result, "Weekly admin PDF report complete");
    } catch (err) {
      logError(reportLogger, err, { operation: "weekly_admin_report" });
    }
  }

  const delay = msUntilNextMonday8amUTC();
  const nextRun = new Date(Date.now() + delay);

  reportLogger.info(
    { nextRunUTC: nextRun.toISOString(), delayMs: delay },
    "Admin report scheduler armed",
  );

  setTimeout(async () => {
    await runReport();
    setInterval(runReport, 7 * 24 * 60 * 60 * 1000).unref();
  }, delay).unref();
}

/**
 * Periodically purge soft-deleted jobs and profiles older than 90 days (runs daily).
 */
function startPurgeDeletedRecords() {
  const { purgeDeletedJobs } = require("./services/jobService");
  const { purgeDeletedProfiles } = require("./services/profileService");
  const purgeLogger = createServiceLogger("purge-deleted");

  async function purge() {
    try {
      const jobsCount = await purgeDeletedJobs(90);
      const profilesCount = await purgeDeletedProfiles(90);
      if (jobsCount > 0 || profilesCount > 0) {
        purgeLogger.info(
          { jobsPurged: jobsCount, profilesPurged: profilesCount },
          "Purged soft-deleted records older than 90 days",
        );
      }
    } catch (err) {
      logError(purgeLogger, err, { operation: "purge_deleted_records" });
    }
  }

  setInterval(purge, 24 * 60 * 60 * 1000).unref();
}

/**
 * Start the recurring escrow ticker (Issue #450).
 */
function startRecurringEscrowTicker() {
  const { startRecurringEscrowTicker: startTicker } = require("./services/recurringEscrowService");
  startTicker();
}

if (process.env.NODE_ENV !== "test") {
  bootstrap();
}

// Expose WebSocket internals for testing backwards-compatibility
app._ws = wsServer;
app._ws.server = server;
app._ws.wsServer = wsServer;
app._ws.realtimeClients = realtimeClients;
app._ws.realtimeRooms = realtimeRooms;
app._ws.userClients = userClients;
app._ws.userLastSeen = userLastSeen;
app._ws.scopeSessionClients = scopeSessionClients;
app._ws.broadcastRealtime = broadcastRealtime;
app._ws.broadcastToUser = (userAddress, event, payload) =>
  broadcastRealtime(event, { ...payload, userAddress });

app.startEscrowTimeoutChecker = () => {};

module.exports = app;
