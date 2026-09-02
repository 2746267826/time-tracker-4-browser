/**
 * PIM reporter: pushes domain-level tracking data to the local PIM Windows
 * daemon bridge (POST {baseUrl}/browser/site/heartbeat).
 *
 * This module is intentionally self-contained (single file, no changes to
 * upstream data flow) so the fork stays easy to merge with upstream.
 *
 * Buffering: events are persisted to chrome.storage.local so a short-lived
 * MV3 service worker never loses more than one unflushed batch. If the PIM
 * daemon is offline, the buffer is kept and retried on the next flush
 * trigger; oldest entries are dropped beyond PIM_MAX_BUFFER to bound growth.
 */

const PIM_BUFFER_KEY = 'pimReportBuffer'
const PIM_BASE_URL_KEY = 'pimReportBaseUrl'
const PIM_ENABLED_KEY = 'pimReportEnabled'

/** Default matches the PIM Windows client daemon bridge port. */
const PIM_DEFAULT_BASE_URL = 'http://localhost:15601'
const PIM_HEARTBEAT_PATH = '/browser/site/heartbeat'
/** Flush cadence: short enough for near-real-time, long enough to batch. */
const PIM_FLUSH_INTERVAL = 15_000
/** Buffer cap: drop oldest beyond this to bound storage growth. */
const PIM_MAX_BUFFER = 500
/** Max attempts per batch before dropping it (prevents unbounded retry). */
const PIM_MAX_AGE_MS = 6 * 60 * 60 * 1000

type PimEventKind = 'focus' | 'tick' | 'visit' | 'run' | 'media'

type PimEvent = {
    kind: PimEventKind
    host: string
    /** epoch ms; focus/tick */
    startMs?: number
    /** epoch ms; focus only */
    endMs?: number
    /** ms; tick/run/media */
    durationMs?: number
    /** YYYY-MM-DD; run/media */
    date?: string
    /** epoch ms when the event was buffered (for age-based dropping) */
    at?: number
}

let buffer: PimEvent[] = []
let bufferLoaded = false
let flushTimer: ReturnType<typeof setTimeout> | undefined

function getBaseUrl(): Promise<string> {
    return new Promise(resolve => {
        chrome.storage.local.get([PIM_BASE_URL_KEY], result => {
            const base = result?.[PIM_BASE_URL_KEY]
            resolve(typeof base === 'string' && base ? base : PIM_DEFAULT_BASE_URL)
        })
    })
}

function isEnabled(): Promise<boolean> {
    return new Promise(resolve => {
        chrome.storage.local.get([PIM_ENABLED_KEY], result => {
            const enabled = result?.[PIM_ENABLED_KEY]
            resolve(enabled !== false)
        })
    })
}

function loadBuffer(): Promise<void> {
    if (bufferLoaded) return Promise.resolve()
    return new Promise(resolve => {
        chrome.storage.local.get([PIM_BUFFER_KEY], result => {
            const stored = result?.[PIM_BUFFER_KEY]
            if (Array.isArray(stored)) buffer = stored
            bufferLoaded = true
            resolve()
        })
    })
}

function persistBuffer(): Promise<void> {
    return new Promise(resolve => {
        chrome.storage.local.set({ [PIM_BUFFER_KEY]: buffer }, () => resolve())
    })
}

function scheduleFlush(): void {
    if (flushTimer) return
    flushTimer = setTimeout(() => {
        flushTimer = undefined
        void flush()
    }, PIM_FLUSH_INTERVAL)
}

async function postBatch(base: string, batch: PimEvent[]): Promise<void> {
    const res = await fetch(`${base.replace(/\/+$/, '')}${PIM_HEARTBEAT_PATH}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            events: batch,
            client: { source: 'tt4b-pim-report' },
        }),
    })
    if (!res.ok && res.status !== 204) {
        throw new Error(`PIM site heartbeat failed with status ${res.status}`)
    }
}

async function flush(): Promise<void> {
    await loadBuffer()
    if (!buffer.length) return
    const enabled = await isEnabled()
    if (!enabled) return

    const now = Date.now()
    // Drop entries that are too old to be worth delivering
    buffer = buffer.filter(e => now - (e.at ?? now) < PIM_MAX_AGE_MS)
    if (!buffer.length) {
        await persistBuffer()
        return
    }

    const batch = buffer
    const base = await getBaseUrl()
    try {
        await postBatch(base, batch)
        buffer = []
        await persistBuffer()
        await markResult(true)
    } catch (err) {
        console.warn('[pim-report] failed to reach PIM daemon, kept', batch.length, 'events:', err)
        // Bound growth: keep the newest half if the daemon stays offline
        if (buffer.length > PIM_MAX_BUFFER) {
            buffer = buffer.slice(buffer.length - PIM_MAX_BUFFER)
        }
        await persistBuffer()
        await markResult(false)
    }
}

async function markResult(success: boolean): Promise<void> {
    const now = new Date().toISOString()
    const update = success
        ? { pimReportLastSuccessAt: now }
        : { pimReportLastErrorAt: now }
    await new Promise<void>(resolve => {
        chrome.storage.local.set<Record<string, string>>(update, () => resolve())
    })
}

async function push(event: PimEvent): Promise<void> {
    await loadBuffer()
    buffer.push({ ...event, at: Date.now() })
    await persistBuffer()
    scheduleFlush()
}

export const pimReporter = {
    /** Focus time segment for one host (from track-server handleTime). */
    reportFocus(host: string, startMs: number, endMs: number): void {
        if (!host) return
        void push({ kind: 'focus', host, startMs, endMs })
    },

    /** Domain-level time tick (from the timeline.tick message). */
    reportTick(host: string, startMs: number, durationMs: number): void {
        if (!host) return
        void push({ kind: 'tick', host, startMs, durationMs })
    },

    /** One page visit for one host. */
    reportVisit(host: string): void {
        if (!host) return
        void push({ kind: 'visit', host })
    },

    /** Per-date browser run time for one host. */
    reportRun(host: string, byDate: Record<string, number>): void {
        if (!host) return
        for (const [date, durationMs] of Object.entries(byDate)) {
            durationMs && void push({ kind: 'run', host, date, durationMs })
        }
    },

    /** Per-date media playing time for one host. */
    reportMedia(host: string, byDate: Record<string, number>): void {
        if (!host) return
        for (const [date, durationMs] of Object.entries(byDate)) {
            durationMs && void push({ kind: 'media', host, date, durationMs })
        }
    },
}

// Best-effort flush when the service worker starts (deliver leftovers)
void flush()
