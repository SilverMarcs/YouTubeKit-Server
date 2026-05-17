/**
 * YouTubeKit-Server (Cathode fork).
 *
 * Single endpoint `GET /extract?videoID=X&itags=...` that does all extraction
 * server-side via youtubei.js with native fetch. Returns JSON.
 *
 * Tradeoff: extraction happens from the Worker IP. Some googlevideo URLs may
 * be soft-bound to the extractor IP; if those URLs ever 403 from the user's
 * device we'd need to reintroduce the WebSocket orchestration. For now,
 * single-RTT extraction is way faster (~1-2s instead of ~3-5s).
 */

import { Innertube, Platform, Log } from 'youtubei.js';
import { evaluateJavaScript } from './youtube/js-evaluator';
import { fileExtensionFromMimeType } from './youtube/file_extension';
import { AvailableInnertubeClient } from './youtube/models/internal';
import { RemoteStream } from './youtube/models/websocket';
import { YouTubeService } from './youtube/service';
import { createCachedFetch } from './youtube/cached-fetch';

// Silence youtubei.js's internal logging entirely. Its "error" messages
// (HypeFanCreditsSectionView, decipher failures for formats without ciphers,
// etc.) are non-fatal noise we don't need. Our own catch blocks surface the
// failures that actually matter.
Log.setLevel(Log.Level.NONE);
export { AppRateLimiter } from './durable-objects/app-rate-limiter';
import { RateLimitDecision } from './durable-objects/app-rate-limiter';

const APP_ID_HEADER = 'X-AppID-v1';
const APP_ID_MAX_LENGTH = 128;

export default {
   async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
      const url = new URL(request.url);
      const appID = normalizeAppID(request.headers.get(APP_ID_HEADER));

      if (url.pathname === '/extract' && request.method === 'GET') {
         try {
            const decision = await checkRateLimit(appID, env);
            if (!decision.allowed) {
               return buildRateLimitResponse(decision);
            }
         } catch (error) {
            return new Response('Rate limiter unavailable', { status: 503 });
         }

         const videoID = url.searchParams.get('videoID');
         if (!videoID) return new Response('Missing videoID', { status: 400 });

         const itagsParam = url.searchParams.get('itags');
         const itagFilter = itagsParam
            ? new Set(itagsParam.split(',').map(s => parseInt(s.trim(), 10)).filter(n => !isNaN(n)))
            : undefined;

         try {
            const streams = await extractStreams(videoID, itagFilter, ctx);
            return new Response(JSON.stringify({ streams }), {
               headers: { 'Content-Type': 'application/json' },
            });
         } catch (error: any) {
            console.error(`extract(${videoID}) failed:`, error);
            return new Response(JSON.stringify({ error: error.message ?? String(error) }), {
               status: 500,
               headers: { 'Content-Type': 'application/json' },
            });
         }
      }

      // Lightweight liveness probe used by client pre-warm. Forces TLS/DNS/worker
      // cold-start to happen during app launch instead of on first video click.
      if (url.pathname === '/warm') {
         return new Response('ok', { status: 200 });
      }

      // WebSocket orchestration: the client provides its residential IP for
      // every YouTube fetch, so URLs YouTube returns are bound to the user's
      // IP. Slower (~3-5s) but consistently higher quality than /extract
      // which uses the Worker's IP. /extract is retained as a fast option for
      // when quality doesn't matter (e.g. download metadata).
      if (url.pathname === '/v1' && request.headers.get('Upgrade') === 'websocket') {
         try {
            const decision = await checkRateLimit(appID, env);
            if (!decision.allowed) return buildRateLimitResponse(decision);
         } catch {
            return new Response('Rate limiter unavailable', { status: 503 });
         }

         const videoID = url.searchParams.get('videoID');
         if (!videoID) return new Response('Missing videoID', { status: 400 });

         const itagsParam = url.searchParams.get('itags');
         const itagFilter = itagsParam
            ? new Set(itagsParam.split(',').map(s => parseInt(s.trim(), 10)).filter(n => !isNaN(n)))
            : undefined;

         const [clientSock, serverSock] = Object.values(new WebSocketPair());
         serverSock.accept();
         new YouTubeService(videoID, serverSock, itagFilter, ctx).start();
         return new Response(null, { status: 101, webSocket: clientSock });
      }

      return new Response('Not found', { status: 404 });
   },
} satisfies ExportedHandler<Env>;

async function extractStreams(videoID: string, itagFilter: Set<number> | undefined, ctx: ExecutionContext): Promise<RemoteStream[]> {
   Platform.shim.eval = async (data, env) => await evaluateJavaScript(data, env);

   // Wrapping fetch in a lambda binds it correctly under wrangler middleware.
   // createCachedFetch then layers Cache-API caching on top for static assets.
   const nativeFetch: typeof fetch = (input, init) => fetch(input as any, init as any);

   try {
      return await tryExtract(nativeFetch, videoID, itagFilter, false, ctx);
   } catch (cachedErr) {
      console.warn('Cached extraction failed, retrying with fresh fetch:', cachedErr);
      return await tryExtract(nativeFetch, videoID, itagFilter, true, ctx);
   }
}

async function tryExtract(
   nativeFetch: typeof fetch,
   videoID: string,
   itagFilter: Set<number> | undefined,
   bypassCache: boolean,
   ctx: ExecutionContext
): Promise<RemoteStream[]> {
   const innertube = await Innertube.create({
      fetch: createCachedFetch(nativeFetch, { bypass: bypassCache, ctx }),
   });

   const clients: AvailableInnertubeClient[] = ['ANDROID_VR', 'WEB'];
   const fallbackClient: AvailableInnertubeClient = 'WEB_EMBEDDED';

   const primaryResults = await Promise.all(
      clients.map(client =>
         streamsFor(innertube, videoID, client, itagFilter).catch(e => {
            console.error(`Client ${client} failed:`, e);
            return [] as RemoteStream[];
         })
      )
   );
   let merged = dedupeByItag(primaryResults.flat());

   if (merged.length === 0) {
      try {
         merged = await streamsFor(innertube, videoID, fallbackClient, itagFilter);
      } catch (e) {
         console.error(`Fallback client ${fallbackClient} failed:`, e);
      }
   }

   return merged;
}

function dedupeByItag(streams: RemoteStream[]): RemoteStream[] {
   const seen = new Set<number>();
   const out: RemoteStream[] = [];
   for (const s of streams) {
      if (seen.has(s.itag)) continue;
      seen.add(s.itag);
      out.push(s);
   }
   return out;
}

async function streamsFor(
   innertube: Innertube,
   videoID: string,
   client: AvailableInnertubeClient,
   itagFilter?: Set<number>
): Promise<RemoteStream[]> {
   const info = await innertube.getInfo(videoID, { client: client as any });
   const f = info.streaming_data || { formats: [], adaptive_formats: [] };
   let formats = [...(f.formats ?? []), ...(f.adaptive_formats ?? [])];

   if (itagFilter && itagFilter.size > 0) {
      formats = formats.filter(fmt => itagFilter.has(fmt.itag));
   }

   // Decipher in batches to bound memory.
   const BATCH = 5;
   const out: RemoteStream[] = [];
   for (let i = 0; i < formats.length; i += BATCH) {
      const batch = formats.slice(i, i + BATCH);
      const results = await Promise.all(
         batch.map(async fmt => {
            let url: string | undefined;
            try {
               url = await fmt.decipher(innertube.session.player);
            } catch {
               url = undefined;
            }
            url ??= (fmt as any).deciphered_url as string | undefined;
            if (!url || fmt.is_dubbed) return null;

            let mimeType = fmt.mime_type;
            let videoCodec: string | undefined, audioCodec: string | undefined;
            if (mimeType?.includes('codecs=')) {
               const codecs = (mimeType.split('codecs=')[1]?.replace(/"/g, '') || '')
                  .split(',')
                  .map(c => c.trim())
                  .filter(Boolean);
               if (fmt.has_video && fmt.has_audio && codecs.length >= 2) {
                  videoCodec = codecs[0];
                  audioCodec = codecs[1];
               } else if (fmt.has_video && codecs.length >= 1) {
                  videoCodec = codecs[0];
               } else if (fmt.has_audio && codecs.length >= 1) {
                  audioCodec = codecs[0];
               }
               mimeType = mimeType.split(';')[0];
            }

            return {
               url,
               itag: fmt.itag,
               ext: fileExtensionFromMimeType(mimeType),
               video_codec: videoCodec,
               audio_codec: audioCodec,
               average_bitrate: fmt.bitrate || undefined,
               audio_bitrate: fmt.has_audio ? fmt.bitrate : undefined,
               video_bitrate: fmt.has_video ? fmt.bitrate : undefined,
               filesize: fmt.content_length ? Number(fmt.content_length) : undefined,
            } as RemoteStream;
         })
      );
      for (const r of results) if (r) out.push(r);
   }
   return out;
}

function normalizeAppID(rawAppID: string | null): string {
   const trimmed = rawAppID?.trim();
   if (!trimmed) return 'unknown';
   return trimmed.slice(0, APP_ID_MAX_LENGTH);
}

async function checkRateLimit(appID: string, env: Env): Promise<RateLimitDecision> {
   const objectID = env.APP_RATE_LIMITER.idFromName(appID);
   const rate_limiter = env.APP_RATE_LIMITER.get(objectID);
   return await rate_limiter.admit({ cost: 1, nowMs: Date.now() });
}

function buildRateLimitResponse(decision: RateLimitDecision): Response {
   return new Response('Too many requests', {
      status: 429,
      headers: {
         'Retry-After': decision.retryAfterSeconds.toString(),
         'X-RateLimit-Limit-Day': decision.limitDaily.toString(),
         'X-RateLimit-Limit-Week': decision.limitWeekly.toString(),
         'X-RateLimit-Remaining-Day': decision.remainingDaily.toString(),
         'X-RateLimit-Remaining-Week': decision.remainingWeekly.toString(),
      },
   });
}
