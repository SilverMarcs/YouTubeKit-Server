/**
 * Fetch wrapper that caches YouTube's static assets (player JS / iframe API
 * code) in Cloudflare's edge Cache API. Everything else passes through.
 *
 * Designed so we can update youtubei.js cleanly: this layer wraps the fetch
 * function passed into `Innertube.create({ fetch })` without touching the lib.
 */

const CACHEABLE_RE = /youtube\.com\/(s\/(?:player|desktop)\/|iframe_api)/;
const CACHE_TTL_SECONDS = 3600;

export interface CachedFetchOptions {
   /** Skip cache reads but still write fresh responses. Use after a stale-cache failure. */
   bypass?: boolean;
   /** ExecutionContext for non-blocking cache writes. Optional; falls back to awaiting. */
   ctx?: ExecutionContext;
}

export function createCachedFetch(underlying: typeof fetch, opts: CachedFetchOptions = {}): typeof fetch {
   return async (input, init) => {
      const req = new Request(input, init);
      const cacheable = req.method === 'GET' && CACHEABLE_RE.test(req.url);

      if (cacheable && !opts.bypass) {
         try {
            const hit = await caches.default.match(req);
            if (hit) {
               console.log(`cache HIT  ${shortenURL(req.url)}`);
               return hit;
            }
         } catch {
            // fall through to network
         }
      }

      const res = await underlying(input as any, init as any);

      if (cacheable && res.ok) {
         // Rebuild the response from scratch with only headers that allow caching.
         // YouTube often returns Set-Cookie / Vary / no-store which Cache API
         // refuses to store. Stripping them and setting our own Cache-Control
         // makes the response cacheable.
         try {
            const body = await res.clone().arrayBuffer();
            const headers = new Headers();
            const contentType = res.headers.get('Content-Type');
            if (contentType) headers.set('Content-Type', contentType);
            headers.set('Cache-Control', `public, max-age=${CACHE_TTL_SECONDS}`);
            const toCache = new Response(body, { status: res.status, headers });

            const putPromise = caches.default.put(req, toCache);
            if (opts.ctx) {
               opts.ctx.waitUntil(putPromise);
            } else {
               // Without ctx, we must await — fire-and-forget doesn't complete
               // before the request handler returns in wrangler dev.
               await putPromise;
            }
            console.log(`cache MISS ${shortenURL(req.url)} (stored ${body.byteLength}B)`);
         } catch (e) {
            console.warn(`cache PUT failed for ${shortenURL(req.url)}:`, e);
         }
      }

      return res;
   };
}

function shortenURL(url: string): string {
   try {
      const u = new URL(url);
      return u.pathname.length > 70 ? u.pathname.slice(0, 70) + '…' : u.pathname;
   } catch {
      return url.slice(0, 70);
   }
}
