import { CallHandler, ExecutionContext, Injectable, NestInterceptor } from '@nestjs/common';
import { Request, Response } from 'express';
import { Observable } from 'rxjs';
import { tap } from 'rxjs/operators';
import { MetricsService } from './metrics.service';

const SKIP_PATHS = ['/metrics', '/health', '/favicon.ico'];

@Injectable()
export class HttpMetricsInterceptor implements NestInterceptor {
  constructor(private readonly metricsService: MetricsService) {}

  intercept(context: ExecutionContext, next: CallHandler): Observable<unknown> {
    if (context.getType() !== 'http') {
      return next.handle();
    }

    const http = context.switchToHttp();
    const req = http.getRequest<Request>();
    const res = http.getResponse<Response>();

    const rawPath = req.path || req.url?.split('?')[0] || '/';

    // Do not instrument observability or probe endpoints to avoid self-noise
    if (SKIP_PATHS.some((skip) => rawPath.startsWith(skip))) {
      return next.handle();
    }

    const startTime = performance.now();
    const method = req.method ? req.method.toUpperCase() : 'GET';
    const route = this.sanitizeRoute(req, rawPath);

    return next.handle().pipe(
      tap({
        next: () => {
          const durationSeconds = (performance.now() - startTime) / 1000;
          const statusCode = res.statusCode || 200;
          this.metricsService.recordHttpRequest(method, route, statusCode, durationSeconds);
        },
        error: (err: unknown) => {
          const durationSeconds = (performance.now() - startTime) / 1000;
          const statusCode = this.extractStatusCode(err);
          this.metricsService.recordHttpRequest(method, route, statusCode, durationSeconds);
        },
      }),
    );
  }

  private sanitizeRoute(req: Request, rawPath: string): string {
    let route =
      req.route && typeof req.route.path === 'string'
        ? (req.baseUrl || '') + req.route.path
        : rawPath;

    // Normalize UUIDs to prevent cardinality explosion
    route = route.replace(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi, ':id');
    // Normalize numeric path segments
    route = route.replace(/\/\d+(?=\/|$)/g, '/:id');
    // Normalize long random IDs (e.g. nanoid or hex tokens)
    route = route.replace(/\/[A-Za-z0-9_-]{20,}(?=\/|$)/g, '/:id');

    return route || '/';
  }

  private extractStatusCode(err: unknown): number {
    if (typeof err === 'object' && err !== null) {
      if ('status' in err && typeof (err as { status: unknown }).status === 'number') {
        return (err as { status: number }).status;
      }
      if ('statusCode' in err && typeof (err as { statusCode: unknown }).statusCode === 'number') {
        return (err as { statusCode: number }).statusCode;
      }
      if ('getStatus' in err && typeof (err as { getStatus: unknown }).getStatus === 'function') {
        return (err as { getStatus: () => number }).getStatus();
      }
    }
    return 500;
  }
}
