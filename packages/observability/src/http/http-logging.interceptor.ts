import { CallHandler, ExecutionContext, Injectable, NestInterceptor } from '@nestjs/common';
import { Request, Response } from 'express';
import { Observable } from 'rxjs';
import { tap } from 'rxjs/operators';
import { StructuredLogger } from '../logging/structured-logger.service';

@Injectable()
export class HttpLoggingInterceptor implements NestInterceptor {
  constructor(private readonly logger: StructuredLogger) {}

  intercept(context: ExecutionContext, next: CallHandler): Observable<unknown> {
    const httpCtx = context.switchToHttp();
    const request = httpCtx.getRequest<Request>();
    const response = httpCtx.getResponse<Response>();

    const startTime = performance.now();
    const { method, url } = request;
    const correlationId =
      (request.headers['x-correlation-id'] as string) ??
      (request.body && typeof request.body === 'object'
        ? (request.body as Record<string, unknown>).correlationId
        : undefined);

    return next.handle().pipe(
      tap({
        next: () => {
          const durationMs = Math.round((performance.now() - startTime) * 100) / 100;
          this.logger.info(`HTTP Request completed [${method} ${url}] -> ${response.statusCode}`, {
            method,
            url,
            statusCode: response.statusCode,
            durationMs,
            correlationId: correlationId ? String(correlationId) : undefined,
          });
        },
      }),
    );
  }
}
