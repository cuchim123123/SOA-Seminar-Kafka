import { ArgumentsHost, Catch, ExceptionFilter, HttpException, HttpStatus } from '@nestjs/common';
import { Request, Response } from 'express';
import { AppError } from '../errors/app-error';
import { DomainError } from '../errors/domain.error';
import { StructuredLogger } from '../logging/structured-logger.service';

@Catch()
export class GlobalHttpExceptionFilter implements ExceptionFilter {
  constructor(private readonly logger: StructuredLogger) {}

  catch(exception: unknown, host: ArgumentsHost): void {
    const ctx = host.switchToHttp();
    const response = ctx.getResponse<Response>();
    const request = ctx.getRequest<Request>();

    let status = HttpStatus.INTERNAL_SERVER_ERROR;
    let message = 'Internal server error';
    let errorName = 'InternalServerError';
    let details: unknown = undefined;

    if (exception instanceof HttpException) {
      status = exception.getStatus();
      const res = exception.getResponse();
      if (typeof res === 'string') {
        message = res;
      } else if (res && typeof res === 'object') {
        const resObj = res as Record<string, unknown>;
        message = (resObj.message as string) ?? exception.message;
        errorName = (resObj.error as string) ?? exception.name;
        details = resObj.details;
      }
    } else if (exception instanceof DomainError) {
      status = HttpStatus.BAD_REQUEST;
      message = exception.message;
      errorName = exception.name;
      details = exception.details;
    } else if (exception instanceof AppError) {
      status = HttpStatus.BAD_REQUEST;
      message = exception.message;
      errorName = exception.name;
      details = exception.details;
    } else if (exception instanceof Error) {
      message = exception.message;
      errorName = exception.name;
    }

    const correlationId =
      (request.headers['x-correlation-id'] as string) ??
      (request.body && typeof request.body === 'object'
        ? (request.body as Record<string, unknown>).correlationId
        : undefined);

    const errorPayload = {
      statusCode: status,
      error: errorName,
      message,
      ...(details ? { details } : {}),
      ...(correlationId ? { correlationId } : {}),
      timestamp: new Date().toISOString(),
      path: request.url,
    };

    if (status >= HttpStatus.INTERNAL_SERVER_ERROR) {
      this.logger.error(`HTTP 5xx Server Error [${request.method} ${request.url}]`, exception, {
        statusCode: status,
        path: request.url,
        method: request.method,
        correlationId: correlationId ? String(correlationId) : undefined,
      });
    } else {
      this.logger.warn(`HTTP 4xx Client Error [${request.method} ${request.url}]: ${message}`, {
        statusCode: status,
        path: request.url,
        method: request.method,
        correlationId: correlationId ? String(correlationId) : undefined,
      });
    }

    response.status(status).json(errorPayload);
  }
}
