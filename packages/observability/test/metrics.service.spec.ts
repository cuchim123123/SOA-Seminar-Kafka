import { describe, expect, it } from 'vitest';
import { ExecutionContext, CallHandler } from '@nestjs/common';
import { of, throwError } from 'rxjs';
import { MetricsService, HttpMetricsInterceptor, METRIC_NAMES } from '../src/metrics';

describe('MetricsService', () => {
  it('instantiates isolated registries without collisions', async () => {
    const service1 = new MetricsService({ serviceName: 'service-1', defaultMetrics: false });
    const service2 = new MetricsService({ serviceName: 'service-2', defaultMetrics: false });

    service1.recordSourceEventCommitted('PurchaseCompleted');
    service2.recordSourceEventCommitted('PurchaseCompleted');

    const metrics1 = await service1.getMetrics();
    const metrics2 = await service2.getMetrics();

    expect(metrics1).toContain('service="service-1"');
    expect(metrics1).not.toContain('service="service-2"');
    expect(metrics2).toContain('service="service-2"');
    expect(metrics2).not.toContain('service="service-1"');
  });

  it('records source events committed counter', async () => {
    const service = new MetricsService({ serviceName: 'producer-api', defaultMetrics: false });
    service.recordSourceEventCommitted('PurchaseCompleted', 'outbox_cdc');

    const metrics = await service.getMetrics();
    expect(metrics).toContain(METRIC_NAMES.SOURCE_EVENTS_COMMITTED_TOTAL);
    expect(metrics).toContain('service="producer-api"');
    expect(metrics).toContain('event_type="PurchaseCompleted"');
    expect(metrics).toContain('mode="outbox_cdc"');
  });

  it('records consumer deliveries, handler commits, duplicate deliveries, and acks', async () => {
    const service = new MetricsService({ serviceName: 'loyalty-worker', defaultMetrics: false });

    service.recordDelivery('loyalty', 'kafka', 'PurchaseCompleted');
    service.recordHandlerCommit('loyalty', 'PurchaseCompleted', 0.045);
    service.recordDuplicateDelivery('loyalty', 'PurchaseCompleted');
    service.recordAck('loyalty', 'kafka', 'success', 0.005);
    service.recordProcessingFailure('loyalty', 'transient_network_error', 0.02);

    const metrics = await service.getMetrics();
    expect(metrics).toContain(METRIC_NAMES.DELIVERIES_TOTAL);
    expect(metrics).toContain(METRIC_NAMES.HANDLER_COMMITS_TOTAL);
    expect(metrics).toContain(METRIC_NAMES.DUPLICATE_DELIVERIES_TOTAL);
    expect(metrics).toContain(METRIC_NAMES.ACK_OPERATIONS_TOTAL);
    expect(metrics).toContain(METRIC_NAMES.PROCESSING_FAILURES_TOTAL);
    expect(metrics).toContain('consumer_role="loyalty"');
  });

  it('sets and updates consumer lifecycle state gauge', async () => {
    const service = new MetricsService({ serviceName: 'analytics-worker', defaultMetrics: false });

    service.setConsumerState('analytics', 'polling');
    let metrics = await service.getMetrics();
    expect(metrics).toContain(
      'lab_consumer_state{service="analytics-worker",consumer_role="analytics",state="polling"} 1',
    );
    expect(metrics).toContain(
      'lab_consumer_state{service="analytics-worker",consumer_role="analytics",state="idle"} 0',
    );

    service.setConsumerState('analytics', 'paused');
    metrics = await service.getMetrics();
    expect(metrics).toContain(
      'lab_consumer_state{service="analytics-worker",consumer_role="analytics",state="polling"} 0',
    );
    expect(metrics).toContain(
      'lab_consumer_state{service="analytics-worker",consumer_role="analytics",state="paused"} 1',
    );
  });

  it('records replay runs and durations', async () => {
    const service = new MetricsService({ serviceName: 'replay-worker', defaultMetrics: false });

    service.recordReplayRun('completed', 1.25);
    const metrics = await service.getMetrics();
    expect(metrics).toContain(METRIC_NAMES.REPLAY_RUNS_TOTAL);
    expect(metrics).toContain(METRIC_NAMES.REPLAY_DURATION_SECONDS);
    expect(metrics).toContain('status="completed"');
  });

  it('gracefully handles invalid values without crashing', () => {
    const service = new MetricsService({ serviceName: 'resilient-service', defaultMetrics: false });
    expect(() => {
      service.recordPublishConfirmation('kafka', 'direct', 'success', -1);
      service.recordSourceToHandlerDuration('loyalty', -5);
      service.recordReplayRun('failed', -0.5);
    }).not.toThrow();
  });
});

describe('HttpMetricsInterceptor', () => {
  it('measures request duration and sanitizes route UUIDs to prevent cardinality explosion', async () => {
    const metricsService = new MetricsService({ serviceName: 'test-api', defaultMetrics: false });
    const interceptor = new HttpMetricsInterceptor(metricsService);

    const mockReq = {
      method: 'GET',
      path: '/api/purchases/018f3a3e-4d56-7890-abcd-ef0123456789/status',
      url: '/api/purchases/018f3a3e-4d56-7890-abcd-ef0123456789/status?debug=true',
    };
    const mockRes = {
      statusCode: 200,
    };

    const mockContext = {
      getType: () => 'http',
      switchToHttp: () => ({
        getRequest: () => mockReq,
        getResponse: () => mockRes,
      }),
    } as unknown as ExecutionContext;

    const mockCallHandler: CallHandler = {
      handle: () => of({ success: true }),
    };

    await new Promise<void>((resolve) => {
      interceptor.intercept(mockContext, mockCallHandler).subscribe({
        next: () => {},
        complete: () => resolve(),
      });
    });

    const metrics = await metricsService.getMetrics();
    expect(metrics).toContain(METRIC_NAMES.HTTP_REQUEST_DURATION_SECONDS);
    expect(metrics).toContain('route="/api/purchases/:id/status"');
    expect(metrics).not.toContain('018f3a3e-4d56-7890-abcd-ef0123456789');
  });

  it('skips /metrics and /health paths to avoid self-noise', async () => {
    const metricsService = new MetricsService({ serviceName: 'test-api', defaultMetrics: false });
    const interceptor = new HttpMetricsInterceptor(metricsService);

    const mockReq = {
      method: 'GET',
      path: '/metrics',
      url: '/metrics',
    };
    const mockRes = { statusCode: 200 };

    const mockContext = {
      getType: () => 'http',
      switchToHttp: () => ({
        getRequest: () => mockReq,
        getResponse: () => mockRes,
      }),
    } as unknown as ExecutionContext;

    const mockCallHandler: CallHandler = {
      handle: () => of('ok'),
    };

    await new Promise<void>((resolve) => {
      interceptor.intercept(mockContext, mockCallHandler).subscribe({
        complete: () => resolve(),
      });
    });

    const metrics = await metricsService.getMetrics();
    expect(metrics).not.toContain('route="/metrics"');
  });

  it('records 500 status code on unhandled error', async () => {
    const metricsService = new MetricsService({ serviceName: 'test-api', defaultMetrics: false });
    const interceptor = new HttpMetricsInterceptor(metricsService);

    const mockReq = {
      method: 'POST',
      path: '/api/purchases',
      url: '/api/purchases',
    };
    const mockRes = { statusCode: 200 };

    const mockContext = {
      getType: () => 'http',
      switchToHttp: () => ({
        getRequest: () => mockReq,
        getResponse: () => mockRes,
      }),
    } as unknown as ExecutionContext;

    const mockCallHandler: CallHandler = {
      handle: () => throwError(() => ({ status: 503, message: 'Service unavailable' })),
    };

    await new Promise<void>((resolve) => {
      interceptor.intercept(mockContext, mockCallHandler).subscribe({
        error: () => resolve(),
      });
    });

    const metrics = await metricsService.getMetrics();
    expect(metrics).toContain('status_code="503"');
    expect(metrics).toContain('route="/api/purchases"');
  });
});
