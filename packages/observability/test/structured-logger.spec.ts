import { describe, expect, it, vi } from 'vitest';
import { LogFormatter } from '../src/logging/log-formatter';
import { StructuredLogger } from '../src/logging/structured-logger.service';

describe('StructuredLogger & LogFormatter', () => {
  it('formats log as valid JSON in json mode', () => {
    const jsonStr = LogFormatter.formatJson('info', 'Test message', 'test-service', 'TestContext', {
      correlationId: 'corr-123',
      eventId: 'evt-456',
      customKey: 'val',
    });

    const parsed = JSON.parse(jsonStr) as Record<string, unknown>;
    expect(parsed.level).toBe('info');
    expect(parsed.service).toBe('test-service');
    expect(parsed.context).toBe('TestContext');
    expect(parsed.message).toBe('Test message');
    expect(parsed.correlationId).toBe('corr-123');
    expect(parsed.eventId).toBe('evt-456');
    expect((parsed.metadata as Record<string, unknown>).customKey).toBe('val');
  });

  it('formats log nicely in pretty mode', () => {
    const pretty = LogFormatter.formatPretty(
      'warn',
      'Warning message',
      'test-service',
      'TestContext',
      { key: 'val' },
    );

    expect(pretty).toContain('[WARN ]');
    expect(pretty).toContain('[test-service]');
    expect(pretty).toContain('[TestContext]');
    expect(pretty).toContain('Warning message');
    expect(pretty).toContain('{"key":"val"}');
  });

  it('StructuredLogger outputs to console', () => {
    const logSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
    const logger = new StructuredLogger('TestContext', {
      serviceName: 'my-service',
      format: 'pretty',
    });

    logger.info('Hello world', { eventId: '123' });
    expect(logSpy).toHaveBeenCalled();
    logSpy.mockRestore();
  });
});
