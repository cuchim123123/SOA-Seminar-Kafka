import { describe, expect, it, vi } from 'vitest';
import { processKafkaRecord } from '../src/kafka/kafka-message-processor';
import { StructuredLogger } from '../src/logging/structured-logger.service';

describe('processKafkaRecord', () => {
  const logger = new StructuredLogger('TestConsumer', { serviceName: 'test-service' });

  it('skips empty payload with warning', async () => {
    const warnSpy = vi.spyOn(logger, 'warn').mockImplementation(() => {});
    const handlerSpy = vi.fn();

    await processKafkaRecord({
      topic: 'test.events',
      partition: 0,
      offset: '10',
      rawValue: undefined,
      logger,
      deserializer: (r) => JSON.parse(r),
      handler: handlerSpy,
    });

    expect(warnSpy).toHaveBeenCalledWith(
      'Received empty message payload — skipping record',
      expect.objectContaining({ topic: 'test.events', offset: '10' }),
    );
    expect(handlerSpy).not.toHaveBeenCalled();
    warnSpy.mockRestore();
  });

  it('catches poison pill, logs structured error, and safely isolates without rethrowing', async () => {
    const errorSpy = vi.spyOn(logger, 'error').mockImplementation(() => {});
    const handlerSpy = vi.fn();

    await expect(
      processKafkaRecord({
        topic: 'test.events',
        partition: 1,
        offset: '25',
        rawValue: 'not-valid-json{{{',
        logger,
        deserializer: () => {
          throw new Error('Unexpected token');
        },
        handler: handlerSpy,
      }),
    ).resolves.not.toThrow();

    expect(errorSpy).toHaveBeenCalledWith(
      'Poison pill detected — unable to deserialize message',
      expect.any(Error),
      expect.objectContaining({
        topic: 'test.events',
        offset: '25',
      }),
    );
    expect(handlerSpy).not.toHaveBeenCalled();
    errorSpy.mockRestore();
  });

  it('successfully processes valid message and logs single unified summary', async () => {
    const infoSpy = vi.spyOn(logger, 'info').mockImplementation(() => {});
    const handlerSpy = vi.fn(async () => ({ points: 50, tier: 'GOLD' }));

    await processKafkaRecord({
      topic: 'test.events',
      partition: 2,
      offset: '100',
      key: 'user-1',
      rawValue: '{"eventId":"e1"}',
      logger,
      deserializer: (r) => JSON.parse(r) as { eventId: string },
      handler: handlerSpy,
    });

    expect(handlerSpy).toHaveBeenCalledWith({ eventId: 'e1' });
    expect(infoSpy).toHaveBeenCalledWith(
      'Kafka record processed successfully',
      expect.objectContaining({
        topic: 'test.events',
        partition: 2,
        offset: '100',
        key: 'user-1',
        points: 50,
        tier: 'GOLD',
      }),
    );
    infoSpy.mockRestore();
  });

  it('rethrows unexpected/transient errors so Kafka offset is NOT committed', async () => {
    const errorSpy = vi.spyOn(logger, 'error').mockImplementation(() => {});
    const handlerSpy = vi.fn(async () => {
      throw new Error('Database connection failed');
    });

    await expect(
      processKafkaRecord({
        topic: 'test.events',
        partition: 0,
        offset: '5',
        rawValue: '{"ok":true}',
        logger,
        deserializer: (r) => JSON.parse(r),
        handler: handlerSpy,
      }),
    ).rejects.toThrow('Database connection failed');

    expect(errorSpy).toHaveBeenCalledWith(
      'Unhandled failure during Kafka message processing',
      expect.any(Error),
      expect.objectContaining({ topic: 'test.events', offset: '5' }),
    );
    errorSpy.mockRestore();
  });
});
