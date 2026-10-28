import { describe, expect, it } from 'vitest';
import { DomainError } from '../src/errors/domain.error';
import { PoisonPillError } from '../src/errors/poison-pill.error';
import { TransientError } from '../src/errors/transient.error';

describe('Error Hierarchy', () => {
  it('PoisonPillError retains details and code', () => {
    const error = new PoisonPillError('Malformed JSON', {
      topic: 'test.topic',
      partition: 0,
      offset: '42',
      rawValuePreview: 'bad json',
    });

    expect(error.code).toBe('POISON_PILL');
    expect(error.isOperational).toBe(true);
    expect(error.message).toBe('Malformed JSON');
    expect(error.details?.topic).toBe('test.topic');
    expect(error.details?.offset).toBe('42');
  });

  it('TransientError marks retriable failure', () => {
    const error = new TransientError('Broker disconnected', { broker: 'localhost:9092' });
    expect(error.code).toBe('TRANSIENT_ERROR');
    expect(error.isOperational).toBe(true);
    expect(error.details?.broker).toBe('localhost:9092');
  });

  it('DomainError marks business rule violation', () => {
    const error = new DomainError('Negative purchase amount', { amountCents: -50 });
    expect(error.code).toBe('DOMAIN_RULE_VIOLATION');
    expect(error.isOperational).toBe(true);
    expect(error.details?.amountCents).toBe(-50);
  });
});
