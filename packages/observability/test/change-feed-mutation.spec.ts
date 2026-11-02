import { ExecutionContext } from '@nestjs/common';
import { firstValueFrom, of, throwError } from 'rxjs';
import { describe, expect, it, vi } from 'vitest';
import { ChangeFeedMutationInterceptor } from '../src/live/change-feed-mutation.interceptor';
import { PostgresChangeFeed } from '../src/live/postgres-change-feed';

describe('application control notifications', () => {
  it('invalidates successful mutations only, avoiding GET feedback loops', async () => {
    const invalidate = vi.fn();
    const interceptor = new ChangeFeedMutationInterceptor({
      invalidate,
    } as unknown as PostgresChangeFeed);
    const context = (method: string) =>
      ({ switchToHttp: () => ({ getRequest: () => ({ method }) }) }) as unknown as ExecutionContext;
    await firstValueFrom(interceptor.intercept(context('GET'), { handle: () => of({}) }));
    expect(invalidate).not.toHaveBeenCalled();
    await firstValueFrom(interceptor.intercept(context('POST'), { handle: () => of({}) }));
    expect(invalidate).toHaveBeenCalledOnce();
    await expect(
      firstValueFrom(
        interceptor.intercept(context('POST'), {
          handle: () => throwError(() => new Error('failed')),
        }),
      ),
    ).rejects.toThrow('failed');
    expect(invalidate).toHaveBeenCalledOnce();
  });
});
