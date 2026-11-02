import { CallHandler, ExecutionContext, Inject, Injectable, NestInterceptor } from '@nestjs/common';
import { tap } from 'rxjs';
import { PostgresChangeFeed } from './postgres-change-feed';

/** Attach only to application control controllers, never query controllers. */
@Injectable()
export class ChangeFeedMutationInterceptor implements NestInterceptor {
  constructor(@Inject(PostgresChangeFeed) private readonly changes: PostgresChangeFeed) {}
  intercept(context: ExecutionContext, next: CallHandler) {
    const mutation = context.switchToHttp().getRequest<{ method: string }>().method === 'POST';
    return next.handle().pipe(
      tap(() => {
        if (mutation) this.changes.invalidate();
      }),
    );
  }
}
