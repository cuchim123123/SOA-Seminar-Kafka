import { Global, Module } from '@nestjs/common';
import { AppConfigService } from './app-config.service';
import { validateEnv } from './env.validation';

export const RAW_ENV_TOKEN = Symbol('RAW_ENV');

@Global()
@Module({
  providers: [
    {
      provide: RAW_ENV_TOKEN,
      useFactory: (): NodeJS.ProcessEnv => process.env,
    },
    {
      provide: AppConfigService,
      inject: [RAW_ENV_TOKEN],
      useFactory: (rawEnv: Record<string, unknown>): AppConfigService => {
        const validated = validateEnv(rawEnv);
        return new AppConfigService(validated);
      },
    },
  ],
  exports: [AppConfigService],
})
export class ConfigModule {}
