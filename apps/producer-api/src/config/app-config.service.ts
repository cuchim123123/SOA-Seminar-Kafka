import { Injectable } from '@nestjs/common';
import { EnvConfig } from './env.validation';

export interface IAppConfig {
  readonly nodeEnv: string;
  readonly port: number;
}

@Injectable()
export class AppConfigService implements IAppConfig {
  constructor(private readonly config: EnvConfig) {}

  get nodeEnv(): string {
    return this.config.NODE_ENV;
  }

  get port(): number {
    return this.config.PORT;
  }
}
