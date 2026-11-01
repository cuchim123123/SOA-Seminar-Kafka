import { DynamicModule, Module } from '@nestjs/common';
import { APP_INTERCEPTOR } from '@nestjs/core';
import { HttpMetricsInterceptor } from './http-metrics.interceptor';
import { METRICS_OPTIONS, MetricsModuleOptions } from './metrics.constants';
import { MetricsController } from './metrics.controller';
import { MetricsService } from './metrics.service';

@Module({})
export class MetricsModule {
  static forRoot(options: MetricsModuleOptions): DynamicModule {
    return {
      module: MetricsModule,
      global: true,
      controllers: [MetricsController],
      providers: [
        {
          provide: METRICS_OPTIONS,
          useValue: options,
        },
        MetricsService,
        {
          provide: APP_INTERCEPTOR,
          useClass: HttpMetricsInterceptor,
        },
      ],
      exports: [MetricsService],
    };
  }
}
