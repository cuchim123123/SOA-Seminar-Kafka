import { Inject, Injectable } from '@nestjs/common';
import { StructuredLogger } from '@lab/observability';
import { Consumer, Kafka } from 'kafkajs';
import { AppConfigService } from '../../../config/app-config.service';
import {
  BoundedReplayScannerPort,
  BoundedScanOptions,
  BoundedScanResult,
} from '../../application/ports/bounded-replay-scanner.port';

@Injectable()
export class KafkaJsBoundedReplayScannerAdapter implements BoundedReplayScannerPort {
  private readonly logger: StructuredLogger;
  private readonly kafka: Kafka;

  constructor(@Inject(AppConfigService) private readonly config: AppConfigService) {
    this.logger = new StructuredLogger(KafkaJsBoundedReplayScannerAdapter.name, {
      serviceName: 'replay-worker',
      format: this.config.nodeEnv === 'production' ? 'json' : 'pretty',
    });
    this.kafka = new Kafka({
      clientId: `${this.config.kafkaClientId}-bounded-scanner`,
      brokers: this.config.kafkaBrokers,
    });
  }

  async scanBoundedTopic(options: BoundedScanOptions): Promise<BoundedScanResult> {
    const consumer: Consumer = this.kafka.consumer({
      groupId: options.groupId,
      allowAutoTopicCreation: false,
      maxWaitTimeInMs: 100,
    });

    this.logger.info('Starting bounded replay scan with on-demand group', {
      groupId: options.groupId,
      topic: options.topic,
      highWatermarks: options.highWatermarks,
    });

    const targetPartitions = Object.keys(options.highWatermarks).map(Number);
    const partitionDone = new Set<number>();
    let scannedCount = 0;

    // Check for partitions that already have 0 records between start and high watermark
    for (const p of targetPartitions) {
      const start = BigInt(options.startOffsets[p] ?? '0');
      const hw = BigInt(options.highWatermarks[p] ?? '0');
      if (start >= hw) {
        partitionDone.add(p);
      }
    }

    if (partitionDone.size >= targetPartitions.length) {
      this.logger.info('All partitions have 0 records to scan; fixed boundary already reached', {
        groupId: options.groupId,
      });
      return {
        scannedCount: 0,
        finalCommittedOffsets: options.highWatermarks,
      };
    }

    await consumer.connect();
    await consumer.subscribe({ topic: options.topic, fromBeginning: true });

    return new Promise<BoundedScanResult>((resolve, reject) => {
      let isCompleted = false;
      let idleTimeout: NodeJS.Timeout | null = null;

      const finish = async () => {
        if (isCompleted) return;
        isCompleted = true;
        if (idleTimeout) clearTimeout(idleTimeout);

        try {
          this.logger.info('Committing fixed boundary offsets for replay group...', {
            groupId: options.groupId,
            offsets: options.highWatermarks,
          });

          try {
            await consumer.commitOffsets(
              targetPartitions.map((partition) => ({
                topic: options.topic,
                partition,
                offset: options.highWatermarks[partition],
              })),
            );
          } catch (commitErr) {
            this.logger.warn(
              'consumer.commitOffsets encountered an error, falling back to admin.setOffsets',
              {
                error: commitErr instanceof Error ? commitErr.message : String(commitErr),
              },
            );
          }

          await consumer.disconnect().catch(() => {});

          const admin = this.kafka.admin();
          await admin.connect();
          try {
            await admin.setOffsets({
              groupId: options.groupId,
              topic: options.topic,
              partitions: targetPartitions.map((partition) => ({
                partition,
                offset: options.highWatermarks[partition],
              })),
            });
          } catch (adminErr) {
            this.logger.warn('admin.setOffsets fallback note', {
              error: adminErr instanceof Error ? adminErr.message : String(adminErr),
            });
          } finally {
            await admin.disconnect().catch(() => {});
          }

          this.logger.info('Bounded replay scan finished successfully', {
            groupId: options.groupId,
            scannedCount,
          });

          resolve({
            scannedCount,
            finalCommittedOffsets: options.highWatermarks,
          });
        } catch (err) {
          reject(err);
        }
      };

      const resetIdleTimeout = () => {
        if (idleTimeout) clearTimeout(idleTimeout);
        // If no records received for 4 seconds and at least one record was processed or all partitions checked
        idleTimeout = setTimeout(() => {
          this.logger.info('Idle timeout reached during bounded scan; finalizing run', {
            groupId: options.groupId,
            scannedCount,
            donePartitions: Array.from(partitionDone),
          });
          void finish();
        }, 4000);
      };

      resetIdleTimeout();

      consumer
        .run({
          autoCommit: false,
          eachMessage: async ({ topic, partition, message }) => {
            resetIdleTimeout();
            const offset = BigInt(message.offset);
            const hw = BigInt(options.highWatermarks[partition] ?? '0');

            if (offset < hw) {
              scannedCount += 1;
              if (options.onRecord) {
                let parsedValue: unknown = null;
                if (message.value) {
                  try {
                    parsedValue = JSON.parse(message.value.toString());
                  } catch (parseError) {
                    this.logger.warn('Skipping malformed message during replay scan', {
                      topic,
                      partition,
                      offset: message.offset,
                      error: parseError instanceof Error ? parseError.message : String(parseError),
                    });
                  }
                }

                await options.onRecord({
                  topic,
                  partition,
                  offset: message.offset,
                  key: message.key ? message.key.toString() : null,
                  timestamp: message.timestamp,
                  value: parsedValue,
                });
              }

              if (offset + 1n >= hw) {
                partitionDone.add(partition);
              }
            } else {
              partitionDone.add(partition);
            }

            if (targetPartitions.every((p) => partitionDone.has(p))) {
              this.logger.info('All partitions reached fixed log-end high-watermarks', {
                groupId: options.groupId,
              });
              void finish();
            }
          },
        })
        .catch((err) => {
          if (!isCompleted) {
            isCompleted = true;
            if (idleTimeout) clearTimeout(idleTimeout);
            void consumer.disconnect().catch(() => {
              /* ignore */
            });
            reject(err);
          }
        });
    });
  }
}
