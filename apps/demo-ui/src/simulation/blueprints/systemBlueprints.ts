import type { SystemBlueprint } from './types';
import { enterpriseProductionBlueprint } from './catalog/enterpriseProductionBlueprint';
import { minimalSystemBlueprint } from './catalog/minimalSystemBlueprint';
import { transactionalOutboxBlueprint } from './catalog/transactionalOutboxBlueprint';
import { competingConsumersBlueprint } from './catalog/competingConsumersBlueprint';
import { deadLetterQueueBlueprint } from './catalog/deadLetterQueueBlueprint';

export type { SystemBlueprint };

export const SYSTEM_BLUEPRINTS: SystemBlueprint[] = [
  enterpriseProductionBlueprint,
  minimalSystemBlueprint,
  transactionalOutboxBlueprint,
  competingConsumersBlueprint,
  deadLetterQueueBlueprint,
];
