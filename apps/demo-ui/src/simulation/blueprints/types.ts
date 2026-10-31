import type { SimCustomNode, SimCustomEdge } from '../domain/types';

export interface SystemBlueprint {
  id: string;
  name: string;
  tagline: string;
  badge: string;
  badgeColor: { bg: string; text: string; border: string };
  icon: string;
  complexity: 'Beginner' | 'Intermediate' | 'Advanced' | 'Enterprise';
  nodeCount: number;
  edgeCount: number;
  description: string;
  highlights: string[];
  nodes: SimCustomNode[];
  edges: SimCustomEdge[];
}
