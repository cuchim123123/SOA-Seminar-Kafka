import type { SimCustomNode } from '../../domain/types';
import type { SimEngine } from '../../engine/sim-engine';

export const GROUP_BOX_PREFIX = 'group-box-';
export const GROUP_BOX_NODE_TYPE = 'consumerGroupContainerNode';

const FALLBACK_WIDTH = 240;
const FALLBACK_HEIGHT = 180;
const MIN_GROUP_WIDTH = 340;
const PAD_X = 28;
const PAD_TOP = 110; // generous room for stacked container header and badges
const PAD_BOTTOM = 36;

/**
 * Derives one non-interactive wrapper rectangle per consumer group from the positions of its
 * consumer nodes. These nodes are view-only: they never enter the simulation store or engine.
 */
export function buildGroupContainerNodes(
  nodes: SimCustomNode[],
  engine: SimEngine,
): SimCustomNode[] {
  const byGroup = new Map<string, SimCustomNode[]>();
  for (const node of nodes) {
    if (node.data.kind !== 'consumer') continue;
    const groupId = (node.data.groupId as string) || 'loyalty-v1';
    byGroup.set(groupId, [...(byGroup.get(groupId) ?? []), node]);
  }

  return Array.from(byGroup.entries()).map(([groupId, members]) => {
    const rects = members.map((n) => ({
      x: n.position.x,
      y: n.position.y,
      w: n.measured?.width ?? n.width ?? FALLBACK_WIDTH,
      h: n.measured?.height ?? n.height ?? FALLBACK_HEIGHT,
    }));
    const minX = Math.min(...rects.map((r) => r.x));
    const minY = Math.min(...rects.map((r) => r.y));
    const maxX = Math.max(...rects.map((r) => r.x + r.w));
    const maxY = Math.max(...rects.map((r) => r.y + r.h));

    const group = engine.consumerGroups.get(groupId);
    const assigned = new Set<number>();
    let activeMembers = 0;
    group?.members.forEach((m) => {
      if (m.status !== 'CRASHED') activeMembers += 1;
      m.assignedPartitions.forEach((p) => assigned.add(p));
    });

    const rawWidth = maxX - minX + PAD_X * 2;
    const boxWidth = Math.max(MIN_GROUP_WIDTH, rawWidth);
    const boxHeight = maxY - minY + PAD_TOP + PAD_BOTTOM;
    // If we enlarged the box to satisfy MIN_GROUP_WIDTH, center it around the members
    const offsetX = (boxWidth - rawWidth) / 2;

    return {
      id: `${GROUP_BOX_PREFIX}${groupId}`,
      type: GROUP_BOX_NODE_TYPE,
      position: { x: minX - PAD_X - offsetX, y: minY - PAD_TOP },
      width: boxWidth,
      height: boxHeight,
      measured: { width: boxWidth, height: boxHeight },
      draggable: true,
      selectable: false,
      focusable: false,
      deletable: false,
      connectable: false,
      zIndex: -1,
      data: {
        id: `${GROUP_BOX_PREFIX}${groupId}`,
        kind: 'group-container',
        title: groupId,
        icon: '👥',
        status: 'ACTIVE',
        groupId,
        memberCount: activeMembers || members.length,
        assignedPartitions: Array.from(assigned).sort((a, b) => a - b),
        assignorStrategy: group?.assignorStrategy ?? 'range',
      },
    } as SimCustomNode;
  });
}
