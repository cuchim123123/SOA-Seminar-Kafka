import React from 'react';
import { BaseEdge, getBezierPath, type EdgeProps } from '@xyflow/react';
import type { SimEdgeData } from '../../domain/types';
import { useEdgeFrame } from './SimulationClockContext';
import { SIMULATION_LIMITS } from '../../domain/limits';
import { tokenProgress } from '../../store/virtual-clock-store';

interface SimFlowEdgeProps extends EdgeProps {
  data?: SimEdgeData;
}

export const SimFlowEdge = React.memo(function SimFlowEdge({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  style = {},
  markerEnd,
  data,
}: SimFlowEdgeProps) {
  const [edgePath, labelX, labelY] = getBezierPath({
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
  });

  const { tokens, now } = useEdgeFrame(id);
  const active = tokens
    .map((token) => ({ token, progress: tokenProgress(token.startTime, token.arriveTime, now) }))
    .filter((item) => item.progress !== null);
  const visible = active.slice(0, SIMULATION_LIMITS.particlesPerWire);
  const portType = data?.portType || 'publish';

  const strokeColor =
    portType === 'publish' ? '#38bdf8' : portType === 'dispatch' ? '#fbbf24' : '#10b981';

  return (
    <>
      <BaseEdge
        path={edgePath}
        markerEnd={markerEnd}
        style={{
          ...style,
          stroke: strokeColor,
          strokeWidth: tokens.length > 0 ? 2.5 : 1.5,
          strokeDasharray: tokens.length > 0 ? '4 4' : undefined,
          transition: 'stroke 0.2s ease',
          opacity: 0.85,
        }}
      />

      {/* No independent SVG timer: pause, speed changes and Step share virtual time. */}
      {visible.map(({ token, progress }) => (
        <circle
          className="simulation-token"
          key={token.id}
          r={5}
          fill={token.color || strokeColor}
          style={{
            offsetPath: `path('${edgePath}')`,
            offsetDistance: `${Number(progress) * 100}%`,
          }}
        >
          <title>
            {token.messageKey} · {token.messageId}
          </title>
        </circle>
      ))}
      {active.length > visible.length && (
        <text x={labelX} y={labelY - 10} textAnchor="middle" fill={strokeColor} fontSize={12}>
          +{active.length - visible.length} records
        </text>
      )}
    </>
  );
});
