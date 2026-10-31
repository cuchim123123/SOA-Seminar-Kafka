import React from 'react';
import {
  BaseEdge,
  getBezierPath,
  type EdgeProps,
} from '@xyflow/react';
import type { SimEdgeData, SimInFlightToken } from '../../domain/types';

interface SimFlowEdgeProps extends EdgeProps {
  data?: SimEdgeData;
}

export const SimFlowEdge: React.FC<SimFlowEdgeProps> = ({
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  style = {},
  markerEnd,
  data,
}) => {
  const [edgePath] = getBezierPath({
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
  });

  const tokens = data?.inFlightTokens || [];
  const portType = data?.portType || 'publish';

  const strokeColor =
    portType === 'publish'
      ? '#38bdf8'
      : portType === 'dispatch'
        ? '#fbbf24'
        : '#10b981';

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

      {/* Render in-flight message tokens traveling along this edge */}
      {tokens.map((token: SimInFlightToken) => (
        <circle
          key={token.id}
          r={5}
          fill={token.color || strokeColor}
          filter="drop-shadow(0 0 4px rgba(255, 255, 255, 0.8))"
        >
          <animateMotion
            path={edgePath}
            dur={`${Math.max(0.1, (token.arriveTime - token.startTime) / 1000)}s`}
            repeatCount="1"
            fill="freeze"
          />
        </circle>
      ))}
    </>
  );
};
