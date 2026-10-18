import React from "react";

export interface NavIconProps {
  size?: number;
  color?: string;
  className?: string;
  style?: React.CSSProperties;
}

/**
 * WhatBreaks Signature Topology Navicon
 * Represents the 3-node connected dependency tree with retro-terminal aesthetics.
 */
export const NavIcon: React.FC<NavIconProps> = ({
  size = 18,
  color = "currentColor",
  className = "",
  style,
}) => {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 36 36"
      fill={color}
      className={className}
      style={{ display: "block", flexShrink: 0, ...style }}
      aria-hidden="true"
    >
      {/* Top Left Node */}
      <path fillRule="evenodd" d="M0,0 H16 V16 H0 Z M4,4 H12 V12 H4 Z" />
      {/* Top Right Node */}
      <path fillRule="evenodd" d="M20,0 H36 V16 H20 Z M24,4 H32 V12 H24 Z" />
      {/* Connector: Stems and Crossbar */}
      <path d="M6,16 H10 V18 H26 V16 H30 V22 H6 Z" />
      {/* Bottom Node */}
      <path fillRule="evenodd" d="M10,20 H26 V36 H10 Z M14,24 H22 V32 H14 Z" />
    </svg>
  );
};
