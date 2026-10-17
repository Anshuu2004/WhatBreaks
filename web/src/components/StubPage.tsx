import React from "react";

interface StubPageProps {
  title: string;
  description: string;
}

export const StubPage: React.FC<StubPageProps> = ({ title, description }) => (
  <div className="stub-page">
    <div className="stub-page-title">{title}</div>
    <div style={{ fontSize: 12, color: "var(--text-dim)", maxWidth: 360, textAlign: "center" }}>
      {description}
    </div>
  </div>
);
