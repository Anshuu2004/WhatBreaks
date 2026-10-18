import { Component } from "react";
import type { ErrorInfo, ReactNode } from "react";
import { AlertTriangle, RefreshCw, Terminal } from "lucide-react";

interface Props {
  children: ReactNode;
  fallbackTitle?: string;
  onReset?: () => void;
}

interface State {
  hasError: boolean;
  error: Error | null;
  errorInfo: ErrorInfo | null;
}

export class ErrorBoundary extends Component<Props, State> {
  constructor(props: Props) {
    super(props);
    this.state = {
      hasError: false,
      error: null,
      errorInfo: null,
    };
  }

  static getDerivedStateFromError(error: Error): State {
    return {
      hasError: true,
      error,
      errorInfo: null,
    };
  }

  componentDidCatch(error: Error, errorInfo: ErrorInfo): void {
    console.error("ErrorBoundary caught an exception:", error, errorInfo);
    this.setState({ errorInfo });
  }

  handleReset = (): void => {
    this.setState({ hasError: false, error: null, errorInfo: null });
    if (this.props.onReset) {
      this.props.onReset();
    }
  };

  render(): ReactNode {
    if (this.state.hasError) {
      return (
        <div
          className="error-boundary-panel"
          style={{
            padding: "24px",
            background: "var(--bg-panel)",
            border: "1px solid var(--status-error)",
            borderRadius: 2,
            margin: "16px 0",
            fontFamily: "var(--font-mono)",
            color: "var(--text-primary)",
          }}
        >
          <div
            style={{
              display: "flex",
              alignItems: "center",
              justifyContent: "space-between",
              marginBottom: 16,
              borderBottom: "1px solid var(--border)",
              paddingBottom: 12,
            }}
          >
            <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
              <AlertTriangle size={20} style={{ color: "var(--status-error)" }} />
              <div>
                <div style={{ fontSize: 11, color: "var(--status-error)", textTransform: "uppercase", letterSpacing: "0.05em" }}>
                  System Runtime Exception Prevented
                </div>
                <div style={{ fontSize: 14, fontWeight: 600 }}>
                  {this.props.fallbackTitle || "An unexpected error occurred while rendering this view"}
                </div>
              </div>
            </div>
            <button
              className="graph-ctrl-btn"
              onClick={this.handleReset}
              style={{ display: "flex", alignItems: "center", gap: 6, padding: "6px 12px" }}
              title="Reset component state"
            >
              <RefreshCw size={13} />
              Reset & Retry
            </button>
          </div>

          <div
            style={{
              background: "rgba(0, 0, 0, 0.4)",
              border: "1px solid var(--border)",
              padding: "12px 14px",
              borderRadius: 2,
              marginBottom: 16,
            }}
          >
            <div style={{ color: "var(--status-error)", fontSize: 13, marginBottom: 6, fontWeight: 500 }}>
              {this.state.error?.name}: {this.state.error?.message}
            </div>
            {this.state.errorInfo?.componentStack && (
              <pre
                style={{
                  margin: 0,
                  fontSize: 11,
                  color: "var(--text-dim)",
                  overflowX: "auto",
                  maxHeight: "180px",
                  lineHeight: 1.4,
                }}
              >
                {this.state.errorInfo.componentStack}
              </pre>
            )}
          </div>

          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", fontSize: 11, color: "var(--text-dim)" }}>
            <span style={{ display: "flex", alignItems: "center", gap: 6 }}>
              <Terminal size={13} /> Application state and navigation preserved
            </span>
            <button
              className="graph-ctrl-btn"
              onClick={this.handleReset}
              style={{ fontSize: 11, padding: "4px 10px" }}
            >
              Dismiss
            </button>
          </div>
        </div>
      );
    }

    return this.props.children;
  }
}
