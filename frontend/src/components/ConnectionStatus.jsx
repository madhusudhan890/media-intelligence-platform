import React from 'react';

const STATE_CONFIG = {
  'disconnected': { label: 'Disconnected', color: 'var(--status-off)', pulse: false },
  'acquiring-media': { label: 'Starting...', color: 'var(--status-warning)', pulse: true },
  'connecting': { label: 'Connecting...', color: 'var(--status-warning)', pulse: true },
  'waiting': { label: 'Waiting for peer', color: 'var(--status-info)', pulse: true },
  'connected': { label: 'Connected', color: 'var(--status-on)', pulse: false },
  'reconnecting': { label: 'Reconnecting...', color: 'var(--status-warning)', pulse: true },
  'failed': { label: 'Failed', color: 'var(--status-error)', pulse: false }
};

function ConnectionStatus({ state }) {
  const config = STATE_CONFIG[state] || STATE_CONFIG['disconnected'];

  return (
    <div className="connection-status" title={config.label}>
      <div
        className={`status-indicator ${config.pulse ? 'status-pulse' : ''}`}
        style={{ backgroundColor: config.color }}
      ></div>
      <span className="status-label">{config.label}</span>
    </div>
  );
}

export default ConnectionStatus;
