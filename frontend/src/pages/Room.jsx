import React, { useEffect, useCallback, useRef } from 'react';
import { useParams, useNavigate, useLocation } from 'react-router-dom';
import useWebRTC from '../hooks/useWebRTC';
import useSSE from '../hooks/useSSE';
import VideoPlayer from '../components/VideoPlayer';
import Controls from '../components/Controls';
import ConnectionStatus from '../components/ConnectionStatus';

function Room() {
  const { roomId } = useParams();
  const navigate = useNavigate();
  const location = useLocation();
  const cleanupRef = useRef(null);
  const {
    localStream,
    remoteStream,
    connectionState,
    isAudioEnabled,
    isVideoEnabled,
    error,
    init,
    cleanup,
    toggleAudio,
    toggleVideo
  } = useWebRTC(roomId);

  const {
    transcripts,
    insights,
    error: sseError
  } = useSSE(roomId);

  const transcriptsEndRef = useRef(null);

  cleanupRef.current = cleanup;

  useEffect(() => {
    if (transcriptsEndRef.current) {
      transcriptsEndRef.current.scrollIntoView({ behavior: 'smooth' });
    }
  }, [transcripts]);

  useEffect(() => {
    // Prevent joining on refresh or direct URL access
    if (!location.state?.fromHome) {
      navigate('/', { replace: true });
      return;
    }

    console.log(`[${new Date().toISOString()}] [Room] Mounting — calling init()`);
    init();
    return () => {
      console.log(`[${new Date().toISOString()}] [Room] Unmounting — calling cleanup()`);
      if (cleanupRef.current) {
        cleanupRef.current();
      }
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const handleLeave = useCallback(() => {
    console.log(`[${new Date().toISOString()}] [Room] User clicked Leave`);
    cleanup();
    navigate('/');
  }, [cleanup, navigate]);

  const handleCopyRoomId = useCallback(async () => {
    try {
      await navigator.clipboard.writeText(roomId);
    } catch (err) {
      console.error('[Room] Failed to copy room ID:', err);
    }
  }, [roomId]);

  return (
    <div className="room-page">
      <header className="room-header">
        <div className="room-header-left">
          <div className="header-logo">
            <svg width="28" height="28" viewBox="0 0 48 48" fill="none">
              <rect x="4" y="4" width="40" height="40" rx="12" fill="url(#hdr-grad)" />
              <path d="M16 20L24 14L32 20V32L24 38L16 32V20Z" fill="rgba(255,255,255,0.95)" />
              <circle cx="24" cy="26" r="4" fill="url(#hdr-grad)" />
              <defs>
                <linearGradient id="hdr-grad" x1="4" y1="4" x2="44" y2="44">
                  <stop stopColor="#6366f1" />
                  <stop offset="1" stopColor="#8b5cf6" />
                </linearGradient>
              </defs>
            </svg>
          </div>
          <div className="room-info">
            <button className="room-id-badge" onClick={handleCopyRoomId} title="Click to copy room ID">
              <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
                <rect x="4" y="4" width="8" height="8" rx="1.5" stroke="currentColor" strokeWidth="1.2" />
                <path d="M10 2H3.5A1.5 1.5 0 002 3.5V10" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round" />
              </svg>
              <span className="room-id-text">{roomId.slice(0, 8)}...</span>
            </button>
          </div>
        </div>
        <ConnectionStatus state={connectionState} />
      </header>

      <main className="room-main">
        {error && (
          <div className="room-error" role="alert">
            <svg width="20" height="20" viewBox="0 0 20 20" fill="none">
              <circle cx="10" cy="10" r="9" stroke="currentColor" strokeWidth="1.5" />
              <path d="M10 6V11M10 14V14.5" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
            </svg>
            <span>{error}</span>
          </div>
        )}

        {sseError && (
          <div className="room-warning" role="alert">
            <svg width="20" height="20" viewBox="0 0 20 20" fill="none">
              <circle cx="10" cy="10" r="9" stroke="currentColor" strokeWidth="1.5" />
              <path d="M10 6V11M10 14V14.5" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
            </svg>
            <span>{sseError}</span>
          </div>
        )}

        <div className="room-layout-grid">
          <div className="video-grid-container">
            <div className="video-grid">
              <div className="video-cell video-cell-remote">
                {remoteStream ? (
                  <VideoPlayer
                    stream={remoteStream}
                    muted={false}
                    label="Remote"
                    isRemote={true}
                  />
                ) : (
                  <div className="video-placeholder">
                    <div className="placeholder-content">
                      {connectionState === 'waiting' ? (
                        <>
                          <div className="pulse-ring">
                            <svg width="48" height="48" viewBox="0 0 48 48" fill="none">
                              <circle cx="24" cy="24" r="16" stroke="currentColor" strokeWidth="1.5" strokeDasharray="4 4" />
                              <circle cx="24" cy="20" r="6" stroke="currentColor" strokeWidth="1.5" />
                              <path d="M14 36C14 30.477 18.477 26 24 26C29.523 26 34 30.477 34 36" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
                            </svg>
                          </div>
                          <p className="placeholder-text">Waiting for peer to join...</p>
                          <p className="placeholder-hint">Share the room ID to connect</p>
                        </>
                      ) : connectionState === 'connecting' || connectionState === 'acquiring-media' ? (
                        <>
                          <div className="connecting-spinner"></div>
                          <p className="placeholder-text">Establishing connection...</p>
                        </>
                      ) : connectionState === 'reconnecting' ? (
                        <>
                          <div className="connecting-spinner"></div>
                          <p className="placeholder-text">Reconnecting...</p>
                        </>
                      ) : (
                        <>
                          <p className="placeholder-text">No remote stream</p>
                        </>
                      )}
                    </div>
                  </div>
                )}
              </div>

              <div className="video-cell video-cell-local">
                {localStream ? (
                  <VideoPlayer
                    stream={localStream}
                    muted={true}
                    label="You"
                    isRemote={false}
                  />
                ) : (
                  <div className="video-placeholder video-placeholder-small">
                    <div className="placeholder-content">
                      <div className="connecting-spinner small"></div>
                      <p className="placeholder-text">Starting camera...</p>
                    </div>
                  </div>
                )}
              </div>
            </div>
          </div>

          <div className="side-panels-container">
            {/* Live Transcript Panel */}
            <div className="side-panel live-transcript-panel">
              <div className="panel-header">
                <span className="pulse-indicator"></span>
                <h3>Live Transcript</h3>
              </div>
              <div className="panel-content transcripts-list">
                {transcripts.length === 0 ? (
                  <div className="panel-empty-state">
                    <p>Waiting for speech to transcribe...</p>
                  </div>
                ) : (
                  transcripts.map((t, idx) => (
                    <div key={t.chunkId || idx} className="transcript-item">
                      <div className="transcript-meta">
                        <span className="speaker-name">
                          {t.peerId === 'local' || t.peerId === 'You' ? 'You' : `Speaker ${t.peerId.slice(0, 8)}`}
                        </span>
                        <span className="transcript-time">
                          {new Date(t.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })}
                        </span>
                      </div>
                      <p className="transcript-text">{t.text}</p>
                    </div>
                  ))
                )}
                <div ref={transcriptsEndRef} />
              </div>
            </div>

            {/* AI Insights Panel */}
            <div className="side-panel ai-insights-panel">
              <div className="panel-header">
                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" style={{ marginRight: '6px' }}>
                  <path d="M12 2v20M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6" />
                </svg>
                <h3>Meeting Insights</h3>
              </div>
              <div className="panel-content insights-body">
                {!insights.summary && insights.topics.length === 0 ? (
                  <div className="panel-empty-state">
                    <p>Insights will generate once transcripts accumulate...</p>
                  </div>
                ) : (
                  <>
                    {insights.summary && (
                      <div className="insight-section">
                        <h4>Summary</h4>
                        <p className="summary-text">{insights.summary}</p>
                      </div>
                    )}

                    {insights.topics && insights.topics.length > 0 && (
                      <div className="insight-section">
                        <h4>Topics</h4>
                        <div className="topics-tags">
                          {insights.topics.map((topic, i) => (
                            <span key={i} className="topic-tag">{topic}</span>
                          ))}
                        </div>
                      </div>
                    )}

                    {insights.decisions && insights.decisions.length > 0 && (
                      <div className="insight-section">
                        <h4>Decisions</h4>
                        <ul className="insights-list-bullets">
                          {insights.decisions.map((d, i) => (
                            <li key={i}>{d.text}</li>
                          ))}
                        </ul>
                      </div>
                    )}

                    {insights.action_items && insights.action_items.length > 0 && (
                      <div className="insight-section">
                        <h4>Action Items</h4>
                        <ul className="insights-action-list">
                          {insights.action_items.map((item, i) => (
                            <li key={i} className="action-item-li">
                              <span className="action-owner">{item.owner}</span>
                              <span className="action-task">{item.task}</span>
                            </li>
                          ))}
                        </ul>
                      </div>
                    )}

                    {insights.deadlines && insights.deadlines.length > 0 && (
                      <div className="insight-section">
                        <h4>Deadlines</h4>
                        <ul className="insights-list-bullets">
                          {insights.deadlines.map((dl, i) => (
                            <li key={i}>{dl.text}</li>
                          ))}
                        </ul>
                      </div>
                    )}

                    {insights.risks && insights.risks.length > 0 && (
                      <div className="insight-section">
                        <h4>Risks</h4>
                        <ul className="insights-risks-list">
                          {insights.risks.map((risk, i) => (
                            <li key={i} className="risk-item-li">{risk.text}</li>
                          ))}
                        </ul>
                      </div>
                    )}
                  </>
                )}
              </div>
            </div>
          </div>
        </div>
      </main>

      <Controls
        isAudioEnabled={isAudioEnabled}
        isVideoEnabled={isVideoEnabled}
        onToggleAudio={toggleAudio}
        onToggleVideo={toggleVideo}
        onLeave={handleLeave}
        connectionState={connectionState}
      />
    </div>
  );
}

export default Room;
