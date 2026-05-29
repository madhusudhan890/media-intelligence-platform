import React, { useEffect, useCallback, useRef } from 'react';
import { useParams, useNavigate, useLocation } from 'react-router-dom';
import useWebRTC from '../hooks/useWebRTC';
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

  cleanupRef.current = cleanup;

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
