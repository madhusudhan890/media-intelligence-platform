import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';

const SIGNALING_HTTP = import.meta.env.VITE_SIGNALING_URL
  ? import.meta.env.VITE_SIGNALING_URL.replace('ws://', 'http://').replace('wss://', 'https://')
  : 'http://localhost:8080';

function Home() {
  const navigate = useNavigate();
  const [roomInput, setRoomInput] = useState('');
  const [isCreating, setIsCreating] = useState(false);
  const [error, setError] = useState('');

  const handleCreateRoom = async () => {
    setIsCreating(true);
    setError('');

    try {
      const response = await fetch(`${SIGNALING_HTTP}/rooms`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      });

      if (!response.ok) {
        throw new Error('Failed to create room');
      }

      const data = await response.json();
      navigate(`/room/${data.roomId}`);
    } catch (err) {
      console.error('[Home] Failed to create room:', err);
      setError('Failed to create room. Is the signaling server running?');
    } finally {
      setIsCreating(false);
    }
  };

  const handleJoinRoom = () => {
    setError('');
    const trimmed = roomInput.trim();
    if (!trimmed) {
      setError('Please enter a room ID');
      return;
    }
    navigate(`/room/${trimmed}`);
  };

  const handleKeyDown = (e) => {
    if (e.key === 'Enter') {
      handleJoinRoom();
    }
  };

  return (
    <div className="home-page">
      <div className="home-backdrop">
        <div className="orb orb-1"></div>
        <div className="orb orb-2"></div>
        <div className="orb orb-3"></div>
      </div>

      <div className="home-container">
        <div className="home-hero">
          <div className="logo-mark">
            <svg width="48" height="48" viewBox="0 0 48 48" fill="none">
              <rect x="4" y="4" width="40" height="40" rx="12" fill="url(#logo-grad)" />
              <path d="M16 20L24 14L32 20V32L24 38L16 32V20Z" fill="rgba(255,255,255,0.95)" />
              <circle cx="24" cy="26" r="4" fill="url(#logo-grad)" />
              <defs>
                <linearGradient id="logo-grad" x1="4" y1="4" x2="44" y2="44">
                  <stop stopColor="#6366f1" />
                  <stop offset="1" stopColor="#8b5cf6" />
                </linearGradient>
              </defs>
            </svg>
          </div>
          <h1 className="home-title">Media Intelligence</h1>
          <p className="home-subtitle">
            Real-time peer-to-peer video communication<br />
            powered by WebRTC
          </p>
        </div>

        <div className="home-card">
          <div className="card-section">
            <h2 className="card-section-title">Start a conversation</h2>
            <p className="card-section-desc">Create a new room and share the ID with your peer</p>
            <button
              id="create-room-btn"
              className="btn btn-primary"
              onClick={handleCreateRoom}
              disabled={isCreating}
            >
              {isCreating ? (
                <span className="btn-loading">
                  <span className="spinner"></span>
                  Creating...
                </span>
              ) : (
                <>
                  <svg width="20" height="20" viewBox="0 0 20 20" fill="none" className="btn-icon">
                    <path d="M10 4V16M4 10H16" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
                  </svg>
                  Create Room
                </>
              )}
            </button>
          </div>

          <div className="card-divider">
            <span className="divider-text">or</span>
          </div>

          <div className="card-section">
            <h2 className="card-section-title">Join existing room</h2>
            <p className="card-section-desc">Enter a room ID to join an active session</p>
            <div className="input-group">
              <input
                id="room-id-input"
                type="text"
                className="input"
                placeholder="Paste room ID here..."
                value={roomInput}
                onChange={(e) => setRoomInput(e.target.value)}
                onKeyDown={handleKeyDown}
              />
              <button
                id="join-room-btn"
                className="btn btn-secondary"
                onClick={handleJoinRoom}
              >
                <svg width="20" height="20" viewBox="0 0 20 20" fill="none" className="btn-icon">
                  <path d="M3 10H14M10 6L14 10L10 14" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
                </svg>
                Join
              </button>
            </div>
          </div>

          {error && (
            <div className="error-banner" role="alert">
              <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
                <circle cx="8" cy="8" r="7" stroke="currentColor" strokeWidth="1.5" />
                <path d="M8 5V9M8 11V11.5" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
              </svg>
              {error}
            </div>
          )}
        </div>

        <div className="home-footer">
          <p>Phase 1 — P2P Communication Foundation</p>
        </div>
      </div>
    </div>
  );
}

export default Home;
