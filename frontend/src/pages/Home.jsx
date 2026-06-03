import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';

const MEDIA_SERVER_URL = import.meta.env.VITE_MEDIA_SERVER_URL || 'http://localhost:8080';

function Home() {
  const navigate = useNavigate();
  const [name, setName] = useState('');
  const [roomInput, setRoomInput] = useState('');
  const [isCreating, setIsCreating] = useState(false);
  const [isJoining, setIsJoining] = useState(false);
  const [error, setError] = useState('');

  const handleCreateRoom = async () => {
    const trimmedName = name.trim();
    if (!trimmedName) {
      setError('Please enter your name first');
      return;
    }

    setIsCreating(true);
    setError('');

    try {
      const response = await fetch(`${MEDIA_SERVER_URL}/rooms`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      });

      if (!response.ok) {
        throw new Error('Failed to create room');
      }

      const data = await response.json();
      navigate(`/room/${data.roomId}`, { state: { fromHome: true, userName: trimmedName } });
    } catch (err) {
      console.error('[Home] Failed to create room:', err);
      setError('Failed to create room. Is the signaling server running?');
    } finally {
      setIsCreating(false);
    }
  };

  const handleJoinRoom = async () => {
    setError('');
    const trimmedName = name.trim();
    if (!trimmedName) {
      setError('Please enter your name first');
      return;
    }
    const trimmed = roomInput.trim();
    if (!trimmed) {
      setError('Please enter a room ID');
      return;
    }

    setIsJoining(true);
    try {
      const response = await fetch(`${MEDIA_SERVER_URL}/rooms/${trimmed}`);
      if (response.status === 404) {
        setError('Room not found. Please verify the room ID or create a new one.');
        return;
      }
      if (!response.ok) {
        throw new Error('Failed to verify room');
      }
      navigate(`/room/${trimmed}`, { state: { fromHome: true, userName: trimmedName } });
    } catch (err) {
      console.error('[Home] Failed to verify room:', err);
      setError('Failed to join room. Is the signaling server running?');
    } finally {
      setIsJoining(false);
    }
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
          <h1 className="home-title">OMNISIGHT</h1>
          <p className="home-subtitle">
            AI-Driven P2P Media Intelligence Platform
          </p>
        </div>

        <div className="home-card">
          <div className="card-section">
            <h2 className="card-section-title">Identity Registration</h2>
            <input
              id="user-name-input"
              type="text"
              className="input"
              placeholder="e.g. John Doe"
              value={name}
              onChange={(e) => setName(e.target.value)}
            />
          </div>

          <div className="card-divider">
            <span className="divider-text">Orchestration Nodes</span>
          </div>

          <div className="card-section">
            <h2 className="card-section-title">Spawn Cognitive Session</h2>
            <p className="card-section-desc">Initialize an isolated session room with dedicated event pipelines, Kafka routing, and low-latency P2P loops.</p>
            <button
              id="create-room-btn"
              className="btn btn-primary"
              onClick={handleCreateRoom}
              disabled={isCreating}
            >
              {isCreating ? (
                <span className="btn-loading">
                  <span className="spinner"></span>
                  Allocating Node Resources...
                </span>
              ) : (
                <>
                  <svg width="20" height="20" viewBox="0 0 20 20" fill="none" className="btn-icon">
                    <path d="M10 4V16M4 10H16" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
                  </svg>
                  Initialize Room Node
                </>
              )}
            </button>
          </div>

          <div className="card-divider">
            <span className="divider-text">or</span>
          </div>

          <div className="card-section">
            <h2 className="card-section-title">Attach to Active Registry</h2>
            <p className="card-section-desc">Establish direct P2P connection to a running signaling registry by providing its cryptographic Room ID token.</p>
            <div className="input-group">
              <input
                id="room-id-input"
                type="text"
                className="input"
                placeholder="Enter Room UUID..."
                value={roomInput}
                onChange={(e) => setRoomInput(e.target.value)}
                onKeyDown={handleKeyDown}
              />
              <button
                id="join-room-btn"
                className="btn btn-secondary"
                onClick={handleJoinRoom}
                disabled={isJoining}
              >
                {isJoining ? (
                  <span className="btn-loading">
                    <span className="spinner"></span>
                    Negotiating...
                  </span>
                ) : (
                  <>
                    <svg width="20" height="20" viewBox="0 0 20 20" fill="none" className="btn-icon">
                      <path d="M3 10H14M10 6L14 10L10 14" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
                    </svg>
                    Establish Link
                  </>
                )}
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

          <div className="card-divider">
            <span className="divider-text">Platform Capabilities</span>
          </div>

          <div className="capabilities-bullets">
            <div className="bullet-item">
              <span className="bullet-symbol">*</span>
              <span className="bullet-text">Decentralized communication network integrating real-time WebRTC media streams</span>
            </div>
            <div className="bullet-item">
              <span className="bullet-symbol">*</span>
              <span className="bullet-text">Automated cognitive processing with diarized transcript logs</span>
            </div>
            <div className="bullet-item">
              <span className="bullet-symbol">*</span>
              <span className="bullet-text">AI-driven semantic synthesis and intelligent summaries</span>
            </div>
          </div>

          <div className="card-divider">
            <span className="divider-text">Topology & Telemetry Spec</span>
          </div>

          <div className="specs-grid">
            <div className="spec-item">
              <span className="spec-status green"></span>
              <div className="spec-details">
                <span className="spec-label">Transport Protocol</span>
                <span className="spec-val">Secure WebRTC P2P Mesh</span>
              </div>
            </div>
            <div className="spec-item">
              <span className="spec-status purple"></span>
              <div className="spec-details">
                <span className="spec-label">Data Ingest Pipeline</span>
                <span className="spec-val">Distributed Kafka Event Broker</span>
              </div>
            </div>
            <div className="spec-item">
              <span className="spec-status blue"></span>
              <div className="spec-details">
                <span className="spec-label">Acoustic Pipeline</span>
                <span className="spec-val">Diarized Whisper Inference</span>
              </div>
            </div>
            <div className="spec-item">
              <span className="spec-status gold"></span>
              <div className="spec-details">
                <span className="spec-label">Cognitive Layer</span>
                <span className="spec-val">AI-Driven LLM Semantic Engine</span>
              </div>
            </div>
          </div>
        </div>

        <div className="home-footer">
          <p>OMNISIGHT CORE NODE PROTOCOLS SECURED BY END-TO-END TELEMETRY</p>
        </div>
      </div>
    </div>
  );
}

export default Home;
