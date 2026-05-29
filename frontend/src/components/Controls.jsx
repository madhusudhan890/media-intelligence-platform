import React from 'react';

function Controls({ isAudioEnabled, isVideoEnabled, onToggleAudio, onToggleVideo, onLeave, connectionState }) {
  return (
    <div className="controls-bar">
      <div className="controls-group">
        <button
          id="toggle-audio-btn"
          className={`control-btn ${!isAudioEnabled ? 'control-btn-off' : ''}`}
          onClick={onToggleAudio}
          title={isAudioEnabled ? 'Mute microphone' : 'Unmute microphone'}
          aria-label={isAudioEnabled ? 'Mute microphone' : 'Unmute microphone'}
        >
          {isAudioEnabled ? (
            <svg width="24" height="24" viewBox="0 0 24 24" fill="none">
              <rect x="9" y="2" width="6" height="12" rx="3" stroke="currentColor" strokeWidth="1.8" />
              <path d="M5 10V11C5 14.866 8.134 18 12 18C15.866 18 19 14.866 19 11V10" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
              <path d="M12 18V22M9 22H15" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
            </svg>
          ) : (
            <svg width="24" height="24" viewBox="0 0 24 24" fill="none">
              <rect x="9" y="2" width="6" height="12" rx="3" stroke="currentColor" strokeWidth="1.8" />
              <path d="M5 10V11C5 14.866 8.134 18 12 18C15.866 18 19 14.866 19 11V10" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
              <path d="M12 18V22M9 22H15" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
              <line x1="3" y1="3" x2="21" y2="21" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
            </svg>
          )}
          <span className="control-label">{isAudioEnabled ? 'Mute' : 'Unmute'}</span>
        </button>

        <button
          id="toggle-video-btn"
          className={`control-btn ${!isVideoEnabled ? 'control-btn-off' : ''}`}
          onClick={onToggleVideo}
          title={isVideoEnabled ? 'Turn off camera' : 'Turn on camera'}
          aria-label={isVideoEnabled ? 'Turn off camera' : 'Turn on camera'}
        >
          {isVideoEnabled ? (
            <svg width="24" height="24" viewBox="0 0 24 24" fill="none">
              <rect x="2" y="6" width="14" height="12" rx="2" stroke="currentColor" strokeWidth="1.8" />
              <path d="M16 9.5L22 6V18L16 14.5" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
            </svg>
          ) : (
            <svg width="24" height="24" viewBox="0 0 24 24" fill="none">
              <rect x="2" y="6" width="14" height="12" rx="2" stroke="currentColor" strokeWidth="1.8" />
              <path d="M16 9.5L22 6V18L16 14.5" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
              <line x1="2" y1="2" x2="22" y2="22" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
            </svg>
          )}
          <span className="control-label">{isVideoEnabled ? 'Video' : 'Video Off'}</span>
        </button>

        <button
          id="leave-btn"
          className="control-btn control-btn-leave"
          onClick={onLeave}
          title="Leave room"
          aria-label="Leave room"
        >
          <svg width="24" height="24" viewBox="0 0 24 24" fill="none">
            <path d="M9 21H5C4.448 21 4 20.552 4 20V4C4 3.448 4.448 3 5 3H9" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
            <path d="M16 17L21 12L16 7" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
            <path d="M21 12H9" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
          </svg>
          <span className="control-label">Leave</span>
        </button>
      </div>
    </div>
  );
}

export default Controls;
