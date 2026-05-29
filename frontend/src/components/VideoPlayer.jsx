import React, { useRef, useEffect } from 'react';

function VideoPlayer({ stream, muted, label, isRemote }) {
  const videoRef = useRef(null);

  useEffect(() => {
    const videoElement = videoRef.current;
    if (!videoElement || !stream) return;

    videoElement.srcObject = stream;

    const handleCanPlay = () => {
      videoElement.play().catch((err) => {
        console.error(`[VideoPlayer] Autoplay failed for ${label}:`, err);
      });
    };

    videoElement.addEventListener('canplay', handleCanPlay);

    return () => {
      videoElement.removeEventListener('canplay', handleCanPlay);
      videoElement.srcObject = null;
    };
  }, [stream, label]);

  return (
    <div className={`video-player ${isRemote ? 'video-player-remote' : 'video-player-local'}`}>
      <video
        ref={videoRef}
        autoPlay
        playsInline
        muted={muted}
        className="video-element"
      />
      <div className="video-label">
        <div className="video-label-dot"></div>
        <span>{label}</span>
      </div>
    </div>
  );
}

export default VideoPlayer;
