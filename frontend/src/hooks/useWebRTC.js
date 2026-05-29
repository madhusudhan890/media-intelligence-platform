import { useState, useRef, useCallback, useEffect } from 'react';
import { v4 as uuidv4 } from 'uuid';

const SIGNALING_URL = import.meta.env.VITE_SIGNALING_URL || 'ws://localhost:8080/ws';
const MEDIA_SERVER_URL = import.meta.env.VITE_MEDIA_SERVER_URL || 'http://localhost:8080';
const ICE_SERVERS = [{ urls: 'stun:stun.l.google.com:19302' }];
const RECONNECT_DELAY = 2000;
const MAX_RECONNECT_ATTEMPTS = 5;

function log(category, action, data = {}) {
  const ts = new Date().toISOString();
  const dataStr = Object.keys(data).length > 0
    ? ' | ' + Object.entries(data).map(([k, v]) => `${k}=${typeof v === 'object' ? JSON.stringify(v) : v}`).join(' ')
    : '';
  console.log(`[${ts}] [WebRTC] [${category}] ${action}${dataStr}`);
}

function logSDP(direction, type, sdp) {
  const ts = new Date().toISOString();
  const lines = sdp.sdp ? sdp.sdp.split('\r\n').slice(0, 8).join(' | ') : 'no-sdp';
  console.log(`[${ts}] [WebRTC] [SDP] ${direction} ${type}`);
  console.log(`  SDP preview: ${lines}...`);
  console.log(`  Full SDP length: ${sdp.sdp ? sdp.sdp.length : 0} bytes`);
}

function logICE(direction, candidate) {
  const ts = new Date().toISOString();
  if (candidate) {
    console.log(`[${ts}] [WebRTC] [ICE] ${direction} | type=${candidate.type || 'unknown'} protocol=${candidate.protocol || 'unknown'} address=${candidate.address || 'hidden'} port=${candidate.port || 'unknown'} candidate=${candidate.candidate ? candidate.candidate.substring(0, 80) : 'null'}...`);
  }
}

export default function useWebRTC(roomId) {
  const [localStream, setLocalStream] = useState(null);
  const [remoteStream, setRemoteStream] = useState(null);
  const [connectionState, setConnectionState] = useState('disconnected');
  const [isAudioEnabled, setIsAudioEnabled] = useState(true);
  const [isVideoEnabled, setIsVideoEnabled] = useState(true);
  const [error, setError] = useState(null);

  const wsRef = useRef(null);
  const pcRef = useRef(null); // P2P PeerConnection
  const uplinkPcRef = useRef(null); // Uplink PeerConnection (Media Server)
  const localStreamRef = useRef(null);
  const peerIdRef = useRef(uuidv4());
  const roomIdRef = useRef(roomId);
  const remotePeerIdRef = useRef(null);
  const reconnectAttemptsRef = useRef(0);
  const reconnectTimerRef = useRef(null);
  const isCleaningUpRef = useRef(false);
  const pendingCandidatesRef = useRef([]);
  const hasRemoteDescRef = useRef(false);
  const makingOfferRef = useRef(false);
  const initGenRef = useRef(0);

  roomIdRef.current = roomId;

  const forceStopAllMedia = useCallback(() => {
    log('MEDIA', 'Force stopping all media tracks');
    if (localStreamRef.current) {
      const tracks = localStreamRef.current.getTracks();
      tracks.forEach((track) => {
        track.stop();
        log('MEDIA', `Track stopped: ${track.kind}`, { readyState: track.readyState, id: track.id });
      });
      localStreamRef.current = null;
    }
    setLocalStream(null);
  }, []);

  const cleanup = useCallback(() => {
    log('LIFECYCLE', 'Cleanup started', { peerId: peerIdRef.current, roomId: roomIdRef.current });
    isCleaningUpRef.current = true;
    initGenRef.current++;

    if (reconnectTimerRef.current) {
      clearTimeout(reconnectTimerRef.current);
      reconnectTimerRef.current = null;
      log('LIFECYCLE', 'Reconnect timer cleared');
    }

    if (pcRef.current) {
      log('RTC', 'Closing P2P PeerConnection');
      pcRef.current.ontrack = null;
      pcRef.current.onicecandidate = null;
      pcRef.current.oniceconnectionstatechange = null;
      pcRef.current.onconnectionstatechange = null;
      pcRef.current.onsignalingstatechange = null;
      pcRef.current.onnegotiationneeded = null;
      pcRef.current.close();
      pcRef.current = null;
    }

    if (uplinkPcRef.current) {
      log('UPLINK', 'Closing Uplink PeerConnection');
      uplinkPcRef.current.onicecandidate = null;
      uplinkPcRef.current.oniceconnectionstatechange = null;
      uplinkPcRef.current.onconnectionstatechange = null;
      uplinkPcRef.current.onsignalingstatechange = null;
      uplinkPcRef.current.close();
      uplinkPcRef.current = null;
    }

    forceStopAllMedia();

    if (wsRef.current) {
      log('SIGNALING', 'Closing WebSocket', { readyState: wsRef.current.readyState });
      wsRef.current.onopen = null;
      wsRef.current.onmessage = null;
      wsRef.current.onclose = null;
      wsRef.current.onerror = null;
      if (wsRef.current.readyState === WebSocket.OPEN ||
        wsRef.current.readyState === WebSocket.CONNECTING) {
        wsRef.current.close(1000, 'User left');
      }
      wsRef.current = null;
    }

    setRemoteStream(null);
    setConnectionState('disconnected');
    setError(null);
    remotePeerIdRef.current = null;
    pendingCandidatesRef.current = [];
    hasRemoteDescRef.current = false;
    makingOfferRef.current = false;
    log('LIFECYCLE', 'Cleanup completed');
  }, [forceStopAllMedia]);

  const sendSignal = useCallback((message) => {
    if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
      log('SIGNALING', `Sending: ${message.type}`, { to: message.targetPeerId || 'server', roomId: message.roomId });
      wsRef.current.send(JSON.stringify(message));
    } else {
      log('SIGNALING', `FAILED to send: ${message.type} (WebSocket not open)`, { readyState: wsRef.current?.readyState });
    }
  }, []);

  const connectUplink = useCallback(async (stream) => {
    if (uplinkPcRef.current) {
      uplinkPcRef.current.close();
    }

    log('UPLINK', 'Creating new Uplink PeerConnection');
    const pc = new RTCPeerConnection({ iceServers: ICE_SERVERS });
    uplinkPcRef.current = pc;

    // Add ONLY audio track for uplink
    const audioTrack = stream.getAudioTracks()[0];
    if (audioTrack) {
      pc.addTransceiver(audioTrack, { direction: 'sendonly' });
      log('UPLINK', 'Added audio track to uplink', { kind: audioTrack.kind, id: audioTrack.id.substring(0, 8) });
    } else {
      log('UPLINK', 'WARNING: No audio track available for uplink');
    }

    pc.oniceconnectionstatechange = () => {
      log('UPLINK', 'ICE connection state changed', { state: pc.iceConnectionState });
      if (pc.iceConnectionState === 'failed') {
        log('UPLINK', 'ICE FAILED — restarting ICE');
        pc.restartIce();
      }
    };

    pc.onconnectionstatechange = () => {
      log('UPLINK', 'Connection state changed', { state: pc.connectionState });
    };

    try {
      const offer = await pc.createOffer();
      await pc.setLocalDescription(offer);
      logSDP('CREATED_UPLINK', 'offer', offer);

      // Wait for ICE gathering to complete before sending (since media server doesn't support trickle via signaling yet)
      await new Promise((resolve) => {
        if (pc.iceGatheringState === 'complete') {
          resolve();
        } else {
          const checkState = () => {
            if (pc.iceGatheringState === 'complete') {
              pc.removeEventListener('icegatheringstatechange', checkState);
              resolve();
            }
          };
          pc.addEventListener('icegatheringstatechange', checkState);
          // Fallback timeout
          setTimeout(() => {
            pc.removeEventListener('icegatheringstatechange', checkState);
            resolve();
          }, 3000);
        }
      });

      log('UPLINK', 'Sending offer to Media Server via HTTP POST');
      const response = await fetch(`${MEDIA_SERVER_URL}/offer`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          roomId: roomIdRef.current,
          peerId: peerIdRef.current,
          sdp: pc.localDescription.sdp,
        }),
      });

      if (!response.ok) {
        throw new Error(`Media server responded with status: ${response.status}`);
      }

      const data = await response.json();
      logSDP('RECEIVED_UPLINK', 'answer', { sdp: data.sdp });

      await pc.setRemoteDescription(new RTCSessionDescription({
        type: 'answer',
        sdp: data.sdp,
      }));
      log('UPLINK', 'Remote description set (answer from media server)');

    } catch (err) {
      log('UPLINK', 'ERROR setting up uplink connection', { error: err.message });
      // We don't set global error here to not break P2P if uplink fails
    }
  }, []);

  const createPeerConnection = useCallback((stream, targetPeerId) => {
    if (pcRef.current) {
      log('RTC', 'Closing existing PeerConnection before creating new one');
      pcRef.current.ontrack = null;
      pcRef.current.onicecandidate = null;
      pcRef.current.oniceconnectionstatechange = null;
      pcRef.current.onconnectionstatechange = null;
      pcRef.current.onsignalingstatechange = null;
      pcRef.current.close();
    }

    log('RTC', 'Creating new P2P PeerConnection', { targetPeerId, iceServers: ICE_SERVERS[0].urls });
    const pc = new RTCPeerConnection({ iceServers: ICE_SERVERS });
    pcRef.current = pc;
    hasRemoteDescRef.current = false;
    pendingCandidatesRef.current = [];

    const trackIds = [];
    stream.getTracks().forEach((track) => {
      pc.addTrack(track, stream);
      trackIds.push(`${track.kind}:${track.id.substring(0, 8)}`);
    });
    log('RTC', 'Local tracks added', { tracks: trackIds.join(', ') });

    pc.ontrack = (event) => {
      log('RTC', 'Remote track received', { kind: event.track.kind, id: event.track.id.substring(0, 8) });
      const [remoteMediaStream] = event.streams;
      if (remoteMediaStream) {
        setRemoteStream(remoteMediaStream);
        log('RTC', 'Remote stream set', { trackCount: remoteMediaStream.getTracks().length });
      }
    };

    pc.onicecandidate = (event) => {
      if (event.candidate) {
        logICE('OUTBOUND', event.candidate);
        sendSignal({
          type: 'ice-candidate',
          roomId: roomIdRef.current,
          peerId: peerIdRef.current,
          targetPeerId,
          payload: { candidate: event.candidate }
        });
      } else {
        log('ICE', 'ICE gathering complete (null candidate)');
      }
    };

    pc.oniceconnectionstatechange = () => {
      const state = pc.iceConnectionState;
      log('RTC', 'ICE connection state changed', { state, connectionState: pc.connectionState });

      switch (state) {
        case 'checking':
          log('RTC', 'ICE checking — negotiating connection');
          break;
        case 'connected':
          log('RTC', 'ICE connected — media flowing');
          setConnectionState('connected');
          reconnectAttemptsRef.current = 0;
          break;
        case 'completed':
          log('RTC', 'ICE completed — optimal route found');
          setConnectionState('connected');
          reconnectAttemptsRef.current = 0;
          break;
        case 'disconnected':
          log('RTC', 'ICE disconnected — connection interrupted');
          setConnectionState('reconnecting');
          break;
        case 'failed':
          log('RTC', 'ICE FAILED — restarting ICE');
          setConnectionState('failed');
          pc.restartIce();
          break;
        case 'closed':
          log('RTC', 'ICE closed');
          setConnectionState('disconnected');
          break;
      }
    };

    pc.onconnectionstatechange = () => {
      log('RTC', 'Connection state changed', { state: pc.connectionState });
    };

    pc.onsignalingstatechange = () => {
      log('RTC', 'Signaling state changed', { state: pc.signalingState });
    };

    pc.onicegatheringstatechange = () => {
      log('ICE', 'Gathering state changed', { state: pc.iceGatheringState });
    };

    return pc;
  }, [sendSignal]);

  const createOffer = useCallback(async (targetPeerId) => {
    const pc = pcRef.current;
    if (!pc) {
      log('RTC', 'ERROR: No PeerConnection for creating offer');
      return;
    }

    try {
      makingOfferRef.current = true;
      log('SDP', 'Creating offer', { targetPeerId, signalingState: pc.signalingState });

      const offer = await pc.createOffer();
      logSDP('CREATED', 'offer', offer);

      await pc.setLocalDescription(offer);
      log('SDP', 'Local description set (offer)');

      sendSignal({
        type: 'offer',
        roomId: roomIdRef.current,
        peerId: peerIdRef.current,
        targetPeerId,
        payload: { sdp: pc.localDescription }
      });

      logSDP('SENT', 'offer', pc.localDescription);
    } catch (err) {
      log('SDP', 'ERROR creating offer', { error: err.message });
      setError('Failed to create offer');
    } finally {
      makingOfferRef.current = false;
    }
  }, [sendSignal]);

  const handleOffer = useCallback(async (fromPeerId, sdp) => {
    const pc = pcRef.current;
    if (!pc) {
      log('SDP', 'ERROR: No PeerConnection for handling offer');
      return;
    }

    try {
      logSDP('RECEIVED', 'offer', sdp);
      log('SDP', 'Handling offer', { fromPeerId, signalingState: pc.signalingState });

      const isStable = pc.signalingState === 'stable' ||
        (pc.signalingState === 'have-local-offer' && !makingOfferRef.current);

      if (!isStable) {
        log('SDP', 'SKIPPING offer — signaling not stable', { signalingState: pc.signalingState, makingOffer: makingOfferRef.current });
        return;
      }

      if (pc.signalingState === 'have-local-offer') {
        log('SDP', 'Rolling back local offer for glare resolution');
        await pc.setLocalDescription({ type: 'rollback' });
      }

      await pc.setRemoteDescription(new RTCSessionDescription(sdp));
      hasRemoteDescRef.current = true;
      log('SDP', 'Remote description set (offer)', { signalingState: pc.signalingState });

      const pendingCount = pendingCandidatesRef.current.length;
      if (pendingCount > 0) {
        log('ICE', `Flushing ${pendingCount} pending ICE candidates`);
      }
      while (pendingCandidatesRef.current.length > 0) {
        const candidate = pendingCandidatesRef.current.shift();
        await pc.addIceCandidate(new RTCIceCandidate(candidate));
      }

      const answer = await pc.createAnswer();
      logSDP('CREATED', 'answer', answer);

      await pc.setLocalDescription(answer);
      log('SDP', 'Local description set (answer)');

      sendSignal({
        type: 'answer',
        roomId: roomIdRef.current,
        peerId: peerIdRef.current,
        targetPeerId: fromPeerId,
        payload: { sdp: pc.localDescription }
      });

      logSDP('SENT', 'answer', pc.localDescription);
    } catch (err) {
      log('SDP', 'ERROR handling offer', { error: err.message, stack: err.stack });
      setError('Failed to handle offer');
    }
  }, [sendSignal]);

  const handleAnswer = useCallback(async (sdp) => {
    const pc = pcRef.current;
    if (!pc) {
      log('SDP', 'ERROR: No PeerConnection for handling answer');
      return;
    }

    try {
      logSDP('RECEIVED', 'answer', sdp);
      log('SDP', 'Handling answer', { signalingState: pc.signalingState });

      if (pc.signalingState !== 'have-local-offer') {
        log('SDP', 'SKIPPING answer — not in have-local-offer state', { signalingState: pc.signalingState });
        return;
      }

      await pc.setRemoteDescription(new RTCSessionDescription(sdp));
      hasRemoteDescRef.current = true;
      log('SDP', 'Remote description set (answer)', { signalingState: pc.signalingState });

      const pendingCount = pendingCandidatesRef.current.length;
      if (pendingCount > 0) {
        log('ICE', `Flushing ${pendingCount} pending ICE candidates`);
      }
      while (pendingCandidatesRef.current.length > 0) {
        const candidate = pendingCandidatesRef.current.shift();
        await pc.addIceCandidate(new RTCIceCandidate(candidate));
      }
    } catch (err) {
      log('SDP', 'ERROR handling answer', { error: err.message, stack: err.stack });
      setError('Failed to handle answer');
    }
  }, []);

  const handleIceCandidate = useCallback(async (candidate) => {
    const pc = pcRef.current;
    if (!pc) {
      log('ICE', 'WARNING: Received ICE candidate but no PeerConnection exists');
      return;
    }

    try {
      logICE('INBOUND', candidate);
      if (hasRemoteDescRef.current && pc.remoteDescription) {
        await pc.addIceCandidate(new RTCIceCandidate(candidate));
        log('ICE', 'Candidate added immediately');
      } else {
        pendingCandidatesRef.current.push(candidate);
        log('ICE', 'Candidate queued (no remote description yet)', { queueSize: pendingCandidatesRef.current.length });
      }
    } catch (err) {
      log('ICE', 'ERROR adding candidate', { error: err.message });
    }
  }, []);

  const connectWebSocket = useCallback((stream) => {
    if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
      log('SIGNALING', 'WebSocket already connected, skipping');
      return;
    }

    log('SIGNALING', 'Connecting WebSocket', { url: SIGNALING_URL, roomId: roomIdRef.current, peerId: peerIdRef.current });
    setConnectionState('connecting');
    const ws = new WebSocket(SIGNALING_URL);
    wsRef.current = ws;

    ws.onopen = () => {
      log('SIGNALING', 'WebSocket OPEN');
      reconnectAttemptsRef.current = 0;

      const joinMsg = {
        type: 'join',
        roomId: roomIdRef.current,
        peerId: peerIdRef.current,
        payload: {}
      };
      log('SIGNALING', 'Sending JOIN', { roomId: joinMsg.roomId, peerId: joinMsg.peerId });
      sendSignal(joinMsg);
    };

    ws.onmessage = async (event) => {
      let message;
      try {
        message = JSON.parse(event.data);
      } catch (err) {
        log('SIGNALING', 'ERROR: Invalid JSON from server', { error: err.message });
        return;
      }

      const { type, payload, peerId: fromPeerId } = message;
      log('SIGNALING', `Received: ${type}`, { from: fromPeerId || 'server' });

      switch (type) {
        case 'joined': {
          log('ROOM', 'Joined room successfully', { peers: payload.peers, isReconnect: payload.isReconnect });
          setConnectionState('waiting');

          const otherPeers = payload.peers.filter((p) => p !== peerIdRef.current);
          if (otherPeers.length > 0) {
            const targetPeer = otherPeers[0];
            remotePeerIdRef.current = targetPeer;
            log('ROOM', 'Other peer already in room, initiating offer', { targetPeer });
            createPeerConnection(stream, targetPeer);
            await createOffer(targetPeer);
          } else {
            log('ROOM', 'Waiting for peer to join (room has 1 peer)');
          }
          break;
        }

        case 'peer-joined': {
          const newPeerId = payload.peerId;
          log('ROOM', 'Peer joined room', { newPeerId });
          remotePeerIdRef.current = newPeerId;
          setConnectionState('connecting');
          createPeerConnection(stream, newPeerId);
          break;
        }

        case 'offer': {
          log('SIGNALING', 'Received OFFER relay', { fromPeerId });
          remotePeerIdRef.current = fromPeerId;

          if (!pcRef.current) {
            log('RTC', 'No existing P2P PeerConnection, creating one for incoming offer');
            createPeerConnection(stream, fromPeerId);
          }

          await handleOffer(fromPeerId, payload.sdp);
          break;
        }

        case 'answer': {
          log('SIGNALING', 'Received ANSWER relay', { fromPeerId });
          await handleAnswer(payload.sdp);
          break;
        }

        case 'ice-candidate': {
          log('SIGNALING', 'Received ICE-CANDIDATE relay', { fromPeerId });
          await handleIceCandidate(payload.candidate);
          break;
        }

        case 'peer-left': {
          log('ROOM', 'Peer LEFT', { peerId: payload.peerId });
          remotePeerIdRef.current = null;
          setRemoteStream(null);
          setConnectionState('waiting');

          if (pcRef.current) {
            log('RTC', 'Closing P2P PeerConnection after peer left');
            pcRef.current.ontrack = null;
            pcRef.current.onicecandidate = null;
            pcRef.current.oniceconnectionstatechange = null;
            pcRef.current.onconnectionstatechange = null;
            pcRef.current.onsignalingstatechange = null;
            pcRef.current.close();
            pcRef.current = null;
          }
          hasRemoteDescRef.current = false;
          pendingCandidatesRef.current = [];
          break;
        }

        case 'error': {
          log('SIGNALING', 'Server ERROR', { message: payload.message });
          setError(payload.message);
          break;
        }

        default:
          log('SIGNALING', 'Unknown message type', { type });
      }
    };

    ws.onclose = (event) => {
      log('SIGNALING', 'WebSocket CLOSED', { code: event.code, reason: event.reason, wasClean: event.wasClean });

      if (!isCleaningUpRef.current && reconnectAttemptsRef.current < MAX_RECONNECT_ATTEMPTS) {
        reconnectAttemptsRef.current += 1;
        const delay = RECONNECT_DELAY * reconnectAttemptsRef.current;
        log('SIGNALING', `Scheduling reconnect`, { attempt: reconnectAttemptsRef.current, delayMs: delay });
        setConnectionState('reconnecting');
        reconnectTimerRef.current = setTimeout(() => {
          log('SIGNALING', 'Attempting reconnect now');
          connectWebSocket(stream);
        }, delay);
      } else if (reconnectAttemptsRef.current >= MAX_RECONNECT_ATTEMPTS) {
        log('SIGNALING', 'Max reconnect attempts reached', { attempts: MAX_RECONNECT_ATTEMPTS });
        setConnectionState('failed');
        setError('Connection lost. Please refresh the page.');
      }
    };

    ws.onerror = (event) => {
      log('SIGNALING', 'WebSocket ERROR');
    };
  }, [createPeerConnection, createOffer, handleOffer, handleAnswer, handleIceCandidate, sendSignal]);

  const init = useCallback(async () => {
    const thisGen = ++initGenRef.current;
    log('LIFECYCLE', 'Init started', { generation: thisGen, peerId: peerIdRef.current, roomId: roomIdRef.current });

    if (localStreamRef.current) {
      log('MEDIA', 'Stopping previous media tracks before re-init');
      localStreamRef.current.getTracks().forEach((track) => track.stop());
      localStreamRef.current = null;
    }

    isCleaningUpRef.current = false;
    setError(null);

    try {
      setConnectionState('acquiring-media');
      log('MEDIA', 'Requesting getUserMedia', { audio: true, video: true });

      const stream = await navigator.mediaDevices.getUserMedia({
        audio: true,
        video: true
      });

      if (thisGen !== initGenRef.current) {
        log('MEDIA', 'Init generation mismatch — stopping stale stream', { thisGen, currentGen: initGenRef.current });
        stream.getTracks().forEach((track) => track.stop());
        return;
      }

      if (isCleaningUpRef.current) {
        log('MEDIA', 'Cleanup in progress — stopping acquired stream');
        stream.getTracks().forEach((track) => track.stop());
        return;
      }

      const trackInfo = stream.getTracks().map((t) => `${t.kind}:${t.id.substring(0, 8)}:${t.readyState}`);
      log('MEDIA', 'getUserMedia SUCCESS', { tracks: trackInfo.join(', ') });

      localStreamRef.current = stream;
      setLocalStream(stream);
      
      // Establish P2P signaling
      connectWebSocket(stream);
      
      // Establish uplink to Media Server
      connectUplink(stream);
    } catch (err) {
      log('MEDIA', 'getUserMedia FAILED', { name: err.name, message: err.message });
      if (err.name === 'NotAllowedError') {
        setError('Camera and microphone access denied. Please allow access and try again.');
      } else if (err.name === 'NotFoundError') {
        setError('No camera or microphone found. Please connect a device and try again.');
      } else {
        setError(`Failed to access media devices: ${err.message}`);
      }
      setConnectionState('failed');
    }
  }, [connectWebSocket, connectUplink, forceStopAllMedia]);

  const toggleAudio = useCallback(() => {
    if (localStreamRef.current) {
      const audioTrack = localStreamRef.current.getAudioTracks()[0];
      if (audioTrack) {
        audioTrack.enabled = !audioTrack.enabled;
        setIsAudioEnabled(audioTrack.enabled);
        log('MEDIA', `Audio ${audioTrack.enabled ? 'UNMUTED' : 'MUTED'}`);
      }
    }
  }, []);

  const toggleVideo = useCallback(() => {
    if (localStreamRef.current) {
      const videoTrack = localStreamRef.current.getVideoTracks()[0];
      if (videoTrack) {
        videoTrack.enabled = !videoTrack.enabled;
        setIsVideoEnabled(videoTrack.enabled);
        log('MEDIA', `Video ${videoTrack.enabled ? 'ON' : 'OFF'}`);
      }
    }
  }, []);

  useEffect(() => {
    const handleBeforeUnload = () => {
      log('LIFECYCLE', 'Page unloading (beforeunload) — force stopping all media');
      if (localStreamRef.current) {
        localStreamRef.current.getTracks().forEach((track) => track.stop());
      }
      if (pcRef.current) {
        pcRef.current.close();
      }
      if (uplinkPcRef.current) {
        uplinkPcRef.current.close();
      }
      if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
        wsRef.current.close(1000, 'Page unload');
      }
    };

    window.addEventListener('beforeunload', handleBeforeUnload);

    return () => {
      window.removeEventListener('beforeunload', handleBeforeUnload);
      cleanup();
    };
  }, [cleanup]);

  return {
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
  };
}
