import { useEffect, useState, useRef } from 'react';

const PUSH_SERVICE_URL = import.meta.env.VITE_PUSH_SERVICE_URL || 'http://localhost:8082';

export default function useSSE(roomId) {
  const [transcripts, setTranscripts] = useState([]);
  const [insights, setInsights] = useState({
    summary: '',
    topics: [],
    decisions: [],
    action_items: [],
    deadlines: [],
    risks: [],
    timestamp: ''
  });
  const [error, setError] = useState(null);
  const eventSourceRef = useRef(null);
  const reconnectTimeoutRef = useRef(null);

  // 1. Fetch initial historical room data
  useEffect(() => {
    let active = true;
    async function fetchHistoricalData() {
      try {
        const response = await fetch(`${PUSH_SERVICE_URL}/rooms/${roomId}/summary`);
        if (!response.ok) {
          throw new Error(`Failed to load historical data: ${response.statusText}`);
        }
        const data = await response.json();
        if (active) {
          if (data.transcripts) {
            setTranscripts(data.transcripts);
          }
          if (data.latestInsight) {
            setInsights(data.latestInsight);
          }
        }
      } catch (err) {
        console.error('[useSSE] Error fetching room history:', err);
        // Do not set global error to allow SSE to attempt connecting
      }
    }

    if (roomId) {
      fetchHistoricalData();
    }

    return () => {
      active = false;
    };
  }, [roomId]);

  // 2. Setup SSE connection with auto-reconnect
  useEffect(() => {
    if (!roomId) return;

    function connect() {
      if (eventSourceRef.current) {
        eventSourceRef.current.close();
      }

      console.log(`[useSSE] Connecting to stream for room: ${roomId}`);
      const url = `${PUSH_SERVICE_URL}/rooms/${roomId}/stream`;
      const es = new EventSource(url);
      eventSourceRef.current = es;

      es.onmessage = (event) => {
        try {
          const payload = JSON.parse(event.data);
          if (payload.type === 'transcript') {
            setTranscripts((prev) => {
              // Avoid duplicate chunks in state
              if (prev.some((t) => t.chunkId === payload.data.chunkId)) {
                return prev;
              }
              return [...prev, payload.data];
            });
          } else if (payload.type === 'insight') {
            setInsights(payload.data);
          }
        } catch (err) {
          console.error('[useSSE] Failed to parse SSE event data:', err);
        }
      };

      es.onerror = (err) => {
        console.error('[useSSE] EventSource failed:', err);
        setError('Realtime sync connection lost. Reconnecting...');
        es.close();

        // Retry connection in 3 seconds
        reconnectTimeoutRef.current = setTimeout(() => {
          connect();
        }, 3000);
      };

      es.onopen = () => {
        console.log('[useSSE] EventSource connection established.');
        setError(null);
      };
    }

    connect();

    return () => {
      if (eventSourceRef.current) {
        eventSourceRef.current.close();
      }
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
      }
    };
  }, [roomId]);

  return { transcripts, insights, error };
}
