package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"github.com/pion/interceptor"
	"github.com/pion/webrtc/v4"
)

var (
	webrtcAPI    *webrtc.API
	kafkaProd    *KafkaProducer
	peerManager  = NewPeerManager()
)

func init() {
	// Initialize WebRTC API with default settings
	m := &webrtc.MediaEngine{}
	if err := m.RegisterDefaultCodecs(); err != nil {
		log.Fatalf("Failed to register default codecs: %v", err)
	}

	i := &interceptor.Registry{}
	if err := webrtc.RegisterDefaultInterceptors(m, i); err != nil {
		log.Fatalf("Failed to register default interceptors: %v", err)
	}

	webrtcAPI = webrtc.NewAPI(webrtc.WithMediaEngine(m), webrtc.WithInterceptorRegistry(i))
}

func main() {
	log.Println("Starting Media Server...")

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080" // Switched to 8080 to match previous signaling port
	}

	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		kafkaBrokers = "localhost:9094"
	}

	// Initialize Kafka Producer
	kafkaProd = NewKafkaProducer(kafkaBrokers, "audio-chunks")
	defer kafkaProd.Close()

	r := mux.NewRouter()

	r.HandleFunc("/health", healthHandler).Methods("GET")
	r.HandleFunc("/rooms", createRoomHandler).Methods("POST", "OPTIONS")
	r.HandleFunc("/rooms/{roomId}", getRoomHandler).Methods("GET", "OPTIONS")
	r.HandleFunc("/offer", offerHandler).Methods("POST", "OPTIONS")
	
	r.HandleFunc("/ws", wsHandler)

	// Add CORS middleware
	r.Use(corsMiddleware)

	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	go func() {
		log.Printf("Listening on :%s", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Listen error: %s\n", err)
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	peerManager.CloseAll()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("Server exiting")
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}
