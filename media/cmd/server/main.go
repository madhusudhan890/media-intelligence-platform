/*
Package main is the entry point for the Media Intelligence Platform's backend service.
It initializes the Kafka producer, WebRTC API, state managers, and the HTTP/WebSocket router.
*/
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

	"media-server/internal/api"
	"media-server/internal/kafka"
	"media-server/internal/signaling"
	mywebrtc "media-server/internal/webrtc"
)

func main() {
	log.Println("Starting Media Server...")

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		kafkaBrokers = "localhost:9094"
	}

	// 1. Initialize Kafka
	kafkaProd := kafka.NewProducer(kafkaBrokers, "audio-chunks")
	defer kafkaProd.Close()

	// 2. Initialize WebRTC API
	m := &webrtc.MediaEngine{}
	if err := m.RegisterDefaultCodecs(); err != nil {
		log.Fatalf("Failed to register default codecs: %v", err)
	}

	i := &interceptor.Registry{}
	if err := webrtc.RegisterDefaultInterceptors(m, i); err != nil {
		log.Fatalf("Failed to register default interceptors: %v", err)
	}
	webrtcAPI := webrtc.NewAPI(webrtc.WithMediaEngine(m), webrtc.WithInterceptorRegistry(i))

	// 3. Initialize State Managers
	peerManager := mywebrtc.NewPeerManager()
	sigManager := signaling.NewManager()

	// 4. Setup Router
	r := mux.NewRouter()

	r.HandleFunc("/health", api.HealthHandler(sigManager)).Methods("GET")
	r.HandleFunc("/rooms", api.CreateRoomHandler(sigManager)).Methods("POST", "OPTIONS")
	r.HandleFunc("/rooms/{roomId}", api.GetRoomHandler(sigManager)).Methods("GET", "OPTIONS")
	r.HandleFunc("/offer", api.OfferHandler(webrtcAPI, peerManager, kafkaProd)).Methods("POST", "OPTIONS")
	
	r.HandleFunc("/ws", signaling.WSHandler(sigManager))

	r.Use(api.CORSMiddleware)

	// 5. Start HTTP Server
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

	// 6. Graceful Shutdown
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
