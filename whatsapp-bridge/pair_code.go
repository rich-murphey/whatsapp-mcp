package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// pairServer serves POST /api/pair-phone while the bridge is logged out, so a
// phone can link it by an 8-character code typed into WhatsApp (Linked
// devices → Link a device → Link with phone number instead) rather than by
// scanning the QR code, which a phone cannot do of its own screen. The REST
// API proper starts only after login, so this listens on the same port until
// then, and stops before it does.
//
// A code is only good inside a pairing session: whatsmeow's PairPhone needs
// the login websocket open, which lasts until its QR codes run out (160 s).
// A logged-out bridge holds no session (Rich, 2026-10-15: "stay fully quiet
// while unlinked"), so a request opens one with open, and waits for it.
// ready is set from the first QR event of a session until the session ends;
// running from when open is called until it returns.
type pairServer struct {
	mu      sync.Mutex
	ready   bool
	running bool
	open    func(*pairServer)
	srv     *http.Server
}

type pairPhoneRequest struct {
	Phone string `json:"phone"` // E.164 or digits: the account's own number
}

type pairPhoneResponse struct {
	Success bool   `json:"success"`
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

func startPairServer(client *whatsmeow.Client, port int, token string, logger waLog.Logger, open func(*pairServer)) *pairServer {
	p := &pairServer{open: open}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/pair-phone", withAuth(token, buildAllowedHosts(port), func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		reply := func(code int, resp pairPhoneResponse) {
			w.WriteHeader(code)
			_ = json.NewEncoder(w).Encode(resp)
		}
		if r.Method != http.MethodPost {
			reply(http.StatusMethodNotAllowed, pairPhoneResponse{Message: "POST only"})
			return
		}
		var req pairPhoneRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024)).Decode(&req); err != nil {
			reply(http.StatusBadRequest, pairPhoneResponse{Message: "body must be {\"phone\": \"+1…\"}"})
			return
		}
		phone := strings.TrimPrefix(strings.TrimSpace(req.Phone), "+")
		if len(phone) < 8 || len(phone) > 15 || strings.Trim(phone, "0123456789") != "" {
			reply(http.StatusBadRequest, pairPhoneResponse{Message: "phone must be 8 to 15 digits"})
			return
		}
		if !p.isReady() {
			p.start()
			// The first QR event marks the session open, a second or two in.
			for i := 0; i < 150 && !p.isReady(); i++ {
				time.Sleep(100 * time.Millisecond)
			}
			if !p.isReady() {
				reply(http.StatusConflict, pairPhoneResponse{Message: "no pairing session is open; try again in a few seconds"})
				return
			}
		}
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		// The push notification takes the phone straight to the code entry.
		code, err := client.PairPhone(ctx, phone, true, whatsmeow.PairClientChrome, "Chrome (Linux)")
		if err != nil {
			reply(http.StatusBadGateway, pairPhoneResponse{Message: fmt.Sprintf("pairing code refused: %v", err)})
			return
		}
		logger.Infof("Issued a pairing code for linking by phone number")
		reply(http.StatusOK, pairPhoneResponse{Success: true, Code: code})
	}))
	p.srv = &http.Server{
		Addr:         fmt.Sprintf("127.0.0.1:%d", port),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		Handler:      mux,
	}
	go func() {
		if err := p.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Errorf("Pairing endpoint error: %v", err)
		}
	}()
	logger.Infof("Logged out: POST /api/pair-phone on %s opens a pairing session and issues a code", p.srv.Addr)
	return p
}

// start opens a pairing session unless one is already open or opening.
func (p *pairServer) start() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.running {
		return
	}
	p.running = true
	go func() {
		p.open(p)
		p.mu.Lock()
		p.running = false
		p.mu.Unlock()
	}()
}

func (p *pairServer) setReady(ready bool) {
	p.mu.Lock()
	p.ready = ready
	p.mu.Unlock()
}

func (p *pairServer) isReady() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.ready
}

// stop frees the port for the REST API.
func (p *pairServer) stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = p.srv.Shutdown(ctx)
}
