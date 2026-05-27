package fcm

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	notify "github.com/elloloop/notify"
	"golang.org/x/oauth2"
)

// staticTokenSource returns a fixed oauth2.Token. It keeps the test off the
// real oauth2.googleapis.com endpoint even when -race / -count=1 / CI runs.
type staticTokenSource struct{ tok *oauth2.Token }

func (s staticTokenSource) Token() (*oauth2.Token, error) { return s.tok, nil }

// errTokenSource simulates an OAuth2 fetch failure.
type errTokenSource struct{ err error }

func (e errTokenSource) Token() (*oauth2.Token, error) { return nil, e.err }

// fakeServiceAccountJSON is a minimal, syntactically valid service-account
// blob that google.JWTConfigFromJSON accepts. The private key is a real
// throwaway RSA key generated only for these tests — it has never been used
// against any real Google account. JWTConfigFromJSON parses the PEM eagerly,
// so a placeholder string won't do.
const fakeServiceAccountJSON = `{
  "type": "service_account",
  "project_id": "notify-test",
  "private_key_id": "deadbeef",
  "private_key": "-----BEGIN RSA PRIVATE KEY-----\nMIIEowIBAAKCAQEAvFNNivXFlAKAQyfWAjC5gE7vMt6PvWB44yfp8h6vXEHc7Skd\nKzL9F0OOsk9k6Fye7BBh8wIaC1l0XwUrCSWAmZpOk2suMpIWfTd6oW1OWfXMfg7n\nlrZmPDqXltb5JxnIcXxBupT43FQB2pZdpY1+Khy11sj7nE13yKL9d9V0pn0jKlff\n79YkZRz0pYlA6cgYxOe24trnSiOyNyl81HOfPdaUq+CmZxcK/02RDqxOgYmI7DkN\nFI22Hcd56v0bsupy7Q5y4Stg3PvXrl+RIw8VWQDg+pmEcyZ4Q8ZcD23k6RmwSEM3\nFBzu5w2hEhmnpYpu/F/wTrn/4t9NgcN+9wPHmwIDAQABAoIBAFLvFR4eJ+wO8sQI\nyhxrZ7n8FzD1cGgKpcAvKsi4SfQR3RDvfQ7w+wG7iuBeavLs5tBLBO2/9fmnAFkO\ntJg9zMP9LFExUkpzgRtN9Hu1HmsyVB9pf06oQRPlXQT4P5RBwbgGdHTzkw/CC8sX\nLngzMQ5CFXKlBz5+x0a/qxbsBJDLh6oCmKnFE0JCMR7CshDmrIEpLPiTM83YJyTu\noaYfsr7d3X8KrxlNCgnDc41a7Sg4N4r6tH0OchupTSlGv1MR2X1F0H7vTSO5Mygy\nu3LPxXOzbVK6QGRf07kEXLkjFsKtdf3RU8lYgsXLywBkFB4ZqB+47vfk1QkSFx9k\nbcQg9ekCgYEA8tGyEbf/zU0lksbsfXUC2zXyfaPe7nL6KuojGiezAlw4Q42hKbR1\nuxVAtNgaT/9wDIxFavQwT4M4nNZRDOTQrA94Tat8aoSpoa9bCnaIBNlBQz4VRfZi\nQ3IRC7iPRwd56iAuoQfYj/jLZJUbKEf0vDgmDDX9DkPbHvJUojOptt0CgYEAxehB\n4j9OUSXxKZIgsKQqEf0YN2DR1Q+S+JZ5JI4IcjQuOmoflcOhUMa9NL+5gZjP6gBI\nTzKK+2ssCqxOdghOAmKaO+sR2hSp1Owm/IH9w+ngwH2wLtqxXEThbVy0gQ3KMcoy\n+yK0PWoVPxPwUg/oA9YhqdrCBkmh9tdGRRJxg7cCgYBXOuNTKxlGSC/8KuvR99Q5\n7LDmoZTzGM2yhpe7nQUlPo8rxsHTV6yJpcBfdDsKbVHEKaitGoMOXh5d6YDXgnK0\nLudaPjVFcKVi/oNHCRyaMYE6S6dz+/qoPxSEakFwsvK7nKxQUbb+lLI28b/PJVrA\n+ovV2KAKQwytX27cJoZyaQKBgQCqx/mWBeWNYy/L8GtH1HoPnvIWXn3vZkj+v0SI\nT4xZ0RnDLqs/r6dN8FlOkc+1ynk8YqLNYNXJxQs7nT8sBudbBjwUFc4xVk7XIPHy\nfNzZGNRyrEN4xdv6E1ED1Yc7OBL7Vr3y5LB1bxhz9b4MeTaTd99dRcMS0sknnxhh\n1nIhPwKBgD/Vw5fxLDeT0Yu5b1HBpvJ3lEzSoctd5+nQ0eaXxJ1eWy+RJrcz58Ke\nQ1bU8tCKMNnEC8MfFx0OEYzfHr3HFNvF8Po5wAOPMaH+ed/MZHmnE0X+iQK62oI3\n21XdjUTfRWbg2RhP0PUDQ4skO3MTb6/dlfsa3RDUS47cn+pTKmFA\n-----END RSA PRIVATE KEY-----\n",
  "client_email": "notify-test@notify-test.iam.gserviceaccount.com",
  "client_id": "1",
  "token_uri": "https://oauth2.googleapis.com/token"
}`

// staticToken is the Bearer token the test token source hands back. Tests
// assert it appears verbatim in the Authorization header.
const staticToken = "test-access-token"

// newTestProvider builds a Provider pointed at the httptest.Server, with a
// static OAuth2 token source so no real network call is ever attempted.
func newTestProvider(t *testing.T, srv *httptest.Server, projectID string) *Provider {
	t.Helper()
	p, err := New(notify.MobilePushConfig{
		FCMProjectID:       projectID,
		FCMCredentialsJSON: fakeServiceAccountJSON,
	},
		WithHTTPClient(srv.Client()),
		WithTokenSource(staticTokenSource{tok: &oauth2.Token{
			AccessToken: staticToken,
			TokenType:   "Bearer",
			Expiry:      time.Now().Add(1 * time.Hour),
		}}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	p.SetBaseURL(srv.URL)
	return p
}

func TestNew_RequiredFields(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		cfg  notify.MobilePushConfig
	}{
		{"missing project id", notify.MobilePushConfig{FCMCredentialsJSON: fakeServiceAccountJSON}},
		{"missing credentials", notify.MobilePushConfig{FCMProjectID: "x"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := New(tc.cfg); err == nil {
				t.Fatalf("expected error for %s", tc.name)
			}
		})
	}
}

func TestNew_RejectsMalformedCredentials(t *testing.T) {
	t.Parallel()
	_, err := New(notify.MobilePushConfig{
		FCMProjectID:       "p",
		FCMCredentialsJSON: `{not valid json`,
	})
	if err == nil {
		t.Fatal("expected error parsing malformed credentials JSON")
	}
}

func TestProvider_KindAndName(t *testing.T) {
	t.Parallel()
	p, err := New(notify.MobilePushConfig{
		FCMProjectID:       "p",
		FCMCredentialsJSON: fakeServiceAccountJSON,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := p.Kind(); got != notify.ChannelMobilePush {
		t.Errorf("Kind = %q, want %q", got, notify.ChannelMobilePush)
	}
	if got := p.Name(); got != "fcm" {
		t.Errorf("Name = %q, want %q", got, "fcm")
	}
}

func TestSend_HappyPath(t *testing.T) {
	t.Parallel()

	const projectID = "notify-test"
	const messageName = "projects/notify-test/messages/0:1234567890"

	var gotPath, gotAuth, gotContentType string
	var gotBody fcmMessage

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		gotContentType = r.Header.Get("Content-Type")
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("decode body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(fcmSendResponse{Name: messageName})
	}))
	t.Cleanup(srv.Close)

	p := newTestProvider(t, srv, projectID)

	rcpt, err := p.Send(context.Background(), notify.Message{
		To:    "device-token-abc",
		Title: "Hello",
		Body:  "World",
		Data: map[string]string{
			"deep_link": "glassa://tasks/42",
			"kind":      "task_created",
		},
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if rcpt.Status != notify.StatusDelivered {
		t.Errorf("Status = %q, want %q", rcpt.Status, notify.StatusDelivered)
	}
	if rcpt.ProviderMessageID != messageName {
		t.Errorf("ProviderMessageID = %q, want %q", rcpt.ProviderMessageID, messageName)
	}

	if got, want := gotPath, "/v1/projects/"+projectID+"/messages:send"; got != want {
		t.Errorf("path = %q, want %q", got, want)
	}
	if got, want := gotAuth, "Bearer "+staticToken; got != want {
		t.Errorf("Authorization = %q, want %q", got, want)
	}
	if !strings.HasPrefix(gotContentType, "application/json") {
		t.Errorf("Content-Type = %q, want application/json", gotContentType)
	}
	if gotBody.Message.Token != "device-token-abc" {
		t.Errorf("body.message.token = %q, want %q", gotBody.Message.Token, "device-token-abc")
	}
	if gotBody.Message.Notification == nil ||
		gotBody.Message.Notification.Title != "Hello" ||
		gotBody.Message.Notification.Body != "World" {
		t.Errorf("body.message.notification = %+v, want {Hello, World}", gotBody.Message.Notification)
	}
	if gotBody.Message.Data["deep_link"] != "glassa://tasks/42" ||
		gotBody.Message.Data["kind"] != "task_created" {
		t.Errorf("body.message.data = %+v, want verbatim msg.Data", gotBody.Message.Data)
	}
}

func TestSend_NoNotificationWhenTitleAndBodyEmpty(t *testing.T) {
	t.Parallel()

	var gotBody fcmMessage
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(fcmSendResponse{Name: "ok"})
	}))
	t.Cleanup(srv.Close)

	p := newTestProvider(t, srv, "p")

	if _, err := p.Send(context.Background(), notify.Message{
		To:   "t",
		Data: map[string]string{"k": "v"},
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if gotBody.Message.Notification != nil {
		t.Errorf("expected omitted notification when title+body empty, got %+v", gotBody.Message.Notification)
	}
	if gotBody.Message.Data["k"] != "v" {
		t.Errorf("data lost: %+v", gotBody.Message.Data)
	}
}

func TestSend_EmptyToken(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("FCM should not be called when token is empty; got %s %s", r.Method, r.URL.Path)
	}))
	t.Cleanup(srv.Close)

	p := newTestProvider(t, srv, "p")

	rcpt, err := p.Send(context.Background(), notify.Message{To: ""})
	if err == nil {
		t.Fatal("expected error for empty token")
	}
	if rcpt.Status != notify.StatusFailed {
		t.Errorf("Status = %q, want %q", rcpt.Status, notify.StatusFailed)
	}
}

func TestSend_UnregisteredToken(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{
		  "error": {
		    "code": 404,
		    "message": "Requested entity was not found.",
		    "status": "NOT_FOUND",
		    "details": [
		      {
		        "@type": "type.googleapis.com/google.firebase.fcm.v1.FcmError",
		        "errorCode": "UNREGISTERED"
		      }
		    ]
		  }
		}`))
	}))
	t.Cleanup(srv.Close)

	p := newTestProvider(t, srv, "p")

	rcpt, err := p.Send(context.Background(), notify.Message{To: "stale"})
	if err == nil {
		t.Fatal("expected error for UNREGISTERED")
	}
	if !errors.Is(err, ErrUnregisteredToken) {
		t.Errorf("err = %v, want errors.Is(err, ErrUnregisteredToken)", err)
	}
	if rcpt.Status != notify.StatusFailed {
		t.Errorf("Status = %q, want %q", rcpt.Status, notify.StatusFailed)
	}
}

// FCM also signals a gone token via plain HTTP 404 / NOT_FOUND without a
// details entry. The provider must still route that to ErrUnregisteredToken.
func TestSend_UnregisteredTokenWithoutDetails(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":{"code":404,"message":"not found","status":"NOT_FOUND"}}`))
	}))
	t.Cleanup(srv.Close)

	p := newTestProvider(t, srv, "p")

	_, err := p.Send(context.Background(), notify.Message{To: "stale"})
	if !errors.Is(err, ErrUnregisteredToken) {
		t.Fatalf("err = %v, want errors.Is(err, ErrUnregisteredToken)", err)
	}
}

func TestSend_Generic500(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":{"code":500,"message":"backend hiccup","status":"INTERNAL"}}`))
	}))
	t.Cleanup(srv.Close)

	p := newTestProvider(t, srv, "p")

	rcpt, err := p.Send(context.Background(), notify.Message{To: "t"})
	if err == nil {
		t.Fatal("expected error for 500")
	}
	if errors.Is(err, ErrUnregisteredToken) {
		t.Error("500 should not unwrap to ErrUnregisteredToken")
	}
	if rcpt.Status != notify.StatusFailed {
		t.Errorf("Status = %q, want %q", rcpt.Status, notify.StatusFailed)
	}
	if !strings.Contains(err.Error(), "backend hiccup") {
		t.Errorf("err = %q, want it to wrap FCM message", err.Error())
	}
}

func TestSend_NetworkError(t *testing.T) {
	t.Parallel()

	// Bind to a port, immediately close — any client Do() against this URL
	// will fail with a connection-refused style error. No real network.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	p, err := New(notify.MobilePushConfig{
		FCMProjectID:       "p",
		FCMCredentialsJSON: fakeServiceAccountJSON,
	},
		WithHTTPClient(&http.Client{Timeout: 200 * time.Millisecond}),
		WithTokenSource(staticTokenSource{tok: &oauth2.Token{
			AccessToken: staticToken,
			TokenType:   "Bearer",
			Expiry:      time.Now().Add(time.Hour),
		}}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	p.SetBaseURL("http://" + addr)

	rcpt, err := p.Send(context.Background(), notify.Message{To: "t"})
	if err == nil {
		t.Fatal("expected network error")
	}
	if rcpt.Status != notify.StatusFailed {
		t.Errorf("Status = %q, want %q", rcpt.Status, notify.StatusFailed)
	}
}

func TestSend_TokenSourceError(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("FCM should not be called when token mint fails; got %s %s", r.Method, r.URL.Path)
	}))
	t.Cleanup(srv.Close)

	p, err := New(notify.MobilePushConfig{
		FCMProjectID:       "p",
		FCMCredentialsJSON: fakeServiceAccountJSON,
	},
		WithHTTPClient(srv.Client()),
		WithTokenSource(errTokenSource{err: errors.New("oauth dead")}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	p.SetBaseURL(srv.URL)

	rcpt, err := p.Send(context.Background(), notify.Message{To: "t"})
	if err == nil {
		t.Fatal("expected error from token source failure")
	}
	if rcpt.Status != notify.StatusFailed {
		t.Errorf("Status = %q, want %q", rcpt.Status, notify.StatusFailed)
	}
	if !strings.Contains(err.Error(), "oauth dead") {
		t.Errorf("err = %q, want it to wrap the token-source error", err.Error())
	}
}

func TestSend_ContextCancel(t *testing.T) {
	t.Parallel()

	// The handler blocks until the test explicitly releases it OR the
	// request context fires. Both are signalled before srv.Close() runs
	// so the server can shut down cleanly even when the client cancelled
	// mid-flight.
	started := make(chan struct{})
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(func() {
		close(release)
		srv.Close()
	})

	p := newTestProvider(t, srv, "p")

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := p.Send(ctx, notify.Message{To: "t"})
		errCh <- err
	}()
	<-started
	cancel()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected error after context cancel")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Send did not return after context cancel")
	}
}
