package server

import (
	"net/http"

	"connectrpc.com/connect"

	notifyv1 "github.com/elloloop/notify/gen/go/notify/v1"
)

// newFakeUnaryReq wraps an arbitrary header set on a real connect.Request[T]
// of a representative shape. The interceptor only cares about req.Header()
// and req.Spec().Procedure — both are populated by connect.NewRequest.
func newFakeUnaryReq(h http.Header) connect.AnyRequest {
	req := connect.NewRequest(&notifyv1.AckNotificationRequest{Id: "test"})
	for k, vals := range h {
		for _, v := range vals {
			req.Header().Add(k, v)
		}
	}
	return req
}
