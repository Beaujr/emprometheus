package octopus

import (
	"fmt"
	"net/http"
)

type jwtAuthTransport struct {
	Transport http.RoundTripper
	token     string
}

func (jwt *jwtAuthTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", jwt.token))
	req.Header.Set("Content-Type", "application/json")
	jwt.Transport = http.DefaultTransport
	return jwt.Transport.RoundTrip(req)
}
