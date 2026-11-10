package driver

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const kubeAPIRequestTimeout = 15 * time.Second

// serviceAccountDir is overridable in tests.
var serviceAccountDir = "/var/run/secrets/kubernetes.io/serviceaccount"

// recreatesPod reports whether a pod's controller recreates it once deleted. Any
// workload controller does, operators included (Strimzi's StrimziPodSet,
// CloudNativePG's Cluster). A static pod's mirror is the exception: the Node is
// its controller, and deleting the mirror restarts nothing.
func recreatesPod(ownerKind string) bool {
	return ownerKind != "Node"
}

// healPod is the part of a pod the mount healer needs.
type healPod struct {
	Namespace string
	Name      string
	UID       string
	// Controlled is set when a workload controller (StatefulSet, ReplicaSet, ...)
	// owns the pod and will recreate it once deleted.
	Controlled bool
	// Terminating is set once the pod has a deletion timestamp.
	Terminating bool
}

// podAPI is the Kubernetes access the mount healer needs.
type podAPI interface {
	ListNodePods(ctx context.Context, nodeName string) ([]healPod, error)
	DeletePod(ctx context.Context, pod healPod) error
}

// inClusterPodAPI talks to the API server with the pod's service account. The
// driver needs two calls, which does not justify depending on client-go.
type inClusterPodAPI struct {
	baseURL string
	client  *http.Client
}

func newInClusterPodAPI() (*inClusterPodAPI, error) {
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, fmt.Errorf("not running in a cluster: KUBERNETES_SERVICE_HOST/PORT are not set")
	}
	caPEM, err := os.ReadFile(serviceAccountDir + "/ca.crt")
	if err != nil {
		return nil, fmt.Errorf("failed to read the service account CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("the service account CA contains no certificates")
	}
	return &inClusterPodAPI{
		baseURL: "https://" + net.JoinHostPort(host, port),
		client: &http.Client{
			Timeout:   kubeAPIRequestTimeout,
			Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}},
		},
	}, nil
}

func (a *inClusterPodAPI) do(ctx context.Context, method, path string, body []byte) (*http.Response, error) {
	// Projected service account tokens are rotated, so the file is read per call.
	token, err := os.ReadFile(serviceAccountDir + "/token")
	if err != nil {
		return nil, fmt.Errorf("failed to read the service account token: %w", err)
	}
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, a.baseURL+path, reader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return a.client.Do(req)
}

type podListResponse struct {
	Items []struct {
		Metadata struct {
			Name              string  `json:"name"`
			Namespace         string  `json:"namespace"`
			UID               string  `json:"uid"`
			DeletionTimestamp *string `json:"deletionTimestamp"`
			OwnerReferences   []struct {
				Kind       string `json:"kind"`
				Controller *bool  `json:"controller"`
			} `json:"ownerReferences"`
		} `json:"metadata"`
	} `json:"items"`
}

func (a *inClusterPodAPI) ListNodePods(ctx context.Context, nodeName string) ([]healPod, error) {
	path := "/api/v1/pods?fieldSelector=" + url.QueryEscape("spec.nodeName="+nodeName)
	resp, err := a.do(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("listing pods on node %s: HTTP %d: %s", nodeName, resp.StatusCode, strings.TrimSpace(string(msg)))
	}

	var list podListResponse
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		return nil, fmt.Errorf("decoding the pod list: %w", err)
	}
	pods := make([]healPod, 0, len(list.Items))
	for _, item := range list.Items {
		pod := healPod{
			Namespace:   item.Metadata.Namespace,
			Name:        item.Metadata.Name,
			UID:         item.Metadata.UID,
			Terminating: item.Metadata.DeletionTimestamp != nil,
		}
		for _, ref := range item.Metadata.OwnerReferences {
			if ref.Controller != nil && *ref.Controller && recreatesPod(ref.Kind) {
				pod.Controlled = true
			}
		}
		pods = append(pods, pod)
	}
	return pods, nil
}

// DeletePod deletes the pod with its normal grace period. The UID precondition
// keeps a pod its controller has already recreated under the same name from
// being deleted in its place.
func (a *inClusterPodAPI) DeletePod(ctx context.Context, pod healPod) error {
	body, err := json.Marshal(map[string]any{
		"apiVersion":    "v1",
		"kind":          "DeleteOptions",
		"preconditions": map[string]string{"uid": pod.UID},
	})
	if err != nil {
		return err
	}
	path := fmt.Sprintf("/api/v1/namespaces/%s/pods/%s", url.PathEscape(pod.Namespace), url.PathEscape(pod.Name))
	resp, err := a.do(ctx, http.MethodDelete, path, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK, http.StatusAccepted, http.StatusNotFound, http.StatusConflict:
		// Not found or a failed UID precondition both mean this pod is gone already.
		return nil
	default:
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("deleting pod %s/%s: HTTP %d: %s", pod.Namespace, pod.Name, resp.StatusCode, strings.TrimSpace(string(msg)))
	}
}
