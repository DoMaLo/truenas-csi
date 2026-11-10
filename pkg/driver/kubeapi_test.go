package driver

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func newTestPodAPI(t *testing.T, handler http.HandlerFunc) *inClusterPodAPI {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "token"), []byte("secret-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	orig := serviceAccountDir
	serviceAccountDir = dir
	t.Cleanup(func() { serviceAccountDir = orig })

	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return &inClusterPodAPI{baseURL: srv.URL, client: srv.Client()}
}

const podListJSON = `{"items":[
 {"metadata":{"name":"loki-0","namespace":"monitoring","uid":"u1",
   "ownerReferences":[{"kind":"StatefulSet","controller":true}]}},
 {"metadata":{"name":"bare","namespace":"default","uid":"u2"}},
 {"metadata":{"name":"leaving","namespace":"default","uid":"u3","deletionTimestamp":"2026-01-01T00:00:00Z",
   "ownerReferences":[{"kind":"ReplicaSet","controller":true}]}},
 {"metadata":{"name":"kube-apiserver-w1","namespace":"kube-system","uid":"u4",
   "ownerReferences":[{"kind":"Node","controller":true}]}},
 {"metadata":{"name":"helper","namespace":"default","uid":"u5",
   "ownerReferences":[{"kind":"ReplicaSet","controller":false}]}}
]}`

func TestListNodePods(t *testing.T) {
	var gotQuery, gotAuth string
	api := newTestPodAPI(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.Query().Get("fieldSelector")
		gotAuth = r.Header.Get("Authorization")
		_, _ = io.WriteString(w, podListJSON)
	})

	pods, err := api.ListNodePods(context.Background(), "w-01")
	if err != nil {
		t.Fatal(err)
	}
	if gotQuery != "spec.nodeName=w-01" {
		t.Errorf("fieldSelector = %q", gotQuery)
	}
	if gotAuth != "Bearer secret-token" {
		t.Errorf("Authorization = %q, want the trimmed token", gotAuth)
	}

	want := map[string]healPod{
		"loki-0":            {Namespace: "monitoring", Name: "loki-0", UID: "u1", Controlled: true},
		"bare":              {Namespace: "default", Name: "bare", UID: "u2"},
		"leaving":           {Namespace: "default", Name: "leaving", UID: "u3", Controlled: true, Terminating: true},
		"kube-apiserver-w1": {Namespace: "kube-system", Name: "kube-apiserver-w1", UID: "u4"}, // mirror pod
		"helper":            {Namespace: "default", Name: "helper", UID: "u5"},
	}
	if len(pods) != len(want) {
		t.Fatalf("got %d pods, want %d", len(pods), len(want))
	}
	for _, p := range pods {
		if p != want[p.Name] {
			t.Errorf("pod %s = %+v, want %+v", p.Name, p, want[p.Name])
		}
	}
}

func TestListNodePods_HTTPError(t *testing.T) {
	api := newTestPodAPI(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "forbidden", http.StatusForbidden)
	})
	if _, err := api.ListNodePods(context.Background(), "w-01"); err == nil || !strings.Contains(err.Error(), "403") {
		t.Errorf("ListNodePods() = %v, want an error naming the 403", err)
	}
}

func TestDeletePod(t *testing.T) {
	tests := []struct {
		status  int
		wantErr bool
	}{
		{http.StatusOK, false},
		{http.StatusAccepted, false},
		{http.StatusNotFound, false},
		{http.StatusConflict, false},
		{http.StatusForbidden, true},
		{http.StatusInternalServerError, true},
	}
	for _, tt := range tests {
		t.Run(http.StatusText(tt.status), func(t *testing.T) {
			var gotMethod, gotPath string
			var gotBody map[string]any
			api := newTestPodAPI(t, func(w http.ResponseWriter, r *http.Request) {
				gotMethod, gotPath = r.Method, r.URL.Path
				_ = json.NewDecoder(r.Body).Decode(&gotBody)
				w.WriteHeader(tt.status)
			})

			err := api.DeletePod(context.Background(), healPod{Namespace: "monitoring", Name: "loki-0", UID: "u1"})
			if (err != nil) != tt.wantErr {
				t.Errorf("DeletePod() = %v, want error: %v", err, tt.wantErr)
			}
			if gotMethod != http.MethodDelete || gotPath != "/api/v1/namespaces/monitoring/pods/loki-0" {
				t.Errorf("request = %s %s", gotMethod, gotPath)
			}
			pre, _ := gotBody["preconditions"].(map[string]any)
			if pre["uid"] != "u1" {
				t.Errorf("body = %v, want a UID precondition", gotBody)
			}
		})
	}
}

func TestNewInClusterPodAPI_OutsideCluster(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")
	if _, err := newInClusterPodAPI(); err == nil {
		t.Error("newInClusterPodAPI() outside a cluster should fail")
	}
}
