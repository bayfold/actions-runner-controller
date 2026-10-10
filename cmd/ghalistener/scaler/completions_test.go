package scaler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/actions/actions-runner-controller/apis/actions.github.com/v1alpha1"
	"github.com/actions/scaleset"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func TestCompletionAcknowledgementsPublishedWithDemand(t *testing.T) {
	var published v1alpha1.EphemeralRunnerSet
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPatch, r.Method)
		require.Equal(t, "/apis/actions.github.com/v1alpha1/namespaces/default/ephemeralrunnersets/test", r.URL.Path)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&published))
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(published))
	}))
	defer server.Close()
	clientset, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	require.NoError(t, err)
	scaler := &Scaler{clientset: clientset, config: Config{
		EphemeralRunnerSetNamespace: "default", EphemeralRunnerSetName: "test", MaxRunners: 2,
	}, patchSeq: 1, targetRunners: 2, logger: discardLogger}
	ctx := context.Background()
	for _, name := range []string{"runner-a", "runner-a", "", "runner-b"} {
		require.NoError(t, scaler.HandleJobCompleted(ctx, &scaleset.JobCompleted{RunnerName: name}))
	}
	_, err = scaler.HandleDesiredRunnerCount(ctx, 5)
	require.NoError(t, err)
	require.Equal(t, 2, published.Spec.Replicas)
	require.Equal(t, 2, published.Spec.PatchID)
	require.Equal(t, []string{"runner-a", "runner-b"}, published.Spec.CompletedRunnerNames)
	// A later publication must retain acknowledgements while termination lags.
	_, err = scaler.HandleDesiredRunnerCount(ctx, 3)
	require.NoError(t, err)
	require.Equal(t, []string{"runner-a", "runner-b"}, published.Spec.CompletedRunnerNames)
	// History is bounded by pool size, even over arbitrarily many jobs.
	require.NoError(t, scaler.HandleJobCompleted(ctx, &scaleset.JobCompleted{RunnerName: "runner-c"}))
	_, err = scaler.HandleDesiredRunnerCount(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, 1, published.Spec.Replicas)
	require.Equal(t, []string{"runner-b", "runner-c"}, published.Spec.CompletedRunnerNames)
	// A restarted listener explicitly clears the previous session's history.
	restarted := &Scaler{clientset: clientset, config: scaler.config, patchSeq: -1, targetRunners: -1, logger: discardLogger}
	_, err = restarted.HandleDesiredRunnerCount(ctx, 0)
	require.NoError(t, err)
	require.Empty(t, published.Spec.CompletedRunnerNames)
}
