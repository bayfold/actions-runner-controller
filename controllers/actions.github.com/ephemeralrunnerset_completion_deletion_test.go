package actionsgithubcom

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/actions/actions-runner-controller/apis/actions.github.com/v1alpha1"
	"github.com/stretchr/testify/require"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// A listener update that acknowledges completion can race the deletion guard.
// A stale guard write must conflict, retry its authoritative read, and leave
// the newer acknowledged desired count free to replenish the slot.
func TestCompletionDeletionGuardRetriesNewAcknowledgement(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha1.AddToScheme(scheme))
	set := &v1alpha1.EphemeralRunnerSet{ObjectMeta: metav1.ObjectMeta{Name: "handover", Namespace: "test-ns", UID: "handover"}, Spec: v1alpha1.EphemeralRunnerSetSpec{PatchID: 5, Replicas: 6, CompletedRunnerNames: []string{"earlier-job"}}}
	key := client.ObjectKeyFromObject(set)
	patches := 0
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(set).WithStatusSubresource(&v1alpha1.EphemeralRunnerSet{}).WithInterceptorFuncs(interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			patches++
			data, err := patch.Data(obj)
			require.NoError(t, err)
			var emitted struct {
				Metadata struct {
					ResourceVersion string `json:"resourceVersion"`
				} `json:"metadata"`
			}
			require.NoError(t, json.Unmarshal(data, &emitted))
			require.NotEmpty(t, emitted.Metadata.ResourceVersion, "guard patch must reject a stale listener snapshot")
			var fresh v1alpha1.EphemeralRunnerSet
			require.NoError(t, c.Get(ctx, key, &fresh))
			fresh.Spec.PatchID = 6
			fresh.Spec.CompletedRunnerNames = []string{"earlier-job", "seed"}
			require.NoError(t, c.Update(ctx, &fresh))
			return kerrors.NewConflict(schema.GroupResource{Group: v1alpha1.GroupVersion.Group, Resource: "ephemeralrunnersets"}, set.Name, nil)
		},
	}).Build()
	r := EphemeralRunnerSetReconciler{Client: c, APIReader: c}
	require.NoError(t, r.recordUnaccountedRunnerDeletion(t.Context(), key, set.UID, "seed"))
	require.Equal(t, 1, patches, "after the conflict the newer acknowledgement must prevent another marker write")
	var got v1alpha1.EphemeralRunnerSet
	require.NoError(t, c.Get(t.Context(), key, &got))
	require.Equal(t, 6, got.Spec.PatchID)
	require.Zero(t, got.Status.FinishedRunnerCleanupPatchID)
}
