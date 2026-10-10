package actionsgithubcom

import (
	"fmt"
	"testing"

	"github.com/actions/actions-runner-controller/apis/actions.github.com/v1alpha1"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// Reproduces the observed handover: the listener already accounted for seed's
// completion, but its status still says Running when five new runners are made.
// Their patch annotations must not prevent filling the sixth slot once seed is
// gone. No new listener patch or restart is supplied during these reconciles.
func TestSamePatchCompletionReplenishment(t *testing.T) {
	for _, tc := range []struct {
		name          string
		acknowledged  bool
		historicalAck bool
		holdDeletion  bool
		selfDelete    bool
		want          int
	}{
		{name: "acknowledged completion fills the slot", acknowledged: true, want: 6},
		{name: "unacknowledged completion does not replay stale demand", historicalAck: true, want: 5},
		{name: "deleting registration still counts toward capacity", acknowledged: true, holdDeletion: true, want: 6},
		{name: "acknowledged runner controller deletion replenishes", acknowledged: true, selfDelete: true, want: 6},
		{name: "unacknowledged runner controller deletion suppresses", historicalAck: true, selfDelete: true, want: 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			require.NoError(t, corev1.AddToScheme(scheme))
			require.NoError(t, v1alpha1.AddToScheme(scheme))
			set := &v1alpha1.EphemeralRunnerSet{ObjectMeta: metav1.ObjectMeta{Name: "handover", Namespace: "test-ns", UID: "handover", Finalizers: []string{EphemeralRunnerSetFinalizerName}}, Spec: v1alpha1.EphemeralRunnerSetSpec{Replicas: 6, PatchID: 5, EphemeralRunnerSpec: v1alpha1.EphemeralRunnerSpec{GitHubConfigURL: "https://github.com/org/repo", PodTemplateSpec: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "runner", Image: "runner:latest"}}}}}}}
			if tc.acknowledged {
				set.Spec.CompletedRunnerNames = []string{"seed"}
			}
			if tc.historicalAck {
				set.Spec.CompletedRunnerNames = []string{"earlier-job"}
			}
			objects := []client.Object{set}
			controller := true
			runner := func(name, patch string, phase v1alpha1.EphemeralRunnerPhase) *v1alpha1.EphemeralRunner {
				return &v1alpha1.EphemeralRunner{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: set.Namespace, Annotations: map[string]string{AnnotationKeyPatchID: patch}, OwnerReferences: []metav1.OwnerReference{{APIVersion: v1alpha1.GroupVersion.String(), Kind: "EphemeralRunnerSet", Name: set.Name, UID: set.UID, Controller: &controller}}}, Status: v1alpha1.EphemeralRunnerStatus{Phase: phase}}
			}
			seed := runner("seed", "4", v1alpha1.EphemeralRunnerPhaseRunning)
			if tc.holdDeletion {
				seed.Finalizers = []string{"test/registration"}
			}
			objects = append(objects, seed)
			for i := 0; i < 5; i++ {
				objects = append(objects, runner(fmt.Sprintf("new-%d", i), "5", v1alpha1.EphemeralRunnerPhasePending))
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).WithStatusSubresource(&v1alpha1.EphemeralRunnerSet{}, &v1alpha1.EphemeralRunner{}).WithIndex(&v1alpha1.EphemeralRunner{}, resourceOwnerKey, newGroupVersionOwnerKindIndexer("EphemeralRunnerSet")).Build()
			r := &EphemeralRunnerSetReconciler{Client: c, APIReader: c, Log: logr.Discard(), Scheme: scheme, ResourceBuilder: ResourceBuilder{Scheme: scheme, ResourceCache: newTestResourceCache()}}
			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(set)}
			reconcile := func() { _, err := r.Reconcile(t.Context(), req); require.NoError(t, err) }
			count := func() int {
				var list v1alpha1.EphemeralRunnerList
				require.NoError(t, c.List(t.Context(), &list))
				return len(list.Items)
			}
			reconcile()
			require.Equal(t, 6, count(), "a still-running runner must continue to count toward capacity")
			require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(seed), seed))
			if tc.selfDelete {
				require.NoError(t, r.recordUnaccountedRunnerDeletion(t.Context(), req.NamespacedName, set.UID, seed.Name))
				require.NoError(t, c.Delete(t.Context(), seed))
			} else {
				seed.Status.Phase = v1alpha1.EphemeralRunnerPhaseSucceeded
				require.NoError(t, c.Status().Update(t.Context(), seed))
				reconcile()
				if tc.holdDeletion {
					require.Equal(t, 6, count(), "deletion must keep the registration until its finalizer is released")
					reconcile()
					require.Equal(t, 6, count(), "no replacement while the registration is deleting")
					require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(seed), seed))
					seed.Finalizers = nil
					require.NoError(t, c.Update(t.Context(), seed))
				} else {
					require.Equal(t, 5, count(), "cleanup returns before replacement")
				}
			}
			reconcile()
			require.Equal(t, tc.want, count(), "same listener snapshot must replenish only an accounted completion")
			reconcile()
			require.Equal(t, tc.want, count(), "repeating the reconcile must not overprovision")
			var got v1alpha1.EphemeralRunnerSet
			require.NoError(t, c.Get(t.Context(), req.NamespacedName, &got))
			require.Equal(t, 5, got.Spec.PatchID)
			if tc.acknowledged {
				require.Zero(t, got.Status.FinishedRunnerCleanupPatchID)
			} else {
				require.Equal(t, 5, got.Status.FinishedRunnerCleanupPatchID)
			}
		})
	}
}
