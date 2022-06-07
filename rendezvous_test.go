package rendezvous

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSekSkeletonRendezvous(t *testing.T) {
	t.Run("create new cluster with nodes should success", func(t *testing.T) {

		sr, err := NewSkeletonRendezvous(FanOut(3), ClusterSize(2), MinClusterSize(2))

		assert.NoError(t, err)

		nodes := []string{"jg1", "jg2", "jg3", "jg4"}

		sr.SetNodes(nodes)

		assert.Equal(t, 2, len(sr.Clusters))
	})

	t.Run("should not contain duplicate nodes", func(t *testing.T) {
		sr, err := NewSkeletonRendezvous(FanOut(3), ClusterSize(2), MinClusterSize(2))

		assert.NoError(t, err)

		nodes := []string{"jg1", "jg1", "jg2", "jg3", "jg4"}

		sr.SetNodes(nodes)

		assert.Equal(t, 2, len(sr.Clusters))
		assert.Equal(t, [][]string{{"jg1", "jg2"}, {"jg3", "jg4"}}, sr.Clusters)
	})

	t.Run("should success when remove nodes into cluster", func(t *testing.T) {
		sr, err := NewSkeletonRendezvous(FanOut(3), ClusterSize(2), MinClusterSize(2))

		assert.NoError(t, err)

		nodes := []string{"jg1", "jg2", "jg3", "jg4"}

		sr.SetNodes(nodes)

		assert.Equal(t, 2, len(sr.Clusters))

		removedNode := []string{"jg2", "jg3"}
		sr.RemoveNodes(removedNode)

		assert.Equal(t, 1, len(sr.Clusters))
	})
}
