package rendezvous

import (
	"hash"
	"hash/fnv"
	"math"
	"strconv"
)

type Option func(*Options) error

// Options can be used to create a customized configuration
// rendezvous skeleton based.
type Options struct {
	// FanOut represents skeleton fan out which will
	// be connected to branch skeleton
	fanOut int

	// Hash is algorithm that will be used for hashing key
	hash hash.Hash64

	// ClusterSize is number of nodes to be filled in a cluster
	clusterSize int

	// MinClusterSize the minimum number of nodes that must exist in a cluster
	minClusterSize int
}

// GetDefaultOptions returns default configuration options
// for the initilization new rendezvous.
func GetDefaultOptions() Options {
	return Options{
		fanOut:         3,
		hash:           fnv.New64(),
		clusterSize:    2,
		minClusterSize: 2,
	}
}

// FanOut sets the number of fan out for spread data into virtual node.
func FanOut(fanOut int) Option {
	return func(o *Options) error {
		o.fanOut = fanOut

		return nil
	}
}

// HashAlgorithm sets the algorithm type that will be used to hash the score.
func HashAlgorithm(hash hash.Hash64) Option {
	return func(o *Options) error {
		o.hash = hash

		return nil
	}
}

// ClusterSize sets the amount of cluster each fan out.
func ClusterSize(size int) Option {
	return func(o *Options) error {
		o.clusterSize = size

		return nil
	}
}

// MinClusterSize sets the minimun data in the cluster.
func MinClusterSize(size int) Option {
	return func(o *Options) error {
		o.minClusterSize = size

		return nil
	}
}

// a SkeletonRendezvous represents list of cluster
// that already process using rendezvous
type SkeletonRendezvous struct {
	options Options

	Clusters     [][]string
	Nodes        []string
	VirtualNodes int
}

func NewSkeletonRendezvous(options ...Option) (*SkeletonRendezvous, error) {
	opts := GetDefaultOptions()

	for _, opt := range options {
		if opt != nil {
			if err := opt(&opts); err != nil {
				return nil, err
			}
		}
	}

	skeletonRendezvous := &SkeletonRendezvous{
		options:      opts,
		Clusters:     make([][]string, 0),
		Nodes:        make([]string, 0),
		VirtualNodes: 0,
	}

	return skeletonRendezvous, nil
}

// SetNodes set new nodes into cluster
func (sr *SkeletonRendezvous) SetNodes(nodes []string) {
	sr.generateCluster(nodes)
}

// RemoveNodes remove nodes from the cluster and generate new cluster
func (sr *SkeletonRendezvous) RemoveNodes(removedNodes []string) {
	deletedNodes := make(map[string]bool)

	for _, removedNode := range removedNodes {
		deletedNodes[removedNode] = true
	}

	newNodes := make([]string, 0)

	for _, node := range sr.Nodes {
		if !deletedNodes[node] {
			newNodes = append(newNodes, node)
		}
	}

	sr.Clusters = make([][]string, 0)
	sr.generateCluster(newNodes)
}

// FindNode given specific key, find selected nodes with highest hash score
func (sr *SkeletonRendezvous) FindNode(key string) string {
	var branch string

	for i := 0; i < sr.VirtualNodes; i++ {
		var highestNode uint64
		var targetBranch string

		for j := 0; j < sr.options.fanOut; j++ {
			branchStr := strconv.Itoa(i) + strconv.Itoa(j)

			hashScore := sr.hash(branchStr, key)

			if hashScore > highestNode {
				highestNode = hashScore
				targetBranch = strconv.Itoa(j)
			}
		}

		branch = branch + targetBranch
	}

	nodes, err := sr.selectClusterNodes(branch)

	if err != nil {
		return ""
	}

	selectedNode := sr.findHighestRandomWeight(key, nodes)

	return selectedNode
}

func (sr *SkeletonRendezvous) generateCluster(nodes []string) {
	lookup := make(map[string]bool)

	newNodes := make([]string, 0)

	for _, node := range nodes {
		if !lookup[node] {
			newNodes = append(newNodes, node)
			lookup[node] = true
		}
	}

	sr.Nodes = append(sr.Nodes, newNodes...)

	clusterCount := float64(len(nodes)) / float64(sr.options.clusterSize)
	clusterAmount := int(math.Ceil(clusterCount))

	for i := 0; i < clusterAmount; i++ {
		sr.Clusters = append(sr.Clusters, make([]string, 0))
	}

	clusterIndex := 0

	for _, node := range newNodes {
		sr.Clusters[clusterIndex] = append(sr.Clusters[clusterIndex], node)

		if len(sr.Clusters[clusterIndex]) >= sr.options.clusterSize {
			clusterIndex++
		}
	}

	if clusterAmount > 1 {
		lastCluster := sr.Clusters[len(sr.Clusters)-1]

		if len(lastCluster) < sr.options.minClusterSize {
			sr.Clusters = sr.Clusters[:len(sr.Clusters)-1]
			clusterAmount--

			spreadClusterIndex := 0

			for _, node := range lastCluster {
				sr.Clusters[spreadClusterIndex] = append(sr.Clusters[spreadClusterIndex], node)

				spreadClusterIndex = (spreadClusterIndex + 1) % clusterAmount
			}
		}
	}

	sr.VirtualNodes = sr.countVirtualNodes(clusterAmount, sr.options.fanOut)
}

func (sr *SkeletonRendezvous) countVirtualNodes(clusterAmount int, fanOut int) int {
	return int(math.Ceil(math.Log(float64(clusterAmount)) / math.Log(float64(fanOut))))
}

func (sr *SkeletonRendezvous) selectClusterNodes(branch string) ([]string, error) {
	if len(branch) == 1 {
		branchCluster, err := strconv.Atoi(branch)

		if err != nil {
			return []string{}, err
		}

		if branchCluster > len(sr.Clusters)-1 {
			return sr.Clusters[branchCluster-1], nil
		}

		return sr.Clusters[branchCluster], nil
	}

	currentBrannchIndex := 0
	branchSize := len(branch) - 1

	for _, v := range branch {
		currentVal, _ := strconv.Atoi(string(v))
		currentBrannchIndex = currentBrannchIndex + (int(math.Pow(float64(sr.options.fanOut), float64(branchSize))) * currentVal)
		branchSize--
	}

	if currentBrannchIndex > len(sr.Clusters)-1 {
		return sr.Clusters[currentBrannchIndex-len(sr.Clusters)-1], nil
	}

	return sr.Clusters[currentBrannchIndex], nil
}

func (sr *SkeletonRendezvous) findHighestRandomWeight(key string, nodes []string) string {
	var highestNode uint64
	var selectedNode string

	for _, node := range nodes {
		nodeScore := sr.hash(node, key)

		if nodeScore > highestNode {
			highestNode = nodeScore
			selectedNode = node
		}
	}

	return selectedNode
}

func (sr *SkeletonRendezvous) hash(target string, key string) uint64 {
	sr.options.hash.Reset()
	sr.options.hash.Write([]byte(target))
	sr.options.hash.Write([]byte(key))
	return sr.options.hash.Sum64()
}
