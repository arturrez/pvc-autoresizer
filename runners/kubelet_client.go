package runners

import (
	"bytes"
	"context"

	"github.com/pkg/errors"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"golang.org/x/sync/errgroup"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type kubeletClient struct {
}

func (c *kubeletClient) GetMetrics(ctx context.Context) (map[types.NamespacedName]*VolumeStats, error) {
	// create a Kubernetes client using in-cluster configuration
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	// get a list of nodes and IP addresses
	nodes, err := clientset.CoreV1().Nodes().List(context.Background(), v1.ListOptions{})
	if err != nil {
		return nil, err
	}

	// create a map to hold PVC usage data
	pvcUsage := make(map[types.NamespacedName]*VolumeStats)

	// use an errgroup to query kubelet for PVC usage on each node
	eg, ctx := errgroup.WithContext(ctx)
	for _, node := range nodes.Items {
		nodeName := node.Name
		eg.Go(func() error {
			return getPVCUsage(clientset, nodeName, pvcUsage, ctx)
		})
	}

	// wait for all queries to complete and handle any errors
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	// print the PVC usage data
	/*for pvc, usage := range pvcUsage {
		fmt.Printf("%s: %s\n", pvc, usage)
	}
	*/
	return pvcUsage, nil
}

func getPVCUsage(clientset *kubernetes.Clientset, nodeName string, pvcUsage map[types.NamespacedName]*VolumeStats, ctx context.Context) error {
	// make the request to the api /metrics endpoint and handle the response
	req := clientset.
		CoreV1().
		RESTClient().
		Get().
		Resource("nodes").
		Name(nodeName).
		SubResource("proxy").
		Suffix("metrics")
	respBody, err := req.DoRaw(ctx)
	if err != nil {
		return errors.Errorf("failed to get stats from kubelet on node %s: with error %s", nodeName, err)
	}
	parser := expfmt.TextParser{}
	metricFamilies, err := parser.TextToMetricFamilies(bytes.NewReader(respBody))
	if err != nil {
		return errors.Wrapf(err, "failed to read response body from kubelet on node %s", nodeName)
	}

	//volumeAvailableQuery
	if gauge, ok := metricFamilies[volumeAvailableQuery]; ok {
		for _, m := range gauge.Metric {
			pvcName, value := parseMetric(m)
			pvcUsage[pvcName] = new(VolumeStats)
			pvcUsage[pvcName].AvailableBytes = int64(value)
		}
	}
	//volumeCapacityQuery
	if gauge, ok := metricFamilies[volumeCapacityQuery]; ok {
		for _, m := range gauge.Metric {
			pvcName, value := parseMetric(m)
			pvcUsage[pvcName].CapacityBytes = int64(value)
		}
	}

	// inodesAvailableQuery
	if gauge, ok := metricFamilies[inodesAvailableQuery]; ok {
		for _, m := range gauge.Metric {
			pvcName, value := parseMetric(m)
			pvcUsage[pvcName].AvailableInodeSize = int64(value)
		}
	}

	// inodesCapacityQuery
	if gauge, ok := metricFamilies[inodesCapacityQuery]; ok {
		for _, m := range gauge.Metric {
			pvcName, value := parseMetric(m)
			pvcUsage[pvcName].CapacityInodeSize = int64(value)
		}
	}
	return nil
}

func parseMetric(m *dto.Metric) (pvcName types.NamespacedName, value uint64) {
	for _, label := range m.GetLabel() {
		if label.GetName() == "namespace" {
			pvcName.Namespace = label.GetValue()
		} else if label.GetName() == "persistentvolumeclaim" {
			pvcName.Name = label.GetValue()
		}
	}
	value = uint64(m.GetGauge().GetValue())
	return pvcName, value
}
