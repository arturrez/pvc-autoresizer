package runners

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type KubeletSummaryResponse struct {
	Pods []*Pod `json:"pods"`
}

type Pod struct {
	PodRef *Ref      `json:"podRef"`
	Volume []*Volume `json:"volume"`
}

type Volume struct {
	UsedBytes     int64  `json:"usedBytes"`
	CapacityBytes int64  `json:"capacityBytes"`
	UsedInodes    int64  `json:"inodesUsed"`
	Inodes        int64  `json:"inodes"`
	Name          string `json:"name"`
	PvcRef        *Ref   `json:"pvcRef"`
}

type Ref struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

func main() {
	// create a Kubernetes client using in-cluster configuration
	config, err := rest.InClusterConfig()
	if err != nil {
		panic(err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err)
	}

	// get a list of nodes and IP addresses
	nodes, err := clientset.CoreV1().Nodes().List(context.Background(), v1.ListOptions{})
	if err != nil {
		panic(err)
	}

	// create a map to hold PVC usage data
	pvcUsage := make(map[string]map[string]string)

	// use an errgroup to query kubelet for PVC usage on each node
	var eg errgroup.Group
	for _, node := range nodes.Items {
		nodeName := node.Name
		nodeIP := node.Status.Addresses[0].Address
		eg.Go(func() error {
			return getPVCUsage(clientset, nodeName, nodeIP, pvcUsage)
		})
	}

	// wait for all queries to complete and handle any errors
	if err := eg.Wait(); err != nil {
		fmt.Println(err)
	}

	// print the PVC usage data
	for pvc, usage := range pvcUsage {
		fmt.Printf("%s: %s\n", pvc, usage)
	}
}

func getPVCUsage(clientset *kubernetes.Clientset, nodeName string, nodeIP string, pvcUsage map[string]map[string]string) error {
	var summary KubeletSummaryResponse
	// get the bearer token from the pod's service account token
	tokenPath := filepath.Join("/var/run/secrets/kubernetes.io/serviceaccount", "token")
	tokenBytes, err := os.ReadFile(tokenPath)
	if err != nil {
		return errors.Wrap(err, "failed to read service account token")
	}
	token := string(tokenBytes)

	//get ca cert
	caPath := filepath.Join("/var/run/secrets/kubernetes.io/serviceaccount", "ca.crt")
	caCert, err := os.ReadFile(caPath)
	if err != nil {
		return err
	}
	caCertPool := x509.NewCertPool()
	ok := caCertPool.AppendCertsFromPEM(caCert)
	if ok == false {
		return fmt.Errorf("fail to load ca: %s", caPath)
	}
	tlsConfig := &tls.Config{}
	tlsConfig.RootCAs = caCertPool

	// create a HTTP client and transport with the bearer token
	tr := &http.Transport{
		TLSClientConfig: tlsConfig,
	}
	httpClient := &http.Client{Transport: tr}
	req, err := http.NewRequest("GET", fmt.Sprintf("https://%s:%d/stats/summary", nodeIP, 10250), nil)
	if err != nil {
		return errors.Wrap(err, "failed to create HTTP request")
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))

	// make the request to the kubelet and handle the response
	resp, err := httpClient.Do(req)
	if err != nil {
		return errors.Wrapf(err, "failed to get stats from kubelet on node %s", nodeName)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.Errorf("failed to get stats from kubelet on node %s: HTTP status code %d", nodeName, resp.StatusCode)
	} else {
		bodyBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			return errors.Wrapf(err, "failed to read response body from kubelet on node %s", nodeName)
		}

		if err := json.Unmarshal(bodyBytes, &summary); err != nil {
			return errors.Wrapf(err, "failed to unmarshal summary stats from kubelet on node %s", nodeName)
		}

		for _, pods := range summary.Pods {
			for _, volume := range pods.Volume {
				if volume.PvcRef == nil {
					continue
				}
				pvcName := volume.Name
				if _, ok := pvcUsage[pvcName]; !ok {
					pvcUsage[pvcName] = make(map[string]string)
				}
				pvcUsage[pvcName]["bytes"] = fmt.Sprintf("%.2f", float64(volume.UsedBytes)/float64(volume.CapacityBytes))
				pvcUsage[pvcName]["inodes"] = fmt.Sprintf("%.2f", float64(volume.UsedInodes)/float64(volume.Inodes))
			}
		}
		return nil
	}
}
