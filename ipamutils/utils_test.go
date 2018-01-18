package ipamutils

import (
	"sync"
	"testing"

	_ "github.com/docker/libnetwork/testutils"
	"github.com/stretchr/testify/assert"
)

func init() {
	InitNetworks(nil)
}

func TestGranularPredefined(t *testing.T) {
	for _, nw := range PredefinedGranularNetworks {
		if ones, bits := nw.Mask.Size(); bits != 32 || ones != 24 {
			t.Fatalf("Unexpected size for network in granular list: %v", nw)
		}
	}

	for _, nw := range PredefinedBroadNetworks {
		if ones, bits := nw.Mask.Size(); bits != 32 || (ones != 20 && ones != 16) {
			t.Fatalf("Unexpected size for network in broad list: %v", nw)
		}
	}

}

func TestInitAddressPools(t *testing.T) {
	prePool1 := &PredefinedPools{"172.80.0.0/16", 24}
	prePool2 := &PredefinedPools{"172.90.0.0/16", 24}
	DefaultAddressPools := []*PredefinedPools{prePool1, prePool2}
	initNetworksOnce = sync.Once{}
	InitNetworks(DefaultAddressPools)

	// Check for Random IPAddresses in PredefinedBroadNetworks  ex: first , last and middle
	assert.Len(t, PredefinedBroadNetworks, 512, "Failed to find PredefinedBroadNetworks")
	assert.Equal(t, PredefinedBroadNetworks[0].String(), "172.80.0.0/24")
	assert.Equal(t, PredefinedBroadNetworks[127].String(), "172.80.127.0/24")
	assert.Equal(t, PredefinedBroadNetworks[255].String(), "172.80.255.0/24")
	assert.Equal(t, PredefinedBroadNetworks[256].String(), "172.90.0.0/24")
	assert.Equal(t, PredefinedBroadNetworks[383].String(), "172.90.127.0/24")
	assert.Equal(t, PredefinedBroadNetworks[511].String(), "172.90.255.0/24")
}
