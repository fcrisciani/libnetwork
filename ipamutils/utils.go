// Package ipamutils provides utility functions for ipam management
package ipamutils

import (
	"fmt"
	"net"
	"sync"

	"github.com/sirupsen/logrus"
)

var (
	// PredefinedBroadNetworks contains a list of 31 IPv4 private networks with host size 16 and 12
	// (172.17-31.x.x/16, 192.168.x.x/20) which do not overlap with the networks in `PredefinedGranularNetworks`
	PredefinedBroadNetworks []*net.IPNet
	// PredefinedGranularNetworks contains a list of 64K IPv4 private networks with host size 8
	// (10.x.x.x/24) which do not overlap with the networks in `PredefinedBroadNetworks`
	PredefinedGranularNetworks []*net.IPNet
	initNetworksOnce           sync.Once
)

// PredefinedPools represent a set of address pools with prefix length Size.
// Each pool in the set is derived from the Base pool. Base is to be passed
// in CIDR format.Currently we support only local scope networks
// Example: a Base "10.10.0.0/16 with Size 24 will define the set of 256
// 10.10.[0-255].0/24 address pools
type PredefinedPools struct {
	Base string `json:"base"`
	Size int    `json:"size"`
}

// InitNetworks initializes the local predefined address pools
// with the default values.
func InitNetworks(defaultAddressPool []*PredefinedPools) {
	initNetworksOnce.Do(func() {
		isPredefinedPool := false
		PredefinedGranularNetworks = initGranularPredefinedNetworks()
		if defaultAddressPool == nil {
			defaultAddressPool = []*PredefinedPools{{"172.17.0.0/12", 16}, {"192.168.0.0/16", 20}}
			isPredefinedPool = true
		}
		if err := InitAddressPools(defaultAddressPool); err != nil {
			logrus.WithError(err).Error("InitAddressPools failed to initialize the default address pool")
		}
		if isPredefinedPool {
			PredefinedBroadNetworks = append(PredefinedBroadNetworks[:0], PredefinedBroadNetworks[1:]...)
		}
	})
}
func initBroadPredefinedNetworks() []*net.IPNet {
	pl := make([]*net.IPNet, 0, 31)
	mask := []byte{255, 255, 0, 0}
	for i := 17; i < 32; i++ {
		pl = append(pl, &net.IPNet{IP: []byte{172, byte(i), 0, 0}, Mask: mask})
	}
	mask20 := []byte{255, 255, 240, 0}
	for i := 0; i < 16; i++ {
		pl = append(pl, &net.IPNet{IP: []byte{192, 168, byte(i << 4), 0}, Mask: mask20})
	}
	return pl
}

func initGranularPredefinedNetworks() []*net.IPNet {
	pl := make([]*net.IPNet, 0, 256*256)
	mask := []byte{255, 255, 255, 0}
	for i := 0; i < 256; i++ {
		for j := 0; j < 256; j++ {
			pl = append(pl, &net.IPNet{IP: []byte{10, byte(i), byte(j), 0}, Mask: mask})
		}
	}
	return pl
}

// InitAddressPools allows to initialize the local scope predefined
// address pools to the desired values. It fails is invalid input is passed
// or if the predefined pools were already initialized.
func InitAddressPools(list []*PredefinedPools) error {
	localPools := make([]*net.IPNet, 0, len(list))

	for _, p := range list {
		if p == nil {
			continue
		}
		_, b, err := net.ParseCIDR(p.Base)
		if err != nil {
			return fmt.Errorf("invalid base pool %q: %v", p.Base, err)
		}
		ones, _ := b.Mask.Size()
		if p.Size <= 0 || p.Size < ones {
			return fmt.Errorf("invalid pools size: %d", p.Size)
		}
		localPools = append(localPools, initPools(p.Size, b)...)
	}
	PredefinedBroadNetworks = localPools
	return nil
}

func initPools(size int, base *net.IPNet) []*net.IPNet {
	one, bits := base.Mask.Size()
	mask := net.CIDRMask(size, bits)
	n := 1 << uint(size-one)
	s := uint(bits - size)
	list := make([]*net.IPNet, 0, n)

	for i := 0; i < n; i++ {
		ip := copyIP(base.IP)
		addIntToIP(ip, uint(i<<s))
		list = append(list, &net.IPNet{IP: ip, Mask: mask})
	}
	return list
}

func copyIP(from net.IP) net.IP {
	ip := make([]byte, len(from))
	copy(ip, from)
	return ip
}

func addIntToIP(array net.IP, ordinal uint) {
	for i := len(array) - 1; i >= 0; i-- {
		array[i] |= (byte)(ordinal & 0xff)
		ordinal >>= 8
	}
}
