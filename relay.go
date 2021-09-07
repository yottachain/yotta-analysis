package ytanalysis

import (
	"strings"
)

//RelayURLCheck check if relay URL exists
func RelayURLCheck(addrs []string) bool {
	return GetRelayURL(addrs) != ""
}

//GetRelayURL find out relay URL
func GetRelayURL(addrs []string) string {
	for _, addr := range addrs {
		if strings.Contains(addr, "/p2p/") {
			return addr
		}
	}
	return ""
}

//CheckPublicAddr check if public address exists
func (analyser *Analyser) CheckPublicAddr(addrs []string) string {
	ret := ""
	for _, addr := range addrs {
		if strings.HasPrefix(addr, "/ip4/127.") ||
			strings.HasPrefix(addr, "/ip4/192.168.") ||
			strings.HasPrefix(addr, "/ip4/169.254.") ||
			strings.HasPrefix(addr, "/ip4/10.") ||
			strings.HasPrefix(addr, "/ip4/172.16.") ||
			strings.HasPrefix(addr, "/ip4/172.17.") ||
			strings.HasPrefix(addr, "/ip4/172.18.") ||
			strings.HasPrefix(addr, "/ip4/172.19.") ||
			strings.HasPrefix(addr, "/ip4/172.20.") ||
			strings.HasPrefix(addr, "/ip4/172.21.") ||
			strings.HasPrefix(addr, "/ip4/172.22.") ||
			strings.HasPrefix(addr, "/ip4/172.23.") ||
			strings.HasPrefix(addr, "/ip4/172.24.") ||
			strings.HasPrefix(addr, "/ip4/172.25.") ||
			strings.HasPrefix(addr, "/ip4/172.26.") ||
			strings.HasPrefix(addr, "/ip4/172.27.") ||
			strings.HasPrefix(addr, "/ip4/172.28.") ||
			strings.HasPrefix(addr, "/ip4/172.29.") ||
			strings.HasPrefix(addr, "/ip4/172.30.") ||
			strings.HasPrefix(addr, "/ip4/172.31.") ||
			strings.HasPrefix(addr, "/ip6/") ||
			strings.HasPrefix(addr, "/p2p-circuit/") ||
			strings.Contains(addr, "/p2p/") {
			if analyser.Params.ExcludeAddrPrefix != "" && strings.HasPrefix(addr, analyser.Params.ExcludeAddrPrefix) {
				ret = addr
			}
			continue
		} else {
			ret = addr
		}
	}
	return ret
}
