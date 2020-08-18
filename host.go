package ytanalysis

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"

	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/mr-tron/base58"
	ma "github.com/multiformats/go-multiaddr"
	ytcrypto "github.com/yottachain/YTCrypto"
	host "github.com/yottachain/YTHost"
	hst "github.com/yottachain/YTHost/interface"
	"github.com/yottachain/YTHost/option"
)

//Host p2p host
type Host struct {
	lhost hst.Host
}

//NewHost create a new host
func NewHost() (*Host, error) {
	sk, _ := ytcrypto.CreateKey()
	ma, _ := ma.NewMultiaddr(fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", 0))
	privbytes, err := base58.Decode(sk)
	if err != nil {
		return nil, err
	}
	pk, err := crypto.UnmarshalSecp256k1PrivateKey(privbytes[1:33])
	if err != nil {
		return nil, err
	}
	lhost, err := host.NewHost(option.ListenAddr(ma), option.Identity(pk))
	if err != nil {
		return nil, err
	}
	go lhost.Accept()
	return &Host{lhost: lhost}, nil
}

//SendMsg send a message to client
func (host *Host) SendMsg(ctx context.Context, id string, msg []byte) ([]byte, error) {
	msid := msg[0:2]
	bytebuff := bytes.NewBuffer(msid)
	var tmp uint16
	err := binary.Read(bytebuff, binary.BigEndian, &tmp)
	msgID := int32(tmp)
	ID, err := peer.Decode(id)
	if err != nil {
		return nil, err
	}
	bytes, err := host.lhost.SendMsg(ctx, ID, msgID, msg[2:])
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

func stringListToMaddrs(addrs []string) ([]ma.Multiaddr, error) {
	maddrs := make([]ma.Multiaddr, len(addrs))
	for k, addr := range addrs {
		maddr, err := ma.NewMultiaddr(addr)
		if err != nil {
			return maddrs, err
		}
		maddrs[k] = maddr
	}
	return maddrs, nil
}
