package ytanalysis

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"

	ma "github.com/multiformats/go-multiaddr"
	server "github.com/yottachain/P2PHost"
	pb "github.com/yottachain/P2PHost/pb"
	ytcrypto "github.com/yottachain/YTCrypto"
)

//Host p2p host
type Host struct {
	lhost *server.Server
}

//NewHost create a new host
func NewHost() (*Host, error) {
	sk, _ := ytcrypto.CreateKey()
	host, err := server.NewServer("0", sk)
	if err != nil {
		return nil, err
	}
	return &Host{lhost: host}, nil
}

//SendMsg send a message to client
func (host *Host) SendMsg(id string, msg []byte) ([]byte, error) {
	sendMsgReq := &pb.SendMsgReq{Id: id, Msgid: msg[0:2], Msg: msg[2:]}
	ctx, cancle := context.WithTimeout(context.Background(), time.Second*time.Duration(1000))
	defer cancle()
	sendMsgResp, err := host.lhost.SendMsg(ctx, sendMsgReq)
	if err != nil {
		return nil, err
	}
	return sendMsgResp.Value, nil
}

//TestNetwork connectivity test
func (host *Host) TestNetwork(nodeID string, addrs []string) error {
	return host.testNetworkN(nodeID, addrs, 2)
}

func (host *Host) testNetworkN(nodeID string, addrs []string, retries int) error {
	err := host.testNetwork(nodeID, addrs)
	if err == nil {
		return nil
	}
	if retries == 0 {
		log.Warnf("connectivity: testNetwork: connect %s failed\n", nodeID)
		return err
	}
	return host.testNetworkN(nodeID, addrs, retries-1)
}

func (host *Host) testNetwork(nodeID string, addrs []string) error {
	ctx, cancle := context.WithTimeout(context.Background(), time.Second*time.Duration(1000))
	defer cancle()
	req := &pb.ConnectReq{Id: nodeID, Addrs: addrs}
	_, err := host.lhost.Connect(ctx, req)
	if err != nil {
		return err
	}
	disreq := &pb.StringMsg{Value: nodeID}
	_, err = host.lhost.DisConnect(context.Background(), disreq)
	if err != nil {
		return err
	}
	return nil
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
