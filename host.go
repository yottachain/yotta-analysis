package ytanalysis

import (
	"context"

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
func (host *Host) SendMsg(ctx context.Context, id string, msg []byte) ([]byte, error) {
	sendMsgReq := &pb.SendMsgReq{Id: id, Msgid: msg[0:2], Msg: msg[2:]}
	// ctx, cancle := context.WithTimeout(context.Background(), time.Second*time.Duration(1000))
	// defer cancle()
	sendMsgResp, err := host.lhost.SendMsg(ctx, sendMsgReq)
	if err != nil {
		return nil, err
	}
	return sendMsgResp.Value, nil
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
