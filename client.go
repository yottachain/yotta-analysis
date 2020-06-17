package ytanalysis

import (
	context "context"

	pb "github.com/yottachain/yotta-analysis/pb"
	"google.golang.org/grpc"
)

//AnalysisClient client of grpc call
type AnalysisClient struct {
	client pb.AnalysisClient
}

//NewClient create a new grpc client
func NewClient(addr string) (*AnalysisClient, error) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	return &AnalysisClient{client: pb.NewAnalysisClient(conn)}, nil
}

//GetSpotCheckList client call of GetSpotCheckList
func (cli *AnalysisClient) GetSpotCheckList(ctx context.Context) (*SpotCheckList, error) {
	msg, err := cli.client.GetSpotCheckList(ctx, &pb.Empty{})
	if err != nil {
		return nil, err
	}
	s := new(SpotCheckList)
	s.Fillby(msg)
	return s, nil
}

//IsNodeSelected if node is selected for spotchecking
func (cli *AnalysisClient) IsNodeSelected(ctx context.Context) (bool, error) {
	msg, err := cli.client.IsNodeSelected(ctx, &pb.Empty{})
	if err != nil {
		return false, err
	}
	return msg.Value, nil
}

//UpdateTaskStatus update status of task
func (cli *AnalysisClient) UpdateTaskStatus(ctx context.Context, id string, invalidNode int32) error {
	_, err := cli.client.UpdateTaskStatus(ctx, &pb.UpdateTaskStatusReq{Id: id, InvalidNode: invalidNode})
	if err != nil {
		return err
	}
	return nil
}
