package ytanalysis

import (
	context "context"

	pb "github.com/yottachain/yotta-analysis/pb"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// Server implemented server API for Analysis service.
type Server struct {
	Analyser *Analyser
	Timeout  int64
}

// GetSpotCheckList implemented GetSpotCheckList function of AnalysisServer
func (server *Server) GetSpotCheckList(ctx context.Context, req *pb.Empty) (*pb.SpotCheckListMsg, error) {
	spotCheckList, err := server.Analyser.GetSpotCheckList()
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return spotCheckList.Convert(), nil
}

// IsNodeSelected implemented IsNodeSelected function of AnalysisServer
func (server *Server) IsNodeSelected(ctx context.Context, req *pb.Empty) (*pb.BoolMessage, error) {
	b, err := server.Analyser.IsNodeSelected()
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return &pb.BoolMessage{Value: b}, nil
}

// UpdateTaskStatus implemented UpdateTaskStatus function of AnalysisServer
func (server *Server) UpdateTaskStatus(ctx context.Context, req *pb.UpdateTaskStatusReq) (*pb.Empty, error) {
	err := server.Analyser.UpdateTaskStatus(req.GetId(), req.GetInvalidNodeList())
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return &pb.Empty{}, nil
}
