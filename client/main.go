package main

import (
	"context"
	pb "file_transfer/proto/idl/file_transfer"
	"google.golang.org/grpc"
	"io/ioutil"
	"log"
	"time"
)

func uploadFile(client pb.FileTransferClient, filePath string) (*pb.UploadResponse, error) {
	file, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	// 创建一个新的流
	stream, err := client.UploadFile(context.Background())
	if err != nil {
		return nil, err
	}

	// 将文件切分成适当大小的块
	blockSize := 1024 * 1024 // 例如：每块1MB
	for i := 0; i < len(file); i += blockSize {
		end := i + blockSize
		if end > len(file) {
			end = len(file)
		}

		chunk := &pb.Chunk{
			Data:      file[i:end],
			Offset:    int64(i),
			ChunkSize: int32(end - i),
		}

		// 发送每个块
		if err := stream.Send(chunk); err != nil {
			return nil, err
		}
	}

	// 发送完所有块后，等待服务端的响应
	response, err := stream.CloseAndRecv()
	if err != nil {
		return nil, err
	}

	return response, nil
}

func main() {
	// 建立与 gRPC 服务器的连接
	conn, err := grpc.NewClient("localhost:50051", grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(10*time.Second))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewFileTransferClient(conn)

	// 调用上传文件函数
	filePath := "/Users/hanzhongqing/Workspace/wefeed-content-spider/internal/tests/data/abc.mkv" // 修改为实际文件路径
	response, err := uploadFile(client, filePath)
	if err != nil {
		log.Fatalf("could not upload file: %v", err)
	}

	log.Printf("Upload response: %v", response)
}
